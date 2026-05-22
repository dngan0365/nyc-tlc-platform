# spark_jobs/job3_aggregate.py

"""
Job 3 — Gold → KPI Aggregations
Optimized for EMR Serverless + Delta Lake.

FIX (2026-05-20)
────────────────
Trước đây register_glue_table() dùng SequenceFileInputFormat + LazySimpleSerDe
+ Columns=[] — Athena KHÔNG đọc được Delta qua cách này.

Áp dụng cùng pattern với job1 (silver) và job2 (gold fact_trips):
  1. SymlinkTextInputFormat + GOLD_KPI_COLUMNS explicit.
  2. Per-partition manifest entries (_add_gold_partitions).
  3. GENERATE symlink_format_manifest sau mỗi Delta write.
  4. Default region thống nhất về ap-southeast-1.

Errors được fix:
    TABLE_NOT_FOUND: nyc_tlc_gold.hourly_kpis
    Column '<name>' cannot be resolved
    No path property defined for table: nyc_tlc_gold.hourly_kpis

FIX (2026-05-22) — defensive schema option alignment
─────────────────────────────────────────────────────
job3 reads from gold fact_trips (not silver directly) and aggregates all
surcharge columns away in the groupBy, so cbd_congestion_fee / airport_fee
never appear in the hourly_kpis output schema. No column list changes needed.

The only change is replacing .option("overwriteSchema", "false") with
.option("mergeSchema", "true") on the Delta write, consistent with job1 and
job2. This keeps all three jobs aligned and ensures job3 will tolerate any
future schema additions to hourly_kpis without a manual intervention.
"""

from __future__ import annotations

import argparse
import logging

import boto3
from botocore.exceptions import ClientError
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

# ─────────────────────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Gold schema — hourly_kpis
# ─────────────────────────────────────────────────────────────────────────────
#
# Explicit column list cho Athena SymlinkTextInputFormat.
# Phải khớp CHÍNH XÁC với output của compute_kpis():
#   - groupBy keys
#   - agg columns
#   - derived columns (pickup_datetime_hour, rolling windows, revenue_per_mile)
#   - pickup_year / pickup_month là PartitionKeys — KHÔNG liệt kê ở đây.
GOLD_KPI_COLUMNS = [
    # ── GroupBy keys ──────────────────────────────────────────────────────────
    {"Name": "pickup_date",           "Type": "date"},
    {"Name": "pickup_hour",           "Type": "int"},
    {"Name": "pickup_borough",        "Type": "string"},
    # ── Aggregated metrics ────────────────────────────────────────────────────
    {"Name": "trip_count",            "Type": "bigint"},
    {"Name": "total_revenue",         "Type": "double"},
    {"Name": "avg_fare",              "Type": "double"},
    {"Name": "avg_distance_mi",       "Type": "double"},
    {"Name": "avg_duration_min",      "Type": "double"},
    {"Name": "avg_tip_rate",          "Type": "double"},
    {"Name": "airport_trips",         "Type": "bigint"},
    {"Name": "weekend_trips",         "Type": "bigint"},
    {"Name": "unique_pickup_zones",   "Type": "bigint"},
    # ── Derived columns ───────────────────────────────────────────────────────
    {"Name": "pickup_datetime_hour",  "Type": "timestamp"},
    {"Name": "rolling_7d_trips",      "Type": "double"},
    {"Name": "rolling_7d_revenue",    "Type": "double"},
    {"Name": "revenue_per_mile",      "Type": "double"},
    # NOTE: pickup_year và pickup_month là PartitionKeys — không liệt kê ở đây.
]


# ─────────────────────────────────────────────────────────────────────────────
# Glue helpers
# ─────────────────────────────────────────────────────────────────────────────

def ensure_glue_database(glue_client, database_name: str, s3_location: str) -> None:
    """Create the Glue database if it does not already exist."""
    try:
        glue_client.get_database(Name=database_name)
        log.info("[glue] Database '%s' already exists", database_name)
    except glue_client.exceptions.EntityNotFoundException:
        glue_client.create_database(
            DatabaseInput={
                "Name": database_name,
                "LocationUri": s3_location,
            }
        )
        log.info("[glue] Created database '%s'", database_name)


def register_delta_table(
    glue_database: str,
    table_name: str,
    s3_location: str,
    partition_keys: list[str],
    columns: list[dict],
    region: str,
) -> None:
    """
    Register Delta Lake table trong Glue dùng SymlinkTextInputFormat.

    Giống hệt pattern của job1 (silver) và job2 (fact_trips):
      - Location trỏ vào _symlink_format_manifest/ (KHÔNG phải table root)
      - SymlinkTextInputFormat + ParquetHiveSerDe
      - Explicit Columns — bắt buộc, không thể infer từ Parquet metadata
    """
    glue = boto3.client("glue", region_name=region)
    ensure_glue_database(glue, glue_database, s3_location)

    partition_col_defs = [{"Name": pk, "Type": "string"} for pk in partition_keys]
    manifest_location = f"{s3_location}/_symlink_format_manifest"

    table_input = {
        "Name": table_name,
        "Description": f"Delta Lake table (gold KPI) — {table_name}",
        "TableType": "EXTERNAL_TABLE",
        "Parameters": {
            "classification": "parquet",
            "EXTERNAL":       "TRUE",
            "table_type":     "DELTA",
        },
        "StorageDescriptor": {
            "Location":     manifest_location,
            "InputFormat":  "org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
            "SerdeInfo": {
                "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                "Parameters": {"serialization.format": "1"},
            },
            "Columns": columns,
        },
        "PartitionKeys": partition_col_defs,
    }

    _upsert_glue_table(glue, glue_database, table_input)


def _add_gold_partitions(
    glue_database: str,
    table_name: str,
    s3_table_root: str,
    year: str,
    month: str,
    columns: list[dict],
    region: str,
) -> None:
    """
    Register (pickup_year, pickup_month) partition trong Glue catalog.

    Location của mỗi partition phải trỏ vào manifest sub-directory:
        <table_root>/_symlink_format_manifest/pickup_year=YYYY/pickup_month=M/

    Thiếu bước này → Athena báo "No path property defined".
    """
    glue = boto3.client("glue", region_name=region)

    month_int = str(int(month))

    partition_manifest_location = (
        f"{s3_table_root}/_symlink_format_manifest"
        f"/pickup_year={year}/pickup_month={month_int}/"
    )

    partition_input = {
        "Values": [year, month_int],
        "StorageDescriptor": {
            "Location":     partition_manifest_location,
            "InputFormat":  "org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
            "SerdeInfo": {
                "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                "Parameters": {"serialization.format": "1"},
            },
            "Columns": columns,
        },
    }

    try:
        glue.create_partition(
            DatabaseName=glue_database,
            TableName=table_name,
            PartitionInput=partition_input,
        )
        log.info(
            "[glue] Added KPI partition pickup_year=%s/pickup_month=%s to %s.%s → %s",
            year, month_int, glue_database, table_name, partition_manifest_location,
        )
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "AlreadyExistsException":
            glue.update_partition(
                DatabaseName=glue_database,
                TableName=table_name,
                PartitionValueList=[year, month_int],
                PartitionInput=partition_input,
            )
            log.info(
                "[glue] Updated KPI partition pickup_year=%s/pickup_month=%s in %s.%s",
                year, month_int, glue_database, table_name,
            )
        else:
            raise


def _upsert_glue_table(glue_client, database: str, table_input: dict) -> None:
    """Create the Glue table; update it if it already exists."""
    name = table_input["Name"]
    try:
        glue_client.create_table(DatabaseName=database, TableInput=table_input)
        log.info("[glue] Created  %s.%s", database, name)
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "AlreadyExistsException":
            glue_client.update_table(DatabaseName=database, TableInput=table_input)
            log.info("[glue] Updated  %s.%s", database, name)
        else:
            raise


# ─────────────────────────────────────────────────────────────────────────────
# Spark Session
# ─────────────────────────────────────────────────────────────────────────────

def build_spark(app_name: str) -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)

        # Delta Lake extensions
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")

        # Auto-generate symlink manifest sau mỗi write
        .config("spark.databricks.delta.properties.defaults"
                ".compatibility.symlinkFormatManifest.enabled", "true")

        # AWS Glue Catalog
        .config("spark.hadoop.hive.metastore.client.factory.class",
                "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")

        # Performance
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", "64")

        # Dynamic overwrite
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")

        .enableHiveSupport()
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ─────────────────────────────────────────────────────────────────────────────
# KPI Computation
# ─────────────────────────────────────────────────────────────────────────────

def compute_kpis(
    spark: SparkSession,
    bucket: str,
    year: str,
    month: str,
    glue_database: str,
    aws_region: str,
) -> None:

    expected_year  = int(year)
    expected_month = int(month)

    gold_path = f"s3://{bucket}/gold/fact_trips"

    log.info("Reading gold fact_trips for %s-%s", year, month)

    df = (
        spark.read
        .format("delta")
        .load(gold_path)
        .filter(
            (F.col("pickup_year")  == expected_year)
            & (F.col("pickup_month") == expected_month)
        )
    )

    source_rows = df.count()
    log.info("Source rows for %s-%s: %d", year, month, source_rows)

    # ── Hourly borough KPIs ───────────────────────────────────────────────────

    hourly = (
        df.groupBy(
            "pickup_year",
            "pickup_month",
            "pickup_date",
            "pickup_hour",
            "pickup_borough",
        )
        .agg(
            F.count("*").alias("trip_count"),
            F.sum("total_amount").alias("total_revenue"),
            F.avg("fare_amount").alias("avg_fare"),
            F.avg("trip_distance").alias("avg_distance_mi"),
            F.avg("trip_duration_min").alias("avg_duration_min"),
            F.avg("tip_rate").alias("avg_tip_rate"),
            F.sum(F.col("is_airport_trip").cast("long")).alias("airport_trips"),
            F.sum(F.col("is_weekend").cast("long")).alias("weekend_trips"),
            F.countDistinct("PULocationID").alias("unique_pickup_zones"),
        )
    )

    # ── Timestamp column ──────────────────────────────────────────────────────

    hourly = hourly.withColumn(
        "pickup_datetime_hour",
        F.to_timestamp(
            F.concat(
                F.col("pickup_date").cast("string"),
                F.lit(" "),
                F.lpad(F.col("pickup_hour").cast("string"), 2, "0"),
                F.lit(":00:00"),
            ),
            "yyyy-MM-dd HH:mm:ss",
        ),
    )

    # ── Rolling windows ───────────────────────────────────────────────────────

    hourly = hourly.repartition(32, "pickup_borough")

    window_7d = (
        Window
        .partitionBy("pickup_borough")
        .orderBy(F.col("pickup_datetime_hour").cast("long"))
        .rangeBetween(-(7 * 24 * 3600), 0)
    )

    hourly = (
        hourly
        .withColumn("rolling_7d_trips",   F.avg("trip_count").over(window_7d))
        .withColumn("rolling_7d_revenue",  F.avg("total_revenue").over(window_7d))
    )

    # ── Revenue efficiency ────────────────────────────────────────────────────

    hourly = hourly.withColumn(
        "revenue_per_mile",
        F.when(
            F.col("avg_distance_mi") > 0,
            F.col("avg_fare") / F.col("avg_distance_mi"),
        )
    )

    # ── Final repartition trước write ─────────────────────────────────────────

    hourly = hourly.repartition(16, "pickup_year", "pickup_month")

    kpi_count = hourly.count()
    log.info("KPI rows computed: %d", kpi_count)

    # ── Delta write ───────────────────────────────────────────────────────────
    # FIX (2026-05-22): mergeSchema=true aligns with job1 and job2, and
    # ensures any future additions to hourly_kpis columns are handled
    # automatically without a manual schema migration.
    # overwriteSchema is intentionally NOT set — Delta rejects the combination
    # of replaceWhere + overwriteSchema=true.

    output_path  = f"s3://{bucket}/gold/hourly_kpis"
    replace_cond = f"pickup_year = {expected_year} AND pickup_month = {expected_month}"

    (
        hourly.write
        .format("delta")
        .mode("overwrite")
        .option("replaceWhere", replace_cond)
        .option("mergeSchema", "true")
        .option("delta.compatibility.symlinkFormatManifest.enabled", "true")
        .partitionBy("pickup_year", "pickup_month")
        .save(output_path)
    )

    log.info("Wrote %d KPI rows to %s", kpi_count, output_path)

    # ── Explicitly generate symlink manifest ──────────────────────────────────
    spark.sql(f"""
        GENERATE symlink_format_manifest
        FOR TABLE delta.`{output_path}`
    """)
    log.info("Generated symlink_format_manifest at %s/_symlink_format_manifest/", output_path)

    # ── Register table trong Glue ─────────────────────────────────────────────
    # FIX: dùng SymlinkTextInputFormat + GOLD_KPI_COLUMNS thay vì
    # SequenceFileInputFormat + Columns=[] (không hoạt động với Athena).
    register_delta_table(
        glue_database=glue_database,
        table_name="hourly_kpis",
        s3_location=output_path,
        partition_keys=["pickup_year", "pickup_month"],
        columns=GOLD_KPI_COLUMNS,
        region=aws_region,
    )

    # ── Register partition trong Glue ─────────────────────────────────────────
    # FIX: per-partition manifest location để Athena resolve đúng path.
    _add_gold_partitions(
        glue_database=glue_database,
        table_name="hourly_kpis",
        s3_table_root=output_path,
        year=year,
        month=month,
        columns=GOLD_KPI_COLUMNS,
        region=aws_region,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Gold → KPI Aggregations")
    parser.add_argument("--year",          required=True)
    parser.add_argument("--month",         required=True)
    parser.add_argument("--bucket",        required=True)
    parser.add_argument("--glue-database", default="nyc_tlc_gold")
    # FIX: thống nhất default region về ap-southeast-1 (giống job1).
    parser.add_argument("--aws-region",    default="ap-southeast-1")
    args = parser.parse_args()

    spark = build_spark(f"nyc-tlc-aggregate-{args.year}-{args.month}")

    try:
        compute_kpis(
            spark=spark,
            bucket=args.bucket,
            year=args.year,
            month=args.month,
            glue_database=args.glue_database,
            aws_region=args.aws_region,
        )
        log.info("Job completed successfully")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()