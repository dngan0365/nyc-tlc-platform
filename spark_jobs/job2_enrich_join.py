# spark_jobs/job2_enrich.py

"""
Job 2 — Silver → Gold enrichment pipeline.
Optimized for EMR Serverless + Delta Lake.

FIX (2026-05-20)
────────────────
Trước đây register_glue_table() dùng SequenceFileInputFormat + LazySimpleSerDe
+ Columns=[] — đây là Hive stub, Athena KHÔNG đọc được Delta qua cách này.

Athena yêu cầu cùng pattern như silver (job1):
  1. SymlinkTextInputFormat — Athena đọc manifest để tìm Parquet files.
  2. Explicit GOLD_FACT_COLUMNS — SymlinkTextInputFormat không tự infer schema.
  3. Per-partition manifest entries — Athena dùng Location của từng partition
     để resolve "No path property defined" error.
  4. GENERATE symlink_format_manifest sau mỗi Delta write — để manifest luôn
     phản ánh đúng data hiện tại.
  5. Default region thống nhất về ap-southeast-1 (giống job1).

Errors được fix:
    TABLE_NOT_FOUND: nyc_tlc_gold.fact_trips
    Column '<name>' cannot be resolved
    No path property defined for table: nyc_tlc_gold.fact_trips

FIX (2026-05-22) — 2025-02 schema evolution (mirrors job1)
───────────────────────────────────────────────────────────
job1 now writes cbd_congestion_fee and airport_fee (lowercase) into silver.
job2 reads those silver Delta tables via unionByName, so the gold schema must
also carry both columns or the write will fail with a Delta AnalysisException.

Two changes fix this:
  1. cbd_congestion_fee added to GOLD_FACT_COLUMNS (airport_fee was already
     present; it was just missing from the list — added here for correctness).
     Older gold partitions will have NULL for cbd_congestion_fee — fine for
     Delta and Athena.
  2. The Delta write uses .option("mergeSchema", "true") so the first run
     carrying the new column extends the gold table schema automatically.
     overwriteSchema is intentionally NOT set — Delta rejects the combination
     of replaceWhere + overwriteSchema=true.
"""

from __future__ import annotations

import argparse
import logging

import boto3
from botocore.exceptions import ClientError
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# ─────────────────────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Schemas
# ─────────────────────────────────────────────────────────────────────────────

ZONE_SCHEMA = StructType([
    StructField("LocationID",    IntegerType(), True),
    StructField("Borough",       StringType(),  True),
    StructField("Zone",          StringType(),  True),
    StructField("service_zone",  StringType(),  True),
])

PAYMENT_DATA = [
    (1, "Credit card"),
    (2, "Cash"),
    (3, "No charge"),
    (4, "Dispute"),
    (5, "Unknown"),
    (6, "Voided trip"),
]

PAYMENT_SCHEMA = StructType([
    StructField("payment_type",      IntegerType(), True),
    StructField("payment_type_name", StringType(),  True),
])


# ─────────────────────────────────────────────────────────────────────────────
# Gold schema — fact_trips
# ─────────────────────────────────────────────────────────────────────────────
#
# Athena's SymlinkTextInputFormat yêu cầu explicit column definitions.
# List này phải khớp CHÍNH XÁC với output của enrich_join():
#   - tất cả columns từ silver (passthrough)
#   - derived columns từ enrich step (tip_rate, is_airport_trip, ...)
#   - dimension columns từ broadcast joins (borough, zone names, payment_type_name)
#   - pickup_year / pickup_month là PartitionKeys — KHÔNG liệt kê ở đây.
#
# Types dùng Hive DDL notation (Glue / Athena standard).
GOLD_FACT_COLUMNS = [
    # ── Silver passthrough columns ────────────────────────────────────────────
    {"Name": "VendorID",              "Type": "bigint"},
    {"Name": "pickup_datetime",       "Type": "timestamp"},
    {"Name": "dropoff_datetime",      "Type": "timestamp"},
    {"Name": "passenger_count",       "Type": "bigint"},
    {"Name": "trip_distance",         "Type": "double"},
    {"Name": "RatecodeID",            "Type": "bigint"},
    {"Name": "store_and_fwd_flag",    "Type": "string"},
    {"Name": "PULocationID",          "Type": "bigint"},
    {"Name": "DOLocationID",          "Type": "bigint"},
    {"Name": "payment_type",          "Type": "bigint"},
    {"Name": "fare_amount",           "Type": "double"},
    {"Name": "extra",                 "Type": "double"},
    {"Name": "mta_tax",               "Type": "double"},
    {"Name": "tip_amount",            "Type": "double"},
    {"Name": "tolls_amount",          "Type": "double"},
    {"Name": "improvement_surcharge", "Type": "double"},
    {"Name": "total_amount",          "Type": "double"},
    {"Name": "congestion_surcharge",  "Type": "double"},
    {"Name": "airport_fee",           "Type": "double"},
    # FIX (2026-05-22): new TLC surcharge column present from 2025 data onward.
    # Rows from older gold partitions will be NULL here — fine for Delta / Athena.
    {"Name": "cbd_congestion_fee",    "Type": "double"},
    # green-only columns (NULL for yellow rows)
    {"Name": "ehail_fee",             "Type": "double"},
    {"Name": "trip_type",             "Type": "bigint"},
    {"Name": "trip_duration_min",     "Type": "double"},
    {"Name": "speed_mph",             "Type": "double"},
    {"Name": "pickup_date",           "Type": "date"},
    {"Name": "pickup_hour",           "Type": "int"},
    {"Name": "pickup_dow",            "Type": "int"},
    {"Name": "vehicle_type",          "Type": "string"},
    {"Name": "trip_id",               "Type": "string"},
    # ── Dimension columns từ broadcast joins ──────────────────────────────────
    {"Name": "pickup_borough",        "Type": "string"},
    {"Name": "pickup_zone",           "Type": "string"},
    {"Name": "pickup_service_zone",   "Type": "string"},
    {"Name": "dropoff_borough",       "Type": "string"},
    {"Name": "dropoff_zone",          "Type": "string"},
    {"Name": "payment_type_name",     "Type": "string"},
    # ── Derived enrichment columns ────────────────────────────────────────────
    {"Name": "tip_rate",              "Type": "double"},
    {"Name": "is_airport_trip",       "Type": "boolean"},
    {"Name": "is_weekend",            "Type": "boolean"},
    {"Name": "time_of_day",           "Type": "string"},
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
    Register a Delta Lake table in Glue dùng SymlinkTextInputFormat,
    giống hệt pattern của job1 (silver tables).

    Tại sao SymlinkTextInputFormat thay vì native Delta?
    ─────────────────────────────────────────────────────
    Athena không native hiểu _delta_log/. Approach được support:
      1. Delta write data dạng Parquet dưới table root.
      2. `GENERATE symlink_format_manifest` → Delta tạo manifest tại
             <table_root>/_symlink_format_manifest/
      3. Glue table trỏ vào manifest via SymlinkTextInputFormat.
      4. Athena đọc files được liệt kê trong manifest.

    Tại sao cần explicit Columns?
    ──────────────────────────────
    SymlinkTextInputFormat không thể tự infer schema từ Parquet metadata.
    Nếu Columns=[], Athena raise "Column '<name>' cannot be resolved".
    """
    glue = boto3.client("glue", region_name=region)
    ensure_glue_database(glue, glue_database, s3_location)

    partition_col_defs = [{"Name": pk, "Type": "string"} for pk in partition_keys]

    # Manifest root — Delta tạo per-partition manifests dưới prefix này.
    manifest_location = f"{s3_location}/_symlink_format_manifest"

    table_input = {
        "Name": table_name,
        "Description": f"Delta Lake table (gold) — {table_name}",
        "TableType": "EXTERNAL_TABLE",
        "Parameters": {
            "classification": "parquet",
            "EXTERNAL":       "TRUE",
            "table_type":     "DELTA",
        },
        "StorageDescriptor": {
            # Trỏ vào manifest root, KHÔNG phải table root.
            "Location":     manifest_location,
            "InputFormat":  "org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
            "SerdeInfo": {
                "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                "Parameters": {"serialization.format": "1"},
            },
            # Explicit columns — bắt buộc cho SymlinkTextInputFormat.
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
    Register (pickup_year, pickup_month) partition trong Glue catalog
    cho gold SymlinkTextInputFormat table.

    Mỗi partition Location phải trỏ vào sub-directory của manifest:
        <table_root>/_symlink_format_manifest/pickup_year=YYYY/pickup_month=M/

    Nếu không có partition entries, Athena báo:
        "No path property defined for table: nyc_tlc_gold.<table>"
    """
    glue = boto3.client("glue", region_name=region)

    # Delta dùng integer month (không có leading zero).
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
            "[glue] Added gold partition pickup_year=%s/pickup_month=%s to %s.%s → %s",
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
                "[glue] Updated gold partition pickup_year=%s/pickup_month=%s in %s.%s",
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
    """
    Build SparkSession cho EMR Serverless.

    `delta.compatibility.symlinkFormatManifest.enabled` = true → Delta
    tự generate symlink manifest sau mỗi write, Athena luôn thấy data mới.
    """
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

        # Broadcast joins
        .config("spark.sql.autoBroadcastJoinThreshold", 10485760)  # 10 MB

        .enableHiveSupport()
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ─────────────────────────────────────────────────────────────────────────────
# Taxi zones
# ─────────────────────────────────────────────────────────────────────────────

def load_taxi_zones(spark: SparkSession, bucket: str):
    path = f"s3://{bucket}/reference/taxi_zone_lookup.csv"
    log.info("Loading taxi zones from %s", path)
    return (
        spark.read
        .schema(ZONE_SCHEMA)
        .option("header", "true")
        .csv(path)
    )


# ─────────────────────────────────────────────────────────────────────────────
# Main transform
# ─────────────────────────────────────────────────────────────────────────────

def enrich_join(
    spark: SparkSession,
    bucket: str,
    year: str,
    month: str,
    glue_database: str,
    aws_region: str,
) -> None:

    expected_year  = int(year)
    expected_month = int(month)

    yellow_path = f"s3://{bucket}/silver/yellow"
    green_path  = f"s3://{bucket}/silver/green"

    log.info("Reading yellow silver table")
    yellow_df = spark.read.format("delta").load(yellow_path)

    log.info("Reading green silver table")
    green_df = spark.read.format("delta").load(green_path)

    df = (
        yellow_df
        .unionByName(green_df, allowMissingColumns=True)
        .filter(
            (F.col("pickup_year")  == expected_year)
            & (F.col("pickup_month") == expected_month)
        )
        .persist()
    )

    row_count = df.count()
    log.info("Silver rows for %s-%s: %d", year, month, row_count)

    # ── Load dimensions ───────────────────────────────────────────────────────

    zones = load_taxi_zones(spark=spark, bucket=bucket)

    payment_dim = spark.createDataFrame(PAYMENT_DATA, schema=PAYMENT_SCHEMA)

    pu_zones = zones.select(
        F.col("LocationID").alias("PULocationID"),
        F.col("Borough").alias("pickup_borough"),
        F.col("Zone").alias("pickup_zone"),
        F.col("service_zone").alias("pickup_service_zone"),
    )

    do_zones = zones.select(
        F.col("LocationID").alias("DOLocationID"),
        F.col("Borough").alias("dropoff_borough"),
        F.col("Zone").alias("dropoff_zone"),
    )

    # ── Broadcast joins ───────────────────────────────────────────────────────

    df = (
        df
        .join(F.broadcast(pu_zones),    on="PULocationID",  how="left")
        .join(F.broadcast(do_zones),    on="DOLocationID",  how="left")
        .join(F.broadcast(payment_dim), on="payment_type",  how="left")
    )

    # ── Derived features ──────────────────────────────────────────────────────

    df = (
        df
        .withColumn(
            "tip_rate",
            F.when(F.col("fare_amount") > 0, F.col("tip_amount") / F.col("fare_amount"))
            .otherwise(F.lit(0.0)),
        )
        .withColumn(
            "is_airport_trip",
            F.col("pickup_service_zone").isin("Airports", "EWR").cast("boolean"),
        )
        .withColumn(
            "is_weekend",
            F.col("pickup_dow").isin(1, 7).cast("boolean"),
        )
        .withColumn(
            "time_of_day",
            F.when(F.col("pickup_hour").between(6,  9),  "morning_rush")
            .when(F.col("pickup_hour").between(10, 15),  "midday")
            .when(F.col("pickup_hour").between(16, 20),  "evening_rush")
            .when(F.col("pickup_hour").between(21, 23),  "night")
            .otherwise("late_night"),
        )
    )

    # ── Repartition trước write ───────────────────────────────────────────────

    df = df.repartition(8, "pickup_year", "pickup_month")

    # ── Delta write ───────────────────────────────────────────────────────────
    # FIX (2026-05-22): use mergeSchema=true so that the first run carrying
    # cbd_congestion_fee (new in 2025 silver data) extends the gold Delta table
    # schema automatically.  Older gold partitions gain a NULL column; newer
    # partitions carry the real values.  Safe to leave on permanently.
    #
    # overwriteSchema is intentionally NOT set — Delta rejects the combination
    # of replaceWhere + overwriteSchema=true.

    output_path  = f"s3://{bucket}/gold/fact_trips"
    replace_cond = f"pickup_year = {expected_year} AND pickup_month = {expected_month}"

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("replaceWhere", replace_cond)
        .option("mergeSchema", "true")
        .option("delta.compatibility.symlinkFormatManifest.enabled", "true")
        .partitionBy("pickup_year", "pickup_month")
        .save(output_path)
    )

    log.info("Wrote %d rows to %s", row_count, output_path)

    # ── Explicitly generate symlink manifest ──────────────────────────────────
    # Đảm bảo Athena luôn thấy data mới nhất, kể cả khi auto-generate chưa kịp.
    spark.sql(f"""
        GENERATE symlink_format_manifest
        FOR TABLE delta.`{output_path}`
    """)
    log.info("Generated symlink_format_manifest at %s/_symlink_format_manifest/", output_path)

    df.unpersist()

    # ── Register table trong Glue ─────────────────────────────────────────────
    # FIX: dùng SymlinkTextInputFormat + GOLD_FACT_COLUMNS thay vì
    # SequenceFileInputFormat + Columns=[] (không đọc được bằng Athena).
    register_delta_table(
        glue_database=glue_database,
        table_name="fact_trips",
        s3_location=output_path,
        partition_keys=["pickup_year", "pickup_month"],
        columns=GOLD_FACT_COLUMNS,
        region=aws_region,
    )

    # ── Register partition trong Glue ─────────────────────────────────────────
    # FIX: thêm per-partition manifest location để Athena resolve đúng path.
    _add_gold_partitions(
        glue_database=glue_database,
        table_name="fact_trips",
        s3_table_root=output_path,
        year=year,
        month=month,
        columns=GOLD_FACT_COLUMNS,
        region=aws_region,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Silver → Gold enrichment")
    parser.add_argument("--year",          required=True)
    parser.add_argument("--month",         required=True)
    parser.add_argument("--bucket",        required=True)
    parser.add_argument("--glue-database", default="nyc_tlc_gold")
    # FIX: thống nhất default region về ap-southeast-1 (giống job1).
    # Glue là regional — nếu job tạo table ở us-east-1 nhưng query từ
    # ap-southeast-1 thì Athena sẽ không thấy table.
    parser.add_argument("--aws-region",    default="ap-southeast-1")
    args = parser.parse_args()

    spark = build_spark(f"nyc-tlc-enrich-{args.year}-{args.month}")

    try:
        enrich_join(
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