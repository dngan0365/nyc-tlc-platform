# spark_jobs/job3_aggregate.py

"""
Job 3 — Gold → KPI Aggregations
Optimized for EMR Serverless + Delta Lake.
"""

from __future__ import annotations

import argparse
import logging

import boto3
from botocore.exceptions import ClientError
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

log = logging.getLogger(__name__)


# -----------------------------------------------------------------------------
# Glue Registration
# -----------------------------------------------------------------------------

def ensure_glue_database(glue_client, database_name: str) -> None:
    """
    Create the Glue database if it does not already exist.
    Safe to call on every job run — does nothing if the DB is already there.
    """
    try:
        glue_client.get_database(Name=database_name)
        log.info(f"[glue] Database '{database_name}' already exists")
    except glue_client.exceptions.EntityNotFoundException:
        glue_client.create_database(
            DatabaseInput={"Name": database_name}
        )
        log.info(f"[glue] Created database '{database_name}'")


def register_glue_table(
    glue_database: str,
    table_name: str,
    s3_location: str,
    partition_keys: list[str],
    region: str = "us-east-1",
) -> None:
    """
    Create or update a Glue table pointing at a Delta Lake S3 location.

    Done AFTER the Delta write so the _delta_log already exists in S3.

    Parameters
    ----------
    glue_database:  Name of the target Glue database (e.g. 'nyc_tlc_gold').
    table_name:     Name of the table to create/update (e.g. 'hourly_kpis').
    s3_location:    S3 URI of the Delta table root (no trailing slash).
    partition_keys: List of partition column names.
    region:         AWS region where Glue lives.
    """

    glue = boto3.client("glue", region_name=region)

    ensure_glue_database(glue, glue_database)

    partition_col_defs = [
        {"Name": pk, "Type": "string"}
        for pk in partition_keys
    ]

    table_input = {
        "Name": table_name,
        "Description": f"Delta Lake table managed by EMR Serverless job3_aggregate — {table_name}",
        "TableType": "EXTERNAL_TABLE",
        "Parameters": {
            "table_type":                 "DELTA",
            "spark.sql.sources.provider": "delta",
        },
        "StorageDescriptor": {
            "Location":     s3_location,
            "InputFormat":  "org.apache.hadoop.mapred.SequenceFileInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat",
            "SerdeInfo": {
                "SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
            },
            # Columns left empty — Delta reader infers schema from _delta_log at query time
            "Columns": [],
        },
        "PartitionKeys": partition_col_defs,
    }

    try:
        glue.create_table(
            DatabaseName=glue_database,
            TableInput=table_input,
        )
        log.info(
            f"[glue] Created table "
            f"{glue_database}.{table_name} → {s3_location}"
        )

    except ClientError as exc:
        if exc.response["Error"]["Code"] == "AlreadyExistsException":
            glue.update_table(
                DatabaseName=glue_database,
                TableInput=table_input,
            )
            log.info(
                f"[glue] Updated table "
                f"{glue_database}.{table_name} → {s3_location}"
            )
        else:
            raise


# -----------------------------------------------------------------------------
# Spark
# -----------------------------------------------------------------------------

def build_spark(app_name: str) -> SparkSession:

    spark = (
        SparkSession.builder
        .appName(app_name)

        # Delta
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )

        # Glue
        .config(
            "spark.hadoop.hive.metastore.client.factory.class",
            "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
        )

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


# -----------------------------------------------------------------------------
# KPI Computation
# -----------------------------------------------------------------------------

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

    log.info(f"Reading gold fact_trips for {year}-{month}")

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
    log.info(f"Source rows for {year}-{month}: {source_rows:,}")

    # ---- Hourly borough KPIs ------------------------------------------------

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

    # ---- Timestamp column ---------------------------------------------------

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

    # ---- Rolling windows ----------------------------------------------------

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

    # ---- Revenue efficiency -------------------------------------------------

    hourly = hourly.withColumn(
        "revenue_per_mile",
        F.when(
            F.col("avg_distance_mi") > 0,
            F.col("avg_fare") / F.col("avg_distance_mi"),
        )
    )

    # ---- Final repartition before write -------------------------------------

    hourly = hourly.repartition(16, "pickup_year", "pickup_month")

    kpi_count = hourly.count()
    log.info(f"KPI rows computed: {kpi_count:,}")

    # ---- Delta write --------------------------------------------------------

    output_path  = f"s3://{bucket}/gold/hourly_kpis"
    replace_cond = f"pickup_year = {expected_year} AND pickup_month = {expected_month}"

    (
        hourly.write
        .format("delta")
        .mode("overwrite")
        .option("replaceWhere", replace_cond)
        .option("overwriteSchema", "false")
        .partitionBy("pickup_year", "pickup_month")
        .save(output_path)
    )

    log.info(f"Wrote {kpi_count:,} KPI rows to {output_path}")

    # ---- Register in Glue ---------------------------------------------------
    # Done AFTER the Delta write so the _delta_log already exists in S3.

    register_glue_table(
        glue_database=glue_database,
        table_name="hourly_kpis",
        s3_location=output_path,
        partition_keys=["pickup_year", "pickup_month"],
        region=aws_region,
    )


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------

def main() -> None:

    parser = argparse.ArgumentParser(description="Gold → KPI Aggregations")
    parser.add_argument("--year",          required=True)
    parser.add_argument("--month",         required=True)
    parser.add_argument("--bucket",        required=True)
    parser.add_argument("--glue-database", default="nyc_tlc_gold", help="Glue DB for gold tables")
    parser.add_argument("--aws-region",    default="us-east-1")
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