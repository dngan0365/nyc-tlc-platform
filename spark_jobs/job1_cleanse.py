# spark_jobs/job1_cleanse.py

"""
Job 1 — Bronze → Silver cleansing pipeline.
Optimized for EMR Serverless + Delta Lake.
"""

from __future__ import annotations

import argparse
import logging

import boto3
from botocore.exceptions import ClientError
from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession, functions as F

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

    Uses EXTERNAL_TABLE with Delta-specific SerDe so Athena can query
    it via the Athena-Delta connector, and so `aws glue get-tables`
    returns it for dbt source validation.

    Parameters
    ----------
    glue_database:  Name of the target Glue database (e.g. 'nyc_tlc_silver').
    table_name:     Name of the table to create/update (e.g. 'yellow').
    s3_location:    S3 URI of the Delta table root (no trailing slash).
    partition_keys: List of partition column names (e.g. ['pickup_year', 'pickup_month']).
    region:         AWS region where Glue lives.
    """

    glue = boto3.client("glue", region_name=region)

    ensure_glue_database(glue, glue_database)

    # Partition columns — Glue needs them declared as StorageDescriptor columns
    # with an empty type so the Delta reader can infer actual types at query time.
    partition_col_defs = [
        {"Name": pk, "Type": "string"}
        for pk in partition_keys
    ]

    table_input = {
        "Name": table_name,
        "Description": f"Delta Lake table managed by EMR Serverless job1_cleanse — {table_name} trips",
        "TableType": "EXTERNAL_TABLE",
        "Parameters": {
            # Tells Glue / Athena this is a Delta table
            "table_type":               "DELTA",
            "spark.sql.sources.provider": "delta",
        },
        "StorageDescriptor": {
            "Location":  s3_location,
            "InputFormat":  "org.apache.hadoop.mapred.SequenceFileInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat",
            "SerdeInfo": {
                "SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
            },
            # Non-partition columns are left empty; Delta reader infers schema
            # from the _delta_log at query time.
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
# Spark Session
# -----------------------------------------------------------------------------

def build_spark(app_name: str) -> SparkSession:
    """
    Build SparkSession for EMR Serverless.

    NOTE:
    Delta JARs MUST be attached via spark-submit parameters:

    --conf spark.jars.packages=io.delta:delta-spark_2.12:3.2.0
    """

    spark = (
        SparkSession.builder
        .appName(app_name)

        # Delta Lake
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )

        # AWS Glue Catalog
        .config(
            "spark.hadoop.hive.metastore.client.factory.class",
            "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
        )

        # Performance tuning
        .config("spark.sql.shuffle.partitions", "50")
        .config("spark.sql.adaptive.enabled", "true")

        # S3 optimization
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        .config("spark.hadoop.fs.s3a.multipart.size", "67108864")

        .enableHiveSupport()
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark


# -----------------------------------------------------------------------------
# Schema enforcement
# -----------------------------------------------------------------------------

STABLE_TYPES: dict[str, str] = {
    "VendorID":              "long",
    "RatecodeID":            "long",
    "payment_type":          "long",
    "passenger_count":       "long",
    "PULocationID":          "long",
    "DOLocationID":          "long",
    "trip_distance":         "double",
    "fare_amount":           "double",
    "tip_amount":            "double",
    "total_amount":          "double",
    "extra":                 "double",
    "mta_tax":               "double",
    "tolls_amount":          "double",
    "improvement_surcharge": "double",
    "congestion_surcharge":  "double",
}


def enforce_stable_types(df: DataFrame) -> DataFrame:
    for col_name, target_type in STABLE_TYPES.items():
        if col_name in df.columns:
            df = df.withColumn(
                col_name,
                F.when(
                    F.trim(F.col(col_name).cast("string")) == "",
                    None,
                ).otherwise(
                    F.col(col_name).cast(target_type)
                )
            )
    return df


# -----------------------------------------------------------------------------
# Cleansing Logic
# -----------------------------------------------------------------------------

def cleanse_vehicle(
    spark: SparkSession,
    bucket: str,
    vehicle_type: str,
    year: str,
    month: str,
    glue_database: str,
    aws_region: str,
) -> int:

    input_path  = f"s3://{bucket}/bronze/{vehicle_type}/{year}-{month}/"
    output_path = f"s3://{bucket}/silver/{vehicle_type}"

    log.info(f"[{vehicle_type}] Reading bronze data from {input_path}")

    df = spark.read.parquet(input_path)
    raw_count = df.count()
    log.info(f"[{vehicle_type}] Raw rows: {raw_count:,}")

    # ---- Normalize schema ---------------------------------------------------

    if vehicle_type == "yellow":
        df = (
            df
            .withColumnRenamed("tpep_pickup_datetime",  "pickup_datetime")
            .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")
        )
    elif vehicle_type == "green":
        df = (
            df
            .withColumnRenamed("lpep_pickup_datetime",  "pickup_datetime")
            .withColumnRenamed("lpep_dropoff_datetime", "dropoff_datetime")
        )

    df = enforce_stable_types(df)

    # ---- Data quality filters -----------------------------------------------

    df = df.filter(
        F.col("pickup_datetime").isNotNull()
        & F.col("dropoff_datetime").isNotNull()
        & F.col("PULocationID").isNotNull()
        & F.col("DOLocationID").isNotNull()
        & (F.col("trip_distance") > 0)
        & (F.col("trip_distance") < 200)
        & (F.col("total_amount") > 0)
        & (F.col("total_amount") < 5000)
        & (F.col("passenger_count") >= 1)
        & (F.col("passenger_count") <= 6)
    )

    # ---- Derived columns ----------------------------------------------------

    df = (
        df
        .withColumn(
            "trip_duration_min",
            (F.unix_timestamp("dropoff_datetime") - F.unix_timestamp("pickup_datetime")) / 60.0,
        )
        .withColumn(
            "speed_mph",
            F.when(
                F.col("trip_duration_min") > 0,
                F.col("trip_distance") / (F.col("trip_duration_min") / 60.0),
            ),
        )
        .withColumn("pickup_date",  F.to_date("pickup_datetime"))
        .withColumn("pickup_year",  F.year("pickup_datetime"))
        .withColumn("pickup_month", F.month("pickup_datetime"))
        .withColumn("pickup_hour",  F.hour("pickup_datetime"))
        .withColumn("pickup_dow",   F.dayofweek("pickup_datetime"))
        .withColumn("vehicle_type", F.lit(vehicle_type))
        .withColumn(
            "trip_id",
            F.sha2(
                F.concat_ws(
                    "|",
                    F.col("VendorID").cast("string"),
                    F.col("pickup_datetime").cast("string"),
                    F.col("PULocationID").cast("string"),
                    F.col("DOLocationID").cast("string"),
                ),
                256,
            ),
        )
    )

    # ---- Sanity checks ------------------------------------------------------

    df = df.filter(
        (F.col("trip_duration_min") > 1)
        & (F.col("trip_duration_min") < 300)
        & (F.col("speed_mph").isNull() | (F.col("speed_mph") < 120))
    )

    df = df.dropDuplicates(["trip_id"])
    df = df.filter(F.col("pickup_month") == int(month))
    df = df.persist(StorageLevel.MEMORY_AND_DISK)

    clean_count = df.count()
    dropped = raw_count - clean_count
    drop_pct = round(dropped / raw_count * 100, 1) if raw_count > 0 else 0

    log.info(
        f"[{vehicle_type}] Clean rows: {clean_count:,} "
        f"(dropped {dropped:,} = {drop_pct}%)"
    )

    # ---- Delta write --------------------------------------------------------

    replace_condition = f"pickup_year = {year} AND pickup_month = {int(month)}"

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("replaceWhere", replace_condition)
        .option("overwriteSchema", "false")
        .partitionBy("pickup_year", "pickup_month")
        .save(output_path)
    )

    log.info(f"[{vehicle_type}] Written to {output_path} ({clean_count:,} rows)")

    df.unpersist()

    # ---- Register in Glue ---------------------------------------------------
    # Done AFTER the Delta write so the _delta_log already exists in S3.

    register_glue_table(
        glue_database=glue_database,
        table_name=vehicle_type,           # → nyc_tlc_silver.yellow / .green
        s3_location=output_path,
        partition_keys=["pickup_year", "pickup_month"],
        region=aws_region,
    )

    return clean_count


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------

def main() -> None:

    parser = argparse.ArgumentParser(description="Job 1: Bronze → Silver cleanse")
    parser.add_argument("--year",           required=True,  help="e.g. 2023")
    parser.add_argument("--month",          required=True,  help="e.g. 03")
    parser.add_argument("--bucket",         required=True,  help="S3 bucket name")
    parser.add_argument("--glue-database",  default="nyc_tlc_silver", help="Glue DB for silver tables")
    parser.add_argument("--aws-region",     default="us-east-1")
    args = parser.parse_args()

    log.info(f"Starting cleanse job for {args.year}-{args.month}")

    spark = build_spark(f"nyc-tlc-cleanse-{args.year}-{args.month}")

    try:
        total_rows = 0
        for vehicle in ["yellow", "green"]:
            rows = cleanse_vehicle(
                spark=spark,
                bucket=args.bucket,
                vehicle_type=vehicle,
                year=args.year,
                month=args.month,
                glue_database=args.glue_database,
                aws_region=args.aws_region,
            )
            total_rows += rows

        log.info(f"Job completed successfully. Total rows written: {total_rows:,}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()