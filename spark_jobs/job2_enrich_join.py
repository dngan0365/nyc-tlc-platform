# spark_jobs/job2_enrich.py

"""
Job 2 — Silver → Gold enrichment pipeline.
Optimized for EMR Serverless + Delta Lake.
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

# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

log = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# Schemas
# -----------------------------------------------------------------------------

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

    Uses EXTERNAL_TABLE with Delta-specific parameters so Athena can query
    it via the Athena-Delta connector, and so `aws glue get-tables` returns
    it for dbt source validation.

    Parameters
    ----------
    glue_database:  Name of the target Glue database (e.g. 'nyc_tlc_gold').
    table_name:     Name of the table to create/update (e.g. 'fact_trips').
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
        "Description": f"Delta Lake table managed by EMR Serverless job2_enrich — {table_name}",
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
            "Columns": [],  # Delta reader infers schema from _delta_log at query time
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

        # Glue catalog
        .config(
            "spark.hadoop.hive.metastore.client.factory.class",
            "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
        )

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


# -----------------------------------------------------------------------------
# Taxi zones
# -----------------------------------------------------------------------------

def load_taxi_zones(spark: SparkSession, bucket: str):
    path = f"s3://{bucket}/reference/taxi_zone_lookup.csv"
    log.info(f"Loading taxi zones from {path}")
    return (
        spark.read
        .schema(ZONE_SCHEMA)
        .option("header", "true")
        .csv(path)
    )


# -----------------------------------------------------------------------------
# Main transform
# -----------------------------------------------------------------------------

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
    log.info(f"Silver rows for {year}-{month}: {row_count:,}")

    # ---- Load dimensions ----------------------------------------------------

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

    # ---- Broadcast joins ----------------------------------------------------

    df = (
        df
        .join(F.broadcast(pu_zones),    on="PULocationID",  how="left")
        .join(F.broadcast(do_zones),    on="DOLocationID",  how="left")
        .join(F.broadcast(payment_dim), on="payment_type",  how="left")
    )

    # ---- Derived features ---------------------------------------------------

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

    # ---- Repartition before write -------------------------------------------

    df = df.repartition(8, "pickup_year", "pickup_month")

    # ---- Delta write --------------------------------------------------------

    output_path  = f"s3://{bucket}/gold/fact_trips"
    replace_cond = f"pickup_year = {expected_year} AND pickup_month = {expected_month}"

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("replaceWhere", replace_cond)
        .option("overwriteSchema", "false")
        .partitionBy("pickup_year", "pickup_month")
        .save(output_path)
    )

    log.info(f"Wrote {row_count:,} rows to {output_path}")

    # ---- Register in Glue ---------------------------------------------------
    # Done AFTER the Delta write so the _delta_log already exists in S3.

    register_glue_table(
        glue_database=glue_database,
        table_name="fact_trips",
        s3_location=output_path,
        partition_keys=["pickup_year", "pickup_month"],
        region=aws_region,
    )


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------

def main() -> None:

    parser = argparse.ArgumentParser(description="Silver → Gold enrichment")
    parser.add_argument("--year",          required=True)
    parser.add_argument("--month",         required=True)
    parser.add_argument("--bucket",        required=True)
    parser.add_argument("--glue-database", default="nyc_tlc_gold", help="Glue DB for gold tables")
    parser.add_argument("--aws-region",    default="us-east-1")
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