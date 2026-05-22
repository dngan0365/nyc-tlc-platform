#!/usr/bin/env python3
"""
repair_silver_glue.py
─────────────────────
One-shot script to fix the nyc_tlc_silver Glue tables for green and yellow
without re-running the full Spark cleanse job.

Run this when:
  - job1_cleanse.py has already written Delta + manifest files to S3
  - But the Glue table was registered with Columns=[] (empty schema)
  - And/or partitions were never added to the Glue catalog
  - And dbt is failing with "Column 'pickup_datetime' cannot be resolved"
    or "No path property defined for table"

Usage
─────
    python repair_silver_glue.py \
        --bucket nyc-tlc-datalake-dev-861276091613 \
        --year 2023 \
        --month 01 \
        --aws-region ap-southeast-1

    # Multiple months:
    for month in 01 02 03; do
        python repair_silver_glue.py \
            --bucket nyc-tlc-datalake-dev-861276091613 \
            --year 2023 --month $month \
            --aws-region ap-southeast-1
    done

What it does
────────────
1. Updates nyc_tlc_silver.yellow and nyc_tlc_silver.green table definitions
   to include the full SILVER_COLUMNS schema in StorageDescriptor["Columns"].
2. Adds (or updates) the Glue partition entry for the given (year, month),
   pointing at the correct manifest sub-directory that Delta already generated.
3. Does NOT touch S3 data or Delta transaction log.
4. Safe to re-run — all operations are idempotent.
"""

from __future__ import annotations

import argparse
import logging

import boto3
from botocore.exceptions import ClientError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Silver schema — must match the output of job1_cleanse.py's cleanse_vehicle()
# ─────────────────────────────────────────────────────────────────────────────

SILVER_COLUMNS = [
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
    {"Name": "ehail_fee",             "Type": "double"},    # green only
    {"Name": "trip_type",             "Type": "bigint"},    # green only
    {"Name": "trip_duration_min",     "Type": "double"},
    {"Name": "speed_mph",             "Type": "double"},
    {"Name": "pickup_date",           "Type": "date"},
    {"Name": "pickup_hour",           "Type": "int"},
    {"Name": "pickup_dow",            "Type": "int"},
    {"Name": "vehicle_type",          "Type": "string"},
    {"Name": "trip_id",               "Type": "string"},
    # pickup_year / pickup_month are PartitionKeys, not Columns
]

SILVER_DATABASE  = "nyc_tlc_silver"
VEHICLE_TYPES    = ["yellow", "green"]
PARTITION_KEYS   = [{"Name": "pickup_year", "Type": "string"},
                    {"Name": "pickup_month", "Type": "string"}]


def _storage_descriptor(location: str) -> dict:
    """Return a SymlinkTextInputFormat StorageDescriptor pointing at `location`."""
    return {
        "Location":     location,
        "InputFormat":  "org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat",
        "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
        "SerdeInfo": {
            "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
            "Parameters": {"serialization.format": "1"},
        },
        "Columns": SILVER_COLUMNS,
    }


def repair_table(glue, bucket: str, vehicle_type: str) -> None:
    """Update the Glue table definition to include explicit column schema."""
    table_root       = f"s3://{bucket}/silver/{vehicle_type}"
    manifest_location = f"{table_root}/_symlink_format_manifest"

    table_input = {
        "Name": vehicle_type,
        "Description": f"Delta Lake table (silver) — {vehicle_type} taxi trips [schema repaired]",
        "TableType": "EXTERNAL_TABLE",
        "Parameters": {
            "classification": "parquet",
            "EXTERNAL":       "TRUE",
            "table_type":     "DELTA",
        },
        "StorageDescriptor": _storage_descriptor(manifest_location),
        "PartitionKeys": PARTITION_KEYS,
    }

    try:
        glue.update_table(DatabaseName=SILVER_DATABASE, TableInput=table_input)
        log.info("[repair] Updated table %s.%s with explicit schema", SILVER_DATABASE, vehicle_type)
    except ClientError as exc:
        code = exc.response["Error"]["Code"]
        if code == "EntityNotFoundException":
            glue.create_table(DatabaseName=SILVER_DATABASE, TableInput=table_input)
            log.info("[repair] Created table %s.%s", SILVER_DATABASE, vehicle_type)
        else:
            raise


def repair_partition(glue, bucket: str, vehicle_type: str, year: str, month: str) -> None:
    """
    Add or update the Glue partition entry for (year, month).

    Delta writes the per-partition manifest to:
        <table_root>/_symlink_format_manifest/pickup_year=YYYY/pickup_month=M/
    where pickup_month has NO leading zero (e.g. 1, not 01).
    """
    table_root = f"s3://{bucket}/silver/{vehicle_type}"
    month_int  = str(int(month))   # strip leading zero

    partition_manifest = (
        f"{table_root}/_symlink_format_manifest"
        f"/pickup_year={year}/pickup_month={month_int}/"
    )

    partition_input = {
        "Values": [year, month_int],
        "StorageDescriptor": _storage_descriptor(partition_manifest),
    }

    try:
        glue.create_partition(
            DatabaseName=SILVER_DATABASE,
            TableName=vehicle_type,
            PartitionInput=partition_input,
        )
        log.info(
            "[repair] Added partition pickup_year=%s/pickup_month=%s to %s.%s → %s",
            year, month_int, SILVER_DATABASE, vehicle_type, partition_manifest,
        )
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "AlreadyExistsException":
            glue.update_partition(
                DatabaseName=SILVER_DATABASE,
                TableName=vehicle_type,
                PartitionValueList=[year, month_int],
                PartitionInput=partition_input,
            )
            log.info(
                "[repair] Updated partition pickup_year=%s/pickup_month=%s in %s.%s",
                year, month_int, SILVER_DATABASE, vehicle_type,
            )
        else:
            raise


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Repair nyc_tlc_silver Glue table schema and partitions for Athena"
    )
    parser.add_argument("--bucket",     required=True, help="S3 bucket name, e.g. nyc-tlc-datalake-dev-861276091613")
    parser.add_argument("--year",       required=True, help="e.g. 2023")
    parser.add_argument("--month",      required=True, help="e.g. 01")
    parser.add_argument("--aws-region", default="ap-southeast-1")
    args = parser.parse_args()

    glue = boto3.client("glue", region_name=args.aws_region)

    for vehicle_type in VEHICLE_TYPES:
        log.info("=== Repairing %s.%s ===", SILVER_DATABASE, vehicle_type)
        repair_table(glue, args.bucket, vehicle_type)
        repair_partition(glue, args.bucket, vehicle_type, args.year, args.month)

    log.info("Done. Re-run `dbt run` — stg_yellow_trips and stg_green_trips should now pass.")


if __name__ == "__main__":
    main()