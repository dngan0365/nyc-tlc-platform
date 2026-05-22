# spark_jobs/job1_cleanse.py

"""
Job 1 — Bronze → Silver cleansing pipeline.
Optimized for EMR Serverless + Delta Lake.

Glue registration
─────────────────
Bronze tables  → raw Parquet, registered with ParquetHiveSerDe
Silver tables  → Delta Lake, registered with SymlinkTextInputFormat
               (Athena reads Delta via the _symlink_format_manifest/
                that Delta generates after `GENERATE symlink_format_manifest`)

FIX (2026-05-20)
────────────────
Previously the silver Glue table was registered with Columns=[] (empty schema).
Athena's SymlinkTextInputFormat does NOT fall back to Parquet file metadata for
column discovery — it requires an explicit schema in the Glue StorageDescriptor.
Without it, Athena cannot resolve any column name, which caused:

    Runtime Error: Column 'pickup_datetime' cannot be resolved
    No path property defined for table: nyc_tlc_silver.green

Two changes fix this:
  1. register_delta_table() now receives the full column list and writes it into
     StorageDescriptor["Columns"], so Athena knows the schema.
  2. _add_silver_partitions() adds per-partition location entries that point each
     (pickup_year, pickup_month) pair at its manifest sub-directory, matching the
     pattern Delta generates:
         <table_root>/_symlink_format_manifest/pickup_year=YYYY/pickup_month=MM/
     Without these, Athena returns "No path property defined" even when the manifest
     file itself exists.

FIX (2026-05-22) — 2025-02 schema evolution
────────────────────────────────────────────
TLC changed the yellow taxi Parquet schema starting with 2025 data:
  1. airport_fee column is now emitted as Airport_fee (capital A).
  2. A new cbd_congestion_fee column was added.

Both differences caused a Delta AnalysisException (schema mismatch) on write.

Three changes fix this:
  1. cleanse_vehicle() normalises Airport_fee → airport_fee immediately after
     reading bronze, before any other processing.
  2. cbd_congestion_fee is added to STABLE_TYPES (cast to double) and
     SILVER_COLUMNS (so Glue/Athena know about it).  Older partitions that
     pre-date the column will have NULL for every row — Delta handles this
     transparently.
  3. The Delta write uses .option("mergeSchema", "true") so that the first
     run against a partition containing the new column extends the table
     schema automatically.  overwriteSchema is left as "false" (its default)
     because replaceWhere + overwriteSchema=true is rejected by Delta.
"""

from __future__ import annotations

import argparse
import logging

import boto3
from botocore.exceptions import ClientError
from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession, functions as F

# ─────────────────────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Silver schema
# ─────────────────────────────────────────────────────────────────────────────

# Explicit column list for the silver Glue table.
# Athena's SymlinkTextInputFormat requires this — it cannot infer column names
# from Parquet file metadata the way native Parquet tables can.
#
# This list must match the output of cleanse_vehicle() exactly:
#   - renamed timestamps (tpep_* / lpep_* → pickup_datetime / dropoff_datetime)
#   - all original TLC columns that pass through enforce_stable_types()
#   - derived columns added in the cleanse step
#   - the vehicle_type and trip_id columns
#   - partition columns pickup_year / pickup_month (added separately as PartitionKeys)
#
# Types use Hive DDL notation (Glue / Athena standard).
SILVER_COLUMNS = [
    # ── Original TLC passthrough columns ─────────────────────────────────────
    {"Name": "VendorID",              "Type": "bigint"},
    {"Name": "pickup_datetime",       "Type": "timestamp"},   # renamed from tpep_/lpep_
    {"Name": "dropoff_datetime",      "Type": "timestamp"},   # renamed from tpep_/lpep_
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
    # Rows from older partitions will be NULL here — fine for Delta / Athena.
    {"Name": "cbd_congestion_fee",    "Type": "double"},
    # green-only columns (NULL for yellow rows)
    {"Name": "ehail_fee",             "Type": "double"},
    {"Name": "trip_type",             "Type": "bigint"},
    # ── Derived columns ───────────────────────────────────────────────────────
    {"Name": "trip_duration_min",     "Type": "double"},
    {"Name": "speed_mph",             "Type": "double"},
    {"Name": "pickup_date",           "Type": "date"},
    {"Name": "pickup_hour",           "Type": "int"},
    {"Name": "pickup_dow",            "Type": "int"},
    {"Name": "vehicle_type",          "Type": "string"},
    {"Name": "trip_id",               "Type": "string"},
    # NOTE: pickup_year and pickup_month are PartitionKeys — not listed here.
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


def register_parquet_table(
    glue_database: str,
    table_name: str,
    s3_location: str,
    partition_keys: list[str],
    region: str,
) -> None:
    """
    Register a raw Parquet table in Glue (used for bronze zone).

    Athena can query Parquet natively via ParquetHiveSerDe.
    Schema inference is left to Athena — columns are empty here so
    Glue doesn't need to be re-registered when the source schema changes.
    """
    glue = boto3.client("glue", region_name=region)
    ensure_glue_database(glue, glue_database, s3_location)

    partition_col_defs = [{"Name": pk, "Type": "string"} for pk in partition_keys]

    table_input = {
        "Name": table_name,
        "Description": f"Bronze raw Parquet — {table_name} taxi trips",
        "TableType": "EXTERNAL_TABLE",
        "Parameters": {
            "classification":               "parquet",
            "parquet.compression":          "SNAPPY",
            "EXTERNAL":                     "TRUE",
        },
        "StorageDescriptor": {
            "Location":     s3_location,
            "InputFormat":  "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
            "SerdeInfo": {
                "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                "Parameters": {"serialization.format": "1"},
            },
            # Native Parquet tables CAN infer schema at query time, so empty is fine here.
            "Columns": [],
        },
        "PartitionKeys": partition_col_defs,
    }

    _upsert_glue_table(glue, glue_database, table_input)


def register_delta_table(
    glue_database: str,
    table_name: str,
    s3_location: str,
    partition_keys: list[str],
    columns: list[dict],        # NEW — explicit schema required for SymlinkTextInputFormat
    region: str,
) -> None:
    """
    Register a Delta Lake table in Glue so Athena can query it via
    the SymlinkTextInputFormat + symlink_format_manifest approach.

    Why symlink manifest instead of native Delta catalog entry?
    ────────────────────────────────────────────────────────────
    Athena does not natively understand the Delta transaction log
    (_delta_log/).  The supported path is:
      1. Delta writes data as Parquet files under the table root.
      2. Call `GENERATE symlink_format_manifest` on the Delta table.
      3. Delta writes a manifest listing current Parquet files to
             <table_root>/_symlink_format_manifest/manifest
      4. Glue table points at the manifest via SymlinkTextInputFormat.
      5. Athena reads only the files listed in the manifest.

    Why explicit Columns are now required
    ──────────────────────────────────────
    Native Parquet tables in Glue can infer their schema from Parquet
    file metadata at query time.  SymlinkTextInputFormat tables CANNOT —
    Athena reads the manifest to get file paths, then uses the Glue schema
    to decode those files.  If Columns=[], Athena has no column definitions
    and raises "Column '<name>' cannot be resolved" for every column.

    Reference:
    https://docs.delta.io/latest/presto-integration.html
    https://docs.aws.amazon.com/athena/latest/ug/delta-lake-tables.html
    """
    glue = boto3.client("glue", region_name=region)
    ensure_glue_database(glue, glue_database, s3_location)

    partition_col_defs = [{"Name": pk, "Type": "string"} for pk in partition_keys]

    # The manifest root — Delta writes per-partition manifests under this prefix.
    # Each partition appears as:
    #   <manifest_root>/pickup_year=YYYY/pickup_month=MM/manifest
    manifest_location = f"{s3_location}/_symlink_format_manifest"

    table_input = {
        "Name": table_name,
        "Description": f"Delta Lake table (silver) — {table_name} taxi trips",
        "TableType": "EXTERNAL_TABLE",
        "Parameters": {
            "classification":               "parquet",
            "EXTERNAL":                     "TRUE",
            "table_type":                   "DELTA",
        },
        "StorageDescriptor": {
            # Point at the manifest root, not the table root.
            "Location":     manifest_location,
            "InputFormat":  "org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
            "SerdeInfo": {
                "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                "Parameters": {"serialization.format": "1"},
            },
            # FIX: explicit column definitions — required for SymlinkTextInputFormat.
            # Without this, Athena cannot resolve any column name.
            "Columns": columns,
        },
        "PartitionKeys": partition_col_defs,
    }

    _upsert_glue_table(glue, glue_database, table_input)


def _add_silver_partitions(
    glue_database: str,
    table_name: str,
    s3_table_root: str,
    year: str,
    month: str,
    columns: list[dict],
    region: str,
) -> None:
    """
    Register the (pickup_year, pickup_month) partition in the Glue catalog
    for the silver SymlinkTextInputFormat table.

    Why this is needed
    ──────────────────
    Athena resolves partition data by looking up the partition Location in
    the Glue catalog.  For a SymlinkTextInputFormat table, each partition
    Location must point at the sub-directory of the manifest where Delta
    wrote the per-partition manifest file:

        <table_root>/_symlink_format_manifest/pickup_year=YYYY/pickup_month=M/

    Delta generates this structure automatically when
    `delta.compatibility.symlinkFormatManifest.enabled = true`.

    Without partition entries in Glue, Athena returns:
        "No path property defined for table: <database>.<table>"
    even though the manifest files exist on S3.
    """
    glue = boto3.client("glue", region_name=region)

    # Delta writes manifests with integer month values (no leading zero).
    month_int = str(int(month))

    # Manifest sub-directory for this specific partition.
    # Delta uses Hive-style partition paths: key=value/key=value/
    partition_manifest_location = (
        f"{s3_table_root}/_symlink_format_manifest"
        f"/pickup_year={year}/pickup_month={month_int}/"
    )

    partition_input = {
        # Partition key values must match the PartitionKeys order: [pickup_year, pickup_month]
        "Values": [year, month_int],
        "StorageDescriptor": {
            "Location":     partition_manifest_location,
            "InputFormat":  "org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
            "SerdeInfo": {
                "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                "Parameters": {"serialization.format": "1"},
            },
            # Each partition SD also needs explicit columns so Athena can
            # decode the files listed in that partition's manifest.
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
            "[glue] Added silver partition pickup_year=%s/pickup_month=%s to %s.%s → %s",
            year, month_int, glue_database, table_name, partition_manifest_location,
        )
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "AlreadyExistsException":
            # Partition exists — update its location in case the manifest path changed.
            glue.update_partition(
                DatabaseName=glue_database,
                TableName=table_name,
                PartitionValueList=[year, month_int],
                PartitionInput=partition_input,
            )
            log.info(
                "[glue] Updated silver partition pickup_year=%s/pickup_month=%s in %s.%s",
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
    Build SparkSession for EMR Serverless.

    Delta JARs must be attached via spark-submit:
      --conf spark.jars.packages=io.delta:delta-spark_2.12:3.2.0

    The `delta.compatibility.symlinkFormatManifest.enabled` property tells
    Delta to auto-generate the symlink manifest after every write, so Athena
    always sees a consistent view without a manual GENERATE call.
    """
    spark = (
        SparkSession.builder
        .appName(app_name)

        # Delta Lake extensions
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")

        # Auto-generate symlink manifest so Athena always has a fresh view
        .config("spark.databricks.delta.properties.defaults"
                ".compatibility.symlinkFormatManifest.enabled", "true")

        # AWS Glue Catalog as Hive metastore
        .config("spark.hadoop.hive.metastore.client.factory.class",
                "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")

        # Performance
        .config("spark.sql.shuffle.partitions", "50")
        .config("spark.sql.adaptive.enabled", "true")

        # S3 optimisation
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        .config("spark.hadoop.fs.s3a.multipart.size", "67108864")

        .enableHiveSupport()
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ─────────────────────────────────────────────────────────────────────────────
# Schema enforcement
# ─────────────────────────────────────────────────────────────────────────────

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
    "airport_fee":           "double",
    # FIX (2026-05-22): new TLC surcharge column present from 2025 data onward.
    "cbd_congestion_fee":    "double",
    "ehail_fee":             "double",
    "trip_type":             "long",
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
                ),
            )
    return df


# ─────────────────────────────────────────────────────────────────────────────
# Bronze registration
# ─────────────────────────────────────────────────────────────────────────────

def register_bronze(
    bucket: str,
    vehicle_type: str,
    year: str,
    month: str,
    glue_database: str,
    aws_region: str,
) -> None:
    """
    Register the bronze Parquet files in Glue.
    """
    table_root = f"s3://{bucket}/bronze/{vehicle_type}"
    year_month = f"{year}-{month}"

    log.info("[bronze] Registering Glue table %s.%s → %s",
             glue_database, vehicle_type, table_root)

    register_parquet_table(
        glue_database=glue_database,
        table_name=vehicle_type,
        s3_location=table_root,
        partition_keys=["year_month"],
        region=aws_region,
    )

    _add_glue_partition(
        database=glue_database,
        table=vehicle_type,
        partition_values=[year_month],
        s3_location=f"{table_root}/{year_month}/",
        region=aws_region,
    )


def _add_glue_partition(
    database: str,
    table: str,
    partition_values: list[str],
    s3_location: str,
    region: str,
) -> None:
    """Add a single partition to a Glue table (idempotent)."""
    glue = boto3.client("glue", region_name=region)
    try:
        glue.create_partition(
            DatabaseName=database,
            TableName=table,
            PartitionInput={
                "Values": partition_values,
                "StorageDescriptor": {
                    "Location": s3_location,
                    "InputFormat":  "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                    "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                    "SerdeInfo": {
                        "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                        "Parameters": {"serialization.format": "1"},
                    },
                },
            },
        )
        log.info("[glue] Added partition %s to %s.%s", partition_values, database, table)
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "AlreadyExistsException":
            log.info("[glue] Partition %s already exists in %s.%s — skipping",
                     partition_values, database, table)
        else:
            raise


# ─────────────────────────────────────────────────────────────────────────────
# Cleansing logic
# ─────────────────────────────────────────────────────────────────────────────

def cleanse_vehicle(
    spark: SparkSession,
    bucket: str,
    vehicle_type: str,
    year: str,
    month: str,
    bronze_database: str,
    silver_database: str,
    aws_region: str,
) -> int:

    input_path  = f"s3://{bucket}/bronze/{vehicle_type}/{year}-{month}/"
    output_path = f"s3://{bucket}/silver/{vehicle_type}"

    log.info("[%s] Reading bronze from %s", vehicle_type, input_path)
    df = spark.read.parquet(input_path)
    raw_count = df.count()
    log.info("[%s] Raw rows: %d", vehicle_type, raw_count)

    # ── Normalise timestamp column names ─────────────────────────────────────
    if vehicle_type == "yellow":
        df = (df
              .withColumnRenamed("tpep_pickup_datetime",  "pickup_datetime")
              .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime"))
    elif vehicle_type == "green":
        df = (df
              .withColumnRenamed("lpep_pickup_datetime",  "pickup_datetime")
              .withColumnRenamed("lpep_dropoff_datetime", "dropoff_datetime"))

    # ── Normalise column names that change across TLC data vintages ──────────
    # FIX (2026-05-22): TLC started emitting Airport_fee (capital A) in 2025
    # yellow Parquet files.  Rename it before enforce_stable_types() runs so
    # the cast logic finds the column under its canonical lowercase name, and
    # the silver schema stays consistent across all vintages.
    if "Airport_fee" in df.columns and "airport_fee" not in df.columns:
        df = df.withColumnRenamed("Airport_fee", "airport_fee")
        log.info("[%s] Renamed Airport_fee → airport_fee", vehicle_type)

    df = enforce_stable_types(df)

    # ── Quality filters ───────────────────────────────────────────────────────
    df = df.filter(
        F.col("pickup_datetime").isNotNull()
        & F.col("dropoff_datetime").isNotNull()
        & F.col("PULocationID").isNotNull()
        & F.col("DOLocationID").isNotNull()
        & (F.col("trip_distance") > 0)
        & (F.col("trip_distance") < 200)
        & (F.col("total_amount")  > 0)
        & (F.col("total_amount")  < 5000)
        & (F.col("passenger_count") >= 1)
        & (F.col("passenger_count") <= 6)
    )

    # ── Derived columns ───────────────────────────────────────────────────────
    df = (
        df
        .withColumn("trip_duration_min",
            (F.unix_timestamp("dropoff_datetime") - F.unix_timestamp("pickup_datetime")) / 60.0)
        .withColumn("speed_mph",
            F.when(F.col("trip_duration_min") > 0,
                   F.col("trip_distance") / (F.col("trip_duration_min") / 60.0)))
        .withColumn("pickup_date",  F.to_date("pickup_datetime"))
        .withColumn("pickup_year",  F.year("pickup_datetime"))
        .withColumn("pickup_month", F.month("pickup_datetime"))
        .withColumn("pickup_hour",  F.hour("pickup_datetime"))
        .withColumn("pickup_dow",   F.dayofweek("pickup_datetime"))
        .withColumn("vehicle_type", F.lit(vehicle_type))
        .withColumn("trip_id",
            F.sha2(F.concat_ws("|",
                F.col("VendorID").cast("string"),
                F.col("pickup_datetime").cast("string"),
                F.col("PULocationID").cast("string"),
                F.col("DOLocationID").cast("string"),
            ), 256))
    )

    # ── Sanity filters ────────────────────────────────────────────────────────
    df = df.filter(
        (F.col("trip_duration_min") > 1)
        & (F.col("trip_duration_min") < 300)
        & (F.col("speed_mph").isNull() | (F.col("speed_mph") < 120))
    )

    df = df.dropDuplicates(["trip_id"])
    df = df.filter(
        (F.col("pickup_year")  == int(year))
        & (F.col("pickup_month") == int(month))
    )
    df = df.persist(StorageLevel.MEMORY_AND_DISK)

    clean_count = df.count()
    dropped  = raw_count - clean_count
    drop_pct = round(dropped / raw_count * 100, 1) if raw_count > 0 else 0
    log.info("[%s] Clean rows: %d (dropped %d = %s%%)",
             vehicle_type, clean_count, dropped, drop_pct)

    # ── Delta write ───────────────────────────────────────────────────────────
    # FIX (2026-05-22): use mergeSchema=true so that the first run carrying
    # the new cbd_congestion_fee column extends the Delta table schema
    # automatically.  Older partitions gain a NULL column; newer partitions
    # carry the real values.  This is safe to leave on permanently — Delta
    # only extends the schema, it never drops or renames columns.
    #
    # Note: overwriteSchema=true is intentionally NOT set here.  Delta rejects
    # the combination of replaceWhere + overwriteSchema=true, and we don't need
    # it — mergeSchema handles additive changes without touching existing data.
    replace_condition = f"pickup_year = {year} AND pickup_month = {int(month)}"

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("replaceWhere", replace_condition)
        .option("mergeSchema", "true")
        .option("delta.compatibility.symlinkFormatManifest.enabled", "true")
        .partitionBy("pickup_year", "pickup_month")
        .save(output_path)
    )
    log.info("[%s] Written to %s (%d rows)", vehicle_type, output_path, clean_count)

    # ── Explicitly generate symlink manifest ─────────────────────────────────
    spark.sql(f"""
        GENERATE symlink_format_manifest
        FOR TABLE delta.`{output_path}`
    """)
    log.info("[%s] Generated symlink_format_manifest at %s/_symlink_format_manifest/",
             vehicle_type, output_path)

    df.unpersist()

    # ── Register bronze in Glue ───────────────────────────────────────────────
    register_bronze(
        bucket=bucket,
        vehicle_type=vehicle_type,
        year=year,
        month=month,
        glue_database=bronze_database,
        aws_region=aws_region,
    )

    # ── Register silver (Delta) in Glue ───────────────────────────────────────
    # FIX: pass explicit SILVER_COLUMNS so Athena's SymlinkTextInputFormat can
    # resolve column names.  Previously Columns=[] caused the
    # "Column 'pickup_datetime' cannot be resolved" error.
    register_delta_table(
        glue_database=silver_database,
        table_name=vehicle_type,
        s3_location=output_path,
        partition_keys=["pickup_year", "pickup_month"],
        columns=SILVER_COLUMNS,          # ← NEW
        region=aws_region,
    )

    # ── Register partition in Glue ────────────────────────────────────────────
    # FIX: add the per-partition manifest location so Athena can find data
    # for this (pickup_year, pickup_month) pair.  Previously absent, which
    # caused "No path property defined for table: nyc_tlc_silver.<table>".
    _add_silver_partitions(
        glue_database=silver_database,
        table_name=vehicle_type,
        s3_table_root=output_path,
        year=year,
        month=month,
        columns=SILVER_COLUMNS,          # ← NEW
        region=aws_region,
    )

    return clean_count


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Job 1: Bronze → Silver cleanse")
    parser.add_argument("--year",             required=True,  help="e.g. 2023")
    parser.add_argument("--month",            required=True,  help="e.g. 03")
    parser.add_argument("--bucket",           required=True,  help="S3 bucket name")
    parser.add_argument("--bronze-database",  default="nyc_tlc_bronze")
    parser.add_argument("--silver-database",  default="nyc_tlc_silver")
    parser.add_argument("--aws-region",       default="ap-southeast-1")
    args = parser.parse_args()

    log.info("Starting cleanse job for %s-%s", args.year, args.month)

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
                bronze_database=args.bronze_database,
                silver_database=args.silver_database,
                aws_region=args.aws_region,
            )
            total_rows += rows

        log.info("Job completed. Total rows written: %d", total_rows)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()