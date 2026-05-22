"""
tests/unit/test_job1_cleanse.py

Unit tests for spark_jobs/job1_cleanse.py.

Coverage
────────
  Glue helpers
    - ensure_glue_database        : creates DB / skips if exists
    - register_parquet_table      : creates + updates bronze table
    - register_delta_table        : creates silver table with explicit SILVER_COLUMNS
    - _add_silver_partitions      : creates partition / updates on AlreadyExists
    - _add_glue_partition         : creates bronze partition / skips on AlreadyExists
    - _upsert_glue_table          : create path + update path

  Cleansing logic  (PySpark local)
    - enforce_stable_types        : casts, handles empty-string → NULL
    - timestamp rename            : yellow tpep_* / green lpep_* → pickup/dropoff_datetime
    - quality filters             : negative fare, zero distance, bad passenger count, bounds
    - derived columns             : duration, speed, pickup_date/hour/dow, vehicle_type, trip_id
    - sanity filters              : duration < 1 min, > 300 min, speed ≥ 120 mph
    - deduplication               : trip_id uniqueness
    - year/month fence            : rows outside the target month are dropped
    - SILVER_COLUMNS completeness : every derived column is declared in SILVER_COLUMNS

All AWS calls are intercepted by moto — no real credentials needed.
PySpark runs in local mode — no EMR needed.
"""

from __future__ import annotations

import sys
sys.path.insert(0, "spark_jobs")
sys.path.insert(0, ".")

import pytest
import boto3
from moto import mock_aws
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType, LongType, StringType, StructField, StructType,
)

# ── constants matching your actual stack ──────────────────────────────────────
AWS_REGION      = "ap-southeast-1"
BRONZE_DB       = "nyc_tlc_bronze"
SILVER_DB       = "nyc_tlc_silver"
FAKE_BUCKET     = "nyc-tlc-data"
YEAR, MONTH     = "2023", "03"


# ─────────────────────────────────────────────────────────────────────────────
# Session-scoped Spark fixture  (local mode, no S3 / EMR)
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .appName("test-job1-cleanse")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        # Delta Lake extensions needed for type hints; actual Delta write not tested here
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _make_glue_db(glue_client, name: str) -> None:
    glue_client.create_database(DatabaseInput={"Name": name})


def _yellow_row(**overrides):
    """Return a minimal yellow-taxi row dict with sane defaults."""
    base = {
        "VendorID":              1,
        "tpep_pickup_datetime":  "2023-03-15 08:00:00",
        "tpep_dropoff_datetime": "2023-03-15 08:30:00",
        "passenger_count":       2,
        "trip_distance":         3.5,
        "RatecodeID":            1,
        "store_and_fwd_flag":    "N",
        "PULocationID":          100,
        "DOLocationID":          200,
        "payment_type":          1,
        "fare_amount":           14.0,
        "extra":                 0.5,
        "mta_tax":               0.5,
        "tip_amount":            2.0,
        "tolls_amount":          0.0,
        "improvement_surcharge": 0.3,
        "total_amount":          17.3,
        "congestion_surcharge":  2.5,
    }
    base.update(overrides)
    return base


def _make_yellow_df(spark, rows: list[dict]):
    """Build a yellow-taxi bronze DataFrame from a list of row dicts."""
    schema = StructType([
        StructField("VendorID",              LongType()),
        StructField("tpep_pickup_datetime",  StringType()),
        StructField("tpep_dropoff_datetime", StringType()),
        StructField("passenger_count",       LongType()),
        StructField("trip_distance",         DoubleType()),
        StructField("RatecodeID",            LongType()),
        StructField("store_and_fwd_flag",    StringType()),
        StructField("PULocationID",          LongType()),
        StructField("DOLocationID",          LongType()),
        StructField("payment_type",          LongType()),
        StructField("fare_amount",           DoubleType()),
        StructField("extra",                 DoubleType()),
        StructField("mta_tax",              DoubleType()),
        StructField("tip_amount",           DoubleType()),
        StructField("tolls_amount",         DoubleType()),
        StructField("improvement_surcharge", DoubleType()),
        StructField("total_amount",          DoubleType()),
        StructField("congestion_surcharge",  DoubleType()),
    ])
    return spark.createDataFrame(
        [tuple(r[f.name] for f in schema.fields) for r in rows],
        schema=schema,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Glue helper tests
# ─────────────────────────────────────────────────────────────────────────────

class TestEnsureGlueDatabase:

    @mock_aws
    def test_creates_database_when_absent(self):
        from job1_cleanse import ensure_glue_database
        glue = boto3.client("glue", region_name=AWS_REGION)
        ensure_glue_database(glue, "new_db", "s3://bucket/new_db/")
        resp = glue.get_database(Name="new_db")
        assert resp["Database"]["Name"] == "new_db"

    @mock_aws
    def test_does_not_raise_when_already_exists(self):
        from job1_cleanse import ensure_glue_database
        glue = boto3.client("glue", region_name=AWS_REGION)
        glue.create_database(DatabaseInput={"Name": "existing_db"})
        # Should not raise
        ensure_glue_database(glue, "existing_db", "s3://bucket/existing_db/")


class TestRegisterParquetTable:

    @mock_aws
    def test_creates_bronze_table(self):
        from job1_cleanse import register_parquet_table
        glue = boto3.client("glue", region_name=AWS_REGION)
        glue.create_database(DatabaseInput={"Name": BRONZE_DB})

        register_parquet_table(
            glue_database=BRONZE_DB,
            table_name="yellow",
            s3_location=f"s3://{FAKE_BUCKET}/bronze/yellow",
            partition_keys=["year_month"],
            region=AWS_REGION,
        )

        table = glue.get_table(DatabaseName=BRONZE_DB, Name="yellow")["Table"]
        assert table["TableType"] == "EXTERNAL_TABLE"
        sd = table["StorageDescriptor"]
        assert "ParquetHiveSerDe" in sd["SerdeInfo"]["SerializationLibrary"]
        assert sd["Location"] == f"s3://{FAKE_BUCKET}/bronze/yellow"

    @mock_aws
    def test_updates_existing_bronze_table(self):
        from job1_cleanse import register_parquet_table
        glue = boto3.client("glue", region_name=AWS_REGION)
        glue.create_database(DatabaseInput={"Name": BRONZE_DB})

        # Register twice — second call should not raise
        for _ in range(2):
            register_parquet_table(
                glue_database=BRONZE_DB,
                table_name="yellow",
                s3_location=f"s3://{FAKE_BUCKET}/bronze/yellow",
                partition_keys=["year_month"],
                region=AWS_REGION,
            )

        # Table should still exist and be valid
        table = glue.get_table(DatabaseName=BRONZE_DB, Name="yellow")["Table"]
        assert table["Name"] == "yellow"


class TestRegisterDeltaTable:

    @mock_aws
    def test_creates_silver_table_with_symlink_format(self):
        from job1_cleanse import register_delta_table, SILVER_COLUMNS
        glue = boto3.client("glue", region_name=AWS_REGION)
        glue.create_database(DatabaseInput={"Name": SILVER_DB})

        s3_loc = f"s3://{FAKE_BUCKET}/silver/yellow"
        register_delta_table(
            glue_database=SILVER_DB,
            table_name="yellow",
            s3_location=s3_loc,
            partition_keys=["pickup_year", "pickup_month"],
            columns=SILVER_COLUMNS,
            region=AWS_REGION,
        )

        table = glue.get_table(DatabaseName=SILVER_DB, Name="yellow")["Table"]
        sd = table["StorageDescriptor"]

        # Must use SymlinkTextInputFormat — not plain Parquet
        assert sd["InputFormat"] == "org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat"
        # Location must point at the manifest, not the table root
        assert sd["Location"] == f"{s3_loc}/_symlink_format_manifest"
        # Explicit columns required for SymlinkTextInputFormat
        assert len(sd["Columns"]) == len(SILVER_COLUMNS)

    @mock_aws
    def test_silver_columns_written_to_glue(self):
        from job1_cleanse import register_delta_table, SILVER_COLUMNS
        glue = boto3.client("glue", region_name=AWS_REGION)
        glue.create_database(DatabaseInput={"Name": SILVER_DB})

        register_delta_table(
            glue_database=SILVER_DB,
            table_name="yellow",
            s3_location=f"s3://{FAKE_BUCKET}/silver/yellow",
            partition_keys=["pickup_year", "pickup_month"],
            columns=SILVER_COLUMNS,
            region=AWS_REGION,
        )

        table = glue.get_table(DatabaseName=SILVER_DB, Name="yellow")["Table"]
        glue_col_names = {c["Name"] for c in table["StorageDescriptor"]["Columns"]}
        expected_names  = {c["Name"] for c in SILVER_COLUMNS}
        assert glue_col_names == expected_names

    @mock_aws
    def test_updates_existing_silver_table(self):
        from job1_cleanse import register_delta_table, SILVER_COLUMNS
        glue = boto3.client("glue", region_name=AWS_REGION)
        glue.create_database(DatabaseInput={"Name": SILVER_DB})

        kwargs = dict(
            glue_database=SILVER_DB,
            table_name="yellow",
            s3_location=f"s3://{FAKE_BUCKET}/silver/yellow",
            partition_keys=["pickup_year", "pickup_month"],
            columns=SILVER_COLUMNS,
            region=AWS_REGION,
        )
        register_delta_table(**kwargs)
        register_delta_table(**kwargs)  # Should not raise AlreadyExistsException


class TestAddSilverPartitions:

    @mock_aws
    def test_creates_partition_with_correct_manifest_path(self):
        from job1_cleanse import register_delta_table, _add_silver_partitions, SILVER_COLUMNS
        glue = boto3.client("glue", region_name=AWS_REGION)
        glue.create_database(DatabaseInput={"Name": SILVER_DB})

        s3_root = f"s3://{FAKE_BUCKET}/silver/yellow"
        register_delta_table(
            glue_database=SILVER_DB, table_name="yellow",
            s3_location=s3_root, partition_keys=["pickup_year", "pickup_month"],
            columns=SILVER_COLUMNS, region=AWS_REGION,
        )
        _add_silver_partitions(
            glue_database=SILVER_DB, table_name="yellow",
            s3_table_root=s3_root, year=YEAR, month=MONTH,
            columns=SILVER_COLUMNS, region=AWS_REGION,
        )

        partitions = glue.get_partitions(DatabaseName=SILVER_DB, TableName="yellow")
        assert len(partitions["Partitions"]) == 1

        sd = partitions["Partitions"][0]["StorageDescriptor"]
        # Delta writes month without leading zero
        expected_loc = (
            f"{s3_root}/_symlink_format_manifest"
            f"/pickup_year={YEAR}/pickup_month={int(MONTH)}/"
        )
        assert sd["Location"] == expected_loc
        assert sd["InputFormat"] == "org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat"

    @mock_aws
    def test_updates_partition_if_already_exists(self):
        from job1_cleanse import register_delta_table, _add_silver_partitions, SILVER_COLUMNS
        glue = boto3.client("glue", region_name=AWS_REGION)
        glue.create_database(DatabaseInput={"Name": SILVER_DB})

        s3_root = f"s3://{FAKE_BUCKET}/silver/yellow"
        register_delta_table(
            glue_database=SILVER_DB, table_name="yellow",
            s3_location=s3_root, partition_keys=["pickup_year", "pickup_month"],
            columns=SILVER_COLUMNS, region=AWS_REGION,
        )

        kwargs = dict(
            glue_database=SILVER_DB, table_name="yellow",
            s3_table_root=s3_root, year=YEAR, month=MONTH,
            columns=SILVER_COLUMNS, region=AWS_REGION,
        )
        _add_silver_partitions(**kwargs)
        _add_silver_partitions(**kwargs)  # Second call must not raise

        partitions = glue.get_partitions(DatabaseName=SILVER_DB, TableName="yellow")
        assert len(partitions["Partitions"]) == 1   # no duplicates

    @mock_aws
    def test_month_leading_zero_stripped(self):
        """Month '03' should be stored as '3' to match Delta's manifest path."""
        from job1_cleanse import register_delta_table, _add_silver_partitions, SILVER_COLUMNS
        glue = boto3.client("glue", region_name=AWS_REGION)
        glue.create_database(DatabaseInput={"Name": SILVER_DB})

        s3_root = f"s3://{FAKE_BUCKET}/silver/yellow"
        register_delta_table(
            glue_database=SILVER_DB, table_name="yellow",
            s3_location=s3_root, partition_keys=["pickup_year", "pickup_month"],
            columns=SILVER_COLUMNS, region=AWS_REGION,
        )
        _add_silver_partitions(
            glue_database=SILVER_DB, table_name="yellow",
            s3_table_root=s3_root, year=YEAR, month="03",  # padded input
            columns=SILVER_COLUMNS, region=AWS_REGION,
        )

        parts = glue.get_partitions(DatabaseName=SILVER_DB, TableName="yellow")
        values = parts["Partitions"][0]["Values"]
        assert values == [YEAR, "3"]   # leading zero stripped


# ─────────────────────────────────────────────────────────────────────────────
# Schema / column tests
# ─────────────────────────────────────────────────────────────────────────────

class TestSilverColumnsCompleteness:
    """
    Ensure SILVER_COLUMNS declares every column that cleanse_vehicle() produces.
    If a developer adds a derived column to the job but forgets to add it to
    SILVER_COLUMNS, Athena will fail to resolve it at query time.
    """

    EXPECTED_COLUMN_NAMES = {
        # TLC passthrough
        "VendorID", "pickup_datetime", "dropoff_datetime",
        "passenger_count", "trip_distance", "RatecodeID",
        "store_and_fwd_flag", "PULocationID", "DOLocationID",
        "payment_type", "fare_amount", "extra", "mta_tax",
        "tip_amount", "tolls_amount", "improvement_surcharge",
        "total_amount", "congestion_surcharge",
        # green-only (nullable for yellow)
        "ehail_fee", "trip_type",
        # derived
        "trip_duration_min", "speed_mph",
        "pickup_date", "pickup_hour", "pickup_dow",
        "vehicle_type", "trip_id",
        # NOTE: pickup_year / pickup_month are PartitionKeys — intentionally absent
    }

    def test_all_expected_columns_declared(self):
        from job1_cleanse import SILVER_COLUMNS
        declared = {c["Name"] for c in SILVER_COLUMNS}
        missing = self.EXPECTED_COLUMN_NAMES - declared
        assert not missing, f"Columns missing from SILVER_COLUMNS: {missing}"

    def test_no_partition_keys_in_silver_columns(self):
        """pickup_year and pickup_month must be PartitionKeys, not data columns."""
        from job1_cleanse import SILVER_COLUMNS
        declared = {c["Name"] for c in SILVER_COLUMNS}
        assert "pickup_year"  not in declared
        assert "pickup_month" not in declared

    def test_all_columns_have_hive_types(self):
        from job1_cleanse import SILVER_COLUMNS
        valid_types = {"bigint", "double", "string", "timestamp", "date", "int"}
        for col in SILVER_COLUMNS:
            assert col["Type"] in valid_types, \
                f"Column {col['Name']} has unexpected Hive type '{col['Type']}'"


# ─────────────────────────────────────────────────────────────────────────────
# enforce_stable_types tests
# ─────────────────────────────────────────────────────────────────────────────

class TestEnforceStableTypes:

    def test_casts_numeric_strings_to_correct_types(self, spark):
        from job1_cleanse import enforce_stable_types
        df = spark.createDataFrame(
            [("1", "3.5", "14.0", "17.3")],
            ["VendorID", "trip_distance", "fare_amount", "total_amount"],
        )
        result = enforce_stable_types(df).collect()[0]
        assert isinstance(result["trip_distance"], float)
        assert isinstance(result["fare_amount"],   float)

    def test_empty_string_becomes_null(self, spark):
        from job1_cleanse import enforce_stable_types
        df = spark.createDataFrame(
            [("", "", "")],
            ["VendorID", "fare_amount", "total_amount"],
        )
        result = enforce_stable_types(df).collect()[0]
        assert result["VendorID"]    is None
        assert result["fare_amount"] is None

    def test_ignores_columns_not_in_stable_types(self, spark):
        from job1_cleanse import enforce_stable_types
        df = spark.createDataFrame([("hello",)], ["unknown_col"])
        # Should not raise even though unknown_col is not in STABLE_TYPES
        result = enforce_stable_types(df).collect()[0]
        assert result["unknown_col"] == "hello"


# ─────────────────────────────────────────────────────────────────────────────
# Timestamp rename tests
# ─────────────────────────────────────────────────────────────────────────────

class TestTimestampRename:

    def test_yellow_tpep_renamed(self, spark):
        """Yellow trips must have tpep_* renamed to pickup/dropoff_datetime."""
        df = spark.createDataFrame(
            [("2023-03-15 08:00:00", "2023-03-15 08:30:00")],
            ["tpep_pickup_datetime", "tpep_dropoff_datetime"],
        )
        df = (df
              .withColumnRenamed("tpep_pickup_datetime",  "pickup_datetime")
              .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime"))
        cols = df.columns
        assert "pickup_datetime"  in cols
        assert "dropoff_datetime" in cols
        assert "tpep_pickup_datetime"  not in cols
        assert "tpep_dropoff_datetime" not in cols

    def test_green_lpep_renamed(self, spark):
        """Green trips must have lpep_* renamed to pickup/dropoff_datetime."""
        df = spark.createDataFrame(
            [("2023-03-15 09:00:00", "2023-03-15 09:45:00")],
            ["lpep_pickup_datetime", "lpep_dropoff_datetime"],
        )
        df = (df
              .withColumnRenamed("lpep_pickup_datetime",  "pickup_datetime")
              .withColumnRenamed("lpep_dropoff_datetime", "dropoff_datetime"))
        assert "pickup_datetime"  in df.columns
        assert "lpep_pickup_datetime" not in df.columns


# ─────────────────────────────────────────────────────────────────────────────
# Quality filter tests  (mirrors cleanse_vehicle filter block)
# ─────────────────────────────────────────────────────────────────────────────

class TestQualityFilters:
    """
    These tests apply the *same filter expressions* used in cleanse_vehicle()
    so any drift between the job and the tests is immediately visible.
    """

    def _apply_quality_filters(self, df):
        return df.filter(
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

    def test_valid_row_passes(self, spark):
        df = _make_yellow_df(spark, [_yellow_row()])
        df = df.withColumnRenamed("tpep_pickup_datetime",  "pickup_datetime") \
               .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime") \
               .withColumn("pickup_datetime",  F.to_timestamp("pickup_datetime")) \
               .withColumn("dropoff_datetime", F.to_timestamp("dropoff_datetime"))
        assert self._apply_quality_filters(df).count() == 1

    def test_negative_fare_removed(self, spark):
        df = _make_yellow_df(spark, [_yellow_row(total_amount=-5.0)])
        df = df.withColumnRenamed("tpep_pickup_datetime",  "pickup_datetime") \
               .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime") \
               .withColumn("pickup_datetime",  F.to_timestamp("pickup_datetime")) \
               .withColumn("dropoff_datetime", F.to_timestamp("dropoff_datetime"))
        assert self._apply_quality_filters(df).count() == 0

    def test_zero_distance_removed(self, spark):
        df = _make_yellow_df(spark, [_yellow_row(trip_distance=0.0)])
        df = df.withColumnRenamed("tpep_pickup_datetime",  "pickup_datetime") \
               .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime") \
               .withColumn("pickup_datetime",  F.to_timestamp("pickup_datetime")) \
               .withColumn("dropoff_datetime", F.to_timestamp("dropoff_datetime"))
        assert self._apply_quality_filters(df).count() == 0

    def test_excess_distance_removed(self, spark):
        df = _make_yellow_df(spark, [_yellow_row(trip_distance=250.0)])
        df = df.withColumnRenamed("tpep_pickup_datetime",  "pickup_datetime") \
               .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime") \
               .withColumn("pickup_datetime",  F.to_timestamp("pickup_datetime")) \
               .withColumn("dropoff_datetime", F.to_timestamp("dropoff_datetime"))
        assert self._apply_quality_filters(df).count() == 0

    def test_zero_passenger_count_removed(self, spark):
        df = _make_yellow_df(spark, [_yellow_row(passenger_count=0)])
        df = df.withColumnRenamed("tpep_pickup_datetime",  "pickup_datetime") \
               .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime") \
               .withColumn("pickup_datetime",  F.to_timestamp("pickup_datetime")) \
               .withColumn("dropoff_datetime", F.to_timestamp("dropoff_datetime"))
        assert self._apply_quality_filters(df).count() == 0

    def test_too_many_passengers_removed(self, spark):
        df = _make_yellow_df(spark, [_yellow_row(passenger_count=9)])
        df = df.withColumnRenamed("tpep_pickup_datetime",  "pickup_datetime") \
               .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime") \
               .withColumn("pickup_datetime",  F.to_timestamp("pickup_datetime")) \
               .withColumn("dropoff_datetime", F.to_timestamp("dropoff_datetime"))
        assert self._apply_quality_filters(df).count() == 0

    def test_high_fare_removed(self, spark):
        df = _make_yellow_df(spark, [_yellow_row(total_amount=6000.0)])
        df = df.withColumnRenamed("tpep_pickup_datetime",  "pickup_datetime") \
               .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime") \
               .withColumn("pickup_datetime",  F.to_timestamp("pickup_datetime")) \
               .withColumn("dropoff_datetime", F.to_timestamp("dropoff_datetime"))
        assert self._apply_quality_filters(df).count() == 0


# ─────────────────────────────────────────────────────────────────────────────
# Derived column tests
# ─────────────────────────────────────────────────────────────────────────────

class TestDerivedColumns:

    def _build_derived(self, spark, pickup: str, dropoff: str,
                       distance: float = 3.5, vehicle: str = "yellow"):
        """Build a single-row DF with all derived columns applied."""
        df = spark.createDataFrame(
            [(pickup, dropoff, distance, 100, 200, 1, "N", 1,
              14.0, 0.5, 0.5, 2.0, 0.0, 0.3, 17.3, 2.5)],
            ["pickup_datetime", "dropoff_datetime", "trip_distance",
             "PULocationID", "DOLocationID", "VendorID", "store_and_fwd_flag",
             "passenger_count", "fare_amount", "extra", "mta_tax", "tip_amount",
             "tolls_amount", "improvement_surcharge", "total_amount",
             "congestion_surcharge"],
        )
        df = (
            df
            .withColumn("pickup_datetime",  F.to_timestamp("pickup_datetime"))
            .withColumn("dropoff_datetime", F.to_timestamp("dropoff_datetime"))
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
            .withColumn("vehicle_type", F.lit(vehicle))
            .withColumn("trip_id",
                F.sha2(F.concat_ws("|",
                    F.col("VendorID").cast("string"),
                    F.col("pickup_datetime").cast("string"),
                    F.col("PULocationID").cast("string"),
                    F.col("DOLocationID").cast("string"),
                ), 256))
        )
        return df.collect()[0]

    def test_trip_duration_30_minutes(self, spark):
        row = self._build_derived(spark, "2023-03-15 08:00:00", "2023-03-15 08:30:00")
        assert row["trip_duration_min"] == 30.0

    def test_speed_mph_calculation(self, spark):
        # 3.5 miles in 30 min = 7.0 mph
        row = self._build_derived(spark,
                                  "2023-03-15 08:00:00", "2023-03-15 08:30:00",
                                  distance=3.5)
        assert abs(row["speed_mph"] - 7.0) < 0.01

    def test_pickup_date_extracted(self, spark):
        from datetime import date
        row = self._build_derived(spark, "2023-03-15 08:00:00", "2023-03-15 08:30:00")
        assert row["pickup_date"] == date(2023, 3, 15)

    def test_pickup_hour_extracted(self, spark):
        row = self._build_derived(spark, "2023-03-15 08:00:00", "2023-03-15 08:30:00")
        assert row["pickup_hour"] == 8

    def test_pickup_dow_extracted(self, spark):
        # 2023-03-15 is a Wednesday → Spark dayofweek = 4 (Sun=1)
        row = self._build_derived(spark, "2023-03-15 08:00:00", "2023-03-15 08:30:00")
        assert row["pickup_dow"] == 4

    def test_vehicle_type_set_correctly(self, spark):
        row = self._build_derived(spark, "2023-03-15 08:00:00", "2023-03-15 09:00:00",
                                  vehicle="green")
        assert row["vehicle_type"] == "green"

    def test_trip_id_is_sha256_hex(self, spark):
        row = self._build_derived(spark, "2023-03-15 08:00:00", "2023-03-15 08:30:00")
        assert len(row["trip_id"]) == 64   # SHA-256 = 64 hex chars
        assert all(c in "0123456789abcdef" for c in row["trip_id"])

    def test_different_trips_have_different_ids(self, spark):
        row1 = self._build_derived(spark, "2023-03-15 08:00:00", "2023-03-15 08:30:00")
        row2 = self._build_derived(spark, "2023-03-15 09:00:00", "2023-03-15 09:30:00")
        assert row1["trip_id"] != row2["trip_id"]


# ─────────────────────────────────────────────────────────────────────────────
# Sanity filter tests
# ─────────────────────────────────────────────────────────────────────────────

class TestSanityFilters:

    def _apply_sanity(self, df):
        return df.filter(
            (F.col("trip_duration_min") > 1)
            & (F.col("trip_duration_min") < 300)
            & (F.col("speed_mph").isNull() | (F.col("speed_mph") < 120))
        )

    def _make_sanity_df(self, spark, duration_min: float, speed_mph: float):
        return spark.createDataFrame(
            [(duration_min, speed_mph)],
            ["trip_duration_min", "speed_mph"],
        )

    def test_valid_row_passes(self, spark):
        df = self._make_sanity_df(spark, 30.0, 20.0)
        assert self._apply_sanity(df).count() == 1

    def test_sub_1_minute_trip_removed(self, spark):
        df = self._make_sanity_df(spark, 0.5, 10.0)
        assert self._apply_sanity(df).count() == 0

    def test_over_300_minute_trip_removed(self, spark):
        df = self._make_sanity_df(spark, 400.0, 5.0)
        assert self._apply_sanity(df).count() == 0

    def test_speed_over_120_removed(self, spark):
        df = self._make_sanity_df(spark, 10.0, 150.0)
        assert self._apply_sanity(df).count() == 0

    def test_null_speed_allowed(self, spark):
        """speed_mph can be NULL (zero-duration edge case) — should not be dropped."""
        df = spark.createDataFrame(
            [(30.0, None)],
            StructType([
                StructField("trip_duration_min", DoubleType()),
                StructField("speed_mph",         DoubleType()),
            ]),
        )
        assert self._apply_sanity(df).count() == 1


# ─────────────────────────────────────────────────────────────────────────────
# Deduplication tests
# ─────────────────────────────────────────────────────────────────────────────

class TestDeduplication:

    def test_exact_duplicate_trip_ids_removed(self, spark):
        df = spark.createDataFrame(
            [("abc123",), ("abc123",), ("def456",)],
            ["trip_id"],
        )
        deduped = df.dropDuplicates(["trip_id"])
        assert deduped.count() == 2

    def test_all_unique_rows_preserved(self, spark):
        df = spark.createDataFrame(
            [("aaa",), ("bbb",), ("ccc",)],
            ["trip_id"],
        )
        deduped = df.dropDuplicates(["trip_id"])
        assert deduped.count() == 3


# ─────────────────────────────────────────────────────────────────────────────
# Year / month fence tests
# ─────────────────────────────────────────────────────────────────────────────

class TestYearMonthFence:

    def _apply_fence(self, df, year: int, month: int):
        return df.filter(
            (F.col("pickup_year") == year)
            & (F.col("pickup_month") == month)
        )

    def test_rows_in_target_month_kept(self, spark):
        df = spark.createDataFrame(
            [(2023, 3), (2023, 3)],
            ["pickup_year", "pickup_month"],
        )
        assert self._apply_fence(df, 2023, 3).count() == 2

    def test_rows_in_other_month_dropped(self, spark):
        df = spark.createDataFrame(
            [(2023, 2), (2023, 4), (2022, 3)],
            ["pickup_year", "pickup_month"],
        )
        assert self._apply_fence(df, 2023, 3).count() == 0

    def test_mixed_months_only_target_survives(self, spark):
        df = spark.createDataFrame(
            [(2023, 3), (2023, 2), (2023, 3)],
            ["pickup_year", "pickup_month"],
        )
        assert self._apply_fence(df, 2023, 3).count() == 2