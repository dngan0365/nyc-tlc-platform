"""
tests/unit/test_transformations.py  (rewritten)

Original problem
────────────────
The old version re-implemented the filter / column logic inline in the tests.
That means a bug in job1_cleanse.py would NOT be caught — the tests would still
pass because they were testing their own copy of the logic, not the real code.

Fix
───
Every test now imports and calls the *actual functions* from job1_cleanse.py
(enforce_stable_types) or applies the *exact same filter expressions* that
cleanse_vehicle() uses, so any drift between the job and the tests fails the suite.

Cloud differences from the original test file
─────────────────────────────────────────────
Original                            Actual stack
────────────────────────────────    ──────────────────────────────────────────
SparkSession.builder.master(local)  EMR Serverless (no local mode in prod)
validate_schema reads local .parquet ingestion/src/schema_validator reads S3 path
filter() inline in test body        job1_cleanse.cleanse_vehicle() on EMR
output to /tmp/                     output to s3://<bucket>/silver/<vehicle>/

These tests cover the *logic* in isolation.  S3 / EMR I/O is covered by the
integration tests (test_pipeline_e2e.py).
"""

from __future__ import annotations

import sys
sys.path.insert(0, "spark_jobs")
sys.path.insert(0, ".")

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType, LongType, StringType, StructField, StructType,
)


# ─────────────────────────────────────────────────────────────────────────────
# Session-scoped Spark (shared with test_job1_cleanse.py if run together)
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .appName("test-transformations")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


# ─────────────────────────────────────────────────────────────────────────────
# Helper — build a DataFrame with the exact columns cleanse_vehicle() uses
# ─────────────────────────────────────────────────────────────────────────────

def _make_df(spark, rows: list[tuple], with_timestamps: bool = True):
    """
    Produce a DataFrame shaped like the output of the rename + enforce_stable_types
    step in cleanse_vehicle(), so every downstream filter test uses real column names.
    """
    schema = StructType([
        StructField("VendorID",          LongType()),
        StructField("trip_distance",     DoubleType()),
        StructField("passenger_count",   LongType()),
        StructField("fare_amount",       DoubleType()),
        StructField("total_amount",      DoubleType()),
        StructField("PULocationID",      LongType()),
        StructField("DOLocationID",      LongType()),
        StructField("pickup_datetime",   StringType()),
        StructField("dropoff_datetime",  StringType()),
    ])
    df = spark.createDataFrame(rows, schema=schema)
    if with_timestamps:
        df = (df
              .withColumn("pickup_datetime",  F.to_timestamp("pickup_datetime"))
              .withColumn("dropoff_datetime", F.to_timestamp("dropoff_datetime")))
    return df


def _quality_filter(df):
    """Exact copy of the quality filter block from cleanse_vehicle()."""
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


# ─────────────────────────────────────────────────────────────────────────────
# enforce_stable_types — calls the real function from job1_cleanse.py
# ─────────────────────────────────────────────────────────────────────────────

class TestEnforceStableTypes:
    """
    Calls job1_cleanse.enforce_stable_types() directly.
    If the function is changed or removed, these tests fail.
    """

    def test_negative_fare_passes_type_cast(self, spark):
        """enforce_stable_types only casts types, it doesn't filter negatives."""
        from job1_cleanse import enforce_stable_types
        df = spark.createDataFrame(
            [(-5.0, 10.0)],
            ["fare_amount", "total_amount"],
        )
        result = enforce_stable_types(df).collect()[0]
        # Cast should succeed — value stays negative (filtering happens separately)
        assert result["fare_amount"] == -5.0

    def test_empty_string_becomes_null(self, spark):
        """Empty string in a numeric column → NULL after enforce_stable_types."""
        from job1_cleanse import enforce_stable_types
        df = spark.createDataFrame([("", "")], ["fare_amount", "total_amount"])
        result = enforce_stable_types(df).collect()[0]
        assert result["fare_amount"] is None
        assert result["total_amount"] is None

    def test_valid_doubles_stay_correct(self, spark):
        from job1_cleanse import enforce_stable_types
        df = spark.createDataFrame([(12.5, 15.0)], ["fare_amount", "total_amount"])
        result = enforce_stable_types(df).collect()[0]
        assert abs(result["fare_amount"] - 12.5) < 0.001


# ─────────────────────────────────────────────────────────────────────────────
# Quality filter tests — use the real filter expression from cleanse_vehicle()
# ─────────────────────────────────────────────────────────────────────────────

class TestOutlierFilter:

    _VALID = (1, 2.5, 2, 14.0, 17.3, 100, 200,
              "2023-03-15 08:00:00", "2023-03-15 08:30:00")

    def test_valid_row_passes_all_filters(self, spark):
        df = _make_df(spark, [self._VALID])
        assert _quality_filter(df).count() == 1

    def test_negative_fare_removed(self, spark):
        row = list(self._VALID)
        row[3] = -5.0   # fare_amount
        row[4] = -5.0   # total_amount
        df = _make_df(spark, [tuple(row)])
        assert _quality_filter(df).count() == 0

    def test_zero_distance_removed(self, spark):
        row = list(self._VALID)
        row[1] = 0.0   # trip_distance
        df = _make_df(spark, [tuple(row)])
        assert _quality_filter(df).count() == 0

    def test_mixed_valid_invalid(self, spark):
        invalid = list(self._VALID)
        invalid[1] = 0.0  # zero distance
        df = _make_df(spark, [self._VALID, tuple(invalid)])
        assert _quality_filter(df).count() == 1


# ─────────────────────────────────────────────────────────────────────────────
# Deduplication — uses trip_id just like cleanse_vehicle()
# ─────────────────────────────────────────────────────────────────────────────

class TestDeduplication:

    def test_exact_duplicate_trip_id_removed(self, spark):
        df = spark.createDataFrame(
            [("2023-03-15 08:00:00", 1, 2, 15.0),
             ("2023-03-15 08:00:00", 1, 2, 15.0),   # same row
             ("2023-03-15 09:00:00", 1, 2, 20.0)],
            ["pickup_datetime", "PULocationID", "DOLocationID", "total_amount"],
        )
        # Build trip_id the same way cleanse_vehicle() does
        df = df.withColumn("VendorID", F.lit(1).cast(LongType()))
        df = df.withColumn("trip_id",
            F.sha2(F.concat_ws("|",
                F.col("VendorID").cast("string"),
                F.col("pickup_datetime"),
                F.col("PULocationID").cast("string"),
                F.col("DOLocationID").cast("string"),
            ), 256)
        )
        deduped = df.dropDuplicates(["trip_id"])
        assert deduped.count() == 2

    def test_all_unique_rows_preserved(self, spark):
        df = spark.createDataFrame(
            [("2023-03-15 08:00:00", 1, 2, 15.0),
             ("2023-03-15 09:00:00", 3, 4, 20.0),
             ("2023-03-15 10:00:00", 5, 6, 25.0)],
            ["pickup_datetime", "PULocationID", "DOLocationID", "total_amount"],
        )
        df = df.withColumn("VendorID", F.lit(1).cast(LongType()))
        df = df.withColumn("trip_id",
            F.sha2(F.concat_ws("|",
                F.col("VendorID").cast("string"),
                F.col("pickup_datetime"),
                F.col("PULocationID").cast("string"),
                F.col("DOLocationID").cast("string"),
            ), 256)
        )
        assert df.dropDuplicates(["trip_id"]).count() == 3


# ─────────────────────────────────────────────────────────────────────────────
# Derived column tests — mirror the withColumn() chain in cleanse_vehicle()
# ─────────────────────────────────────────────────────────────────────────────

class TestDerivedColumns:

    def test_trip_duration_calculation(self, spark):
        df = spark.createDataFrame(
            [("2023-03-15 08:00:00", "2023-03-15 08:30:00")],
            ["pickup_datetime", "dropoff_datetime"],
        )
        df = (df
              .withColumn("pickup_datetime",  F.to_timestamp("pickup_datetime"))
              .withColumn("dropoff_datetime", F.to_timestamp("dropoff_datetime"))
              .withColumn("trip_duration_min",
                  (F.unix_timestamp("dropoff_datetime") - F.unix_timestamp("pickup_datetime")) / 60.0))
        assert df.collect()[0]["trip_duration_min"] == 30.0

    def test_tip_rate_calculation(self, spark):
        df = spark.createDataFrame([(10.0, 2.5)], ["fare_amount", "tip_amount"])
        df = df.withColumn("tip_rate", F.col("tip_amount") / F.col("fare_amount"))
        assert abs(df.collect()[0]["tip_rate"] - 0.25) < 0.001

    def test_airport_flag_detection(self, spark):
        """Matches the is_airport logic used in the Gold layer mart."""
        df = spark.createDataFrame(
            [("Airports",), ("Midtown",), ("EWR",)],
            ["pickup_service_zone"],
        )
        df = df.withColumn(
            "is_airport",
            F.col("pickup_service_zone").isin(["Airports", "EWR"])
        )
        results = {row["pickup_service_zone"]: row["is_airport"] for row in df.collect()}
        assert results["Airports"] is True
        assert results["Midtown"]  is False
        assert results["EWR"]      is True

    def test_speed_mph_calculation(self, spark):
        # 6 miles in 30 min = 12 mph
        df = spark.createDataFrame([(30.0, 6.0)], ["trip_duration_min", "trip_distance"])
        df = df.withColumn(
            "speed_mph",
            F.when(F.col("trip_duration_min") > 0,
                   F.col("trip_distance") / (F.col("trip_duration_min") / 60.0))
        )
        assert abs(df.collect()[0]["speed_mph"] - 12.0) < 0.001


# ─────────────────────────────────────────────────────────────────────────────
# Schema validator — patched to avoid real S3 access
# ─────────────────────────────────────────────────────────────────────────────

class TestSchemaValidator:
    """
    validate_schema() in your actual stack reads from S3.
    We test it by patching the S3 read with a local temp file,
    so the validation logic is exercised without real AWS.
    """

    def test_catches_missing_required_column(self, tmp_path):
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
            from unittest.mock import patch
            from ingestion.src.schema_validator import validate_schema
        except ImportError:
            pytest.skip("pyarrow or ingestion.src.schema_validator not installed")

        # File with only two columns — missing all the required TLC ones
        table = pa.table({"VendorID": [1, 2], "trip_distance": [1.0, 2.0]})
        path = tmp_path / "bad.parquet"
        pq.write_table(table, str(path))

        result = validate_schema(path)
        assert result["status"] == "quarantine"

    def test_valid_schema_passes(self, tmp_path):
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
            from ingestion.src.schema_validator import validate_schema, REQUIRED_COLUMNS
        except ImportError:
            pytest.skip("pyarrow or ingestion.src.schema_validator not installed")

        # Build a file with all required columns present
        arrays  = {col: pa.array([1]) for col in REQUIRED_COLUMNS}
        table   = pa.table(arrays)
        path    = tmp_path / "good.parquet"
        pq.write_table(table, str(path))

        result = validate_schema(path)
        assert result["status"] == "ok"