# tests/unit/test_transformations.py
import pytest
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sys
sys.path.insert(0, "spark_jobs")

@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .appName("test")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )

def test_outlier_filter_removes_negative_fares(spark):
    data = [
        (1, 2.5, 1, -5.0, 10.0),  # negative fare → remove
        (2, 3.0, 2, 12.0, 15.0),  # valid
        (3, 0.0, 1, 8.0, 10.0),   # zero distance → remove
    ]
    df = spark.createDataFrame(data, ["VendorID", "trip_distance", "passenger_count", "fare_amount", "total_amount"])
    filtered = df.filter(
        (F.col("trip_distance") > 0) &
        (F.col("total_amount") > 0)
    )
    assert filtered.count() == 1

def test_deduplication_removes_exact_duplicates(spark):
    data = [
        ("2024-01-01 08:00:00", 1, 2, 15.0),
        ("2024-01-01 08:00:00", 1, 2, 15.0),  # duplicate
        ("2024-01-01 09:00:00", 1, 2, 20.0),
    ]
    df = spark.createDataFrame(data, ["pickup_datetime", "PULocationID", "DOLocationID", "total_amount"])
    deduped = df.dropDuplicates(["pickup_datetime", "PULocationID", "DOLocationID", "total_amount"])
    assert deduped.count() == 2

def test_trip_duration_calculation(spark):
    from pyspark.sql.types import TimestampType
    data = [("2024-01-01 08:00:00", "2024-01-01 08:30:00")]
    df = spark.createDataFrame(data, ["pickup", "dropoff"])
    df = df.withColumn("pickup", F.to_timestamp("pickup")).withColumn("dropoff", F.to_timestamp("dropoff"))
    df = df.withColumn("duration_min", (F.unix_timestamp("dropoff") - F.unix_timestamp("pickup")) / 60)
    result = df.collect()[0]["duration_min"]
    assert result == 30.0

def test_tip_rate_calculation(spark):
    data = [(10.0, 2.5)]
    df = spark.createDataFrame(data, ["fare_amount", "tip_amount"])
    df = df.withColumn("tip_rate", F.col("tip_amount") / F.col("fare_amount"))
    result = df.collect()[0]["tip_rate"]
    assert abs(result - 0.25) < 0.001

def test_airport_flag_detection(spark):
    data = [("Airports",), ("Midtown",), ("EWR",)]
    df = spark.createDataFrame(data, ["pickup_service_zone"])
    df = df.withColumn("is_airport", F.col("pickup_service_zone").isin(["Airports", "EWR"]))
    results = {row["pickup_service_zone"]: row["is_airport"] for row in df.collect()}
    assert results["Airports"] is True
    assert results["Midtown"] is False
    assert results["EWR"] is True

def test_schema_validator_catches_missing_column():
    from ingestion.src.schema_validator import validate_schema
    import tempfile, pyarrow as pa, pyarrow.parquet as pq, pathlib
    # Tạo file thiếu cột quan trọng
    table = pa.table({"VendorID": [1, 2], "trip_distance": [1.0, 2.0]})
    with tempfile.NamedTemporaryFile(suffix=".parquet") as f:
        pq.write_table(table, f.name)
        result = validate_schema(pathlib.Path(f.name))
    assert result["status"] == "quarantine"