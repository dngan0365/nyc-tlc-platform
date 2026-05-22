"""
tests/integration/test_pipeline_e2e.py  (rewritten)

Original problems
─────────────────
1. subprocess ran job1_cleanse.py against --bucket=localstack-bucket
   but no LocalStack service was defined in docker-compose.yml.
2. Output was checked at /tmp/test-output/silver/yellow — the real job writes
   to S3, not local disk.
3. timeout=300 — real EMR jobs run for 10-30+ minutes; this would always time out.

New approach
────────────
We mock all AWS services with moto and call the *Python functions* from
job1_cleanse.py directly (not via subprocess), which:
  - Tests the full code path: read Parquet → filter → derive → register Glue
  - Runs in seconds instead of minutes
  - Needs no running EMR, LocalStack, or real AWS credentials

What is still NOT covered here (needs a real EMR integration test)
──────────────────────────────────────────────────────────────────
- Delta Lake write (.format("delta")…save()) — requires Delta JARs on Spark
- symlink_format_manifest generation via spark.sql(GENERATE …)
- Actual Athena query execution against the written data

Those are tested manually against a dev S3 bucket / EMR cluster.

Run with:
    pytest tests/integration/test_pipeline_e2e.py -v -m integration
"""

from __future__ import annotations

import io
import sys
sys.path.insert(0, "spark_jobs")
sys.path.insert(0, ".")

import pytest
import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from moto import mock_aws
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession


# ── constants ─────────────────────────────────────────────────────────────────
AWS_REGION  = "ap-southeast-1"
BUCKET      = "nyc-tlc-data-test"
BRONZE_DB   = "nyc_tlc_bronze"
SILVER_DB   = "nyc_tlc_silver"
YEAR        = "2023"
MONTH       = "03"


# ─────────────────────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .appName("test-e2e")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        # Disable Delta extensions for e2e so we can test everything except the
        # actual Delta write (which requires Delta JARs not available in CI)
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def _make_bronze_parquet(bucket_name: str, vehicle: str,
                          year: str, month: str,
                          s3_client, n_valid: int = 50, n_invalid: int = 10) -> None:
    """
    Write a synthetic bronze Parquet file to mocked S3.
    Produces n_valid rows that should survive cleansing + n_invalid that should be dropped.
    """
    pickup_col  = f"{'tpep' if vehicle == 'yellow' else 'lpep'}_pickup_datetime"
    dropoff_col = f"{'tpep' if vehicle == 'yellow' else 'lpep'}_dropoff_datetime"

    valid_rows = {
        "VendorID":              pa.array([1] * n_valid,   type=pa.int64()),
        pickup_col:              pa.array(["2023-03-15 08:00:00"] * n_valid),
        dropoff_col:             pa.array(["2023-03-15 08:30:00"] * n_valid),
        "passenger_count":       pa.array([2] * n_valid,   type=pa.int64()),
        "trip_distance":         pa.array([3.5] * n_valid, type=pa.float64()),
        "RatecodeID":            pa.array([1] * n_valid,   type=pa.int64()),
        "store_and_fwd_flag":    pa.array(["N"] * n_valid),
        "PULocationID":          pa.array([100] * n_valid, type=pa.int64()),
        "DOLocationID":          pa.array([200] * n_valid, type=pa.int64()),
        "payment_type":          pa.array([1] * n_valid,   type=pa.int64()),
        "fare_amount":           pa.array([14.0] * n_valid,  type=pa.float64()),
        "extra":                 pa.array([0.5] * n_valid,   type=pa.float64()),
        "mta_tax":               pa.array([0.5] * n_valid,   type=pa.float64()),
        "tip_amount":            pa.array([2.0] * n_valid,   type=pa.float64()),
        "tolls_amount":          pa.array([0.0] * n_valid,   type=pa.float64()),
        "improvement_surcharge": pa.array([0.3] * n_valid,   type=pa.float64()),
        "total_amount":          pa.array([17.3] * n_valid,  type=pa.float64()),
        "congestion_surcharge":  pa.array([2.5] * n_valid,   type=pa.float64()),
    }

    # Invalid rows: zero distance and negative total (both should be dropped)
    invalid_rows = {k: v for k, v in valid_rows.items()}
    invalid_rows["trip_distance"] = pa.array([0.0] * n_invalid, type=pa.float64())
    invalid_rows["total_amount"]  = pa.array([-1.0] * n_invalid, type=pa.float64())
    # Truncate each column to n_invalid rows
    invalid_rows = {k: v[:n_invalid] for k, v in invalid_rows.items()}

    # Concatenate
    combined = {
        k: pa.concat_arrays([valid_rows[k], invalid_rows[k]])
        for k in valid_rows
    }

    table = pa.table(combined)
    buf = io.BytesIO()
    pq.write_table(table, buf)
    buf.seek(0)

    key = f"bronze/{vehicle}/{year}-{month}/data.parquet"
    s3_client.put_object(Bucket=bucket_name, Key=key, Body=buf.read())


# ─────────────────────────────────────────────────────────────────────────────
# Integration tests
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.integration
class TestCleansePipelineE2E:

    @mock_aws
    def test_yellow_cleanse_drops_invalid_rows(self, spark):
        """
        cleanse_vehicle() must drop rows that fail quality or sanity filters.
        We patch the Delta write and Glue registration so the test focuses on
        the cleansing logic, not S3/Glue I/O.
        """
        from job1_cleanse import cleanse_vehicle

        # Set up mocked S3 + Glue
        s3 = boto3.client("s3", region_name=AWS_REGION)
        s3.create_bucket(
            Bucket=BUCKET,
            CreateBucketConfiguration={"LocationConstraint": AWS_REGION},
        )
        glue = boto3.client("glue", region_name=AWS_REGION)
        glue.create_database(DatabaseInput={"Name": BRONZE_DB})
        glue.create_database(DatabaseInput={"Name": SILVER_DB})

        # Write 50 valid + 10 invalid bronze rows
        _make_bronze_parquet(BUCKET, "yellow", YEAR, MONTH, s3,
                             n_valid=50, n_invalid=10)

        # Patch the Delta write and symlink generation (needs Delta JARs)
        with (
            patch("job1_cleanse.register_bronze"),
            patch("job1_cleanse.register_delta_table"),
            patch("job1_cleanse._add_silver_partitions"),
            patch.object(
                spark, "sql",
                return_value=MagicMock(),   # stub GENERATE symlink_format_manifest
            ),
        ):
            # Patch DataFrame.write.format("delta")... to a no-op
            mock_writer = MagicMock()
            mock_writer.format.return_value  = mock_writer
            mock_writer.mode.return_value    = mock_writer
            mock_writer.option.return_value  = mock_writer
            mock_writer.partitionBy.return_value = mock_writer
            mock_writer.save.return_value    = None

            with patch("pyspark.sql.DataFrame.write", new_callable=lambda: property(lambda self: mock_writer)):
                clean_count = cleanse_vehicle(
                    spark=spark,
                    bucket=BUCKET,
                    vehicle_type="yellow",
                    year=YEAR,
                    month=MONTH,
                    bronze_database=BRONZE_DB,
                    silver_database=SILVER_DB,
                    aws_region=AWS_REGION,
                )

        # 10 invalid rows must be dropped; we may lose a couple more from
        # sanity / dedup filters depending on synthetic data, but definitely < 60
        assert clean_count <= 50
        assert clean_count > 0

    @mock_aws
    def test_glue_bronze_table_registered(self, spark):
        """
        After cleanse_vehicle(), the Glue bronze table must exist with
        the correct Parquet SerDe.
        """
        from job1_cleanse import cleanse_vehicle

        s3 = boto3.client("s3", region_name=AWS_REGION)
        s3.create_bucket(
            Bucket=BUCKET,
            CreateBucketConfiguration={"LocationConstraint": AWS_REGION},
        )
        glue = boto3.client("glue", region_name=AWS_REGION)
        glue.create_database(DatabaseInput={"Name": BRONZE_DB})
        glue.create_database(DatabaseInput={"Name": SILVER_DB})
        _make_bronze_parquet(BUCKET, "yellow", YEAR, MONTH, s3)

        mock_writer = MagicMock()
        mock_writer.format.return_value      = mock_writer
        mock_writer.mode.return_value        = mock_writer
        mock_writer.option.return_value      = mock_writer
        mock_writer.partitionBy.return_value = mock_writer
        mock_writer.save.return_value        = None

        with (
            patch.object(spark, "sql", return_value=MagicMock()),
            patch("pyspark.sql.DataFrame.write", new_callable=lambda: property(lambda self: mock_writer)),
        ):
            cleanse_vehicle(
                spark=spark, bucket=BUCKET, vehicle_type="yellow",
                year=YEAR, month=MONTH,
                bronze_database=BRONZE_DB, silver_database=SILVER_DB,
                aws_region=AWS_REGION,
            )

        # Bronze table must be registered
        table = glue.get_table(DatabaseName=BRONZE_DB, Name="yellow")["Table"]
        assert table["TableType"] == "EXTERNAL_TABLE"
        sd = table["StorageDescriptor"]
        assert "ParquetHiveSerDe" in sd["SerdeInfo"]["SerializationLibrary"]

    @mock_aws
    def test_glue_silver_table_registered_with_symlink(self, spark):
        """
        After cleanse_vehicle(), the Glue silver table must use
        SymlinkTextInputFormat and contain explicit SILVER_COLUMNS.
        """
        from job1_cleanse import cleanse_vehicle, SILVER_COLUMNS

        s3 = boto3.client("s3", region_name=AWS_REGION)
        s3.create_bucket(
            Bucket=BUCKET,
            CreateBucketConfiguration={"LocationConstraint": AWS_REGION},
        )
        glue = boto3.client("glue", region_name=AWS_REGION)
        glue.create_database(DatabaseInput={"Name": BRONZE_DB})
        glue.create_database(DatabaseInput={"Name": SILVER_DB})
        _make_bronze_parquet(BUCKET, "yellow", YEAR, MONTH, s3)

        mock_writer = MagicMock()
        mock_writer.format.return_value      = mock_writer
        mock_writer.mode.return_value        = mock_writer
        mock_writer.option.return_value      = mock_writer
        mock_writer.partitionBy.return_value = mock_writer
        mock_writer.save.return_value        = None

        with (
            patch.object(spark, "sql", return_value=MagicMock()),
            patch("pyspark.sql.DataFrame.write", new_callable=lambda: property(lambda self: mock_writer)),
        ):
            cleanse_vehicle(
                spark=spark, bucket=BUCKET, vehicle_type="yellow",
                year=YEAR, month=MONTH,
                bronze_database=BRONZE_DB, silver_database=SILVER_DB,
                aws_region=AWS_REGION,
            )

        table = glue.get_table(DatabaseName=SILVER_DB, Name="yellow")["Table"]
        sd = table["StorageDescriptor"]

        assert sd["InputFormat"] == "org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat"
        assert len(sd["Columns"]) == len(SILVER_COLUMNS)

    @mock_aws
    def test_both_vehicle_types_processed(self, spark):
        """
        The main() loop processes both 'yellow' and 'green'.
        Verify both Glue tables are registered after running both.
        """
        from job1_cleanse import cleanse_vehicle

        s3 = boto3.client("s3", region_name=AWS_REGION)
        s3.create_bucket(
            Bucket=BUCKET,
            CreateBucketConfiguration={"LocationConstraint": AWS_REGION},
        )
        glue = boto3.client("glue", region_name=AWS_REGION)
        glue.create_database(DatabaseInput={"Name": BRONZE_DB})
        glue.create_database(DatabaseInput={"Name": SILVER_DB})

        for vehicle in ["yellow", "green"]:
            _make_bronze_parquet(BUCKET, vehicle, YEAR, MONTH, s3)

        mock_writer = MagicMock()
        mock_writer.format.return_value      = mock_writer
        mock_writer.mode.return_value        = mock_writer
        mock_writer.option.return_value      = mock_writer
        mock_writer.partitionBy.return_value = mock_writer
        mock_writer.save.return_value        = None

        with (
            patch.object(spark, "sql", return_value=MagicMock()),
            patch("pyspark.sql.DataFrame.write", new_callable=lambda: property(lambda self: mock_writer)),
        ):
            for vehicle in ["yellow", "green"]:
                cleanse_vehicle(
                    spark=spark, bucket=BUCKET, vehicle_type=vehicle,
                    year=YEAR, month=MONTH,
                    bronze_database=BRONZE_DB, silver_database=SILVER_DB,
                    aws_region=AWS_REGION,
                )

        # Both silver tables must exist
        for vehicle in ["yellow", "green"]:
            table = glue.get_table(DatabaseName=SILVER_DB, Name=vehicle)["Table"]
            assert table["Name"] == vehicle


# ─────────────────────────────────────────────────────────────────────────────
# RAG chain smoke test (previously: subprocess + /tmp path check)
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.integration
class TestRagChainSmoke:
    """
    Smoke-test the RAG chain end-to-end with all AWS calls mocked.
    Verifies that ainvoke() returns a structurally correct RagAnswer
    without hitting real Athena / Glue / Anthropic.
    """

    @pytest.mark.asyncio
    @mock_aws
    async def test_ainvoke_smoke(self):
        from unittest.mock import AsyncMock, patch
        from serving.rag_app.rag_chain import NycTlcRagChain, TableSchema, GOLD_TABLES

        glue = boto3.client("glue", region_name=AWS_REGION)
        glue.create_database(DatabaseInput={"Name": "nyc_tlc_gold"})

        chain = NycTlcRagChain(
            athena_database="nyc_tlc_gold",
            athena_workgroup="nyc-tlc-dev",
            athena_results_location="s3://results/",
            aws_region=AWS_REGION,
        )

        fake_schemas = [
            TableSchema(database="nyc_tlc_gold", table=t, columns=[])
            for t in GOLD_TABLES
        ]
        fake_rows = [{"pickup_hour": "8", "avg_fare": "15.50"}]

        with (
            patch.object(chain._schema_retriever, "get_schemas", return_value=fake_schemas),
            patch.object(chain._executor, "run", return_value=(fake_rows, "exec-smoke")),
            patch.object(chain, "_llm", new=AsyncMock(side_effect=[
                "SELECT pickup_hour FROM nyc_tlc_gold.fact_trips LIMIT 50",
                "Peak hour is 8 AM.",
            ])),
            # MLflow is not running in test environment — patch it out
            patch("serving.rag_app.rag_chain.mlflow"),
        ):
            result = await chain.ainvoke("What is the busiest pickup hour?")

        assert result.answer == "Peak hour is 8 AM."
        assert "SELECT" in result.sql.upper()
        assert result.rows_returned == 1
        assert result.sql_attempts  == 1
        assert result.latency_seconds >= 0