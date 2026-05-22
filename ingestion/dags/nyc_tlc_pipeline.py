from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
import airflow.exceptions
from operators.emr_serverless_operator import EMRServerlessSparkOperator

# ──────────────────────────────────────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────────────────────────────────────

AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "ap-southeast-1")

BUCKET = os.getenv("TLC_S3_BUCKET")
APP_ID = os.getenv("EMR_APPLICATION_ID")
EXEC_ROLE_ARN = os.getenv("EMR_EXECUTION_ROLE_ARN")

SCRIPT_BASE = f"s3://{BUCKET}/spark-scripts"

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=20),
    "execution_timeout": timedelta(hours=3),
}

# ──────────────────────────────────────────────────────────────────────────────
# Tasks
# ──────────────────────────────────────────────────────────────────────────────

def resolve_year_month(**context) -> str:
    """
    Returns the manually supplied year_month param (e.g. "2023-06"),
    or falls back to the execution date (ds[:7]) for scheduled runs.
    Validates format when a param is supplied.
    """
    import re

    param = context["params"].get("year_month", "").strip()

    if param:
        if not re.fullmatch(r"\d{4}-\d{2}", param):
            raise ValueError(
                f"year_month param '{param}' must be in YYYY-MM format."
            )
        return param

    return context["ds"][:7]


def ingest_and_validate(vehicle_type: str, **context):
    import tempfile
    import boto3
    import structlog
    from src.downloader import download_month, upload_to_s3_bronze
    from src.schema_validator import validate_schema

    log = structlog.get_logger()

    # Pull the resolved year_month from the upstream XCom
    year_month: str = context["ti"].xcom_pull(task_ids="resolve_year_month")

    with tempfile.TemporaryDirectory() as tmpdir:
        try:
            local_file = download_month(
                vehicle_type=vehicle_type,
                year_month=year_month,
                output_dir=Path(tmpdir),
            )
        except FileNotFoundError as e:
            log.warning(
                "source_data_unavailable",
                vehicle_type=vehicle_type,
                year_month=year_month,
                reason=str(e),
            )
            raise airflow.exceptions.AirflowSkipException(str(e))

        report = validate_schema(local_file)

        if report["status"] == "quarantine":
            quarantine_key = (
                f"quarantine/{vehicle_type}/{year_month}/{local_file.name}"
            )
            boto3.client("s3").upload_file(
                str(local_file),
                BUCKET,
                quarantine_key,
            )
            log.error(
                "schema_validation_failed",
                quarantine_key=quarantine_key,
                issues=report["issues"],
            )
            raise ValueError(
                f"Schema validation failed: {report['issues']}"
            )

        s3_key = upload_to_s3_bronze(
            local_file=local_file,
            bucket=BUCKET,
            year_month=year_month,
            vehicle_type=vehicle_type,
        )

        ti = context["ti"]
        ti.xcom_push(key="row_count", value=report["row_count"])
        ti.xcom_push(key="s3_key", value=s3_key)
        ti.xcom_push(key="year_month", value=year_month)

        log.info("ingest_complete", s3_key=s3_key, rows=report["row_count"])


def upload_spark_scripts():
    """Upload local Spark scripts to S3."""
    import boto3
    import structlog

    log = structlog.get_logger()
    s3 = boto3.client("s3")
    scripts_dir = Path("/opt/airflow/spark_jobs")

    for script in scripts_dir.glob("job*.py"):
        key = f"spark-scripts/{script.name}"
        s3.upload_file(str(script), BUCKET, key)
        log.info("spark_script_uploaded", key=key)


def run_dbt_and_test():
    """Execute dbt run + dbt test."""
    import subprocess
    import structlog

    log = structlog.get_logger()
    dbt_dir = "/opt/airflow/dbt_project"

    commands = [
        ["dbt", "run", "--select", "marts"],
        ["dbt", "test", "--select", "marts"],
    ]

    for cmd in commands:
        result = subprocess.run(
            cmd + ["--profiles-dir", dbt_dir, "--project-dir", dbt_dir],
            capture_output=True,
            text=True,
        )
        log.info(
            "dbt_command_finished",
            command=" ".join(cmd),
            stdout=result.stdout[-3000:],
        )
        if result.returncode != 0:
            raise RuntimeError(f"dbt command failed:\n{result.stderr}")

    log.info("dbt_completed")


def run_great_expectations(**context):
    import subprocess

    year_month: str = context["ti"].xcom_pull(task_ids="resolve_year_month")

    result = subprocess.run(
        [
            "python", "/opt/airflow/data_quality/run_expectations.py",
            "--partition",             year_month + "-01",
            "--env",                   os.getenv("ENV", "dev"),
            "--workgroup",             os.getenv("DBT_ATHENA_WORKGROUP"),
            "--datalake-bucket",       os.getenv("TLC_S3_BUCKET"),
            "--athena-results-bucket", os.getenv("ATHENA_RESULTS_BUCKET"),
            "--region",                os.getenv("AWS_DEFAULT_REGION", "ap-southeast-1"),
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr)


# ──────────────────────────────────────────────────────────────────────────────
# DAG
# ──────────────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="nyc_tlc_monthly_pipeline",
    start_date=datetime(2026, 5, 10),
    schedule="0 8 10 * *",
    catchup=True,
    max_active_runs=1,
    default_args=default_args,
    tags=["nyc-tlc", "emr-serverless", "lakehouse"],
    params={
        "year_month": Param(
            default="",
            type="string",
            description=(
                "Month to process in YYYY-MM format. "
                "Leave blank to use the scheduled execution date."
            ),
            pattern=r"^(\d{4}-\d{2})?$",
        ),
    },
    doc_md="""
    ## NYC TLC Monthly Pipeline

    End-to-end pipeline:

    - Resolve year_month (param override or execution date)
    - Download TLC data
    - Validate schema
    - Upload bronze layer
    - EMR Serverless Spark transforms
    - dbt marts
    - Great Expectations checks

    ### Manual / bootstrap runs
    Trigger via UI → **Trigger DAG w/ config**:
    ```json
    { "year_month": "2023-06" }
    ```
    Leave `year_month` blank (or omit it) for normal scheduled runs.
    """,
) as dag:

    # ──────────────────────────────────────────────────────────────────────────
    # Resolve year_month
    # ──────────────────────────────────────────────────────────────────────────

    resolve_ym = PythonOperator(
        task_id="resolve_year_month",
        python_callable=resolve_year_month,
        do_xcom_push=True,  # return value pushed as XCom "return_value"
    )

    # Jinja shorthand — pulls the resolved value for use in template fields
    _YM      = "{{ task_instance.xcom_pull(task_ids='resolve_year_month') }}"
    _YM_YEAR = "{{ task_instance.xcom_pull(task_ids='resolve_year_month')[:4] }}"
    _YM_MON  = "{{ task_instance.xcom_pull(task_ids='resolve_year_month')[5:7] }}"

    # ──────────────────────────────────────────────────────────────────────────
    # Upload Spark Scripts
    # ──────────────────────────────────────────────────────────────────────────

    upload_scripts = PythonOperator(
        task_id="upload_spark_scripts",
        python_callable=upload_spark_scripts,
    )

    # ──────────────────────────────────────────────────────────────────────────
    # Ingestion
    # ──────────────────────────────────────────────────────────────────────────

    ingest_yellow = PythonOperator(
        task_id="ingest_yellow_taxi",
        python_callable=ingest_and_validate,
        op_kwargs={"vehicle_type": "yellow"},
    )

    ingest_green = PythonOperator(
        task_id="ingest_green_taxi",
        python_callable=ingest_and_validate,
        op_kwargs={"vehicle_type": "green"},
    )

    # ──────────────────────────────────────────────────────────────────────────
    # EMR Job 1 — Cleanse
    # ──────────────────────────────────────────────────────────────────────────

    emr_cleanse = EMRServerlessSparkOperator(
        task_id="emr_cleanse",
        application_id=APP_ID,
        execution_role_arn=EXEC_ROLE_ARN,
        script_s3_path=f"{SCRIPT_BASE}/job1_cleanse.py",
        script_args=[
            f"--year={_YM_YEAR}",
            f"--month={_YM_MON}",
            f"--bucket={BUCKET}",
        ],
        executor_cores=1,
        executor_memory="2g",
        num_executors=1,
        aws_region=AWS_REGION,
        retries=1,
        retry_delay=timedelta(minutes=2),
    )

    # ──────────────────────────────────────────────────────────────────────────
    # EMR Job 2 — Enrich
    # ──────────────────────────────────────────────────────────────────────────

    emr_enrich = EMRServerlessSparkOperator(
        task_id="emr_enrich_join",
        application_id=APP_ID,
        execution_role_arn=EXEC_ROLE_ARN,
        script_s3_path=f"{SCRIPT_BASE}/job2_enrich_join.py",
        script_args=[
            f"--year={_YM_YEAR}",
            f"--month={_YM_MON}",
            f"--bucket={BUCKET}",
        ],
        executor_cores=1,
        executor_memory="2g",
        num_executors=1,
        aws_region=AWS_REGION,
    )

    # ──────────────────────────────────────────────────────────────────────────
    # EMR Job 3 — Aggregation
    # ──────────────────────────────────────────────────────────────────────────

    emr_aggregate = EMRServerlessSparkOperator(
        task_id="emr_aggregate",
        application_id=APP_ID,
        execution_role_arn=EXEC_ROLE_ARN,
        script_s3_path=f"{SCRIPT_BASE}/job3_aggregations.py",
        script_args=[
            f"--year={_YM_YEAR}",
            f"--month={_YM_MON}",
            f"--bucket={BUCKET}",
        ],
        spark_conf={"spark.sql.shuffle.partitions": "20"},
        executor_cores=1,
        executor_memory="2g",
        num_executors=1,
        aws_region=AWS_REGION,
    )

    # ──────────────────────────────────────────────────────────────────────────
    # dbt
    # ──────────────────────────────────────────────────────────────────────────

    dbt_run = PythonOperator(
        task_id="dbt_run_and_test",
        python_callable=run_dbt_and_test,
    )

    # ──────────────────────────────────────────────────────────────────────────
    # Great Expectations
    # ──────────────────────────────────────────────────────────────────────────

    dq_checks = PythonOperator(
        task_id="great_expectations_checks",
        python_callable=run_great_expectations,
    )

    # ──────────────────────────────────────────────────────────────────────────
    # Dependencies
    # ──────────────────────────────────────────────────────────────────────────

    resolve_ym >> [ingest_yellow, ingest_green]
    upload_scripts >> [ingest_yellow, ingest_green]

    [ingest_yellow, ingest_green] >> emr_cleanse

    emr_cleanse >> emr_enrich >> emr_aggregate

    emr_aggregate >> dbt_run >> dq_checks