"""
emr_serverless_operator.py

Custom Airflow Operator for EMR Serverless Spark jobs.
"""

from __future__ import annotations

import os
import time
from typing import Any

import boto3
import structlog
from airflow.models import BaseOperator

# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------

log = structlog.get_logger()

# -----------------------------------------------------------------------------
# Constants
# -----------------------------------------------------------------------------

TERMINAL_STATES = {
    "SUCCESS",
    "FAILED",
    "CANCELLED",
    "CANCEL_FAILED",
}

POLL_INTERVAL = 30

BUCKET = os.getenv("TLC_S3_BUCKET")

# Delta package for EMR 7.1 / Spark 3.5
DELTA_PACKAGE = "io.delta:delta-spark_2.12:3.2.0"


# -----------------------------------------------------------------------------
# Operator
# -----------------------------------------------------------------------------

class EMRServerlessSparkOperator(BaseOperator):
    """
    Submit a Spark job to EMR Serverless and wait for completion.
    """

    template_fields = (
        "script_args",
        "script_s3_path",
    )

    def __init__(
        self,
        *,
        application_id: str,
        execution_role_arn: str,
        script_s3_path: str,
        script_args: list[str] | None = None,
        spark_conf: dict[str, str] | None = None,

        # Resource configs
        executor_cores: int = 1,
        executor_memory: str = "2g",
        num_executors: int = 1,
        driver_cores: int = 1,
        driver_memory: str = "2g",

        aws_region: str = "ap-southeast-1",

        poll_interval: int = 30,

        **kwargs: Any,
    ) -> None:

        super().__init__(**kwargs)

        self.application_id = application_id
        self.execution_role_arn = execution_role_arn
        self.script_s3_path = script_s3_path

        self.script_args = script_args or []
        self.spark_conf = spark_conf or {}

        self.executor_cores = executor_cores
        self.executor_memory = executor_memory
        self.num_executors = num_executors

        self.driver_cores = driver_cores
        self.driver_memory = driver_memory

        self.aws_region = aws_region
        self.poll_interval = poll_interval

    # -------------------------------------------------------------------------
    # Build Spark submit params
    # -------------------------------------------------------------------------

    def _build_spark_submit_params(self) -> dict[str, Any]:

        conf: dict[str, str] = {

            # -----------------------------------------------------------------
            # Delta Lake
            # -----------------------------------------------------------------

            "spark.jars.packages": DELTA_PACKAGE,

            "spark.sql.extensions":
                "io.delta.sql.DeltaSparkSessionExtension",

            "spark.sql.catalog.spark_catalog":
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                
            # Add to the conf dict in _build_spark_submit_params
            "spark.dynamicAllocation.enabled": "false",  # disable dynamic alloc
            "spark.sql.adaptive.advisoryPartitionSizeInBytes": "134217728",  # 128MB, reduce partition count
            
            # -----------------------------------------------------------------
            # Spark resources
            # -----------------------------------------------------------------

            "spark.executor.instances":
                str(self.num_executors),

            "spark.executor.cores":
                str(self.executor_cores),

            "spark.executor.memory":
                self.executor_memory,

            "spark.driver.cores":
                str(self.driver_cores),

            "spark.driver.memory":
                self.driver_memory,

            # -----------------------------------------------------------------
            # Adaptive Query Execution
            # -----------------------------------------------------------------

            "spark.sql.adaptive.enabled": "true",

            "spark.sql.adaptive.coalescePartitions.enabled": "true",

            # -----------------------------------------------------------------
            # Reduce vCPU pressure
            # -----------------------------------------------------------------

            "spark.sql.shuffle.partitions": "50",

            # -----------------------------------------------------------------
            # S3 optimizations
            # -----------------------------------------------------------------

            "spark.hadoop.fs.s3a.fast.upload": "true",

            "spark.hadoop.fs.s3a.multipart.size": "67108864",

            # -----------------------------------------------------------------
            # Glue catalog
            # -----------------------------------------------------------------

            "spark.hadoop.hive.metastore.client.factory.class":
                "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",

            # User overrides
            **self.spark_conf,
        }

        conf_str = " ".join(
            f"--conf {k}={v}"
            for k, v in conf.items()
        )

        return {
            "entryPoint": self.script_s3_path,
            "entryPointArguments": self.script_args,
            "sparkSubmitParameters": conf_str,
        }

    # -------------------------------------------------------------------------
    # Execute
    # -------------------------------------------------------------------------

    def execute(self, context: Any) -> str:

        client = boto3.client(
            "emr-serverless",
            region_name=self.aws_region,
        )

        submit_params = self._build_spark_submit_params()

        log.info(
            "submitting_emr_job",
            application_id=self.application_id,
            script=self.script_s3_path,
            args=self.script_args,
        )

        response = client.start_job_run(
            applicationId=self.application_id,
            executionRoleArn=self.execution_role_arn,

            jobDriver={
                "sparkSubmit": submit_params,
            },

            configurationOverrides={
                "monitoringConfiguration": {
                    "s3MonitoringConfiguration": {
                        "logUri":
                            f"s3://{BUCKET}/emr-logs/"
                            f"{self.application_id}/",
                    }
                }
            },
        )

        job_run_id = response["jobRunId"]

        log.info(
            "emr_job_submitted",
            job_run_id=job_run_id,
        )

        # ---------------------------------------------------------------------
        # Poll status
        # ---------------------------------------------------------------------

        while True:

            status_resp = client.get_job_run(
                applicationId=self.application_id,
                jobRunId=job_run_id,
            )

            job_run = status_resp["jobRun"]

            state = job_run["state"]

            log.info(
                "emr_job_state",
                job_run_id=job_run_id,
                state=state,
            )

            if state in TERMINAL_STATES:
                break

            time.sleep(self.poll_interval)

        # ---------------------------------------------------------------------
        # Final status
        # ---------------------------------------------------------------------

        if state != "SUCCESS":

            details = job_run.get(
                "stateDetails",
                "No details available",
            )

            log.error(
                "emr_job_failed",
                job_run_id=job_run_id,
                state=state,
                details=details,
            )

            raise RuntimeError(
                f"EMR Serverless job failed\n"
                f"JobRunId: {job_run_id}\n"
                f"State: {state}\n"
                f"Details: {details}"
            )

        log.info(
            "emr_job_succeeded",
            job_run_id=job_run_id,
        )

        return job_run_id