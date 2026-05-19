"""
data_quality/run_expectations.py
─────────────────────────────────────────────────────────────────────────────
Runs the Great Expectations suite (nyc_tlc_gold_suite.json) against a single
pickup_date partition of the gold fact_trips table via Athena.

Invoked by the Airflow DAG after each dbt run:

    python run_expectations.py \
        --partition 2023-01-15 \
        --env dev \
        --workgroup nyc-tlc-dev \
        --datalake-bucket nyc-tlc-datalake-dev-123456789 \
        --athena-results-bucket nyc-tlc-athena-results-dev-123456789 \
        --region us-east-1

Exit codes:
    0  All checks passed (or only warnings fired)
    1  At least one CRITICAL check failed → Airflow marks task as failed
       and the quarantine step copies the partition to s3://…/quarantine/

Flow:
    1. Load the expectation suite from JSON.
    2. Build a pandas DataFrame by querying Athena for the target partition.
    3. Run each expectation against the DataFrame via a PandasDataset.
    4. For each failure:
         CRITICAL → add partition to quarantine list, set exit_code = 1
         WARNING  → emit a CloudWatch metric (count or pct of failing rows)
    5. If exit_code == 1, copy the S3 partition prefix to the quarantine zone.
    6. Write a JSON validation report to S3 for audit purposes.
    7. Exit with exit_code.
─────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import boto3
import pandas as pd
import pyathena
from great_expectations.dataset import PandasDataset

# ── Logging ──────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("dq.runner")

# ── Constants ────────────────────────────────────────────────────────────────

SUITE_PATH = Path(__file__).parent / "expectations" / "nyc_tlc_gold_suite.json"
QUARANTINE_PREFIX = "quarantine/fact_trips"
REPORT_PREFIX = "dq-reports/fact_trips"
GOLD_TABLE = "nyc_tlc_gold.fact_trips"

# CloudWatch namespace for all DQ metrics
CW_NAMESPACE = "NycTlc/DataQuality"

# Maximum rows fetched from Athena for GE validation.
# 500 000 rows gives statistical confidence for percentage-based checks
# while keeping memory and query cost manageable.
SAMPLE_LIMIT = 500_000


# ─────────────────────────────────────────────────────────────────────────────
# Argument parsing
# ─────────────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run GE suite against a gold partition")
    p.add_argument("--partition",            required=True,  help="pickup_date value, e.g. 2023-01-15")
    p.add_argument("--env",                  required=True,  help="dev | staging | prod")
    p.add_argument("--workgroup",            required=True,  help="Athena workgroup name")
    p.add_argument("--datalake-bucket",      required=True,  help="S3 datalake bucket name")
    p.add_argument("--athena-results-bucket",required=True,  help="S3 Athena results bucket name")
    p.add_argument("--region",               default="us-east-1")
    p.add_argument("--dry-run",              action="store_true",
                   help="Run checks but skip quarantine and CloudWatch emission")
    return p.parse_args()


# ─────────────────────────────────────────────────────────────────────────────
# Athena query helpers
# ─────────────────────────────────────────────────────────────────────────────

def build_athena_connection(args: argparse.Namespace) -> pyathena.Connection:
    """Return a PyAthena connection pointed at the project workgroup."""
    s3_staging = f"s3://{args.athena_results_bucket}/dq-staging/"
    return pyathena.connect(
        s3_staging_dir=s3_staging,
        region_name=args.region,
        work_group=args.workgroup,
        schema_name="nyc_tlc_gold",
    )


def fetch_partition(conn: pyathena.Connection, partition: str) -> pd.DataFrame:
    """
    Pull a sample of the target pickup_date partition from fact_trips.

    Uses ORDER BY random() so the sample is representative rather than just
    the first N rows (which in a partitioned Parquet table would all come
    from the same file).
    """
    sql = f"""
        SELECT
            trip_key,
            pickup_date,
            pickup_at,
            dropoff_at,
            cab_type,
            pickup_location_id,
            dropoff_location_id,
            fare_amount,
            trip_duration_seconds,
            total_amount,
            payment_type,
            passenger_count
        FROM {GOLD_TABLE}
        WHERE pickup_date = DATE '{partition}'
        ORDER BY rand()
        LIMIT {SAMPLE_LIMIT}
    """
    log.info("Fetching partition pickup_date='%s' from Athena (limit %d rows)…",
             partition, SAMPLE_LIMIT)
    t0 = time.monotonic()
    df = pd.read_sql(sql, conn)
    elapsed = time.monotonic() - t0
    log.info("Fetched %d rows in %.1fs", len(df), elapsed)
    return df


def get_exact_row_count(conn: pyathena.Connection, partition: str) -> int:
    """
    COUNT(*) for the partition — used by the row-count expectation.
    We don't use len(df) because the DataFrame is sampled.
    """
    sql = f"""
        SELECT COUNT(*) AS cnt
        FROM {GOLD_TABLE}
        WHERE pickup_date = DATE '{partition}'
    """
    result = pd.read_sql(sql, conn)
    return int(result["cnt"].iloc[0])


# ─────────────────────────────────────────────────────────────────────────────
# Expectation suite loader
# ─────────────────────────────────────────────────────────────────────────────

def load_suite(path: Path) -> dict[str, Any]:
    with open(path) as f:
        return json.load(f)


# ─────────────────────────────────────────────────────────────────────────────
# Validation runner
# ─────────────────────────────────────────────────────────────────────────────

def run_suite(
    df: pd.DataFrame,
    exact_row_count: int,
    suite: dict[str, Any],
) -> list[dict[str, Any]]:
    """
    Execute each expectation in the suite against the DataFrame.

    Returns a list of result dicts, one per expectation, with keys:
        check_id, expectation_type, severity, success, result, meta
    """
    dataset = PandasDataset(df)
    results = []

    for exp in suite["expectations"]:
        exp_type = exp["expectation_type"]
        kwargs    = dict(exp["kwargs"])
        meta      = exp["meta"]
        check_id  = meta["check_id"]
        severity  = meta["severity"]

        log.info("Running %s (%s, %s)…", check_id, exp_type, severity)

        # ── Row count check is special: use the exact COUNT(*) value ─────────
        if exp_type == "expect_table_row_count_to_be_between":
            success = kwargs["min_value"] <= exact_row_count <= kwargs["max_value"]
            result_dict = {
                "observed_value": exact_row_count,
                "expected_min": kwargs["min_value"],
                "expected_max": kwargs["max_value"],
            }

        # ── All other checks go through PandasDataset ─────────────────────
        else:
            ge_method = getattr(dataset, exp_type, None)
            if ge_method is None:
                log.warning("Unknown expectation type '%s' — skipping", exp_type)
                continue

            ge_result  = ge_method(**kwargs, result_format="SUMMARY")
            success    = ge_result["success"]
            result_dict = ge_result.get("result", {})

        status = "✓ PASS" if success else ("✗ FAIL" if severity == "CRITICAL" else "⚠ WARN")
        log.info("%s  %s", status, check_id)

        results.append({
            "check_id":        check_id,
            "expectation_type": exp_type,
            "severity":         severity,
            "success":          success,
            "result":           result_dict,
            "meta":             meta,
        })

    return results


# ─────────────────────────────────────────────────────────────────────────────
# CloudWatch emission
# ─────────────────────────────────────────────────────────────────────────────

def emit_cloudwatch_metrics(
    results: list[dict],
    partition: str,
    env: str,
    region: str,
    dry_run: bool,
) -> None:
    """
    For every WARNING check that failed, emit a CloudWatch metric so that
    dashboards and alarms can track data quality trends over time without
    stopping the pipeline.

    Metric dimensions:
        Env         dev | staging | prod
        Partition   the pickup_date value
        CheckId     e.g. CHK-005
    """
    if dry_run:
        log.info("[dry-run] Skipping CloudWatch emission")
        return

    cw = boto3.client("cloudwatch", region_name=region)
    metric_data = []

    for r in results:
        if r["success"]:
            continue
        meta = r["meta"]
        if meta["severity"] != "WARNING":
            continue
        metric_name = meta.get("cloudwatch_metric", f"dq/{r['check_id']}_failure")
        # Strip namespace prefix if present (CW metric name must not contain /)
        metric_name = metric_name.split("/")[-1]

        # Use unexpected_percent when available, otherwise 1.0 (binary failure)
        value = r["result"].get("unexpected_percent", 100.0)

        metric_data.append({
            "MetricName": metric_name,
            "Dimensions": [
                {"Name": "Env",       "Value": env},
                {"Name": "Partition", "Value": partition},
                {"Name": "CheckId",   "Value": r["check_id"]},
            ],
            "Value":     value,
            "Unit":      "Percent",
            "Timestamp": datetime.now(timezone.utc),
        })

    if metric_data:
        log.info("Emitting %d CloudWatch metric(s) to namespace %s",
                 len(metric_data), CW_NAMESPACE)
        # CloudWatch PutMetricData accepts max 20 metrics per call
        for i in range(0, len(metric_data), 20):
            cw.put_metric_data(
                Namespace=CW_NAMESPACE,
                MetricData=metric_data[i:i + 20],
            )
    else:
        log.info("No WARNING failures to emit to CloudWatch")


# ─────────────────────────────────────────────────────────────────────────────
# Quarantine
# ─────────────────────────────────────────────────────────────────────────────

def quarantine_partition(
    partition: str,
    datalake_bucket: str,
    region: str,
    dry_run: bool,
) -> None:
    """
    Copy the failed partition from gold/ to quarantine/ using S3 copy
    (server-side, no data egress).

    Source:      s3://{bucket}/gold/fact_trips/pickup_date={partition}/
    Destination: s3://{bucket}/quarantine/fact_trips/pickup_date={partition}/

    We copy rather than move so the gold partition remains in place for
    manual inspection.  The Airflow DAG's cleanup task can delete it after
    the incident is resolved.
    """
    if dry_run:
        log.info("[dry-run] Would quarantine pickup_date=%s", partition)
        return

    s3 = boto3.client("s3", region_name=region)
    src_prefix  = f"gold/fact_trips/pickup_date={partition}/"
    dst_prefix  = f"{QUARANTINE_PREFIX}/pickup_date={partition}/"

    log.warning("CRITICAL failure — quarantining partition pickup_date=%s", partition)
    log.warning("  src: s3://%s/%s", datalake_bucket, src_prefix)
    log.warning("  dst: s3://%s/%s", datalake_bucket, dst_prefix)

    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=datalake_bucket, Prefix=src_prefix)

    copied = 0
    for page in pages:
        for obj in page.get("Contents", []):
            src_key = obj["Key"]
            dst_key = src_key.replace(src_prefix, dst_prefix, 1)
            s3.copy_object(
                Bucket=datalake_bucket,
                CopySource={"Bucket": datalake_bucket, "Key": src_key},
                Key=dst_key,
            )
            copied += 1

    log.warning("Quarantine complete: %d object(s) copied", copied)


# ─────────────────────────────────────────────────────────────────────────────
# Validation report
# ─────────────────────────────────────────────────────────────────────────────

def write_report(
    results: list[dict],
    partition: str,
    exact_row_count: int,
    datalake_bucket: str,
    region: str,
    dry_run: bool,
) -> None:
    """
    Write a JSON validation report to S3 for audit and trend analysis.

    Path: s3://{bucket}/dq-reports/fact_trips/pickup_date={partition}/report.json
    """
    report = {
        "suite":           "nyc_tlc_gold_suite",
        "partition":       partition,
        "exact_row_count": exact_row_count,
        "run_timestamp":   datetime.now(timezone.utc).isoformat(),
        "results":         results,
        "summary": {
            "total":    len(results),
            "passed":   sum(1 for r in results if r["success"]),
            "failed":   sum(1 for r in results if not r["success"]),
            "critical_failures": [
                r["check_id"] for r in results
                if not r["success"] and r["severity"] == "CRITICAL"
            ],
            "warnings": [
                r["check_id"] for r in results
                if not r["success"] and r["severity"] == "WARNING"
            ],
        },
    }

    key = f"{REPORT_PREFIX}/pickup_date={partition}/report.json"
    body = json.dumps(report, indent=2, default=str)

    if dry_run:
        log.info("[dry-run] Would write report to s3://%s/%s", datalake_bucket, key)
        log.info(body)
        return

    s3 = boto3.client("s3", region_name=region)
    s3.put_object(
        Bucket=datalake_bucket,
        Key=key,
        Body=body.encode(),
        ContentType="application/json",
    )
    log.info("Report written to s3://%s/%s", datalake_bucket, key)


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main() -> int:
    args = parse_args()

    log.info("═══════════════════════════════════════════════════════")
    log.info("NYC TLC Data Quality Runner")
    log.info("  Suite:     %s", SUITE_PATH.name)
    log.info("  Partition: %s", args.partition)
    log.info("  Env:       %s", args.env)
    log.info("  Dry-run:   %s", args.dry_run)
    log.info("═══════════════════════════════════════════════════════")

    # 1. Load suite
    suite = load_suite(SUITE_PATH)
    log.info("Loaded suite '%s' (%d expectations)",
             suite["expectation_suite_name"], len(suite["expectations"]))

    # 2. Connect to Athena and fetch data
    conn            = build_athena_connection(args)
    exact_row_count = get_exact_row_count(conn, args.partition)
    df              = fetch_partition(conn, args.partition)

    if df.empty:
        log.error("Partition pickup_date='%s' returned 0 rows from Athena. "
                  "Check that the dbt run completed successfully.", args.partition)
        return 1

    # 3. Run all expectations
    results = run_suite(df, exact_row_count, suite)

    # 4. Determine outcome
    critical_failures = [
        r for r in results
        if not r["success"] and r["meta"]["severity"] == "CRITICAL"
    ]
    warnings = [
        r for r in results
        if not r["success"] and r["meta"]["severity"] == "WARNING"
    ]

    log.info("─── Summary ───────────────────────────────────────────")
    log.info("  Passed:            %d / %d", sum(r["success"] for r in results), len(results))
    log.info("  Critical failures: %d", len(critical_failures))
    log.info("  Warnings:          %d", len(warnings))

    # 5. Emit CloudWatch metrics for warnings
    emit_cloudwatch_metrics(results, args.partition, args.env, args.region, args.dry_run)

    # 6. Quarantine if any CRITICAL check failed
    exit_code = 0
    if critical_failures:
        log.error("CRITICAL failures: %s",
                  [r["check_id"] for r in critical_failures])
        quarantine_partition(
            args.partition,
            args.datalake_bucket,
            args.region,
            args.dry_run,
        )
        exit_code = 1

    # 7. Write audit report regardless of outcome
    write_report(
        results,
        args.partition,
        exact_row_count,
        args.datalake_bucket,
        args.region,
        args.dry_run,
    )

    if exit_code == 0:
        log.info("✓ All checks passed — partition is healthy")
    else:
        log.error("✗ Partition quarantined — fix upstream issues and re-run")

    return exit_code


if __name__ == "__main__":
    sys.exit(main())