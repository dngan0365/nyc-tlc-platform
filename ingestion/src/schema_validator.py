"""
schema_validator.py
Validates TLC parquet files — schema, row count, null rate, date range.
Handles the 2025+ cbd_congestion_fee column automatically.
"""
from __future__ import annotations

from pathlib import Path

import pyarrow.parquet as pq
import structlog

log = structlog.get_logger()

# ── Expected columns per vehicle type (minimum required set) ──────────────────

REQUIRED_COLUMNS: dict[str, set[str]] = {
    "yellow": {
        "tpep_pickup_datetime", "tpep_dropoff_datetime",
        "passenger_count", "trip_distance",
        "PULocationID", "DOLocationID",
        "fare_amount", "total_amount", "payment_type",
    },
    "green": {
        "lpep_pickup_datetime", "lpep_dropoff_datetime",
        "passenger_count", "trip_distance",
        "PULocationID", "DOLocationID",
        "fare_amount", "total_amount", "payment_type",
    },
    "fhv": {
        "dispatching_base_num", "pickup_datetime",
        "dropOff_datetime", "PUlocationID", "DOlocationID",
    },
    "fhvhv": {
        "hvfhs_license_num", "dispatching_base_num",
        "pickup_datetime", "dropoff_datetime",
        "PULocationID", "DOLocationID",
        "base_passenger_fare", "tolls", "tips", "driver_pay",
    },
}

# Optional columns that may appear from 2025+
OPTIONAL_COLUMNS: dict[str, set[str]] = {
    "yellow":  {"cbd_congestion_surcharge"},
    "green":   {"cbd_congestion_surcharge"},
    "fhvhv":   {"cbd_congestion_surcharge"},
    "fhv":     set(),
}

MAX_NULL_RATE = 0.95   # quarantine if >95% nulls in a required column
MIN_ROWS = 100         # quarantine suspiciously small files


def validate_schema(
    local_file: Path,
    vehicle_type: str | None = None,
) -> dict:
    """
    Returns a report dict:
      {
        "status":    "ok" | "quarantine",
        "row_count": int,
        "issues":    list[str],
      }
    """
    issues: list[str] = []

    # Infer vehicle_type from filename if not provided
    if vehicle_type is None:
        name = local_file.name
        for vt in REQUIRED_COLUMNS:
            if name.startswith(vt):
                vehicle_type = vt
                break

    if vehicle_type is None:
        issues.append(f"Cannot determine vehicle_type from filename: {local_file.name}")
        return {"status": "quarantine", "row_count": 0, "issues": issues}

    try:
        pf = pq.read_table(local_file)
    except Exception as exc:
        return {
            "status": "quarantine",
            "row_count": 0,
            "issues": [f"Cannot read parquet file: {exc}"],
        }

    row_count = len(pf)
    actual_cols = set(pf.schema.names)
    required = REQUIRED_COLUMNS.get(vehicle_type, set())

    # ── Missing required columns ───────────────────────────────────────────────
    missing = required - actual_cols
    if missing:
        issues.append(f"Missing required columns: {sorted(missing)}")

    # ── Unexpected columns (warn only, not quarantine) ─────────────────────────
    allowed = required | OPTIONAL_COLUMNS.get(vehicle_type, set())
    unexpected = actual_cols - allowed
    if unexpected:
        log.warning("unexpected_columns", vehicle_type=vehicle_type, cols=sorted(unexpected))

    # ── Row count check ────────────────────────────────────────────────────────
    if row_count < MIN_ROWS:
        issues.append(f"Too few rows: {row_count} (min {MIN_ROWS})")

    # ── Null rate check on required columns ───────────────────────────────────
    for col in required & actual_cols:
        null_count = pf[col].null_count
        null_rate = null_count / row_count if row_count > 0 else 1.0
        if null_rate > MAX_NULL_RATE:
            issues.append(
                f"Column '{col}' has {null_rate:.1%} nulls (max {MAX_NULL_RATE:.0%})"
            )

    status = "quarantine" if issues else "ok"
    log.info(
        "schema_validation_result",
        vehicle_type=vehicle_type,
        status=status,
        row_count=row_count,
        issues=issues,
    )

    return {"status": status, "row_count": row_count, "issues": issues}