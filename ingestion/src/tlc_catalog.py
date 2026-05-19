"""
tlc_catalog.py
Knows which vehicle types are actually published for each year-month,
based on the live TLC Trip Record Data page.
URL pattern: https://d37ci6vzurychx.cloudfront.net/trip-data/{type}_tripdata_{YYYY-MM}.parquet
"""
from __future__ import annotations

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

# Vehicle type → parquet filename prefix
VEHICLE_PREFIX = {
    "yellow": "yellow_tripdata",
    "green":  "green_tripdata",
    "fhv":    "fhv_tripdata",
    "fhvhv":  "fhvhv_tripdata",
}

# Months where a vehicle type is NOT published.
# Everything NOT listed here is assumed available.
# Format: (year, month) → set of unavailable types
_UNAVAILABLE: dict[tuple[int, int], set[str]] = {
    # 2026 — only fhvhv Jan published so far (as of May 2026)
    (2026, 1):  {"yellow", "green", "fhv"},
    (2026, 2):  {"yellow", "green", "fhv", "fhvhv"},
    (2026, 3):  {"yellow", "green", "fhv", "fhvhv"},
    (2026, 4):  {"yellow", "green", "fhv", "fhvhv"},
    (2026, 5):  {"yellow", "green", "fhv", "fhvhv"},
    (2026, 6):  {"yellow", "green", "fhv", "fhvhv"},
    (2026, 7):  {"yellow", "green", "fhv", "fhvhv"},
    (2026, 8):  {"yellow", "green", "fhv", "fhvhv"},
    (2026, 9):  {"yellow", "green", "fhv", "fhvhv"},
    (2026, 10): {"yellow", "green", "fhv", "fhvhv"},
    (2026, 11): {"yellow", "green", "fhv", "fhvhv"},
    (2026, 12): {"yellow", "green", "fhv", "fhvhv"},
    # 2025 Dec — yellow/green not yet published
    (2025, 12): {"yellow", "green"},
}

# Columns added from 2025-01 onwards
CONGESTION_FEE_TYPES = {"yellow", "green", "fhvhv"}
CONGESTION_FEE_START = (2025, 1)


def is_available(vehicle_type: str, year: int, month: int) -> bool:
    """Return True if TLC publishes this vehicle_type for the given year/month."""
    return vehicle_type not in _UNAVAILABLE.get((year, month), set())


def get_url(vehicle_type: str, year: int, month: int) -> str:
    prefix = VEHICLE_PREFIX[vehicle_type]
    return f"{BASE_URL}/{prefix}_{year}-{month:02d}.parquet"


def has_congestion_fee(vehicle_type: str, year: int, month: int) -> bool:
    return (
        vehicle_type in CONGESTION_FEE_TYPES
        and (year, month) >= CONGESTION_FEE_START
    )