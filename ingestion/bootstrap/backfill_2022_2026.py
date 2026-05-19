"""
backfill_2025_2026.py
One-shot script: stream all available TLC data for 2025–2026 directly
from TLC CloudFront into S3 bronze. Skips months already present.

Usage:
    export TLC_S3_BUCKET=my-bucket
    python bootstrap/backfill_2025_2026.py
    python bootstrap/backfill_2025_2026.py --dry-run
    python bootstrap/backfill_2025_2026.py --vehicle-types yellow green fhvhv
    python bootstrap/backfill_2025_2026.py --years 2025
"""
from __future__ import annotations

import argparse
import io
import os
from datetime import date
from dotenv import load_dotenv
import boto3
import requests
import structlog
from pathlib import Path

from ingestion.src.tlc_catalog import get_url, is_available, VEHICLE_PREFIX
log = structlog.get_logger()

ENV_PATH = Path(__file__).resolve().parents[2] / ".env.generated"
load_dotenv(dotenv_path=ENV_PATH)

BUCKET = os.getenv("TLC_S3_BUCKET")
CHUNK_SIZE = 8 * 1024 * 1024   # 8 MB
ALL_TYPES  = list(VEHICLE_PREFIX.keys())  # yellow, green, fhv, fhvhv


# ── Helpers ───────────────────────────────────────────────────────────────────

def iter_year_months(years: list[int]) -> list[tuple[int, int]]:
    today = date.today()
    result = []
    for year in years:
        for month in range(1, 13):
            if date(year, month, 1) > today:
                break
            result.append((year, month))
    return result


def bronze_prefix(vehicle_type: str, year: int, month: int) -> str:
    return f"bronze/{vehicle_type}/{year}-{month:02d}/"


def already_exists(s3_client, vehicle_type: str, year: int, month: int) -> bool:
    prefix = bronze_prefix(vehicle_type, year, month)
    resp = s3_client.list_objects_v2(Bucket=BUCKET, Prefix=prefix, MaxKeys=1)
    return resp.get("KeyCount", 0) > 0


def stream_to_s3(
    s3_client,
    url: str,
    bucket: str,
    s3_key: str,
) -> int:
    """
    Stream a URL directly into S3 using multipart upload.
    Returns bytes transferred.
    """
    mpu = s3_client.create_multipart_upload(Bucket=bucket, Key=s3_key)
    upload_id = mpu["UploadId"]
    parts = []
    part_number = 1
    total_bytes = 0

    try:
        with requests.get(url, stream=True, timeout=300) as resp:
            resp.raise_for_status()
            buf = io.BytesIO()

            for chunk in resp.iter_content(chunk_size=CHUNK_SIZE):
                buf.write(chunk)
                total_bytes += len(chunk)

                if buf.tell() >= CHUNK_SIZE:
                    buf.seek(0)
                    part = s3_client.upload_part(
                        Bucket=bucket, Key=s3_key,
                        UploadId=upload_id, PartNumber=part_number,
                        Body=buf.read(),
                    )
                    parts.append({"PartNumber": part_number, "ETag": part["ETag"]})
                    part_number += 1
                    buf = io.BytesIO()

            # Final part
            remaining = buf.tell()
            if remaining > 0:
                buf.seek(0)
                part = s3_client.upload_part(
                    Bucket=bucket, Key=s3_key,
                    UploadId=upload_id, PartNumber=part_number,
                    Body=buf.read(remaining),
                )
                parts.append({"PartNumber": part_number, "ETag": part["ETag"]})

        s3_client.complete_multipart_upload(
            Bucket=bucket, Key=s3_key,
            UploadId=upload_id,
            MultipartUpload={"Parts": parts},
        )

    except Exception as exc:
        s3_client.abort_multipart_upload(
            Bucket=bucket, Key=s3_key, UploadId=upload_id
        )
        raise exc

    return total_bytes


# ── Main ──────────────────────────────────────────────────────────────────────

def backfill(
    vehicle_types: list[str],
    years: list[int],
    dry_run: bool = False,
) -> None:
    s3 = boto3.client("s3")
    year_months = iter_year_months(years)

    uploaded, skipped_exists, skipped_unavailable, failed = [], [], [], []

    for vehicle_type in vehicle_types:
        for year, month in year_months:
            year_month = f"{year}-{month:02d}"
            logger = log.bind(vehicle_type=vehicle_type, year_month=year_month)

            # TLC doesn't publish this type/month
            if not is_available(vehicle_type, year, month):
                logger.info("not_published_by_tlc__skipping")
                skipped_unavailable.append((vehicle_type, year_month))
                continue

            # Already in S3 bronze
            if already_exists(s3, vehicle_type, year, month):
                logger.info("already_in_bronze__skipping")
                skipped_exists.append((vehicle_type, year_month))
                continue

            url = get_url(vehicle_type, year, month)
            filename = url.split("/")[-1]
            s3_key = f"bronze/{vehicle_type}/{year_month}/{filename}"

            if dry_run:
                logger.info("dry_run__would_upload", url=url, s3_key=s3_key)
                uploaded.append((vehicle_type, year_month))
                continue

            try:
                logger.info("streaming_to_s3", url=url, s3_key=s3_key)
                total = stream_to_s3(s3, url, BUCKET, s3_key)
                logger.info(
                    "upload_complete",
                    s3_uri=f"s3://{BUCKET}/{s3_key}",
                    mb=round(total / 1024 ** 2, 1),
                )
                uploaded.append((vehicle_type, year_month))

            except Exception as exc:
                logger.exception("upload_failed", error=str(exc))
                failed.append((vehicle_type, year_month, str(exc)))

    # ── Summary ───────────────────────────────────────────────────────────────
    print("\n" + "=" * 65)
    print(f"{'[DRY RUN] ' if dry_run else ''}TLC BACKFILL SUMMARY  ({', '.join(str(y) for y in years)})")
    print("=" * 65)
    print(f"  ✅  Uploaded            : {len(uploaded)}")
    print(f"  ⏭️   Already in bronze   : {len(skipped_exists)}")
    print(f"  🚫  Not published by TLC: {len(skipped_unavailable)}")
    print(f"  ❌  Failed              : {len(failed)}")

    if failed:
        print("\nFailed:")
        for vt, ym, err in failed:
            print(f"    {vt}/{ym}  →  {err}")
        raise SystemExit(1)


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--vehicle-types", nargs="+", default=["yellow", "green"],
                        choices=ALL_TYPES, help="Vehicle types to backfill")
    parser.add_argument("--years", nargs="+", type=int, default=[2022, 2023, 2024, 2025, 2026])
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    backfill(
        vehicle_types=args.vehicle_types,
        years=args.years,
        dry_run=args.dry_run,
    )