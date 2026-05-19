"""
downloader.py
Downloads TLC parquet files directly from CloudFront and uploads to S3 bronze.
"""
from __future__ import annotations

import os
from pathlib import Path

import boto3
import requests
import structlog

from src.tlc_catalog import get_url, is_available

log = structlog.get_logger()

CHUNK_SIZE = 8 * 1024 * 1024  # 8 MB


def download_month(
    vehicle_type: str,
    year_month: str,          # "YYYY-MM"
    output_dir: Path,
) -> Path:
    """
    Download a single TLC parquet file for (vehicle_type, year_month).
    Returns the local Path of the downloaded file.
    Raises FileNotFoundError if TLC doesn't publish this combination.
    """
    year, month = int(year_month[:4]), int(year_month[5:7])

    if not is_available(vehicle_type, year, month):
        raise FileNotFoundError(
            f"TLC does not publish {vehicle_type} data for {year_month}"
        )

    url = get_url(vehicle_type, year, month)
    filename = url.split("/")[-1]
    local_path = output_dir / filename

    log.info("downloading_tlc_file", url=url, dest=str(local_path))

    with requests.get(url, stream=True, timeout=120) as resp:
        resp.raise_for_status()
        with open(local_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=CHUNK_SIZE):
                f.write(chunk)

    size_mb = local_path.stat().st_size / (1024 ** 2)
    log.info("download_complete", file=filename, size_mb=round(size_mb, 1))

    return local_path


def upload_to_s3_bronze(
    local_file: Path,
    bucket: str,
    year_month: str,
    vehicle_type: str,
) -> str:
    """
    Upload a local parquet file to s3://<bucket>/bronze/<vehicle_type>/<year_month>/<filename>
    Returns the S3 key.
    """
    s3_key = f"bronze/{vehicle_type}/{year_month}/{local_file.name}"

    log.info("uploading_to_s3", bucket=bucket, key=s3_key)

    boto3.client("s3").upload_file(
        Filename=str(local_file),
        Bucket=bucket,
        Key=s3_key,
        ExtraArgs={"ContentType": "application/octet-stream"},
    )

    log.info("s3_upload_complete", s3_uri=f"s3://{bucket}/{s3_key}")
    return s3_key