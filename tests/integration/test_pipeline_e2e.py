# tests/integration/test_pipeline_e2e.py
"""Integration test: chạy mini pipeline end-to-end với sample data."""
import pytest
import subprocess
from pathlib import Path

@pytest.mark.integration
def test_full_pipeline_on_sample():
    """Chạy pipeline với 1 tháng sample nhỏ (2023-01 subsample)."""
    result = subprocess.run(
        ["python", "spark_jobs/job1_cleanse.py",
         "--year=2023", "--month=01",
         "--bucket=localstack-bucket"],
        capture_output=True, text=True, timeout=300
    )
    assert result.returncode == 0, f"Cleanse job failed:\n{result.stderr}"

    # Verify output exists
    output_path = Path("/tmp/test-output/silver/yellow")
    assert output_path.exists() or "silver_written" in result.stdout