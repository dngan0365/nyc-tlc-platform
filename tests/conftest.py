# tests/conftest.py
"""
Shared pytest configuration.
 
Sets dummy AWS credentials before any test runs so that moto intercepts
boto3 calls instead of looking for real credentials in ~/.aws or env vars.
Without these, moto raises NoCredentialsError even though no real AWS
calls are made.
"""
 
import os
 
 
def pytest_configure(config):
    """Set fake AWS credentials globally for all tests."""
    os.environ.setdefault("AWS_ACCESS_KEY_ID",     "testing")
    os.environ.setdefault("AWS_SECRET_ACCESS_KEY",  "testing")
    os.environ.setdefault("AWS_SECURITY_TOKEN",     "testing")
    os.environ.setdefault("AWS_SESSION_TOKEN",      "testing")
    os.environ.setdefault("AWS_DEFAULT_REGION",     "ap-southeast-1")
 
    # Prevent MLflow from trying to connect to a real server during tests
    os.environ.setdefault("MLFLOW_TRACKING_URI",    "sqlite:///mlflow_test.db")
 