import os

from airflow.models import Variable

mapping = {
    "TLC_S3_BUCKET": os.getenv("TLC_S3_BUCKET"),
    "EMR_APPLICATION_ID": os.getenv("EMR_APPLICATION_ID"),
    "EMR_EXECUTION_ROLE_ARN": os.getenv("EMR_EXECUTION_ROLE_ARN"),
}

for airflow_var, value in mapping.items():

    if value is None:
        raise ValueError(
            f"Missing environment variable: {airflow_var}"
        )

    Variable.set(airflow_var, value)

    print(
        f"[OK] Set Airflow Variable: "
        f"{airflow_var} = {value}"
    )

print("\nEnvironment variables synced to Airflow Variables.")