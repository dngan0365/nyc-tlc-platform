import json
from pathlib import Path

OUTPUTS_FILE = Path("infra/terraform/generated/outputs.json")
ENV_OUTPUT = Path(".env.generated")

with open(OUTPUTS_FILE, "r", encoding="utf-8") as f:
    outputs = json.load(f)

def v(key):
    return outputs[key]["value"]

mapping = {
    # ── EMR Serverless ────────────────────────────────────────────────────────
    "EMR_APPLICATION_ID":       v("emr_application_id"),
    "EMR_EXECUTION_ROLE_ARN":   v("emr_execution_role_arn"),

    # ── S3 ───────────────────────────────────────────────────────────────────
    "TLC_S3_BUCKET":            v("datalake_bucket_name"),
    "ATHENA_RESULTS_BUCKET":    v("athena_results_bucket_name"),
    "EMR_LOGS_BUCKET":          v("emr_logs_bucket_name"),

    # ── Athena / dbt ─────────────────────────────────────────────────────────
    "DBT_ATHENA_WORKGROUP":     v("athena_workgroup_name"),
    "DBT_S3_STAGING_DIR":       v("athena_results_s3_path"),

    # ── Glue databases ───────────────────────────────────────────────────────
    "GLUE_DATABASE_BRONZE":     v("glue_database_bronze"),
    "GLUE_DATABASE_SILVER":     v("glue_database_silver"),
    "GLUE_DATABASE_GOLD":       v("glue_database_gold"),

    # ── Networking (used by EMR operator if it needs subnet/VPC context) ─────
    "VPC_ID":                   v("vpc_id"),
    "PRIVATE_SUBNET_IDS":       ",".join(v("private_subnet_ids")),
}

with open(ENV_OUTPUT, "w", encoding="utf-8") as f:
    f.write("# Auto-generated from terraform outputs — do not edit manually\n\n")
    for key, value in mapping.items():
        f.write(f"{key}={value}\n")

print(f"Generated {ENV_OUTPUT} ({len(mapping)} variables)")
print()
for k, val in mapping.items():
    print(f"  {k}={val}")