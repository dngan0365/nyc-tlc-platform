# ============================================================================
# OUTPUTS
# ============================================================================
#
# Consumed by:
#   - dbt profiles.yml         (workgroup, database names)
#   - Airflow variables        (application_id, role_arn, bucket_name)
#   - RAG app / eval harness   (workgroup, results bucket)
#   - CI/CD scripts            (bucket names for artifact upload)
# ============================================================================

# ----------------------------------------------------------------------------
# S3
# ----------------------------------------------------------------------------

output "datalake_bucket_name" {
  description = "Name of the primary datalake S3 bucket (bronze/silver/gold zones)"
  value       = aws_s3_bucket.datalake.bucket
}

output "emr_logs_bucket_name" {
  description = "Name of the EMR Serverless log bucket"
  value       = aws_s3_bucket.emr_logs.bucket
}

output "athena_results_bucket_name" {
  description = "Name of the Athena query-results bucket"
  value       = aws_s3_bucket.athena_results.bucket
}

# ----------------------------------------------------------------------------
# EMR SERVERLESS
# ----------------------------------------------------------------------------

output "emr_application_id" {
  description = "EMR Serverless application ID — used by the Airflow operator"
  value       = aws_emrserverless_application.spark.id
}

output "emr_execution_role_arn" {
  description = "ARN of the IAM role passed to each EMR job run"
  value       = aws_iam_role.emr_execution.arn
}

# ----------------------------------------------------------------------------
# GLUE
# ----------------------------------------------------------------------------

output "glue_database_bronze" {
  description = "Glue catalog database name for the bronze zone"
  value       = aws_glue_catalog_database.bronze.name
}

output "glue_database_silver" {
  description = "Glue catalog database name for the silver zone"
  value       = aws_glue_catalog_database.silver.name
}

output "glue_database_gold" {
  description = "Glue catalog database name for the gold zone"
  value       = aws_glue_catalog_database.gold.name
}

# ----------------------------------------------------------------------------
# ATHENA
# ----------------------------------------------------------------------------

output "athena_workgroup_name" {
  description = "Athena workgroup name — set as s3_staging_dir in dbt profiles.yml"
  value       = aws_athena_workgroup.main.name
}

output "athena_results_s3_path" {
  description = "S3 URI for Athena query results (use as s3_staging_dir in PyAthena)"
  value       = "s3://${aws_s3_bucket.athena_results.bucket}/results/"
}

# ----------------------------------------------------------------------------
# NETWORK
# ----------------------------------------------------------------------------

output "vpc_id" {
  description = "VPC ID"
  value       = aws_vpc.main.id
}

output "private_subnet_ids" {
  description = "Private subnet IDs used by EMR Serverless workers"
  value       = [aws_subnet.private_a.id, aws_subnet.private_b.id]
}
