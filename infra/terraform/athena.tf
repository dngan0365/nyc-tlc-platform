# ============================================================================
# S3 — ATHENA QUERY RESULTS
# ============================================================================
#
# Dedicated bucket for Athena output so result data is separated from the
# datalake zones and lifecycle-cleaned independently.
# ============================================================================

resource "aws_s3_bucket" "athena_results" {
  bucket = "nyc-tlc-athena-results-${var.env}-${data.aws_caller_identity.current.account_id}"

  tags = {
    Name        = "nyc-tlc-athena-results"
    Environment = var.env
    ManagedBy   = "terraform"
  }
}

resource "aws_s3_bucket_versioning" "athena_results" {
  bucket = aws_s3_bucket.athena_results.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "athena_results" {
  bucket = aws_s3_bucket.athena_results.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Query results are transient — expire after 7 days to control storage costs.
resource "aws_s3_bucket_lifecycle_configuration" "athena_results" {
  bucket = aws_s3_bucket.athena_results.id

  rule {
    id     = "expire-query-results"
    status = "Enabled"

    expiration {
      days = 7
    }

    # Also remove incomplete multipart uploads (Athena large results)
    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }
}

# Block all public access — Athena results must never be public.
resource "aws_s3_bucket_public_access_block" "athena_results" {
  bucket = aws_s3_bucket.athena_results.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ============================================================================
# ATHENA WORKGROUP
# ============================================================================
#
# One workgroup per environment so dev/staging/prod queries are isolated,
# cost-tagged separately, and can have different data-scan limits.
#
# Key settings:
#   output_location        — all results land in the dedicated bucket above
#   encrypt_configuration  — SSE_S3 on every result file
#   bytes_scanned_cutoff   — hard-kill queries scanning > 10 GB (cost guardrail)
#   enforce_workgroup_config — clients cannot override these settings
#   publish_cloudwatch     — enables the Athena metrics dashboard in CloudWatch
# ============================================================================

resource "aws_athena_workgroup" "main" {
  name        = "nyc-tlc-${var.env}"
  description = "Primary Athena workgroup for dbt, Superset, and ad-hoc queries (${var.env})"
  state       = "ENABLED"

  configuration {
    # -----------------------------------------------------------------------
    # OUTPUT & ENCRYPTION
    # -----------------------------------------------------------------------

    result_configuration {
      output_location = "s3://${aws_s3_bucket.athena_results.bucket}/results/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }

    # -----------------------------------------------------------------------
    # COST GUARDRAILS
    # -----------------------------------------------------------------------

    # Hard limit: kill any query that would scan more than 10 GB.
    # Adjust upward for production if full-table scans are expected.
    bytes_scanned_cutoff_per_query = 10737418240 # 10 GB in bytes

    # Prevent clients (dbt, Superset, boto3) from overriding workgroup settings.
    enforce_workgroup_configuration = true

    # -----------------------------------------------------------------------
    # OBSERVABILITY
    # -----------------------------------------------------------------------

    publish_cloudwatch_metrics_enabled = true

    # -----------------------------------------------------------------------
    # ENGINE VERSION
    # -----------------------------------------------------------------------

    engine_version {
      selected_engine_version = "Athena engine version 3"
    }
  }

  tags = {
    Project     = "nyc-tlc-platform"
    Environment = var.env
    ManagedBy   = "terraform"
  }
}

# ============================================================================
# IAM — ATHENA EXECUTION POLICY (attached to existing EMR role)
# ============================================================================
#
# The EMR execution role already covers S3 + Glue. This inline policy
# adds the Athena permissions needed for the RAG app and eval harness
# (which submit queries via boto3 using the same role).
#
# If your RAG app runs under a separate IAM identity, attach this policy
# to that role/user instead and remove it from emr_execution.
# ============================================================================

resource "aws_iam_role_policy" "athena_access" {
  name = "nyc-tlc-athena-access"
  role = aws_iam_role.emr_execution.id

  policy = jsonencode({
    Version = "2012-10-17"

    Statement = [

      # ----------------------------------------------------------------------
      # ATHENA QUERY EXECUTION
      # ----------------------------------------------------------------------

      {
        Sid    = "AthenaQueryExecution"
        Effect = "Allow"

        Action = [
          "athena:StartQueryExecution",
          "athena:StopQueryExecution",
          "athena:GetQueryExecution",
          "athena:GetQueryResults",
          "athena:GetWorkGroup",
          "athena:ListWorkGroups",
          "athena:ListQueryExecutions",
          "athena:BatchGetQueryExecution"
        ]

        Resource = [
          aws_athena_workgroup.main.arn
        ]
      },

      # ----------------------------------------------------------------------
      # ATHENA RESULTS BUCKET
      # ----------------------------------------------------------------------

      {
        Sid    = "AthenaResultsBucket"
        Effect = "Allow"

        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ]

        Resource = [
          aws_s3_bucket.athena_results.arn,
          "${aws_s3_bucket.athena_results.arn}/*"
        ]
      }
    ]
  })
}

# ============================================================================
# OUTPUTS (consumed by outputs.tf — referenced here for clarity)
# ============================================================================

# See outputs.tf for:
#   output "athena_workgroup_name"
#   output "athena_results_bucket"
#   output "glue_database_bronze"
#   output "glue_database_silver"
#   output "glue_database_gold"
