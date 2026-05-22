# ============================================================================
# GLUE DATA CATALOG — DATABASES
# ============================================================================
#
# Three databases mirror the medallion zones in S3:
#   bronze  — raw parquet landed by the ingestion layer
#   silver  — cleansed + deduplicated data from job1_cleanse.py
#   gold    — enriched, aggregated, query-ready tables from job2/3
#
# EMR Serverless writes Delta/Parquet to S3 and registers partitions here.
# Athena and dbt read from these databases via the Glue catalog.
# ============================================================================

resource "aws_glue_catalog_database" "bronze" {
  name        = "nyc_tlc_bronze"
  description = "Raw TLC parquet files landed from the ingestion layer (bronze zone)"

  location_uri = "s3://${aws_s3_bucket.datalake.bucket}/bronze/"

  tags = {
    Project     = "nyc-tlc-platform"
    Environment = var.env
    Zone        = "bronze"
    ManagedBy   = "terraform"
  }
}

resource "aws_glue_catalog_database" "silver" {
  name        = "nyc_tlc_silver"
  description = "Cleansed and deduplicated TLC data produced by job1_cleanse.py (silver zone)"

  location_uri = "s3://${aws_s3_bucket.datalake.bucket}/silver/"

  tags = {
    Project     = "nyc-tlc-platform"
    Environment = var.env
    Zone        = "silver"
    ManagedBy   = "terraform"
  }
}

resource "aws_glue_catalog_database" "gold" {
  name        = "nyc_tlc_gold"
  description = "Enriched, aggregated, query-ready tables produced by job2/3 (gold zone)"

  location_uri = "s3://${aws_s3_bucket.datalake.bucket}/gold/"

  tags = {
    Project     = "nyc-tlc-platform"
    Environment = var.env
    Zone        = "gold"
    ManagedBy   = "terraform"
  }
}

# ============================================================================
# GLUE CATALOG — RESOURCE POLICY (optional hardening)
# ============================================================================
#
# Restricts cross-account Glue catalog access to this account only.
# Safe to remove if you don't need the extra guardrail.
# ============================================================================

data "aws_iam_policy_document" "glue_catalog_policy" {
  statement {
    sid    = "DenyExternalAccounts"
    effect = "Deny"

    principals {
      type        = "*"
      identifiers = ["*"]
    }

    actions = ["glue:*"]

    resources = [
      "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:catalog",
      "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:database/*",
      "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:table/*/*",
    ]

    condition {
      test     = "StringNotEquals"
      variable = "aws:PrincipalAccount"
      values   = [data.aws_caller_identity.current.account_id]
    }
  }
}

resource "aws_glue_resource_policy" "catalog" {
  policy = data.aws_iam_policy_document.glue_catalog_policy.json
}
resource "aws_iam_role_policy" "emr_glue" {
  name = "emr-glue-policy"
  role = aws_iam_role.emr_execution.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "glue:GetDatabase",
          "glue:CreateDatabase",
          "glue:GetTable",
          "glue:CreateTable",
          "glue:UpdateTable",
          "glue:GetPartitions",
          "glue:CreatePartition",
          "glue:BatchCreatePartition",
        ]
        Resource = [
          "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:catalog",

          "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:database/nyc_tlc_silver",
          "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:table/nyc_tlc_silver/*",

          "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:database/nyc_tlc_gold",
          "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:table/nyc_tlc_gold/*"
        ]
      }
    ]
  })
}