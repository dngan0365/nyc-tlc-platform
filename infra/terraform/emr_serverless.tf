# ============================================================================
# DATA
# ============================================================================

data "aws_caller_identity" "current" {}

# ============================================================================
# S3 — DATALAKE
# ============================================================================

resource "aws_s3_bucket" "datalake" {
  bucket = "nyc-tlc-datalake-${var.env}-${data.aws_caller_identity.current.account_id}"

  tags = {
    Name        = "nyc-tlc-datalake"
    Environment = var.env
  }
}

resource "aws_s3_bucket_versioning" "datalake" {
  bucket = aws_s3_bucket.datalake.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "datalake" {
  bucket = aws_s3_bucket.datalake.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# ============================================================================
# S3 — EMR LOGS
# ============================================================================

resource "aws_s3_bucket" "emr_logs" {
  bucket = "nyc-tlc-emr-logs-${var.env}-${data.aws_caller_identity.current.account_id}"

  tags = {
    Name        = "nyc-tlc-emr-logs"
    Environment = var.env
  }
}

resource "aws_s3_bucket_versioning" "emr_logs" {
  bucket = aws_s3_bucket.emr_logs.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "emr_logs" {
  bucket = aws_s3_bucket.emr_logs.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Optional — auto cleanup logs after 30 days
resource "aws_s3_bucket_lifecycle_configuration" "emr_logs" {
  bucket = aws_s3_bucket.emr_logs.id

  rule {
    id     = "cleanup-old-logs"
    status = "Enabled"

    expiration {
      days = 30
    }
  }
}

# ============================================================================
# SECURITY GROUP
# ============================================================================

resource "aws_security_group" "emr_serverless" {
  name        = "nyc-tlc-emr-serverless-${var.env}"
  description = "EMR Serverless workers"
  vpc_id      = aws_vpc.main.id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "nyc-tlc-emr-serverless"
  }
}

# ============================================================================
# S3 VPC ENDPOINT (IMPORTANT)
# ============================================================================

resource "aws_vpc_endpoint" "s3" {
  vpc_id            = aws_vpc.main.id
  service_name      = "com.amazonaws.${var.aws_region}.s3"
  vpc_endpoint_type = "Gateway"

  route_table_ids = [
    aws_route_table.private.id
  ]

  tags = {
    Name = "s3-endpoint"
  }
}

# ============================================================================
# IAM ROLE — EMR EXECUTION
# ============================================================================

resource "aws_iam_role" "emr_execution" {
  name = "nyc-tlc-emr-execution-${var.env}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"

    Statement = [
      {
        Effect = "Allow"

        Principal = {
          Service = "emr-serverless.amazonaws.com"
        }

        Action = "sts:AssumeRole"

        Condition = {
          StringEquals = {
            "aws:SourceAccount" = data.aws_caller_identity.current.account_id
          }

          ArnLike = {
            "aws:SourceArn" = "arn:aws:emr-serverless:${var.aws_region}:${data.aws_caller_identity.current.account_id}:/applications/*"
          }
        }
      }
    ]
  })
}

# ============================================================================
# IAM POLICY — EMR EXECUTION
# ============================================================================

resource "aws_iam_role_policy" "emr_execution_policy" {
  name = "nyc-tlc-emr-execution-policy"
  role = aws_iam_role.emr_execution.id

  policy = jsonencode({
    Version = "2012-10-17"

    Statement = [

      # ----------------------------------------------------------------------
      # DATALAKE ACCESS
      # ----------------------------------------------------------------------

      {
        Sid    = "DatalakeAccess"
        Effect = "Allow"

        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]

        Resource = [
          aws_s3_bucket.datalake.arn,
          "${aws_s3_bucket.datalake.arn}/*"
        ]
      },

      # ----------------------------------------------------------------------
      # EMR LOGGING
      # ----------------------------------------------------------------------

      {
        Sid    = "EMRLogging"
        Effect = "Allow"

        Action = [
          "s3:PutObject",
          "s3:GetObject",
          "s3:ListBucket"
        ]

        Resource = [
          aws_s3_bucket.emr_logs.arn,
          "${aws_s3_bucket.emr_logs.arn}/*"
        ]
      },

      # ----------------------------------------------------------------------
      # GLUE CATALOG
      # ----------------------------------------------------------------------

      {
        Sid    = "GlueCatalog"
        Effect = "Allow"

        Action = [
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:GetTable",
          "glue:GetTables",
          "glue:CreateTable",
          "glue:UpdateTable",
          "glue:GetPartition",
          "glue:GetPartitions",
          "glue:CreatePartition",
          "glue:BatchCreatePartition"
        ]

        Resource = "*"
      },

      # ----------------------------------------------------------------------
      # CLOUDWATCH LOGS (OPTIONAL BUT RECOMMENDED)
      # ----------------------------------------------------------------------

      {
        Sid    = "CloudWatchLogs"
        Effect = "Allow"

        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]

        Resource = "*"
      }
    ]
  })
}

# ============================================================================
# EMR SERVERLESS APPLICATION
# ============================================================================

resource "aws_emrserverless_application" "spark" {
  name          = "nyc-tlc-spark-${var.env}"
  release_label = "emr-7.1.0"
  type          = "SPARK"

  # --------------------------------------------------------------------------
  # PRE-INITIALIZED CAPACITY
  # --------------------------------------------------------------------------

  initial_capacity {
    initial_capacity_type = "Driver"

    initial_capacity_config {
      worker_count = 1

      worker_configuration {
        cpu    = "4 vCPU"
        memory = "16 GB"
        disk   = "20 GB"
      }
    }
  }

  initial_capacity {
    initial_capacity_type = "Executor"

    initial_capacity_config {
      worker_count = 2

      worker_configuration {
        cpu    = "4 vCPU"
        memory = "16 GB"
        disk   = "20 GB"
      }
    }
  }

  # --------------------------------------------------------------------------
  # MAXIMUM CAPACITY
  # --------------------------------------------------------------------------

  maximum_capacity {
    cpu    = "12 vCPU"
    memory = "48 GB"
    disk   = "60 GB"
  }

  # --------------------------------------------------------------------------
  # AUTO STOP
  # --------------------------------------------------------------------------

  auto_stop_configuration {
    enabled              = true
    idle_timeout_minutes = 15
  }

  # --------------------------------------------------------------------------
  # NETWORK
  # --------------------------------------------------------------------------

  network_configuration {
    subnet_ids = [
      aws_subnet.private_a.id,
      aws_subnet.private_b.id
    ]

    security_group_ids = [
      aws_security_group.emr_serverless.id
    ]
  }

  tags = {
    Project     = "nyc-tlc-platform"
    Environment = var.env
    ManagedBy   = "terraform"
  }
}