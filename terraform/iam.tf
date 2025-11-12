# IAM Role and Policy for Snowflake S3 Access
# This role allows Snowflake to read data from the S3 bucket

# IAM Role for Snowflake
resource "aws_iam_role" "snowflake_s3_access" {
  name        = "${var.project_name}-snowflake-s3-access-${var.environment}"
  description = "IAM role for Snowflake to access S3 data lake"

  # Trust relationship allowing Snowflake AWS account
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          AWS = "arn:aws:iam::${var.snowflake_account_id}:root"
        }
        Action = "sts:AssumeRole"
        Condition = {
          StringEquals = {
            "sts:ExternalId" = var.snowflake_external_id
          }
        }
      }
    ]
  })

  tags = merge(
    var.tags,
    {
      Name        = "${var.project_name}-snowflake-s3-access-${var.environment}"
      Description = "Snowflake S3 access role"
    }
  )
}

# IAM Policy for S3 Access
resource "aws_iam_policy" "snowflake_s3_policy" {
  name        = "${var.project_name}-snowflake-s3-policy-${var.environment}"
  description = "Policy granting Snowflake read access to S3 data lake"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "AllowListBucket"
        Effect = "Allow"
        Action = [
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ]
        Resource = aws_s3_bucket.data_lake.arn
      },
      {
        Sid    = "AllowGetObject"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:GetObjectVersion"
        ]
        Resource = "${aws_s3_bucket.data_lake.arn}/*"
      }
    ]
  })

  tags = merge(
    var.tags,
    {
      Name        = "${var.project_name}-snowflake-s3-policy-${var.environment}"
      Description = "Snowflake S3 access policy"
    }
  )
}

# Attach Policy to Role
resource "aws_iam_role_policy_attachment" "snowflake_s3_attach" {
  role       = aws_iam_role.snowflake_s3_access.name
  policy_arn = aws_iam_policy.snowflake_s3_policy.arn
}
