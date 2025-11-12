# S3 Bucket Configuration for Snowflake Data Lake
# This bucket stores customer and transaction data for ingestion into Snowflake

# S3 Bucket
resource "aws_s3_bucket" "data_lake" {
  bucket = "${var.project_name}-data-${var.environment}"

  tags = merge(
    var.tags,
    {
      Name        = "${var.project_name}-data-${var.environment}"
      Description = "Data lake bucket for Snowflake Customer 360 Analytics"
    }
  )
}

# Enable Versioning
resource "aws_s3_bucket_versioning" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  versioning_configuration {
    status = "Enabled"
  }
}

# Enable Server-Side Encryption (SSE-S3)
resource "aws_s3_bucket_server_side_encryption_configuration" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
    bucket_key_enabled = true
  }
}

# Block All Public Access
resource "aws_s3_bucket_public_access_block" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Lifecycle Policy - Transition to Glacier after 90 days
resource "aws_s3_bucket_lifecycle_configuration" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  rule {
    id     = "archive-old-data"
    status = "Enabled"

    transition {
      days          = 90
      storage_class = "GLACIER"
    }

    filter {
      prefix = ""
    }
  }

  rule {
    id     = "delete-old-versions"
    status = "Enabled"

    noncurrent_version_transition {
      noncurrent_days = 30
      storage_class   = "GLACIER"
    }

    noncurrent_version_expiration {
      noncurrent_days = 365
    }

    filter {
      prefix = ""
    }
  }
}

# Create folder structure using S3 objects with trailing slashes
# Note: S3 doesn't have true folders, but objects with trailing slashes
# act as folder markers for UI and organizational purposes

resource "aws_s3_object" "customers_folder" {
  bucket  = aws_s3_bucket.data_lake.id
  key     = "customers/"
  content = ""

  depends_on = [aws_s3_bucket.data_lake]
}

resource "aws_s3_object" "transactions_folder" {
  bucket  = aws_s3_bucket.data_lake.id
  key     = "transactions/"
  content = ""

  depends_on = [aws_s3_bucket.data_lake]
}

resource "aws_s3_object" "transactions_historical_folder" {
  bucket  = aws_s3_bucket.data_lake.id
  key     = "transactions/historical/"
  content = ""

  depends_on = [aws_s3_bucket.data_lake]
}

resource "aws_s3_object" "transactions_streaming_folder" {
  bucket  = aws_s3_bucket.data_lake.id
  key     = "transactions/streaming/"
  content = ""

  depends_on = [aws_s3_bucket.data_lake]
}
