# Terraform Outputs for Snowflake Customer 360 Analytics Platform
# These outputs are used to configure Snowflake storage integration

output "s3_bucket_name" {
  description = "Name of the S3 data lake bucket"
  value       = aws_s3_bucket.data_lake.id
}

output "s3_bucket_arn" {
  description = "ARN of the S3 data lake bucket"
  value       = aws_s3_bucket.data_lake.arn
}

output "s3_bucket_region" {
  description = "AWS region of the S3 bucket"
  value       = aws_s3_bucket.data_lake.region
}

output "iam_role_arn" {
  description = "ARN of the IAM role for Snowflake S3 access (use this in CREATE STORAGE INTEGRATION)"
  value       = aws_iam_role.snowflake_s3_access.arn
}

output "iam_role_name" {
  description = "Name of the IAM role for Snowflake S3 access"
  value       = aws_iam_role.snowflake_s3_access.name
}

output "folder_structure" {
  description = "S3 folder structure created for data organization"
  value = {
    customers               = "${aws_s3_bucket.data_lake.id}/customers/"
    transactions            = "${aws_s3_bucket.data_lake.id}/transactions/"
    transactions_historical = "${aws_s3_bucket.data_lake.id}/transactions/historical/"
    transactions_streaming  = "${aws_s3_bucket.data_lake.id}/transactions/streaming/"
  }
}

# Summary output for easy copy-paste to Snowflake
output "snowflake_integration_config" {
  description = "Configuration values for Snowflake STORAGE INTEGRATION"
  value = {
    storage_aws_role_arn = aws_iam_role.snowflake_s3_access.arn
    storage_allowed_locations = [
      "s3://${aws_s3_bucket.data_lake.id}/customers/",
      "s3://${aws_s3_bucket.data_lake.id}/transactions/"
    ]
  }
}
