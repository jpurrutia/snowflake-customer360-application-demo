# Terraform Configuration for Snowflake Customer 360 Analytics Platform
# Main configuration file with provider and backend setup

terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    null = {
      source  = "hashicorp/null"
      version = "~> 3.2"
    }
  }

  # Local backend for development
  # To use S3 backend for remote state, uncomment and configure:
  #
  # backend "s3" {
  #   bucket         = "your-terraform-state-bucket"
  #   key            = "snowflake-customer-analytics/terraform.tfstate"
  #   region         = "us-east-1"
  #   encrypt        = true
  #   dynamodb_table = "terraform-state-locks"
  # }
  #
  # Benefits of S3 backend:
  # - Remote state storage (team collaboration)
  # - State locking via DynamoDB (prevents concurrent modifications)
  # - State encryption at rest
  # - Version history
}

# AWS Provider Configuration
provider "aws" {
  region = var.aws_region

  default_tags {
    tags = var.tags
  }
}
