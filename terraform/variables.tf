# Terraform Variables for Snowflake Customer 360 Analytics Platform
# AWS Infrastructure Configuration

variable "project_name" {
  description = "Project name used for resource naming"
  type        = string
  default     = "snowflake-customer-analytics"

  validation {
    condition     = can(regex("^[a-z0-9-]+$", var.project_name))
    error_message = "Project name must contain only lowercase letters, numbers, and hyphens."
  }
}

variable "environment" {
  description = "Environment name (e.g., dev, staging, demo, prod)"
  type        = string
  default     = "demo"

  validation {
    condition     = contains(["dev", "staging", "demo", "prod"], var.environment)
    error_message = "Environment must be one of: dev, staging, demo, prod."
  }
}

variable "aws_region" {
  description = "AWS region for resource deployment"
  type        = string
  default     = "us-east-1"
}

variable "snowflake_account_id" {
  description = "Snowflake AWS account ID for IAM trust relationship (obtain from DESC STORAGE INTEGRATION)"
  type        = string

  validation {
    condition     = can(regex("^[0-9]{12}$", var.snowflake_account_id))
    error_message = "Snowflake account ID must be a 12-digit AWS account number."
  }
}

variable "snowflake_external_id" {
  description = "Snowflake external ID for IAM trust relationship (obtain from DESC STORAGE INTEGRATION)"
  type        = string

  validation {
    condition     = length(var.snowflake_external_id) > 0
    error_message = "Snowflake external ID cannot be empty."
  }
}

variable "tags" {
  description = "Common tags to apply to all resources"
  type        = map(string)
  default = {
    Project     = "Snowflake Customer 360"
    ManagedBy   = "Terraform"
    Environment = "demo"
  }
}
