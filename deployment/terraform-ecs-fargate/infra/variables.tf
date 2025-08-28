variable "aws_region" {
  description = "AWS region for deployment"
  type        = string
  default     = "us-east-1"
}

variable "primary_availability_zone" {
  description = "Primary availability zone for single-AZ resources (RDS, Redis)"
  type        = string
  default     = "us-east-1a"
}

variable "secondary_availability_zone" {
  description = "Secondary availability zone for multi-AZ resources (ALB, subnets)"
  type        = string
  default     = "us-east-1b"
}



# ==============================================================================
# NETWORKING CONFIGURATION
# ==============================================================================

variable "vpc_cidr_block" {
  description = "CIDR block for the VPC - provides IP address range for your infrastructure"
  type        = string
  default     = "10.0.0.0/16"

  validation {
    condition     = can(cidrhost(var.vpc_cidr_block, 0))
    error_message = "VPC CIDR block must be a valid IPv4 CIDR."
  }
}


# ==============================================================================
# COMPUTE RESOURCES
# ==============================================================================

variable "architecture" {
  description = "The CPU architecture for AMIs (x86_64 or arm64)"
  type        = string
  default     = "x86_64"

  validation {
    condition     = contains(["x86_64", "arm64"], var.architecture)
    error_message = "Architecture must be either 'x86_64' or 'arm64'."
  }
}


# ==============================================================================
# DATABASE CONFIGURATION
# ==============================================================================

variable "db_name" {
  description = "Name of the PostgreSQL database to create"
  type        = string
  default     = "sequin_prod"
}

variable "rds_instance_type" {
  description = "RDS instance class. Recommended: db.t4g.micro is fine for testing, db.m5.large is OK for lighter prod workloads. db.m5.xlarge+ recommended for heavy workloads"
  type        = string
  default     = "db.m5.large"
}

variable "rds_allocated_storage" {
  description = "Initial allocated storage for RDS in GB. Will auto-scale up to max_allocated_storage"
  type        = number
  default     = 20
}

variable "rds_max_allocated_storage" {
  description = "Maximum storage for RDS auto-scaling in GB"
  type        = number
  default     = 100
}

# ==============================================================================
# REDIS CONFIGURATION
# ==============================================================================

variable "redis_instance_type" {
  description = "ElastiCache Redis node type. Recommended: cache.t4g.micro for testing, cache.t4g.small+ for production"
  type        = string
  default     = "cache.t4g.micro"
}

# ==============================================================================
# SSL CERTIFICATE (OPTIONAL)
# ==============================================================================

variable "ssl_certificate_arn" {
  description = "ARN of SSL certificate for HTTPS load balancer. Leave empty to skip HTTPS (HTTP only)"
  type        = string
  default     = ""
}

# ==============================================================================
# MONITORING & ALERTING (OPTIONAL)
# ==============================================================================

variable "alarm_action_arn" {
  description = "ARN of SNS topic for CloudWatch alarms. Leave empty to disable alerts"
  type        = string
  default     = ""
}

# ==============================================================================
# AUTO-GENERATED VALUES (DO NOT MODIFY)
# ==============================================================================


data "aws_caller_identity" "current" {}

locals {
  # Auto-generate ARNs using account ID
  rds_monitoring_role_arn = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/rds-monitoring-role"

  # Resource naming
  project_name = "sequin"
}
