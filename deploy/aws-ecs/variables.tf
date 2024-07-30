variable "vpc_cidr" {
  description = "The CIDR block for the VPC"
  type        = string
  default     = "10.0.0.0/16"
}

variable "pg_port" {
  description = "The port number for the PostgreSQL database"
  type        = string
  default     = "5432"
}

variable "pg_hostname" {
  description = "The hostname of the PostgreSQL database"
  type        = string
}

variable "pg_database" {
  description = "The name of the PostgreSQL database"
  type        = string
}

variable "pg_username" {
  description = "The username for the PostgreSQL database"
  type        = string
}

variable "pg_password" {
  description = "The password for the PostgreSQL database"
  type        = string
  sensitive   = true
}

variable "secret_key_base" {
  description = "The secret key base for Sequin (will be stored in SSM Parameter Store)"
  type        = string
  sensitive   = true
}

variable "vault_key" {
  description = "The vault key for Sequin (will be stored in SSM Parameter Store)"
  type        = string
  sensitive   = true
}

data "aws_availability_zones" "available" {}
