# Generate secure password for PostgreSQL database
resource "random_password" "db_password" {
  length  = 16
  special = true
  # Exclude characters that break PostgreSQL connection URLs
  override_special = "!#$%&*()-_=+[]{}<>?."
}

resource "aws_db_parameter_group" "sequin-prod-pg-17" {
  description = "For sequin-prod"
  family      = "postgres17"
  name        = "prod-params-pg17"

  parameter {
    apply_method = "pending-reboot"
    name         = "shared_preload_libraries"
    value        = "pg_stat_statements,pg_cron"
  }

  parameter {
    apply_method = "immediate"
    name         = "max_slot_wal_keep_size"
    value        = "4096"
  }
  parameter {
    apply_method = "pending-reboot"
    name         = "rds.logical_replication"
    value        = "1"
  }
}

resource "aws_db_subnet_group" "sequin-default-group" {
  name       = "sequin-default-rds-subnet-group"
  subnet_ids = [aws_subnet.sequin-private-primary.id, aws_subnet.sequin-private-secondary.id]

  tags = {
    Name = "Sequin single AZ RDS subnet group"
  }
}

resource "aws_kms_key" "sequin-rds-encryption-key" {
  description             = "KMS key for Sequin RDS encryption"
  deletion_window_in_days = 30
  enable_key_rotation     = true

  lifecycle {
    prevent_destroy = true
  }
}

resource "aws_db_instance" "sequin-prod" {
  allocated_storage                     = var.rds_allocated_storage
  auto_minor_version_upgrade            = "true"
  availability_zone                     = aws_subnet.sequin-private-primary.availability_zone
  backup_retention_period               = "7"
  backup_window                         = "11:43-12:13"
  db_name                               = var.db_name
  ca_cert_identifier                    = "rds-ca-rsa2048-g1"
  copy_tags_to_snapshot                 = "true"
  customer_owned_ip_enabled             = "false"
  db_subnet_group_name                  = aws_db_subnet_group.sequin-default-group.name
  deletion_protection                   = "true"
  engine                                = "postgres"
  engine_version                        = "17.6"
  iam_database_authentication_enabled   = "false"
  identifier                            = "sequin-prod"
  instance_class                        = var.rds_instance_type
  maintenance_window                    = "thu:11:11-thu:11:41"
  max_allocated_storage                 = var.rds_max_allocated_storage
  monitoring_interval                   = "60"
  monitoring_role_arn                   = local.rds_monitoring_role_arn
  multi_az                              = "false"
  network_type                          = "IPV4"
  parameter_group_name                  = aws_db_parameter_group.sequin-prod-pg-17.name
  performance_insights_enabled          = "true"
  performance_insights_kms_key_id       = aws_kms_key.sequin-rds-encryption-key.arn
  performance_insights_retention_period = "7"
  port                                  = "5432"
  publicly_accessible                   = "false"
  storage_encrypted                     = "true"
  kms_key_id                            = aws_kms_key.sequin-rds-encryption-key.arn
  storage_type                          = "gp3"
  username                              = "postgres"
  vpc_security_group_ids                = [aws_security_group.sequin-rds-sg.id]

  password = random_password.db_password.result

  lifecycle {
    prevent_destroy = true
    ignore_changes  = [password]
  }
}
