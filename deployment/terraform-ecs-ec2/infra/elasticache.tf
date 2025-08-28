resource "aws_elasticache_cluster" "sequin-main" {
  auto_minor_version_upgrade = "true"
  availability_zone          = aws_subnet.sequin-private-primary.availability_zone
  az_mode                    = "single-az"
  cluster_id                 = "sequin-main"
  engine                     = "redis"
  engine_version             = "7.1"
  ip_discovery               = "ipv4"
  maintenance_window         = "wed:07:00-wed:08:00"
  network_type               = "ipv4"
  node_type                  = var.redis_instance_type
  num_cache_nodes            = "1"
  parameter_group_name       = "default.redis7"
  port                       = "6379"
  security_group_ids         = [aws_security_group.sequin-redis-sg.id]
  snapshot_retention_limit   = "15"
  snapshot_window            = "00:00-01:00"
  subnet_group_name          = aws_elasticache_subnet_group.sequin-subnet.name
}

resource "aws_elasticache_subnet_group" "sequin-subnet" {
  description = "Managed by Terraform"
  name        = "sequin-subnet"
  subnet_ids  = [aws_subnet.sequin-private-primary.id, aws_subnet.sequin-private-secondary.id]
}
