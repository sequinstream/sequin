output "alb_dns_name" {
  description = "DNS name of the load balancer - use this to access your Sequin application"
  value       = aws_lb.sequin-main.dns_name
}

output "sequin_pg_url" {
  description = "PostgreSQL URL for Sequin configuration"
  value       = "postgres://postgres:${var.db_password}@${aws_db_instance.sequin-prod.endpoint}:5432/sequin_prod"
  sensitive   = true
}

output "sequin_redis_url" {
  description = "Redis URL for Sequin configuration"
  value       = "redis://${aws_elasticache_cluster.sequin-main.cache_nodes[0].address}:6379"
}

# Additional outputs needed by app/
output "ecs_cluster_name" {
  description = "Name of the ECS cluster"
  value       = aws_ecs_cluster.sequin.name
}

output "target_group_arn" {
  description = "ARN of the load balancer target group"
  value       = aws_lb_target_group.sequin-main.arn
}

output "private_subnet_primary_id" {
  description = "ID of the primary private subnet for ECS services"
  value       = aws_subnet.sequin-private-primary.id
}

output "private_subnet_secondary_id" {
  description = "ID of the secondary private subnet for ECS services"
  value       = aws_subnet.sequin-private-secondary.id
}

output "ecs_security_group_id" {
  description = "ID of the ECS security group"
  value       = aws_security_group.sequin-ecs-sg.id
}

output "ecs_task_execution_role_arn" {
  description = "ARN of the ECS task execution role"
  value       = aws_iam_role.sequin-ecs-task-execution-role.arn
}

output "ecs_task_role_arn" {
  description = "ARN of the ECS task role"
  value       = aws_iam_role.sequin-ecs-task-role.arn
}
