output "alb_dns_name" {
  description = "DNS name of the load balancer - use this to access your Sequin application"
  value       = aws_lb.sequin-main.dns_name
}

output "sequin_pg_url" {
  description = "PostgreSQL URL for Sequin configuration"
  value       = "postgres://postgres:${random_password.db_password.result}@${aws_db_instance.sequin-prod.endpoint}/${var.db_name}"
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

# SQS outputs for HTTP Push Consumer
output "sequin_http_push_queue_url" {
  description = "URL of the HTTP Push SQS queue"
  value       = aws_sqs_queue.sequin_http_push_queue.url
}

output "sequin_http_push_queue_arn" {
  description = "ARN of the HTTP Push SQS queue"
  value       = aws_sqs_queue.sequin_http_push_queue.arn
}

output "sequin_http_push_dlq_url" {
  description = "URL of the HTTP Push DLQ"
  value       = aws_sqs_queue.sequin_http_push_dlq.url
}

output "sequin_http_push_dlq_arn" {
  description = "ARN of the HTTP Push DLQ"
  value       = aws_sqs_queue.sequin_http_push_dlq.arn
}

output "sequin_http_push_dead_dlq_url" {
  description = "URL of the HTTP Push Dead DLQ"
  value       = aws_sqs_queue.sequin_http_push_dead_dlq.url
}

output "sequin_http_push_dead_dlq_arn" {
  description = "ARN of the HTTP Push Dead DLQ"
  value       = aws_sqs_queue.sequin_http_push_dead_dlq.arn
}

output "sequin_http_push_sqs_user_access_key" {
  description = "Access key ID for the SQS user"
  value       = aws_iam_access_key.sequin_http_push_sqs_user_key.id
}

output "sequin_http_push_sqs_user_secret_key" {
  description = "Secret access key for the SQS user"
  value       = aws_iam_access_key.sequin_http_push_sqs_user_key.secret
  sensitive   = true
}
