# ECS Task Execution Role (for pulling images, logs, etc.)
resource "aws_iam_role" "sequin-ecs-task-execution-role" {
  name = "sequin-ecs-task-execution-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "ecs-tasks.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "sequin-ecs-task-execution-role-policy" {
  role       = aws_iam_role.sequin-ecs-task-execution-role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

resource "aws_iam_role_policy" "sequin-ecs-task-execution-secrets-policy" {
  name = "sequin-ecs-task-execution-secrets-policy"
  role = aws_iam_role.sequin-ecs-task-execution-role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "secretsmanager:GetSecretValue"
        ]
        Resource = [
          "arn:aws:secretsmanager:*:*:secret:sequin/*"
        ]
      }
    ]
  })
}

# ECS Task Role (for application permissions)
resource "aws_iam_role" "sequin-ecs-task-role" {
  name = "sequin-ecs-task-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "ecs-tasks.amazonaws.com"
        }
      }
    ]
  })
}

# ECS Instance Role (for EC2 instances in cluster)
resource "aws_iam_role" "sequin-ecs-instance-role" {
  name = "sequin-ecsInstanceRole"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "ec2.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "sequin-ecs-instance-role-policy" {
  role       = aws_iam_role.sequin-ecs-instance-role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonEC2ContainerServiceforEC2Role"
}

resource "aws_iam_instance_profile" "sequin-ecs-instance-profile" {
  name = "sequin-ecsInstanceRole"
  role = aws_iam_role.sequin-ecs-instance-role.name
}

# RDS Monitoring Role
resource "aws_iam_role" "sequin-rds-monitoring-role" {
  name = "sequin-rds-monitoring-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "monitoring.rds.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "sequin-rds-monitoring-role-policy" {
  role       = aws_iam_role.sequin-rds-monitoring-role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonRDSEnhancedMonitoringRole"
}
