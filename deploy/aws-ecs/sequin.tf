# VPC and Networking
resource "aws_vpc" "sequin_vpc" {
  cidr_block           = var.vpc_cidr
  enable_dns_hostnames = true
  enable_dns_support   = true

  tags = {
    Name = "sequin-vpc"
  }
}

resource "aws_subnet" "private_subnets" {
  count             = 2
  vpc_id            = aws_vpc.sequin_vpc.id
  cidr_block        = cidrsubnet(var.vpc_cidr, 8, count.index)
  availability_zone = data.aws_availability_zones.available.names[count.index]

  tags = {
    Name = "sequin-private-subnet-${count.index + 1}"
  }
}

resource "aws_route_table" "private_rt" {
  vpc_id = aws_vpc.sequin_vpc.id

  tags = {
    Name = "sequin-private-rt"
  }
}

resource "aws_route_table_association" "private" {
  count          = 2
  subnet_id      = aws_subnet.private_subnets[count.index].id
  route_table_id = aws_route_table.private_rt.id
}

# ECS Cluster
resource "aws_ecs_cluster" "sequin_cluster" {
  name = "sequin-cluster"

  setting {
    name  = "containerInsights"
    value = "enabled"
  }
}

# Security Groups
resource "aws_security_group" "sequin_sg" {
  name        = "sequin-sg"
  description = "Security group for Sequin tasks"
  vpc_id      = aws_vpc.sequin_vpc.id

  ingress {
    from_port   = 7376
    to_port     = 7376
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "sequin-sg"
  }
}

# IAM Roles
resource "aws_iam_role" "ecs_execution_role" {
  name = "sequin-ecs-execution-role"

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

resource "aws_iam_role_policy_attachment" "ecs_execution_role_policy" {
  role       = aws_iam_role.ecs_execution_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

resource "aws_iam_role" "ecs_task_role" {
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

resource "aws_ssm_parameter" "pg_password" {
  name  = "/sequin/pg_password"
  type  = "SecureString"
  value = var.pg_password
}

resource "aws_ssm_parameter" "vault_key" {
  name  = "/sequin/vault_key"
  type  = "SecureString"
  value = var.vault_key
}

resource "aws_ssm_parameter" "secret_key_base" {
  name  = "/sequin/secret_key_base"
  type  = "SecureString"
  value = var.secret_key_base
}

# Sequin-specific resources
resource "aws_service_discovery_private_dns_namespace" "sequin_namespace" {
  name        = "sequin.internal"
  description = "Private DNS namespace for Sequin services"
  vpc         = aws_vpc.sequin_vpc.id
}

resource "aws_service_discovery_service" "sequin" {
  name = "sequin"

  dns_config {
    namespace_id = aws_service_discovery_private_dns_namespace.sequin_namespace.id

    dns_records {
      ttl  = 10
      type = "A"
    }

    routing_policy = "MULTIVALUE"
  }

  health_check_custom_config {
    failure_threshold = 1
  }
}

resource "aws_ecs_task_definition" "sequin_task" {
  family                   = "sequin-task"
  execution_role_arn       = aws_iam_role.ecs_execution_role.arn
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = "2048"
  memory                   = "4096"

  container_definitions = jsonencode([
    {
      name  = "sequin"
      image = "sequin/sequin:latest"
      portMappings = [
        {
          containerPort = 7376
          hostPort      = 7376
        }
      ]
      environment = [
        {
          name  = "PG_PORT"
          value = var.pg_port
        },
        {
          name  = "PG_HOSTNAME"
          value = var.pg_hostname
        },
        {
          name  = "PG_DATABASE"
          value = var.pg_database
        },
        {
          name  = "PG_USERNAME"
          value = var.pg_username
        }
      ]
      secrets = [
        {
          name      = "PG_PASSWORD"
          valueFrom = aws_ssm_parameter.pg_password.arn
        },
        {
          name      = "SECRET_KEY_BASE"
          valueFrom = aws_ssm_parameter.secret_key_base.arn
        },
        {
          name      = "VAULT_KEY"
          valueFrom = aws_ssm_parameter.vault_key.arn
        }
      ]
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = "/ecs/sequin"
          awslogs-region        = "ap-southeast-2"
          awslogs-stream-prefix = "sequin"
        }
      }
    }
  ])
}

resource "aws_ecs_service" "sequin_service" {
  name            = "sequin-service"
  cluster         = aws_ecs_cluster.sequin_cluster.id
  task_definition = aws_ecs_task_definition.sequin_task.arn
  desired_count   = 1
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = aws_subnet.private_subnets[*].id
    security_groups  = [aws_security_group.sequin_sg.id]
    assign_public_ip = false
  }

  service_registries {
    registry_arn = aws_service_discovery_service.sequin.arn
  }
}

resource "aws_iam_role_policy" "ecs_ssm_access" {
  name = "ecs_ssm_access"
  role = aws_iam_role.ecs_task_role.name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "ssm:GetParameters",
          "ssm:GetParameter"
        ]
        Resource = [
          aws_ssm_parameter.pg_password.arn,
          aws_ssm_parameter.secret_key_base.arn,
          aws_ssm_parameter.vault_key.arn
        ]
      }
    ]
  })
}
