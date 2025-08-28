# Reference infrastructure from ../infra
data "terraform_remote_state" "infra" {
  backend = "s3"
  config = {
    bucket = var.infra_state_bucket
    key    = var.infra_state_key
    region = var.infra_state_region
  }
}

# Generate secure random secrets (base64 encoded as per Sequin docs)
resource "random_bytes" "secret_key_base" {
  length = 64
}

resource "random_bytes" "vault_key" {
  length = 32
}

resource "random_password" "admin_password" {
  length  = 16
  special = true
}


# Create Sequin secrets store with placeholders
resource "aws_secretsmanager_secret" "sequin-config" {
  name        = "sequin/config"
  description = "Sequin application configuration"

  tags = {
    Name = "sequin-config"
  }
}

resource "aws_secretsmanager_secret_version" "sequin-config" {
  secret_id = aws_secretsmanager_secret.sequin-config.id
  secret_string = jsonencode({
    # Database and Redis URLs - populated from infra outputs
    PG_URL    = data.terraform_remote_state.infra.outputs.sequin_pg_url
    REDIS_URL = data.terraform_remote_state.infra.outputs.sequin_redis_url

    # Auto-generated secure secrets
    SECRET_KEY_BASE = random_bytes.secret_key_base.base64
    ADMIN_PASSWORD  = random_password.admin_password.result
    VAULT_KEY       = random_bytes.vault_key.base64

    # Optional third-party integrations - leave empty if not needed
    GITHUB_CLIENT_ID     = ""
    GITHUB_CLIENT_SECRET = ""
    SENDGRID_API_KEY     = ""
    RETOOL_WORKFLOW_KEY  = ""
    LOOPS_API_KEY        = ""
    DATADOG_API_KEY      = ""
    DATADOG_APP_KEY      = ""
    # Not a valid DSN, but required to boot
    SENTRY_DSN                = "https://f8f11937067b2ef151cda3abe652667b@o398678.ingest.us.sentry.io/4508033603469312"
    PAGERDUTY_INTEGRATION_KEY = ""
  })

  lifecycle {
    ignore_changes = [secret_string]
  }
}

# ECS Service
resource "aws_ecs_service" "sequin-main" {
  name            = "sequin-main"
  cluster         = data.terraform_remote_state.infra.outputs.ecs_cluster_name
  task_definition = aws_ecs_task_definition.sequin-main.arn
  desired_count   = 1
  launch_type     = "EC2"

  deployment_maximum_percent         = 200
  deployment_minimum_healthy_percent = 100

  deployment_circuit_breaker {
    enable   = true
    rollback = true
  }


  network_configuration {
    subnets = [
      data.terraform_remote_state.infra.outputs.private_subnet_primary_id,
      data.terraform_remote_state.infra.outputs.private_subnet_secondary_id
    ]
    security_groups = [data.terraform_remote_state.infra.outputs.ecs_security_group_id]
  }

  load_balancer {
    target_group_arn = data.terraform_remote_state.infra.outputs.target_group_arn
    container_name   = "sequin"
    container_port   = 7376
  }

  enable_execute_command            = true
  health_check_grace_period_seconds = 45
  enable_ecs_managed_tags           = true
  propagate_tags                    = "SERVICE"

  lifecycle {
    prevent_destroy = true
  }

  tags = {
    Name = "sequin-main-service"
  }
}

# ECS Task Definition
resource "aws_ecs_task_definition" "sequin-main" {
  family                   = "sequin-task-main"
  execution_role_arn       = data.terraform_remote_state.infra.outputs.ecs_task_execution_role_arn
  task_role_arn            = data.terraform_remote_state.infra.outputs.ecs_task_role_arn
  network_mode             = "awsvpc"
  requires_compatibilities = ["EC2"]

  container_definitions = jsonencode([
    {
      name      = "sequin"
      image     = "${var.image_repository}:${var.image_tag}"
      essential = true

      memory            = var.memory
      memoryReservation = var.memory_reservation
      cpu               = 0

      environment = [
        {
          name  = "CURRENT_GIT_SHA"
          value = var.image_tag
        },
        {
          name  = "PG_SSL"
          value = "true"
        },
        {
          name  = "LAUNCH_TYPE"
          value = "EC2"
        },
        {
          name  = "ADMIN_USER"
          value = "admin"
        },
        {
          name  = "RELEASE_DISTRIBUTION"
          value = "name"
        },
        {
          name  = "RELEASE_NODE"
          value = "sequin"
        },
        {
          name  = "SERVER_PORT"
          value = "7376"
        }
      ]

      secrets = [
        {
          name      = "SECRET_KEY_BASE"
          valueFrom = "${aws_secretsmanager_secret.sequin-config.arn}:SECRET_KEY_BASE::"
        },
        {
          name      = "ADMIN_PASSWORD"
          valueFrom = "${aws_secretsmanager_secret.sequin-config.arn}:ADMIN_PASSWORD::"
        },
        {
          name      = "VAULT_KEY"
          valueFrom = "${aws_secretsmanager_secret.sequin-config.arn}:VAULT_KEY::"
        },
        {
          name      = "PG_URL"
          valueFrom = "${aws_secretsmanager_secret.sequin-config.arn}:PG_URL::"
        },
        {
          name      = "REDIS_URL"
          valueFrom = "${aws_secretsmanager_secret.sequin-config.arn}:REDIS_URL::"
        },
        {
          name      = "GITHUB_CLIENT_ID"
          valueFrom = "${aws_secretsmanager_secret.sequin-config.arn}:GITHUB_CLIENT_ID::"
        },
        {
          name      = "GITHUB_CLIENT_SECRET"
          valueFrom = "${aws_secretsmanager_secret.sequin-config.arn}:GITHUB_CLIENT_SECRET::"
        },
        {
          name      = "SENDGRID_API_KEY"
          valueFrom = "${aws_secretsmanager_secret.sequin-config.arn}:SENDGRID_API_KEY::"
        },
        {
          name      = "RETOOL_WORKFLOW_KEY"
          valueFrom = "${aws_secretsmanager_secret.sequin-config.arn}:RETOOL_WORKFLOW_KEY::"
        },
        {
          name      = "LOOPS_API_KEY"
          valueFrom = "${aws_secretsmanager_secret.sequin-config.arn}:LOOPS_API_KEY::"
        },
        {
          name      = "DATADOG_API_KEY"
          valueFrom = "${aws_secretsmanager_secret.sequin-config.arn}:DATADOG_API_KEY::"
        },
        {
          name      = "DATADOG_APP_KEY"
          valueFrom = "${aws_secretsmanager_secret.sequin-config.arn}:DATADOG_APP_KEY::"
        },
        {
          name      = "SENTRY_DSN"
          valueFrom = "${aws_secretsmanager_secret.sequin-config.arn}:SENTRY_DSN::"
        },
        {
          name      = "PAGERDUTY_INTEGRATION_KEY"
          valueFrom = "${aws_secretsmanager_secret.sequin-config.arn}:PAGERDUTY_INTEGRATION_KEY::"
        }
      ]

      portMappings = [
        {
          name          = "sequin-7376-tcp"
          containerPort = 7376
          hostPort      = 7376
          protocol      = "tcp"
        },
        {
          name          = "sequin-4369-tcp"
          containerPort = 4369
          hostPort      = 4369
          protocol      = "tcp"
        }
      ]

      healthCheck = {
        command     = ["CMD-SHELL", "curl -f http://localhost:7376/health || exit 1"]
        interval    = 30
        timeout     = 5
        retries     = 3
        startPeriod = 45
      }

      systemControls = [
        {
          namespace = "net.ipv4.tcp_keepalive_time"
          value     = "60"
        },
        {
          namespace = "net.ipv4.tcp_keepalive_intvl"
          value     = "60"
        }
      ]

      ulimits = [
        {
          name      = "nofile"
          softLimit = 10240
          hardLimit = 40960
        }
      ]

      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = "/ecs/sequin"
          "awslogs-region"        = "us-east-1"
          "awslogs-stream-prefix" = "ecs"
        }
      }

      mountPoints = []
      volumesFrom = []
    }
  ])

  skip_destroy = true

  tags = {
    Name = "sequin-main-task"
  }
}

# CloudWatch Log Group for ECS logs
resource "aws_cloudwatch_log_group" "sequin" {
  name              = "/ecs/sequin"
  retention_in_days = 7

  tags = {
    Name = "sequin-ecs-logs"
  }
}
