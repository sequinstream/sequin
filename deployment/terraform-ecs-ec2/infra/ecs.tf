resource "aws_ecs_cluster" "sequin" {
  name = "sequin"

  lifecycle {
    prevent_destroy = true
  }

  setting {
    name  = "containerInsights"
    value = "disabled"
  }
}

# resource "aws_ecs_task_definition" "datadog-agent-task" {
#   family                   = "datadog-agent-task"
#   requires_compatibilities = ["EC2"]

#   container_definitions = jsonencode([
#     {
#       cpu       = 10
#       image     = "datadog/agent:latest"
#       memory    = 2560
#       name      = "datadog-agent"
#       essential = true

#       hostname = "datadog-agent.local"

#       environment = [
#         {
#           name  = "DD_API_KEY"
#           value = "TODO"
#         },
#         {
#           name  = "DD_APM_NON_LOCAL_TRAFFIC"
#           value = "true"
#         },
#         {
#           name  = "DD_CONTAINER_EXCLUDE_LOGS"
#           value = "name:datadog-agent"
#         },
#         {
#           name  = "DD_DOGSTATSD_NON_LOCAL_TRAFFIC"
#           value = "true"
#         },
#         {
#           name  = "DD_LOGS_CONFIG_CONTAINER_COLLECT_ALL"
#           value = "true"
#         },
#         {
#           name  = "DD_LOGS_ENABLED"
#           value = "true"
#         },
#         {
#           name  = "DD_OTLP_CONFIG_RECEIVER_PROTOCOLS_HTTP_ENDPOINT"
#           value = "0.0.0.0:4318"
#         },
#         {
#           name  = "DD_SITE"
#           value = "datadoghq.com"
#         }
#       ]

#       healthCheck = {
#         command     = ["CMD-SHELL", "agent health"]
#         interval    = 30
#         retries     = 3
#         startPeriod = 15
#         timeout     = 5
#       }

#       mountPoints = [
#         {
#           containerPath = "/var/run/docker.sock"
#           readOnly      = true
#           sourceVolume  = "docker_sock"
#         },
#         {
#           containerPath = "/host/sys/fs/cgroup"
#           readOnly      = true
#           sourceVolume  = "cgroup"
#         },
#         {
#           containerPath = "/host/proc"
#           readOnly      = true
#           sourceVolume  = "proc"
#         }
#       ]

#       portMappings = [
#         {
#           containerPort = 8125
#           hostPort      = 8125
#           protocol      = "udp"
#         },
#         {
#           containerPort = 4318
#           hostPort      = 4318
#           protocol      = "tcp"
#         }
#       ]

#       volumesFrom = []
#     }
#   ])

#   volume {
#     host_path = "/proc/"
#     name      = "proc"
#   }

#   volume {
#     host_path = "/sys/fs/cgroup/"
#     name      = "cgroup"
#   }

#   volume {
#     host_path = "/var/run/docker.sock"
#     name      = "docker_sock"
#   }
# }

# resource "aws_ecs_service" "sequin-datadog-agent" {
#   cluster = "sequin"

#   deployment_controller {
#     type = "ECS"
#   }

#   deployment_maximum_percent         = "100"
#   deployment_minimum_healthy_percent = "0"
#   enable_ecs_managed_tags            = "true"
#   enable_execute_command             = "false"
#   health_check_grace_period_seconds  = "0"
#   launch_type                        = "EC2"
#   name                               = "datadog-agent"
#   scheduling_strategy                = "DAEMON"
#   task_definition                    = aws_ecs_task_definition.datadog-agent-task.arn
# }
