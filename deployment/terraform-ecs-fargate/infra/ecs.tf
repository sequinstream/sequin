resource "aws_ecs_cluster" "sequin" {
  name = "sequin"

  setting {
    name  = "containerInsights"
    value = "disabled"
  }
}
