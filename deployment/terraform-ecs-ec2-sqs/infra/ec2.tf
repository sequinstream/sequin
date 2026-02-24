resource "aws_instance" "sequin-bastion" {
  ami           = data.aws_ssm_parameter.sequin-ami-standard.value
  instance_type = "t3.micro"
  key_name      = var.ec2_key_name

  vpc_security_group_ids = [aws_security_group.sequin-bastion-sg.id]
  subnet_id              = aws_subnet.sequin-public-primary.id

  associate_public_ip_address = true

  tags = {
    Name = "sequin-bastion-host"
  }

  lifecycle {
    # No need to upgrade every time AMI changes.
    ignore_changes = [ami]
  }

  metadata_options {
    http_tokens   = "required"
    http_endpoint = "enabled"
  }
}

resource "aws_autoscaling_group" "sequin-main" {
  capacity_rebalance        = "false"
  default_cooldown          = "300"
  default_instance_warmup   = "0"
  desired_capacity          = "1"
  force_delete              = "false"
  health_check_grace_period = "0"
  health_check_type         = "EC2"

  launch_template {
    id      = aws_launch_template.sequin-main.id
    version = "$Latest"
  }

  max_instance_lifetime   = "0"
  max_size                = "2"
  metrics_granularity     = "1Minute"
  min_size                = "1"
  protect_from_scale_in   = "false"

  tag {
    key                 = "Name"
    propagate_at_launch = "true"
    value               = "sequin-ecs-instance"
  }

  vpc_zone_identifier       = [aws_subnet.sequin-private-primary.id]
  wait_for_capacity_timeout = "10m"
}

resource "aws_launch_template" "sequin-main" {
  block_device_mappings {
    device_name = "/dev/xvda"

    ebs {
      delete_on_termination = "true"
      encrypted             = "false"
      volume_size           = "100"
      volume_type           = "gp3"
    }
  }

  disable_api_stop        = "false"
  disable_api_termination = "false"
  ebs_optimized           = "false"

  iam_instance_profile {
    name = local.ecs_instance_profile_name
  }

  image_id      = data.aws_ssm_parameter.sequin-ami-ecs.value
  instance_type = var.ecs_instance_type
  key_name      = var.ec2_key_name

  monitoring {
    enabled = "true"
  }

  name = "sequin-ecs-launch-template"

  user_data = base64encode(<<-EOF
    #!/bin/bash
    # Create ECS config directory if it doesn't exist
    mkdir -p /etc/ecs

    # Configure ECS agent to join the sequin cluster
    echo "ECS_CLUSTER=${aws_ecs_cluster.sequin.name}" > /etc/ecs/ecs.config
    echo "ECS_BACKEND_HOST=" >> /etc/ecs/ecs.config
    echo "ECS_INSTANCE_ATTRIBUTES={\"sequin\":\"true\"}" >> /etc/ecs/ecs.config
    EOF
  )

  vpc_security_group_ids = [aws_security_group.sequin-ecs-sg.id]

  metadata_options {
    http_tokens   = "required"
    http_endpoint = "enabled"
  }
}
