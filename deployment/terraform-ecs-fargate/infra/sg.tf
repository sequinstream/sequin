resource "aws_security_group" "sequin-ecs-sg" {
  description = "ECS Allowed Ports"
  vpc_id      = aws_vpc.sequin-main.id

  egress {
    cidr_blocks      = ["0.0.0.0/0"]
    ipv6_cidr_blocks = ["::/0"]
    from_port        = "0"
    protocol         = "-1"
    self             = "false"
    to_port          = "0"
  }

  # ingress {
  #   description = "Datadog OTEL TCP"
  #   from_port   = "4318"
  #   protocol    = "tcp"
  #   self        = "true"
  #   to_port     = "4318"
  # }

  # ingress {
  #   description = "Datadog Statsd TCP"
  #   from_port   = "8126"
  #   protocol    = "tcp"
  #   self        = "true"
  #   to_port     = "8126"
  # }

  # ingress {
  #   description = "Datadog Statsd UDP"
  #   from_port   = "8125"
  #   protocol    = "udp"
  #   self        = "true"
  #   to_port     = "8125"
  # }


  ingress {
    description     = "Allow inbound traffic from ALB"
    from_port       = 7376
    to_port         = 7376
    protocol        = "tcp"
    security_groups = [aws_security_group.sequin-alb-sg.id]
  }

  name = "sequin-ecs-sg"
}

resource "aws_security_group" "sequin-alb-sg" {
  name        = "sequin-alb-sg"
  description = "Security group for Sequin ALB"
  vpc_id      = aws_vpc.sequin-main.id

  ingress {
    from_port        = 80
    to_port          = 80
    protocol         = "tcp"
    cidr_blocks      = ["0.0.0.0/0"]
    ipv6_cidr_blocks = ["::/0"]
  }

  ingress {
    from_port        = 443
    to_port          = 443
    protocol         = "tcp"
    cidr_blocks      = ["0.0.0.0/0"]
    ipv6_cidr_blocks = ["::/0"]
  }

  egress {
    from_port        = 0
    to_port          = 0
    protocol         = "-1"
    cidr_blocks      = ["0.0.0.0/0"]
    ipv6_cidr_blocks = ["::/0"]
  }
  tags = {
    Name = "sequin-alb-sg"
  }
}


resource "aws_security_group" "sequin-rds-sg" {
  name        = "sequin-rds-sg"
  description = "Security group for Sequin RDS"
  vpc_id      = aws_vpc.sequin-main.id

  ingress {
    from_port       = 0
    to_port         = 5432
    protocol        = "tcp"
    security_groups = [aws_security_group.sequin-ecs-sg.id]
  }

  egress {
    from_port        = 0
    to_port          = 0
    protocol         = "-1"
    cidr_blocks      = ["0.0.0.0/0"]
    ipv6_cidr_blocks = ["::/0"]
  }

  tags = {
    Name = "sequin-rds-sg"
  }
}

resource "aws_security_group" "sequin-redis-sg" {
  description = "Security group for redis"
  name        = "sequin-redis-sg"

  egress {
    cidr_blocks      = ["0.0.0.0/0"]
    ipv6_cidr_blocks = ["::/0"]
    from_port        = "0"
    protocol         = "-1"
    self             = "false"
    to_port          = "0"
  }

  ingress {
    from_port       = "0"
    protocol        = "-1"
    security_groups = [aws_security_group.sequin-ecs-sg.id]
    self            = true
    to_port         = "0"
  }

  vpc_id = aws_vpc.sequin-main.id
}
