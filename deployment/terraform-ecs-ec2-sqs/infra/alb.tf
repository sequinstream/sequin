resource "aws_lb" "sequin-main" {
  enable_http2       = "true"
  name               = "sequin-main-lb"
  idle_timeout       = "60"
  internal           = false
  load_balancer_type = "application"
  security_groups    = [aws_security_group.sequin-alb-sg.id]
  subnets            = [aws_subnet.sequin-public-primary.id, aws_subnet.sequin-public-secondary.id]

  ip_address_type = "dualstack"
}

resource "aws_lb_listener" "sequin-main-80" {
  load_balancer_arn = aws_lb.sequin-main.arn
  port              = "80"
  protocol          = "HTTP"

  default_action {
    type = var.ssl_certificate_arn != "" ? "redirect" : "forward"

    dynamic "redirect" {
      for_each = var.ssl_certificate_arn != "" ? [1] : []
      content {
        host        = "#{host}"
        path        = "/#{path}"
        port        = "443"
        protocol    = "HTTPS"
        query       = "#{query}"
        status_code = "HTTP_301"
      }
    }

    target_group_arn = var.ssl_certificate_arn == "" ? aws_lb_target_group.sequin-main.arn : null
  }
}

resource "aws_lb_listener" "sequin-main-443" {
  count = var.ssl_certificate_arn != "" ? 1 : 0

  load_balancer_arn = aws_lb.sequin-main.arn
  port              = "443"
  protocol          = "HTTPS"
  ssl_policy        = "ELBSecurityPolicy-2016-08"
  certificate_arn   = var.ssl_certificate_arn

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.sequin-main.arn
  }
}

resource "aws_lb_target_group" "sequin-main" {
  name                 = "sequin-main-tg"
  port                 = 80
  protocol             = "HTTP"
  vpc_id               = aws_vpc.sequin-main.id
  deregistration_delay = 60

  health_check {
    enabled             = "true"
    healthy_threshold   = "2"
    interval            = "30"
    matcher             = "200"
    path                = "/health"
    port                = "traffic-port"
    protocol            = "HTTP"
    timeout             = "5"
    unhealthy_threshold = "5"
  }

  target_type     = "ip"
  ip_address_type = "ipv4"

  stickiness {
    cookie_duration = "86400"
    enabled         = "false"
    type            = "lb_cookie"
  }

  load_balancing_algorithm_type     = "round_robin"
  load_balancing_cross_zone_enabled = "use_load_balancer_configuration"
  protocol_version                  = "HTTP1"
  slow_start                        = "0"
}
