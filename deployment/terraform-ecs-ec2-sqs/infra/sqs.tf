# SQS Queue for HTTP Push Consumer
# This setup mirrors the Goldsky Pulumi configuration for specialized SQS-based HTTP push functionality

# Create a dead-dead letter queue for messages that fail too many times in the DLQ
resource "aws_sqs_queue" "sequin_http_push_dead_dlq" {
  name                      = "sequin-http-push-dead-dlq"
  message_retention_seconds = 604800 # 7 days retention

  tags = {
    Name    = "sequin-http-push-dead-dlq"
    Purpose = "HTTP Push Final Dead Letter Queue"
  }
}

# Create a dead letter queue for failed messages  
resource "aws_sqs_queue" "sequin_http_push_dlq" {
  name                      = "sequin-http-push-dlq"
  message_retention_seconds = 1209600 # 14 days (maximum retention)

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.sequin_http_push_dead_dlq.arn
    maxReceiveCount     = 50
  })

  tags = {
    Name    = "sequin-http-push-dlq"
    Purpose = "HTTP Push Consumer DLQ"
  }
}

# Create the main HTTP Push queue
resource "aws_sqs_queue" "sequin_http_push_queue" {
  name                        = "sequin-http-push-queue"
  visibility_timeout_seconds  = 60
  message_retention_seconds   = 1209600 # 14 days (maximum retention)

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.sequin_http_push_dlq.arn
    maxReceiveCount     = 1
  })

  tags = {
    Name    = "sequin-http-push-queue"
    Purpose = "HTTP Push Consumer"
  }
}

# Create IAM user for application to read from and write to the queue
resource "aws_iam_user" "sequin_http_push_sqs_user" {
  name = "sequin-http-push-sqs-user"

  tags = {
    Name    = "sequin-http-push-sqs-user"
    Purpose = "HTTP Push Queue Access"
  }
}

# Create access keys for the SQS user
resource "aws_iam_access_key" "sequin_http_push_sqs_user_key" {
  user = aws_iam_user.sequin_http_push_sqs_user.name
}

# Create policy for both reading from and writing to the SQS queue
resource "aws_iam_policy" "sequin_sqs_access_policy" {
  name        = "sequin-http-push-sqs-access-policy"
  description = "Policy for read/write access to the HTTP Push SQS queue"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          # Send operations
          "sqs:SendMessage",
          "sqs:SendMessageBatch",

          # Receive operations
          "sqs:ReceiveMessage",
          "sqs:DeleteMessage",
          "sqs:DeleteMessageBatch",
          "sqs:ChangeMessageVisibility",
          "sqs:ChangeMessageVisibilityBatch",

          # Queue management operations
          "sqs:GetQueueUrl",
          "sqs:GetQueueAttributes",
          "sqs:ListQueues",
          "sqs:ListQueueTags",
        ]
        Resource = [
          aws_sqs_queue.sequin_http_push_queue.arn,
          aws_sqs_queue.sequin_http_push_dlq.arn,
          aws_sqs_queue.sequin_http_push_dead_dlq.arn
        ]
      }
    ]
  })

  tags = {
    Name = "sequin-http-push-sqs-access-policy"
  }
}

# Attach the policy to the SQS user
resource "aws_iam_user_policy_attachment" "sequin_http_push_sqs_user_attachment" {
  user       = aws_iam_user.sequin_http_push_sqs_user.name
  policy_arn = aws_iam_policy.sequin_sqs_access_policy.arn
}