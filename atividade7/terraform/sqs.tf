resource "aws_sqs_queue" "producer_queue" {
  name = "${var.project_name}-producer-queue"
  visibility_timeout_seconds = 10
}

resource "aws_sqs_queue_policy" "producer_queue_policy" {
  queue_url = aws_sqs_queue.producer_queue.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = "*"
        Action = "SQS:SendMessage"
        Resource = aws_sqs_queue.producer_queue.arn
        Condition = {
          ArnEquals = {
            "AWS:SourceArn" = module.lambda_function_with_docker_build_from_ecr_producer.lambda_function_arn
          }
        }
      }
    ]
  })
}

// Output queue URL and ARN for reference
output "producer_queue_url" {
  value = aws_sqs_queue.producer_queue.id
}

output "producer_queue_arn" {
  value = aws_sqs_queue.producer_queue.arn
}