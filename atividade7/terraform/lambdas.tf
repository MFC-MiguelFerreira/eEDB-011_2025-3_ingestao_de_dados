# resource "aws_iam_role" "producer_lambda_role" {
#   name = "${var.project_name}-producer-lambda-role"
#   assume_role_policy = jsonencode({
#     Version = "2012-10-17"
#     Statement = [{
#       Effect = "Allow"
#       Principal = { Service = "lambda.amazonaws.com" }
#       Action = "sts:AssumeRole"
#     }]
#   })
# }

# resource "aws_iam_role_policy" "producer_lambda_policy" {
#   name = "${var.project_name}-producer-lambda-policy"
#   role = aws_iam_role.producer_lambda_role.id
#   policy = jsonencode({
#     Version = "2012-10-17"
#     Statement = [
#       {
#         Effect = "Allow"
#         Action = [
#           "s3:GetObject",
#           "s3:ListBucket"
#         ]
#         Resource = [
#           "${aws_s3_bucket.atividade5_bucket.arn}",
#           "${aws_s3_bucket.atividade5_bucket.arn}/*"
#         ]
#       },
#       {
#         Effect = "Allow"
#         Action = [
#           "sqs:SendMessage"
#         ]
#         Resource = aws_sqs_queue.producer_queue.arn
#       },
#       {
#         Effect = "Allow"
#         Action = [
#           "logs:CreateLogGroup",
#           "logs:CreateLogStream",
#           "logs:PutLogEvents"
#         ]
#         Resource = "*"
#       }
#     ]
#   })
# }

# resource "aws_iam_role" "consumer_lambda_role" {
#   name = "${var.project_name}-consumer-lambda-role"
#   assume_role_policy = jsonencode({
#     Version = "2012-10-17"
#     Statement = [{
#       Effect = "Allow"
#       Principal = { Service = "lambda.amazonaws.com" }
#       Action = "sts:AssumeRole"
#     }]
#   })
# }

# resource "aws_iam_role_policy" "consumer_lambda_policy" {
#   name = "${var.project_name}-consumer-lambda-policy"
#   role = aws_iam_role.consumer_lambda_role.id
#   policy = jsonencode({
#     Version = "2012-10-17"
#     Statement = [
#       {
#         Effect = "Allow"
#         Action = [
#           "sqs:ReceiveMessage",
#           "sqs:DeleteMessage",
#           "sqs:GetQueueAttributes"
#         ]
#         Resource = aws_sqs_queue.producer_queue.arn
#       },
#       {
#         Effect = "Allow"
#         Action = [
#           "athena:StartQueryExecution",
#           "athena:GetQueryExecution",
#           "athena:GetQueryResults",
#           "athena:StopQueryExecution",
#           "athena:GetWorkGroup",
#           "glue:DeleteTable",
#           "glue:CreateTable",
#           "glue:UpdateTable",
#           "glue:GetTable",
#           "glue:GetTables",
#           "glue:GetDatabase",
#           "glue:GetDatabases"
#         ]
#         Resource = "*"
#       },
#       {
#         Effect = "Allow"
#         Action = [
#           "s3:GetObject",
#           "s3:PutObject",
#           "s3:DeleteObject",
#           "s3:ListBucket"
#         ]
#         Resource = [
#           "arn:aws:s3:::athena-query-results-753251897225",
#           "arn:aws:s3:::athena-query-results-753251897225/*",
#           "arn:aws:s3:::athena-query-results-753251897225-us-east-1",
#           "arn:aws:s3:::athena-query-results-753251897225-us-east-1/*",
#           "${aws_s3_bucket.atividade5_bucket.arn}",
#           "${aws_s3_bucket.atividade5_bucket.arn}/*"
#         ]
#       },
#       {
#         Effect = "Allow"
#         Action = [
#           "logs:CreateLogGroup",
#           "logs:CreateLogStream",
#           "logs:PutLogEvents"
#         ]
#         Resource = "*"
#       }
#     ]
#   })
# }

module "lambda_function_with_docker_build_from_ecr_producer" {
  source = "terraform-aws-modules/lambda/aws"

  function_name = "${var.project_name}_producer"
  description   = "Lambda function to produce the data from the source."
  handler       = "producer.handler"
  runtime       = "python3.12"
  timeout       = 120
  memory_size   = 256
  publish       = true

  environment_variables = {
    raw_bucket_name = "${aws_s3_bucket.atividade5_bucket.bucket}",
    sqs_url = "${aws_sqs_queue.producer_queue.id}"
  }

  source_path = ["../lambdas/producer.py"]

  store_on_s3 = true
  s3_bucket   = aws_s3_bucket.atividade5_bucket.bucket

  create_layer = false
  layers       = [
    "arn:aws:lambda:us-east-1:336392948345:layer:AWSSDKPandas-Python312:16"
  ]

  # create_package = false
  # package_type   = "Image"
  # architectures  = ["x86_64"]

  # image_uri            = module.docker_image.image_uri
  # image_config_command = ["lambdas/producer.handler"]

  create_role = false
  attach_policy_statements = false
  lambda_role = "arn:aws:iam::${local.account_id}:role/LabRole"
  # lambda_role = aws_iam_role.producer_lambda_role.arn
}

module "lambda_function_with_docker_build_from_ecr_consumer" {
  source = "terraform-aws-modules/lambda/aws"

  function_name = "${var.project_name}_consumer"
  description   = "Lambda function to consume the data from the source."
  handler       = "consumer.handler"
  runtime       = "python3.12"
  timeout       = 120
  memory_size   = 512
  reserved_concurrent_executions = 1

  environment_variables = {
    raw_bucket_name = "${aws_s3_bucket.atividade5_bucket.bucket}"
  }

  source_path = ["../lambdas/consumer.py"]

  store_on_s3 = true
  s3_bucket   = aws_s3_bucket.atividade5_bucket.bucket

  create_layer = false
  layers       = [
    "arn:aws:lambda:us-east-1:336392948345:layer:AWSSDKPandas-Python312:16"
  ]

  # create_package = false
  # package_type   = "Image"
  # architectures  = ["x86_64"]

  # image_uri            = module.docker_image.image_uri
  # image_config_command = ["lambdas/consumer.handler"]

  create_role = false
  lambda_role = "arn:aws:iam::${local.account_id}:role/LabRole"
  # lambda_role = aws_iam_role.consumer_lambda_role.arn
}

resource "aws_lambda_event_source_mapping" "consumer_sqs_trigger" {
  event_source_arn = aws_sqs_queue.producer_queue.arn
  function_name    = module.lambda_function_with_docker_build_from_ecr_consumer.lambda_function_name
  batch_size       = 25
  maximum_batching_window_in_seconds = 5  # wait up to 5s to collect a batch
  enabled          = true
}
