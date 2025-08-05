module "lambda_function_bancos" {
  source = "terraform-aws-modules/lambda/aws"

  environment_variables = {
    raw_bucket_name     = aws_s3_bucket.raw_datalake_bucket.bucket,
    trusted_bucket_name = aws_s3_bucket.trusted_datalake_bucket.bucket,
    POLARS_TEMP_DIR     = "/tmp/polars"
  }

  function_name = "to_trusted_bancos"
  handler       = "bancos.lambda_handler"
  runtime       = "python3.12"
  timeout       = 60
  publish       = true

  source_path = ["../lambdas/scripts/to_trusted/bancos.py"]

  store_on_s3 = true
  s3_bucket   = aws_s3_bucket.lambda_bucket.bucket

  create_layer = false
  layers       = [module.lambda_layer_s3.lambda_layer_arn, ]

  ######################
  # Additional policies
  ######################
  create_role = false
  lambda_role = "arn:aws:iam::${local.account_id}:role/LabRole"
}

module "lambda_function_glassdoor" {
  source = "terraform-aws-modules/lambda/aws"

  environment_variables = {
    raw_bucket_name     = aws_s3_bucket.raw_datalake_bucket.bucket,
    trusted_bucket_name = aws_s3_bucket.trusted_datalake_bucket.bucket,
    POLARS_TEMP_DIR     = "/tmp/polars"
  }

  function_name = "to_trusted_glassdoor"
  handler       = "glassdoor.lambda_handler"
  runtime       = "python3.12"
  timeout       = 60
  publish       = true

  source_path = ["../lambdas/scripts/to_trusted/glassdoor.py"]

  store_on_s3 = true
  s3_bucket   = aws_s3_bucket.lambda_bucket.bucket

  create_layer = false
  layers       = [module.lambda_layer_s3.lambda_layer_arn, ]

  ######################
  # Additional policies
  ######################
  create_role = false
  lambda_role = "arn:aws:iam::${local.account_id}:role/LabRole"
}

module "lambda_function_reclamacoes" {
  source = "terraform-aws-modules/lambda/aws"

  environment_variables = {
    raw_bucket_name     = aws_s3_bucket.raw_datalake_bucket.bucket,
    trusted_bucket_name = aws_s3_bucket.trusted_datalake_bucket.bucket,
    POLARS_TEMP_DIR     = "/tmp/polars"
  }

  function_name = "to_trusted_reclamacoes"
  handler       = "reclamacoes.lambda_handler"
  runtime       = "python3.12"
  memory_size   = 512
  timeout       = 60
  publish       = true

  source_path = ["../lambdas/scripts/to_trusted/reclamacoes.py"]

  store_on_s3 = true
  s3_bucket   = aws_s3_bucket.lambda_bucket.bucket

  create_layer = false
  layers       = [
    "arn:aws:lambda:us-east-1:336392948345:layer:AWSSDKPandas-Python312:16"
  ]

  ######################
  # Additional policies
  ######################
  create_role = false
  lambda_role = "arn:aws:iam::${local.account_id}:role/LabRole"
}
