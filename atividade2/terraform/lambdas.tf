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

module "lambda_function_result" {
  source = "terraform-aws-modules/lambda/aws"

  environment_variables = {
    trusted_bucket_name = aws_s3_bucket.trusted_datalake_bucket.bucket,
    delivery_bucket_name = aws_s3_bucket.delivery_datalake_bucket.bucket,
    POLARS_TEMP_DIR     = "/tmp/polars"
  }

  function_name = "to_delivery_result"
  handler       = "result.lambda_handler"
  runtime       = "python3.12"
  memory_size   = 512
  timeout       = 60
  publish       = true

  source_path = ["../lambdas/scripts/to_delivery/result.py"]

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

module "lambda_function_to_db" {
  source = "terraform-aws-modules/lambda/aws"

  environment_variables = {
    delivery_bucket_name = aws_s3_bucket.delivery_datalake_bucket.bucket,
  }

  function_name = "to_db"
  handler       = "to_db.lambda_handler"
  runtime       = "python3.12"
  memory_size   = 1024
  ephemeral_storage_size = 512
  timeout       = 300
  publish       = true

  source_path = ["../lambdas/scripts/to_db/to_db.py"]

  store_on_s3 = true
  s3_bucket   = aws_s3_bucket.lambda_bucket.bucket

  create_layer = false
  layers       = [module.lambda_layer_s3_to_db.lambda_layer_arn]

  ######################
  # Additional policies
  ######################
  create_role = false
  lambda_role = "arn:aws:iam::${local.account_id}:role/LabRole"

  # ✅ VPC Configuration to access Aurora
  vpc_subnet_ids         = [
    "subnet-03cacd8897dbb5e9f",
    "subnet-07bd42120c3d1d120",
    "subnet-0b38f6e311d149cbf",
    "subnet-0173a2cec145c7e6d",
    "subnet-0844c11dda16c40ee",
    "subnet-0421061737a616939",
  ]     # Replace with your actual subnet IDs
  vpc_security_group_ids = [
    "sg-0135aebcef6d4d241"
  ]  # Create one below or use existing
}
