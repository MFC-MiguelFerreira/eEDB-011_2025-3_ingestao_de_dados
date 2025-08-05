module "lambda_layer_s3" {
  source = "terraform-aws-modules/lambda/aws"

  create_layer = true

  layer_name          = "ingestao-de-dados-atividade2"
  description         = "Lambda Layer with necessary dependencies to Atividade2"
  compatible_runtimes = ["python3.12"]

  source_path = "../lambdas/layer"

  store_on_s3 = true
  s3_bucket   = aws_s3_bucket.lambda_bucket.bucket
}
