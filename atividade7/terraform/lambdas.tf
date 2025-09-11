locals {
  raw_bucket_name = "ativadade-05-${local.account_id}"
}

module "lambda_function_with_docker_build_from_ecr" {
  source = "terraform-aws-modules/lambda/aws"

  function_name = "${var.project_name}_producer"
  description   = "Lambda function to producer the data from the source."
  timeout       = 120

  environment_variables = {
    raw_bucket_name = local.raw_bucket_name
  }

  ##################
  # Container Image
  ##################
  create_package = false

  package_type  = "Image"
  architectures = ["x86_64"]

  image_uri            = module.docker_image.image_uri
  image_config_command = ["lambdas/producer.handler"]

  ##################
  # Policy
  ##################
  create_role = false
  attach_policy_statements = false
  lambda_role = "arn:aws:iam::${local.account_id}:role/LabRole"
}
