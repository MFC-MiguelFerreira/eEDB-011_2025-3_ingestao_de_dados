locals {
  source_path   = "../"
  path_include  = ["**/lambdas/**", "**/sql/**", "**/pipe/**"]
  path_exclude  = ["**/__pycache__/**"]
  files_include = setunion([for f in local.path_include : fileset(local.source_path, f)]...)
  files_exclude = setunion([for f in local.path_exclude : fileset(local.source_path, f)]...)
  files         = sort(setsubtract(local.files_include, local.files_exclude))

  dir_sha = sha1(join("", [for f in local.files : filesha1("${local.source_path}/${f}")]))
}

module "docker_image" {
  source = "terraform-aws-modules/lambda/aws//modules/docker-build"

  ecr_repo = module.ecr.repository_name

  use_image_tag = false

  source_path = local.source_path
  platform    = "linux/amd64"

  triggers = {
    dir_sha = local.dir_sha
  }
}

module "ecr" {
  source = "terraform-aws-modules/ecr/aws"

  repository_name                 = var.project_name
  repository_force_delete         = true
  repository_image_tag_mutability = "MUTABLE"

  create_lifecycle_policy = false

  repository_lambda_read_access_arns = [
    module.lambda_function_with_docker_build_from_ecr_producer.lambda_function_arn,
    module.lambda_function_with_docker_build_from_ecr_consumer.lambda_function_arn
  ]
}