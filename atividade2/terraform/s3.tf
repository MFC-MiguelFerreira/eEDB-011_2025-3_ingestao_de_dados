resource "aws_s3_bucket" "raw_datalake_bucket" {
  bucket        = "00-raw-${local.account_id}"
  force_destroy = true
}

resource "aws_s3_bucket" "trusted_datalake_bucket" {
  bucket        = "01-trusted-${local.account_id}"
  force_destroy = true
}

resource "aws_s3_bucket" "delivery_datalake_bucket" {
  bucket        = "02-delivery-${local.account_id}"
  force_destroy = true
}

resource "aws_s3_bucket" "lambda_bucket" {
  bucket        = "lambda-${local.account_id}"
  force_destroy = true
}