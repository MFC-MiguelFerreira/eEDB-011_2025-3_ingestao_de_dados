resource "aws_s3_bucket" "atividade5_bucket" {
  bucket        = "${var.project_name}-${local.account_id}"
  force_destroy = true
}
