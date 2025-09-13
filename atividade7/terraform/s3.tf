resource "aws_s3_bucket" "atividade5_bucket" {
  bucket        = "${var.project_name}"
  force_destroy = true
}
