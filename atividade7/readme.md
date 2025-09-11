aws ecr get-login-password --profile mba | docker login --username AWS --password-stdin 472916995593.dkr.ecr.us-east-1.amazonaws.com

terraform apply -var-file="variable.tfvars"