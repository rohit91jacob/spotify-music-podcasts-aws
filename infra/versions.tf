terraform {
  required_version = ">= 1.9.0, < 2.0.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.68"
    }
  }

  # Remote state for a real deployment (S3 native locking, no DynamoDB table needed).
  # Supply the settings with `terraform init -backend-config=environments/dev.s3.tfbackend`.
  # CI only runs `terraform init -backend=false` and validate: nothing is applied from CI.
  backend "s3" {}
}
