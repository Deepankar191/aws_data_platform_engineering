# =============================================================================
# Terraform + provider version pins.
# =============================================================================
terraform {
  required_version = ">= 1.6.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.60"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }

  # Backend is intentionally partial — pass -backend-config per env at init time so
  # dev/pre/prod keep separate state (see README). Example:
  #   terraform init -backend-config=backends/prod.hcl
  backend "s3" {
    key     = "credit-decision-platform/terraform.tfstate"
    encrypt = true
  }
}
