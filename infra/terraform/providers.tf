# =============================================================================
# Provider configuration. Credentials/account come from the caller's environment
# (assumed role / SSO profile) — never hardcoded. Each env is a separate account.
# =============================================================================
provider "aws" {
  region = var.aws_region

  default_tags {
    tags = local.common_tags
  }
}

provider "random" {}

# Account id is read (not set) — used to build ARNs without hardcoding.
data "aws_caller_identity" "current" {}

data "aws_region" "current" {}
