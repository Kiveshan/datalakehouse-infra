# versions.tf
#
# Module-level constraints only. There is deliberately NO provider block here:
# the calling project supplies its own configured aws provider (and therefore
# its own account and region).

terraform {
  required_version = ">= 1.11"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
}
