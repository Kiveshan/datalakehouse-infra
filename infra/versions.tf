terraform {
  required_version = ">= 1.11" # native S3 locking needs 1.11+

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0" # allow 6.x, block 7.0 breaking changes
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.4"
    }
  }
}