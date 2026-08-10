# backend.tf
terraform {
  backend "s3" {
    bucket       = "gov-skills-tfstate"
    key          = "etl-pipeline/terraform.tfstate"
    region       = "af-south-1"
    encrypt      = true
    use_lockfile = true # native S3 locking — replaces dynamodb_table
  }
}