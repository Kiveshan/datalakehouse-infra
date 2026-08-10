# locals.tf
#
# Locals shared across domain files (storage.tf, ingestion.tf, and future
# additions like catalog.tf / query.tf). Domain-specific locals stay in their
# own file next to the resources that use them.

locals {
  name_prefix = "${var.bucket-prefix}-${var.environment}"
  common_tags = {
    Project     = "gov-skills-pipeline"
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}
