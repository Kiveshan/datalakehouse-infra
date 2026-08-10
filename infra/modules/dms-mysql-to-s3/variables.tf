# variables.tf
#
# Anything account-, region- or VPC-specific is a variable with NO default,
# so a missing value is a plan-time error rather than a silent deploy into
# the wrong place. Behavioural settings keep defaults matching the original
# af-south-1 setup.

# ---------------------------------------------------------------------------
# Naming
# ---------------------------------------------------------------------------

variable "source_endpoint_id" {
  description = "Identifier for the MySQL source endpoint. Unique per account+region."
  type        = string
  default     = "lms-prod"
}

variable "target_endpoint_id" {
  description = "Identifier for the S3 target endpoint. Unique per account+region."
  type        = string
  default     = "s3-target"
}

variable "replication_instance_id" {
  description = "Identifier for the DMS replication instance. Unique per account+region."
  type        = string
  default     = "azure-mysql-replication"
}

variable "replication_task_id" {
  description = "Identifier for the replication task. Unique per account+region."
  type        = string
  default     = "lms-prod-migration"
}

# ---------------------------------------------------------------------------
# Source: MySQL on Azure
# ---------------------------------------------------------------------------

variable "source_server_name" {
  description = "Hostname of the source MySQL server, e.g. prod-source.mysql.database.azure.com"
  type        = string
}

variable "source_port" {
  description = "Port of the source MySQL server."
  type        = number
  default     = 3306
}

variable "source_username" {
  description = "MySQL user DMS connects as. Needs SELECT on the migrated schema."
  type        = string
}

variable "source_password" {
  description = <<-EOT
    Password for source_username.

    DMS never returns endpoint passwords via its API, so this can never be
    imported — it must always be supplied. Provide it via TF_VAR_source_password,
    a secrets backend, or a tfvars file that is NOT committed.
  EOT
  type        = string
  sensitive   = true
}

variable "source_ssl_mode" {
  description = <<-EOT
    SSL mode for the source connection.

    NOTE: "require" is NOT valid here. DMS accepts it for some engines, but a
    mysql endpoint rejects it at CreateEndpoint with:

      InvalidParameterCombinationException: The require SSL mode is not
      supported by the 'mysql' engine.

    The original endpoint used "none", meaning credentials and data crossed the
    public internet to Azure unencrypted. Prefer verify-full, which needs
    source_certificate_arn to be set as well.
  EOT
  type        = string
  default     = "none"

  validation {
    # "require" is deliberately excluded — see above.
    condition     = contains(["none", "verify-ca", "verify-full"], var.source_ssl_mode)
    error_message = "source_ssl_mode must be one of: none, verify-ca, verify-full. MySQL endpoints reject 'require'."
  }

  validation {
    condition     = var.source_ssl_mode == "none" || var.source_certificate_arn != null
    error_message = "source_certificate_arn is required when source_ssl_mode is verify-ca or verify-full."
  }
}

variable "source_certificate_arn" {
  description = <<-EOT
    ARN of an aws_dms_certificate holding the CA that signed the source server's
    certificate. Required for verify-ca and verify-full.

    For Azure Database for MySQL this is the DigiCert Global Root CA / Microsoft
    RSA Root CA 2017 bundle that Microsoft publishes.
  EOT
  type        = string
  default     = null
}

# ---------------------------------------------------------------------------
# Target: S3
# ---------------------------------------------------------------------------

variable "target_bucket_name" {
  description = <<-EOT
    S3 bucket DMS writes into. Must already exist in the target account.

    S3 bucket names are GLOBALLY unique — you cannot reuse the bucket name
    from the original account. Pick a new name.
  EOT
  type        = string
}

variable "target_bucket_folder" {
  description = "Key prefix within the bucket. Note the task uses DROP_AND_CREATE, so existing data under this prefix is overwritten."
  type        = string
  default     = "raw"
}

variable "dms_s3_role_arn" {
  description = <<-EOT
    ARN of an IAM role in the TARGET account that DMS assumes to write to S3.

    Must trust dms.<region>.amazonaws.com (or dms.amazonaws.com) and allow
    s3:PutObject, s3:DeleteObject, s3:ListBucket and s3:GetObject on the bucket.
    This module does not create the role — that usually belongs with the bucket.
  EOT
  type        = string
}

# ---------------------------------------------------------------------------
# Networking (VPC-specific — no defaults are possible)
# ---------------------------------------------------------------------------

variable "replication_subnet_group_id" {
  description = "DMS replication subnet group in the target account's VPC."
  type        = string
}

variable "vpc_security_group_ids" {
  description = "Security groups for the replication instance. Must permit egress to the source MySQL host on source_port."
  type        = list(string)
}

variable "publicly_accessible" {
  description = <<-EOT
    Whether the replication instance gets a public IP.

    The original was public because it reaches a MySQL server hosted on Azure.
    If you keep this true, note that the instance receives a NEW public IP, which
    must be added to the Azure MySQL firewall allowlist or the task cannot connect.
  EOT
  type        = bool
  default     = true
}

# ---------------------------------------------------------------------------
# Replication instance sizing
# ---------------------------------------------------------------------------

variable "replication_instance_class" {
  description = "DMS instance class."
  type        = string
  default     = "dms.t3.medium"
}

variable "allocated_storage" {
  description = "Storage in GB for the replication instance."
  type        = number
  default     = 50
}

variable "engine_version" {
  description = "DMS engine version. Pinned so a provider upgrade cannot silently move it."
  type        = string
  default     = "3.5.4"
}

variable "multi_az" {
  description = "Whether to run the replication instance across multiple AZs."
  type        = bool
  default     = false
}

variable "auto_minor_version_upgrade" {
  description = "Allow DMS to apply minor engine upgrades during the maintenance window."
  type        = bool
  default     = true
}

variable "preferred_maintenance_window" {
  description = "Weekly maintenance window, UTC, format ddd:hh24:mi-ddd:hh24:mi."
  type        = string
  default     = "tue:14:43-tue:15:13"
}

variable "kms_key_arn" {
  description = <<-EOT
    Optional customer-managed KMS key for DMS storage encryption.

    Left null, DMS uses the AWS-managed `aws/dms` key in whichever account it
    runs in. The original account's key ARN is deliberately not carried over —
    it does not exist elsewhere.
  EOT
  type        = string
  default     = null
}

# ---------------------------------------------------------------------------
# Task behaviour
# ---------------------------------------------------------------------------

variable "source_schema_name" {
  description = "Schema selected by the default table-mapping rule."
  type        = string
  default     = "source_schema"
}

variable "table_mappings" {
  description = <<-EOT
    Optional full override of the task's table mappings, as a JSON string.

    Left null, the module includes every table in source_schema_name.
  EOT
  type        = string
  default     = null
}

variable "migration_type" {
  description = "One of: full-load, cdc, full-load-and-cdc."
  type        = string
  default     = "full-load"

  validation {
    condition     = contains(["full-load", "cdc", "full-load-and-cdc"], var.migration_type)
    error_message = "migration_type must be one of: full-load, cdc, full-load-and-cdc."
  }
}

variable "start_replication_task" {
  description = <<-EOT
    Whether Terraform starts/stops the task.

    Default null means Terraform does not manage run state at all, so an apply
    can never start or stop a migration as a side effect. Set true only when you
    deliberately want `terraform apply` to kick off the migration.
  EOT
  type        = bool
  default     = null
}

variable "tags" {
  description = "Tags applied to all resources in this module."
  type        = map(string)
  default     = {}
}
