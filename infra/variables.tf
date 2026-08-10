variable "environment" {
  description = "Deployment Environment"
  type        = string
  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Environment must be one of: dev, staging, prod"
  }
}

variable "bucket-prefix" {
  description = "Prefix for the S3 bucket name"
  type        = string
  default     = "gov-seta-lakehouse"
}

variable "aws_region" {
  description = "Region everything is deployed into. Must match the backend region."
  type        = string
  default     = "af-south-1"
}

# ---------------------------------------------------------------------------
# DMS: source (the client's Azure MySQL)
# ---------------------------------------------------------------------------

variable "dms_source_server_name" {
  description = "Hostname of the client's Azure MySQL server, e.g. prod-source.mysql.database.azure.com"
  type        = string
}

variable "dms_source_port" {
  description = "Port of the source MySQL server."
  type        = number
  default     = 3306
}

variable "dms_source_username" {
  description = "MySQL user DMS connects as. Needs SELECT on the migrated schema."
  type        = string
}

variable "dms_source_password" {
  description = <<-EOT
    Password for dms_source_username.

    DMS never returns endpoint passwords through its API, so this can never be
    imported and must always be supplied. Set it via TF_VAR_dms_source_password
    or in prod.tfvars, which .gitignore excludes.
  EOT
  type        = string
  sensitive   = true
}

variable "dms_source_ca_cert_path" {
  description = <<-EOT
    Path to the PEM of the CA that signed the Azure MySQL server certificate.

    Left empty, the connection forces ssl_mode = "none" — credentials and every
    migrated row cross the public internet unencrypted. Set this to turn TLS
    verification on.
  EOT
  type        = string
  default     = ""
}

variable "dms_source_ssl_mode" {
  description = <<-EOT
    SSL mode for the source connection.

    No default on purpose: a fresh apply must explicitly choose a value. If
    dms_source_ca_cert_path is left empty, this must be set to "none" —
    plan fails otherwise, so an unencrypted connection is an explicit,
    acknowledged choice rather than a silent fallback. "require" is not an
    option: DMS rejects it for mysql endpoints.
  EOT
  type        = string

  validation {
    condition     = contains(["none", "verify-ca", "verify-full"], var.dms_source_ssl_mode)
    error_message = "dms_source_ssl_mode must be one of: none, verify-ca, verify-full. MySQL endpoints reject 'require'."
  }

  validation {
    condition     = var.dms_source_ca_cert_path != "" || var.dms_source_ssl_mode == "none"
    error_message = "dms_source_ca_cert_path is empty, which forces an unencrypted connection. Set dms_source_ssl_mode = \"none\" explicitly to confirm that's intended, or set dms_source_ca_cert_path to enable TLS."
  }
}

variable "dms_source_schema_name" {
  description = "Schema on the source to migrate. Every table in it is included."
  type        = string
  default     = "source_db"
}

# ---------------------------------------------------------------------------
# DMS: target and networking
# ---------------------------------------------------------------------------

variable "dms_target_bucket_folder" {
  description = <<-EOT
    Prefix inside the lakehouse bucket that DMS writes to.

    The task runs with TargetTablePrepMode = DROP_AND_CREATE, so each run
    overwrites everything under this prefix. Set to plain "raw" so the raw-folder
    crawler (catalog.tf) has a single, stable prefix to crawl.
  EOT
  type        = string
  default     = "raw"
}

variable "dms_vpc_id" {
  description = <<-EOT
    VPC the DMS security group is created in.

    Left empty, the default VPC is looked up, which needs ec2:DescribeVpcs.
    Set it together with dms_subnet_ids to skip the lookup entirely.
  EOT
  type        = string
  default     = ""
}

variable "dms_subnet_ids" {
  description = <<-EOT
    Subnets for the DMS replication subnet group. Needs at least two, in
    different AZs, with a route to an internet gateway.

    Left empty, the default VPC's subnets are looked up, which needs
    ec2:DescribeSubnets. Set it together with dms_vpc_id to skip the lookup.
  EOT
  type        = list(string)
  default     = []
}

variable "notify_failure_email" {
  description = <<-EOT
    Email address subscribed to the etl-pipeline-notifications SNS topic that
    notify-failure publishes to. Left empty (the default), the topic exists
    with no subscriptions -- failures publish successfully but nothing reads
    them until something subscribes.
  EOT
  type        = string
  default     = ""
}

variable "create_dms_vpc_role" {
  description = <<-EOT
    Whether to create the account-wide `dms-vpc-role` service role.

    DMS refuses to create a replication subnet group without it. AWS creates it
    automatically the first time DMS is used from the console, so set this true
    only in an account where DMS has never been used — otherwise the apply fails
    with EntityAlreadyExists.
  EOT
  type        = bool
  default     = false
}