# endpoints.tf

# Source: MySQL (originally hosted on Azure).
resource "aws_dms_endpoint" "source" {
  endpoint_id   = var.source_endpoint_id
  endpoint_type = "source"
  engine_name   = "mysql"

  server_name = var.source_server_name
  port        = var.source_port
  username    = var.source_username
  password    = var.source_password

  # Only none, verify-ca and verify-full are accepted by the mysql engine;
  # "require" is rejected at CreateEndpoint. See variables.tf.
  ssl_mode        = var.source_ssl_mode
  certificate_arn = var.source_certificate_arn

  kms_key_arn = var.kms_key_arn

  tags = var.tags
}

# Target: S3, written as parquet.
#
# Uses aws_dms_s3_endpoint, not aws_dms_endpoint: the AWS provider v6 no longer
# accepts engine_name = "s3" on the generic endpoint resource.
resource "aws_dms_s3_endpoint" "target" {
  endpoint_id   = var.target_endpoint_id
  endpoint_type = "target"

  bucket_name             = var.target_bucket_name
  bucket_folder           = var.target_bucket_folder
  service_access_role_arn = var.dms_s3_role_arn

  data_format       = "parquet"
  compression_type  = "NONE"
  enable_statistics = true

  csv_delimiter     = ","
  csv_row_delimiter = "\\n"

  # Carried over from the source configuration. Inert while data_format is
  # parquet, but pinned so the provider's schema default (true) cannot quietly
  # change behaviour if the format is ever switched to csv.
  rfc_4180 = false

  date_partition_enabled = false
  ssl_mode               = "none"

  tags = var.tags
}
