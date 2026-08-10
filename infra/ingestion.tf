# ingestion.tf
#
# Wires the dms-mysql-to-s3 module (extracted from account 123456789012) into
# this project. The module deliberately creates DMS resources only; the four
# prerequisites listed in its README live here:
#
#   1. the S3 bucket            -> aws_s3_bucket.lakehouse-bucket (storage.tf)
#   2. the DMS S3 IAM role      -> aws_iam_role.dms_s3 below (scoped to raw/)
#   3. subnet group + SG        -> below
#   4. Azure firewall allowlist -> manual, see the dms_replication_instance_public_ips output
#
# Additional ingestion sources (more DMS tasks, other pipelines) belong in this
# file or a sibling ingestion_*.tf file, following the same shape.

# ---------------------------------------------------------------------------
# Networking
#
# This project has no VPC of its own, so the default VPC is used unless
# dms_vpc_id is set. Its subnets have a route to an internet gateway, which is
# what publicly_accessible = true on the replication instance needs in order to
# reach Azure.
#
# The lookups are skipped entirely when dms_vpc_id and dms_subnet_ids are both
# supplied, which is the way out if the Terraform principal lacks
# ec2:DescribeVpcs / ec2:DescribeSubnets.
# ---------------------------------------------------------------------------

locals {
  dms_lookup_vpc = var.dms_vpc_id == "" || length(var.dms_subnet_ids) == 0

  dms_vpc_id     = var.dms_vpc_id != "" ? var.dms_vpc_id : one(data.aws_vpc.default[*].id)
  dms_subnet_ids = length(var.dms_subnet_ids) > 0 ? var.dms_subnet_ids : one(data.aws_subnets.default[*].ids)
}

data "aws_vpc" "default" {
  count   = local.dms_lookup_vpc ? 1 : 0
  default = true
}

data "aws_subnets" "default" {
  count = local.dms_lookup_vpc ? 1 : 0

  filter {
    name   = "vpc-id"
    values = [local.dms_vpc_id]
  }
}

resource "aws_dms_replication_subnet_group" "lakehouse" {
  replication_subnet_group_id          = "${local.name_prefix}-dms"
  replication_subnet_group_description = "Subnets for the ${local.name_prefix} DMS replication instance"
  subnet_ids                           = local.dms_subnet_ids

  tags = local.common_tags
}

resource "aws_security_group" "dms" {
  name        = "${local.name_prefix}-dms"
  description = "DMS replication instance: egress to the Azure MySQL source"
  vpc_id      = local.dms_vpc_id

  tags = local.common_tags
}

# The source is a hostname on Azure with no stable published IP range, so egress
# cannot be narrowed to a CIDR. It is narrowed to the MySQL port instead.
resource "aws_vpc_security_group_egress_rule" "dms_mysql" {
  security_group_id = aws_security_group.dms.id
  description       = "MySQL to the Azure source"
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "tcp"
  from_port         = var.dms_source_port
  to_port           = var.dms_source_port
}

# S3 and CloudWatch are reached over the public endpoints from the default VPC.
resource "aws_vpc_security_group_egress_rule" "dms_https" {
  security_group_id = aws_security_group.dms.id
  description       = "HTTPS to S3 and CloudWatch Logs"
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "tcp"
  from_port         = 443
  to_port           = 443
}

# ---------------------------------------------------------------------------
# IAM: the role DMS assumes to write into the lakehouse bucket
# ---------------------------------------------------------------------------

data "aws_iam_policy_document" "dms_s3_assume" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["dms.${var.aws_region}.amazonaws.com", "dms.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "dms_s3" {
  name               = "${local.name_prefix}-dms-s3"
  description        = "Lets DMS write migrated tables into the lakehouse bucket"
  assume_role_policy = data.aws_iam_policy_document.dms_s3_assume.json

  tags = local.common_tags
}

data "aws_iam_policy_document" "dms_s3" {
  statement {
    sid     = "WriteObjects"
    actions = ["s3:PutObject", "s3:GetObject", "s3:DeleteObject"]
    # Scoped to the DMS prefix so a task can never overwrite curated or archive.
    resources = ["${aws_s3_bucket.lakehouse-bucket.arn}/${var.dms_target_bucket_folder}/*"]
  }

  statement {
    sid       = "ListBucket"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.lakehouse-bucket.arn]
  }
}

resource "aws_iam_role_policy" "dms_s3" {
  name   = "${local.name_prefix}-dms-s3"
  role   = aws_iam_role.dms_s3.id
  policy = data.aws_iam_policy_document.dms_s3.json
}

# The account-wide service role DMS requires before it will accept a replication
# subnet group. It is created once per account, outside this project's lifecycle,
# and may already exist if DMS was ever used from the console — hence the flag.
resource "aws_iam_role" "dms_vpc" {
  count = var.create_dms_vpc_role ? 1 : 0

  name = "dms-vpc-role" # the name is fixed by AWS; it cannot be prefixed
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Action    = "sts:AssumeRole"
      Principal = { Service = "dms.amazonaws.com" }
    }]
  })

  tags = local.common_tags
}

resource "aws_iam_role_policy_attachment" "dms_vpc" {
  count = var.create_dms_vpc_role ? 1 : 0

  role       = aws_iam_role.dms_vpc[0].name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonDMSVPCManagementRole"
}

# ---------------------------------------------------------------------------
# TLS to the source
#
# The source is a MySQL server on the public internet, so the connection should
# be verified, not merely encrypted. That needs the CA that signed the Azure
# server's certificate uploaded to DMS.
#
# Microsoft publishes the bundle at:
#   https://learn.microsoft.com/azure/mysql/flexible-server/concepts-networking-ssl-tls
#
# Download it, save it next to this file, and set dms_source_ca_cert_path.
# Leaving the path empty forces dms_source_ssl_mode = "none" (matching what the
# original hand-built endpoint did) — and requires that variable be set to
# "none" explicitly, so plan fails instead of silently going unencrypted.
# ---------------------------------------------------------------------------

resource "aws_dms_certificate" "source_ca" {
  count = var.dms_source_ca_cert_path != "" ? 1 : 0

  certificate_id  = "${local.name_prefix}-azure-mysql-ca"
  certificate_pem = file(var.dms_source_ca_cert_path)

  tags = local.common_tags
}

# ---------------------------------------------------------------------------
# The migration itself
# ---------------------------------------------------------------------------

module "lms_dms" {
  source = "./modules/dms-mysql-to-s3"

  # Source: MySQL on Azure, owned by the client.
  source_server_name = var.dms_source_server_name
  source_port        = var.dms_source_port
  source_username    = var.dms_source_username
  source_password    = var.dms_source_password
  source_schema_name = var.dms_source_schema_name

  source_ssl_mode        = var.dms_source_ca_cert_path != "" ? var.dms_source_ssl_mode : "none"
  source_certificate_arn = one(aws_dms_certificate.source_ca[*].certificate_arn)

  # Target: this project's lakehouse bucket.
  target_bucket_name   = aws_s3_bucket.lakehouse-bucket.id
  target_bucket_folder = var.dms_target_bucket_folder
  dms_s3_role_arn      = aws_iam_role.dms_s3.arn

  # Networking in this account.
  replication_subnet_group_id = aws_dms_replication_subnet_group.lakehouse.id
  vpc_security_group_ids      = [aws_security_group.dms.id]

  # Names are unique per account+region, so they are prefixed like everything else.
  source_endpoint_id      = "${local.name_prefix}-lms-source"
  target_endpoint_id      = "${local.name_prefix}-s3-target"
  replication_instance_id = "${local.name_prefix}-dms"
  replication_task_id     = "${local.name_prefix}-lms-migration"

  migration_type = "full-load"

  # No apostrophes: DMS restricts tag values to letters, digits, whitespace and
  # _ . : / = + \ - @ and rejects the whole CreateEndpoint call otherwise.
  tags = merge(local.common_tags, {
    description = "Full load from the client-owned Azure MySQL into the lakehouse"
  })

  depends_on = [aws_iam_role_policy.dms_s3]
}
