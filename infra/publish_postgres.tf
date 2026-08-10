# publish_postgres.tf
#
# RDS Postgres that mirrors the layer2 Iceberg warehouse (dims + facts) for
# downstream reporting, plus the VPC networking the Glue publish jobs need to
# reach it. This is a fresh instance for this account/environment — NOT the
# original client-staging RDS endpoint hardcoded in the original docx scripts.
# That endpoint's password was sitting in plaintext across several shared
# Word docs and must be treated as compromised; rotate it independently of
# anything here.
#
# Credentials are never embedded in a Glue script or job argument: they live
# only in Secrets Manager, and each publish job's Python reads them at runtime
# (USE_SECRETS = True, SECRET_NAME passed as a job argument — see
# infra/glue/scripts/publish_*.py and build_layer2_facts_ctas.py).
#
# Reuses the same default-VPC lookup as the DMS replication instance
# (local.dms_vpc_id / local.dms_subnet_ids, defined in ingestion.tf).

resource "random_password" "publish_postgres" {
  length  = 32
  special = false # avoid characters that need escaping in a JDBC URL
}

resource "aws_db_subnet_group" "publish_postgres" {
  name       = "${local.name_prefix}-publish-postgres"
  subnet_ids = local.dms_subnet_ids

  tags = local.common_tags
}

# Inbound to RDS is restricted to the Glue connection's own security group
# below — nothing else can reach it.
resource "aws_security_group" "publish_postgres_rds" {
  name        = "${local.name_prefix}-publish-postgres-rds"
  description = "RDS Postgres mirroring layer2 dims/facts. Inbound only from the Glue publish jobs security group."
  vpc_id      = local.dms_vpc_id

  tags = local.common_tags
}

# Attached to the Glue NETWORK connection (below) that the publish jobs use to
# place an ENI in the VPC. AWS requires a NETWORK connection's security group
# to allow all-TCP to itself, in addition to whatever it needs to reach.
resource "aws_security_group" "glue_publish_postgres" {
  name        = "${local.name_prefix}-glue-publish-postgres"
  description = "Attached to the Glue NETWORK connection used by the layer2-to-Postgres publish jobs"
  vpc_id      = local.dms_vpc_id

  tags = local.common_tags
}

resource "aws_vpc_security_group_ingress_rule" "glue_publish_postgres_self" {
  security_group_id            = aws_security_group.glue_publish_postgres.id
  referenced_security_group_id = aws_security_group.glue_publish_postgres.id
  ip_protocol                  = "-1"
  description                  = "Required by AWS Glue: ENIs on this connection must reach each other on all ports"
}

resource "aws_vpc_security_group_egress_rule" "glue_publish_postgres_self" {
  security_group_id            = aws_security_group.glue_publish_postgres.id
  referenced_security_group_id = aws_security_group.glue_publish_postgres.id
  ip_protocol                  = "-1"
  description                  = "Required by AWS Glue: ENIs on this connection must reach each other on all ports"
}

resource "aws_vpc_security_group_egress_rule" "glue_publish_postgres_to_rds" {
  security_group_id            = aws_security_group.glue_publish_postgres.id
  referenced_security_group_id = aws_security_group.publish_postgres_rds.id
  ip_protocol                  = "tcp"
  from_port                    = 5432
  to_port                      = 5432
  description                  = "Postgres"
}

resource "aws_vpc_security_group_ingress_rule" "publish_postgres_rds_from_glue" {
  security_group_id            = aws_security_group.publish_postgres_rds.id
  referenced_security_group_id = aws_security_group.glue_publish_postgres.id
  ip_protocol                  = "tcp"
  from_port                    = 5432
  to_port                      = 5432
  description                  = "Postgres, from the Glue publish jobs only"
}

resource "aws_db_instance" "publish_postgres" {
  identifier     = "${local.name_prefix}-publish-postgres"
  engine         = "postgres"
  engine_version = "16"
  instance_class = "db.t4g.micro"

  allocated_storage     = 20
  max_allocated_storage = 100
  storage_type          = "gp3"
  storage_encrypted     = true

  db_name  = "postgres"
  username = "postgres"
  password = random_password.publish_postgres.result
  port     = 5432

  db_subnet_group_name   = aws_db_subnet_group.publish_postgres.name
  vpc_security_group_ids = [aws_security_group.publish_postgres_rds.id]
  publicly_accessible    = false

  backup_retention_period = 7
  skip_final_snapshot     = true # rebuildable in full from the Iceberg warehouse; no need to retain a final snapshot
  deletion_protection     = false

  tags = local.common_tags
}

resource "aws_secretsmanager_secret" "publish_postgres" {
  name        = "${local.name_prefix}/publish-postgres"
  description = "Credentials for the layer2-to-Postgres publish jobs (build_layer2_facts_ctas.py, publish_*_postgres.py)"

  tags = local.common_tags
}

resource "aws_secretsmanager_secret_version" "publish_postgres" {
  secret_id = aws_secretsmanager_secret.publish_postgres.id
  secret_string = jsonencode({
    username = aws_db_instance.publish_postgres.username
    password = random_password.publish_postgres.result
  })
}

# A specific subnet (and its AZ) is needed for the Glue connection's
# physical_connection_requirements; the DMS subnet group only gives a list.
data "aws_subnet" "glue_publish_postgres" {
  id = local.dms_subnet_ids[0]
}

# NETWORK connection: pure VPC placement, no JDBC URL/credentials of its own.
# The publish jobs open their own JDBC connection to aws_db_instance.publish_postgres
# using creds read from Secrets Manager at runtime.
resource "aws_glue_connection" "publish_postgres" {
  name            = "${local.name_prefix}-publish-postgres"
  connection_type = "NETWORK"

  physical_connection_requirements {
    availability_zone      = data.aws_subnet.glue_publish_postgres.availability_zone
    subnet_id              = data.aws_subnet.glue_publish_postgres.id
    security_group_id_list = [aws_security_group.glue_publish_postgres.id]
  }
}
