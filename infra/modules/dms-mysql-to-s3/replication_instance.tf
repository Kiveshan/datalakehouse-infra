# replication_instance.tf
#
# NOTE: `availability_zone` is deliberately NOT set.
#
# The original instance pinned "af-south-1c", but AZ *names* are shuffled per
# account — af-south-1c in one account is generally not the same physical AZ as
# af-south-1c in another. Carrying the name across accounts is meaningless, so
# DMS picks an AZ from the subnet group instead. Pin it here only if you have a
# specific reason, and prefer AZ IDs over names when you do.

resource "aws_dms_replication_instance" "this" {
  replication_instance_id    = var.replication_instance_id
  replication_instance_class = var.replication_instance_class
  allocated_storage          = var.allocated_storage
  engine_version             = var.engine_version

  replication_subnet_group_id = var.replication_subnet_group_id
  vpc_security_group_ids      = var.vpc_security_group_ids
  multi_az                    = var.multi_az
  publicly_accessible         = var.publicly_accessible

  auto_minor_version_upgrade   = var.auto_minor_version_upgrade
  preferred_maintenance_window = var.preferred_maintenance_window

  kms_key_arn = var.kms_key_arn

  tags = var.tags
}
