# layer2_dims.tf
#
# Layer-2 SCD2 dimension pipeline: turns curated/<table>/ (transform.tf's
# output) into slowly-changing-dimension Iceberg tables under layer2/
# (storage.tf), tracked in their own Glue Catalog database.
#
# Two Glue jobs, both manually triggered like the rest of this pipeline
# (see "Cataloging and transforming raw" in the root CLAUDE.md):
#
#   1. create_layer2_dims  — bootstrap. Creates any dim_<table> Iceberg table
#      that doesn't exist yet, from the curated/<table>/ schema plus the fixed
#      SCD2 columns. Idempotent (CREATE TABLE IF NOT EXISTS); run once for any
#      newly mapped table before the nightly merge is pointed at it.
#   2. build_layer2_dims_scd2 — the nightly-run merge. Assumes the dim tables
#      already exist and updates them via Iceberg MERGE/UPDATE.
#
# Both scripts share the same table mapping (MAPPING_TEXT, hardcoded in each
# script — see infra/glue/scripts/) and the same job arguments below, so they
# always point at the same bucket/database/warehouse.

locals {
  layer2_dims_database     = "${replace(local.name_prefix, "-", "_")}_layer2"
  layer2_dims_catalog_name = "glue_catalog" # internal Spark catalog alias, not an AWS resource name
  layer2_warehouse_path    = "s3://${aws_s3_bucket.lakehouse-bucket.id}/layer2/"
}

resource "aws_glue_catalog_database" "layer2" {
  name        = local.layer2_dims_database
  description = "Iceberg SCD2 dim_<table> tables built from curated/ (see create_layer2_dims / build_layer2_dims_scd2 Glue jobs)."
}

resource "aws_s3_object" "create_layer2_dims_script" {
  bucket = aws_s3_bucket.lakehouse-bucket.id
  key    = "scripts/create_layer2_dims.py"
  source = "${path.module}/glue/scripts/create_layer2_dims.py"
  etag   = filemd5("${path.module}/glue/scripts/create_layer2_dims.py")
}

resource "aws_s3_object" "build_layer2_dims_scd2_script" {
  bucket = aws_s3_bucket.lakehouse-bucket.id
  key    = "scripts/build_layer2_dims_scd2.py"
  source = "${path.module}/glue/scripts/build_layer2_dims_scd2.py"
  etag   = filemd5("${path.module}/glue/scripts/build_layer2_dims_scd2.py")
}

data "aws_iam_policy_document" "glue_layer2_assume" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "glue_layer2" {
  name               = "${local.name_prefix}-glue-layer2"
  description        = "Shared by the layer2 dims bootstrap and nightly-merge jobs: reads curated/, reads/writes layer2/, and manages the layer2 Glue Catalog database"
  assume_role_policy = data.aws_iam_policy_document.glue_layer2_assume.json

  tags = local.common_tags
}

resource "aws_iam_role_policy_attachment" "glue_layer2_service" {
  role       = aws_iam_role.glue_layer2.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

data "aws_iam_policy_document" "glue_layer2_s3" {
  statement {
    sid       = "ReadCurated"
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.lakehouse-bucket.arn}/curated/*"]
  }

  statement {
    sid       = "ReadWriteLayer2Warehouse"
    actions   = ["s3:PutObject", "s3:GetObject", "s3:DeleteObject"]
    resources = ["${aws_s3_bucket.lakehouse-bucket.arn}/layer2/*"]
  }

  statement {
    sid     = "ReadScripts"
    actions = ["s3:GetObject"]
    resources = [
      aws_s3_object.create_layer2_dims_script.arn,
      aws_s3_object.build_layer2_dims_scd2_script.arn,
    ]
  }

  statement {
    sid       = "ListBucket"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.lakehouse-bucket.arn]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["curated/*", "layer2/*", "scripts/*"]
    }
  }
}

resource "aws_iam_role_policy" "glue_layer2_s3" {
  name   = "${local.name_prefix}-glue-layer2-s3"
  role   = aws_iam_role.glue_layer2.id
  policy = data.aws_iam_policy_document.glue_layer2_s3.json
}

# Also shared by the facts-builder, CTAS-auto, and publish-to-postgres jobs
# (layer2_facts.tf) — they read Postgres creds from this secret at runtime
# rather than embedding them in a script or job argument.
data "aws_iam_policy_document" "glue_layer2_secrets" {
  statement {
    sid       = "ReadPublishPostgresSecret"
    actions   = ["secretsmanager:GetSecretValue"]
    resources = [aws_secretsmanager_secret.publish_postgres.arn]
  }
}

resource "aws_iam_role_policy" "glue_layer2_secrets" {
  name   = "${local.name_prefix}-glue-layer2-secrets"
  role   = aws_iam_role.glue_layer2.id
  policy = data.aws_iam_policy_document.glue_layer2_secrets.json
}

resource "aws_glue_job" "create_layer2_dims" {
  name              = "${local.name_prefix}-create-layer2-dims"
  description       = "Bootstrap: creates any missing dim_<table> Iceberg tables from curated/<table>/ schemas plus SCD2 columns. Idempotent; run before pointing the nightly merge at a newly mapped table."
  role_arn          = aws_iam_role.glue_layer2.arn
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 60

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.lakehouse-bucket.id}/${aws_s3_object.create_layer2_dims_script.key}"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--datalake-formats"                 = "iceberg"
    "--curated_bucket"                   = aws_s3_bucket.lakehouse-bucket.id
    "--curated_prefix"                   = "curated/"
    "--dims_database"                    = local.layer2_dims_database
    "--dims_catalog_name"                = local.layer2_dims_catalog_name
    "--warehouse_path"                   = local.layer2_warehouse_path
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
  }

  tags = local.common_tags
}

resource "aws_glue_job" "build_layer2_dims_scd2" {
  name              = "${local.name_prefix}-build-layer2-dims-scd2"
  description       = "Nightly SCD2 merge from curated/<table>/ into the layer2 Iceberg dim tables. Run manually after a curated/ refresh; assumes create_layer2_dims has already been run for any new table."
  role_arn          = aws_iam_role.glue_layer2.arn
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 5
  timeout           = 180

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.lakehouse-bucket.id}/${aws_s3_object.build_layer2_dims_scd2_script.key}"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--datalake-formats"                 = "iceberg"
    "--curated_bucket"                   = aws_s3_bucket.lakehouse-bucket.id
    "--curated_prefix"                   = "curated/"
    "--dims_database"                    = local.layer2_dims_database
    "--dims_catalog_name"                = local.layer2_dims_catalog_name
    "--warehouse_path"                   = local.layer2_warehouse_path
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
  }

  tags = local.common_tags
}
