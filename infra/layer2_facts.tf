# layer2_facts.tf
#
# Rest of the layer2 pipeline (see "glue scripts" order in the root
# CLAUDE.md), all manually triggered like the rest of the pipeline:
#
#   3. build_layer2_facts        — Kimball fact_<x> Iceberg tables, joined from dim_<x>.
#   5. build_layer2_facts_ctas   — fact_sdf_application / fact_wsp_submission via
#                                   Iceberg CTAS, then publishes those + fact_company
#                                   to Postgres.
#   6. publish_layer2_dims_postgres  — autodiscovers dim_<x> and mirrors to Postgres.
#   7. publish_layer2_facts_postgres — mirrors the build_layer2_facts.py facts to
#                                       Postgres with PK/FK constraints.
#   4. publish_etqa_facts_postgres   — mirrors fact_certifylearners /
#                                       fact_assessor_moderator to Postgres. Those
#                                       two facts are built manually, outside this
#                                       pipeline; this job only publishes what's
#                                       already there.
#
# All five reuse aws_iam_role.glue_layer2 (layer2_dims.tf). The four that talk to
# Postgres also attach aws_glue_connection.publish_postgres (publish_postgres.tf)
# for VPC access and read DB creds from Secrets Manager at runtime — never a
# hardcoded password (see publish_postgres.tf for why that matters here).

locals {
  pg_job_args = {
    "--pg_host"       = aws_db_instance.publish_postgres.address
    "--pg_port"       = tostring(aws_db_instance.publish_postgres.port)
    "--pg_database"   = aws_db_instance.publish_postgres.db_name
    "--pg_schema"     = "public"
    "--pg_secret_arn" = aws_secretsmanager_secret.publish_postgres.arn
  }

  layer2_dims_job_args = {
    "--dims_database"     = local.layer2_dims_database
    "--dims_catalog_name" = local.layer2_dims_catalog_name
    "--warehouse_path"    = local.layer2_warehouse_path
  }
}

resource "aws_s3_object" "build_layer2_facts_script" {
  bucket = aws_s3_bucket.lakehouse-bucket.id
  key    = "scripts/build_layer2_facts.py"
  source = "${path.module}/glue/scripts/build_layer2_facts.py"
  etag   = filemd5("${path.module}/glue/scripts/build_layer2_facts.py")
}

resource "aws_s3_object" "build_layer2_facts_ctas_script" {
  bucket = aws_s3_bucket.lakehouse-bucket.id
  key    = "scripts/build_layer2_facts_ctas.py"
  source = "${path.module}/glue/scripts/build_layer2_facts_ctas.py"
  etag   = filemd5("${path.module}/glue/scripts/build_layer2_facts_ctas.py")
}

resource "aws_s3_object" "publish_layer2_dims_postgres_script" {
  bucket = aws_s3_bucket.lakehouse-bucket.id
  key    = "scripts/publish_layer2_dims_postgres.py"
  source = "${path.module}/glue/scripts/publish_layer2_dims_postgres.py"
  etag   = filemd5("${path.module}/glue/scripts/publish_layer2_dims_postgres.py")
}

resource "aws_s3_object" "publish_layer2_facts_postgres_script" {
  bucket = aws_s3_bucket.lakehouse-bucket.id
  key    = "scripts/publish_layer2_facts_postgres.py"
  source = "${path.module}/glue/scripts/publish_layer2_facts_postgres.py"
  etag   = filemd5("${path.module}/glue/scripts/publish_layer2_facts_postgres.py")
}

resource "aws_s3_object" "publish_etqa_facts_postgres_script" {
  bucket = aws_s3_bucket.lakehouse-bucket.id
  key    = "scripts/publish_etqa_facts_postgres.py"
  source = "${path.module}/glue/scripts/publish_etqa_facts_postgres.py"
  etag   = filemd5("${path.module}/glue/scripts/publish_etqa_facts_postgres.py")
}

resource "aws_s3_object" "build_etqa_facts_script" {
  bucket = aws_s3_bucket.lakehouse-bucket.id
  key    = "scripts/build_etqa_facts.py"
  source = "${path.module}/glue/scripts/build_etqa_facts.py"
  etag   = filemd5("${path.module}/glue/scripts/build_etqa_facts.py")
}

# glue_layer2's S3 policy (layer2_dims.tf) only grants curated/* and layer2/*,
# which already covers everything these scripts read/write in S3 (Iceberg data
# + scripts/). Reading the five new script objects needs an explicit grant like
# the original two, since ReadScripts lists objects by ARN.
data "aws_iam_policy_document" "glue_layer2_facts_scripts" {
  statement {
    sid     = "ReadFactsAndPublishScripts"
    actions = ["s3:GetObject"]
    resources = [
      aws_s3_object.build_layer2_facts_script.arn,
      aws_s3_object.build_layer2_facts_ctas_script.arn,
      aws_s3_object.publish_layer2_dims_postgres_script.arn,
      aws_s3_object.publish_layer2_facts_postgres_script.arn,
      aws_s3_object.publish_etqa_facts_postgres_script.arn,
      aws_s3_object.build_etqa_facts_script.arn,
    ]
  }
}

resource "aws_iam_role_policy" "glue_layer2_facts_scripts" {
  name   = "${local.name_prefix}-glue-layer2-facts-scripts"
  role   = aws_iam_role.glue_layer2.id
  policy = data.aws_iam_policy_document.glue_layer2_facts_scripts.json
}

resource "aws_glue_job" "build_layer2_facts" {
  name              = "${local.name_prefix}-build-layer2-facts"
  description       = "Builds the Kimball fact_<x> Iceberg tables from dim_<x>. Run after build_layer2_dims_scd2."
  role_arn          = aws_iam_role.glue_layer2.arn
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 5
  timeout           = 180

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.lakehouse-bucket.id}/${aws_s3_object.build_layer2_facts_script.key}"
    python_version  = "3"
  }

  default_arguments = merge(local.layer2_dims_job_args, {
    "--job-language"                     = "python"
    "--datalake-formats"                 = "iceberg"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
  })

  tags = local.common_tags
}

resource "aws_glue_job" "build_layer2_facts_ctas" {
  name              = "${local.name_prefix}-build-layer2-facts-ctas"
  description       = "Builds fact_sdf_application/fact_wsp_submission via Iceberg CTAS, then publishes those plus fact_company to Postgres. Run after build_layer2_facts."
  role_arn          = aws_iam_role.glue_layer2.arn
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 5
  timeout           = 180
  connections       = [aws_glue_connection.publish_postgres.name]

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.lakehouse-bucket.id}/${aws_s3_object.build_layer2_facts_ctas_script.key}"
    python_version  = "3"
  }

  default_arguments = merge(local.layer2_dims_job_args, local.pg_job_args, {
    "--job-language"                     = "python"
    "--datalake-formats"                 = "iceberg"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
  })

  tags = local.common_tags
}

resource "aws_glue_job" "publish_layer2_dims_postgres" {
  name              = "${local.name_prefix}-publish-layer2-dims-postgres"
  description       = "Autodiscovers dim_<x> Iceberg tables and mirrors them incrementally to Postgres. Run after build_layer2_dims_scd2."
  role_arn          = aws_iam_role.glue_layer2.arn
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 5
  timeout           = 180
  connections       = [aws_glue_connection.publish_postgres.name]

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.lakehouse-bucket.id}/${aws_s3_object.publish_layer2_dims_postgres_script.key}"
    python_version  = "3"
  }

  default_arguments = merge(local.layer2_dims_job_args, local.pg_job_args, {
    "--job-language"                     = "python"
    "--datalake-formats"                 = "iceberg"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
  })

  tags = local.common_tags
}

resource "aws_glue_job" "publish_layer2_facts_postgres" {
  name              = "${local.name_prefix}-publish-layer2-facts-postgres"
  description       = "Mirrors the build_layer2_facts.py fact_<x> tables to Postgres with PK/FK constraints. Run after build_layer2_facts and publish_layer2_dims_postgres."
  role_arn          = aws_iam_role.glue_layer2.arn
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 5
  timeout           = 180
  connections       = [aws_glue_connection.publish_postgres.name]

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.lakehouse-bucket.id}/${aws_s3_object.publish_layer2_facts_postgres_script.key}"
    python_version  = "3"
  }

  default_arguments = merge(local.layer2_dims_job_args, local.pg_job_args, {
    "--job-language"                     = "python"
    "--datalake-formats"                 = "iceberg"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
  })

  tags = local.common_tags
}

resource "aws_glue_job" "build_etqa_facts" {
  name              = "${local.name_prefix}-build-etqa-facts"
  description       = "Builds fact_certifylearners/fact_assessor_moderator via Iceberg CTAS. Run after build_layer2_dims_scd2 and before publish_etqa_facts_postgres. PLACEHOLDER join logic -- see infra/glue/scripts/build_etqa_facts.py header before relying on this in production."
  role_arn          = aws_iam_role.glue_layer2.arn
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 5
  timeout           = 180

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.lakehouse-bucket.id}/${aws_s3_object.build_etqa_facts_script.key}"
    python_version  = "3"
  }

  default_arguments = merge(local.layer2_dims_job_args, {
    "--job-language"                     = "python"
    "--datalake-formats"                 = "iceberg"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
  })

  tags = local.common_tags
}

resource "aws_glue_job" "publish_etqa_facts_postgres" {
  name              = "${local.name_prefix}-publish-etqa-facts-postgres"
  description       = "Mirrors the manually-built fact_certifylearners/fact_assessor_moderator to Postgres with FK constraints. Run after publish_layer2_dims_postgres."
  role_arn          = aws_iam_role.glue_layer2.arn
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 60
  connections       = [aws_glue_connection.publish_postgres.name]

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.lakehouse-bucket.id}/${aws_s3_object.publish_etqa_facts_postgres_script.key}"
    python_version  = "3"
  }

  default_arguments = merge(local.layer2_dims_job_args, local.pg_job_args, {
    "--job-language"                     = "python"
    "--datalake-formats"                 = "iceberg"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
  })

  tags = local.common_tags
}
