# transform.tf
#
# The Glue ETL job that reads tables cataloged from raw/ (catalog.tf) and writes
# cleaned Parquet into curated/, one prefix per table. Script source lives at
# infra/glue/scripts/move_raw_tables.py; edit that file and re-apply to redeploy.

resource "aws_s3_object" "move_raw_tables_script" {
  bucket = aws_s3_bucket.lakehouse-bucket.id
  key    = "scripts/move_raw_tables.py"
  source = "${path.module}/glue/scripts/move_raw_tables.py"
  etag   = filemd5("${path.module}/glue/scripts/move_raw_tables.py")
}

data "aws_iam_policy_document" "glue_etl_assume" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "glue_etl" {
  name               = "${local.name_prefix}-glue-etl"
  description        = "Lets the move-raw-tables job read raw/, write curated/, and read the Glue Catalog"
  assume_role_policy = data.aws_iam_policy_document.glue_etl_assume.json

  tags = local.common_tags
}

resource "aws_iam_role_policy_attachment" "glue_etl_service" {
  role       = aws_iam_role.glue_etl.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

data "aws_iam_policy_document" "glue_etl_s3" {
  statement {
    sid       = "ReadRaw"
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.lakehouse-bucket.arn}/raw/*"]
  }

  statement {
    sid       = "WriteCurated"
    actions   = ["s3:PutObject", "s3:GetObject", "s3:DeleteObject"]
    resources = ["${aws_s3_bucket.lakehouse-bucket.arn}/curated/*"]
  }

  statement {
    sid       = "ReadScript"
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_object.move_raw_tables_script.arn}"]
  }

  statement {
    sid       = "ListBucket"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.lakehouse-bucket.arn]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["raw/*", "curated/*", "scripts/*"]
    }
  }

  statement {
    sid = "PutPipelineMetrics"
    # PutMetricData does not support resource-level permissions.
    actions   = ["cloudwatch:PutMetricData"]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "glue_etl_s3" {
  name   = "${local.name_prefix}-glue-etl-s3"
  role   = aws_iam_role.glue_etl.id
  policy = data.aws_iam_policy_document.glue_etl_s3.json
}

resource "aws_glue_job" "move_raw_tables" {
  name              = "${local.name_prefix}-move-raw-tables"
  description       = "Reads tables cataloged from raw/, drops file/path-like columns, writes curated/<table>/ as Parquet"
  role_arn          = aws_iam_role.glue_etl.arn
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 5
  timeout           = 120

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.lakehouse-bucket.id}/${aws_s3_object.move_raw_tables_script.key}"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--source_database"                  = aws_glue_catalog_database.raw.name
    "--target_bucket"                    = aws_s3_bucket.lakehouse-bucket.id
    "--target_prefix"                    = "curated/"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
  }

  tags = local.common_tags
}
