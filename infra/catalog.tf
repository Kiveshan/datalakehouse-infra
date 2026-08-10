# catalog.tf
#
# Glue Data Catalog for the lakehouse. A crawler reads raw/ (DMS's DROP_AND_CREATE
# output) and registers its tables in a Glue database, so the transform job
# (transform.tf) and Athena can query it.
#
# The crawler is not scheduled: raw/ only changes when a DMS reload runs, so it
# is started manually after one, same as the DMS task itself is started manually
# (see the "Starting a DMS run" section in the root CLAUDE.md).

resource "aws_glue_catalog_database" "raw" {
  name        = "${replace(local.name_prefix, "-", "_")}_raw"
  description = "Tables crawled from raw/ in the lakehouse bucket (DMS output)."
}

data "aws_iam_policy_document" "glue_crawler_assume" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "glue_crawler" {
  name               = "${local.name_prefix}-glue-crawler"
  description        = "Lets the raw-folder crawler read the lakehouse bucket and write to the Glue Catalog"
  assume_role_policy = data.aws_iam_policy_document.glue_crawler_assume.json

  tags = local.common_tags
}

resource "aws_iam_role_policy_attachment" "glue_crawler_service" {
  role       = aws_iam_role.glue_crawler.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

data "aws_iam_policy_document" "glue_crawler_s3" {
  statement {
    sid       = "ReadRaw"
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.lakehouse-bucket.arn}/raw/*"]
  }

  statement {
    sid       = "ListRawPrefix"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.lakehouse-bucket.arn]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["raw/*"]
    }
  }
}

resource "aws_iam_role_policy" "glue_crawler_s3" {
  name   = "${local.name_prefix}-glue-crawler-s3"
  role   = aws_iam_role.glue_crawler.id
  policy = data.aws_iam_policy_document.glue_crawler_s3.json
}

resource "aws_glue_crawler" "raw" {
  name          = "${local.name_prefix}-raw-crawler"
  description   = "Crawls raw/ in the lakehouse bucket into ${aws_glue_catalog_database.raw.name}"
  role          = aws_iam_role.glue_crawler.arn
  database_name = aws_glue_catalog_database.raw.name

  s3_target {
    path = "s3://${aws_s3_bucket.lakehouse-bucket.id}/raw/"
  }

  # DMS drops and recreates everything under raw/ on every reload, so the
  # catalog should follow suit: update tables whose schema changed, and drop
  # catalog entries for tables that no longer exist under raw/.
  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "DELETE_FROM_DATABASE"
  }

  tags = local.common_tags
}
