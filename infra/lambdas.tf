# lambdas.tf
#
# Deploys the 9 orchestration Lambdas checked into ../lambda/ (source lives
# outside infra/ since it predates this Terraform project -- see that
# directory's own history). Each is zipped straight from its own folder
# (index.js + node_modules, already installed against the AWS SDK v3 clients
# each one actually imports) and deployed under this project's naming
# convention, so the ARNs referenced by step_functions/*.tftpl always match
# what's actually deployed.
#
# All nine share one IAM role (lambda_pipeline, below) since they're one
# cohesive orchestration layer running the same pipeline; permissions are
# still scoped per-action to only the resources each function touches.

locals {
  raw_prefix = "raw/${var.dms_source_schema_name}/"
}

data "archive_file" "src_archive_raw" {
  type        = "zip"
  source_dir  = "${path.module}/../lambda/src-archive-raw"
  output_path = "${path.module}/lambda_builds/src-archive-raw.zip"
}

resource "aws_cloudwatch_log_group" "src_archive_raw" {
  name              = "/aws/lambda/${local.name_prefix}-src-archive-raw"
  retention_in_days = 14
  tags              = local.common_tags
}

resource "aws_lambda_function" "src_archive_raw" {
  function_name    = "${local.name_prefix}-src-archive-raw"
  role             = aws_iam_role.lambda_pipeline.arn
  handler          = "index.handler"
  runtime          = "nodejs20.x"
  filename         = data.archive_file.src_archive_raw.output_path
  source_code_hash = data.archive_file.src_archive_raw.output_base64sha256
  timeout          = 300
  memory_size      = 256

  environment {
    variables = {
      LAKEHOUSE_BUCKET = aws_s3_bucket.lakehouse-bucket.id
      SOURCE_PREFIX    = local.raw_prefix
    }
  }

  depends_on = [aws_cloudwatch_log_group.src_archive_raw]
  tags       = local.common_tags
}

data "archive_file" "src_start_dms" {
  type        = "zip"
  source_dir  = "${path.module}/../lambda/src-start-dms"
  output_path = "${path.module}/lambda_builds/src-start-dms.zip"
}

resource "aws_cloudwatch_log_group" "src_start_dms" {
  name              = "/aws/lambda/${local.name_prefix}-src-start-dms"
  retention_in_days = 14
  tags              = local.common_tags
}

resource "aws_lambda_function" "src_start_dms" {
  function_name    = "${local.name_prefix}-src-start-dms"
  role             = aws_iam_role.lambda_pipeline.arn
  handler          = "index.handler"
  runtime          = "nodejs20.x"
  filename         = data.archive_file.src_start_dms.output_path
  source_code_hash = data.archive_file.src_start_dms.output_base64sha256
  timeout          = 30

  environment {
    variables = {
      REPLICATION_TASK_ARN = module.lms_dms.replication_task_arn
    }
  }

  depends_on = [aws_cloudwatch_log_group.src_start_dms]
  tags       = local.common_tags
}

data "archive_file" "src_check_dms_status" {
  type        = "zip"
  source_dir  = "${path.module}/../lambda/src-check-dms-status"
  output_path = "${path.module}/lambda_builds/src-check-dms-status.zip"
}

resource "aws_cloudwatch_log_group" "src_check_dms_status" {
  name              = "/aws/lambda/${local.name_prefix}-src-check-dms-status"
  retention_in_days = 14
  tags              = local.common_tags
}

resource "aws_lambda_function" "src_check_dms_status" {
  function_name    = "${local.name_prefix}-src-check-dms-status"
  role             = aws_iam_role.lambda_pipeline.arn
  handler          = "index.handler"
  runtime          = "nodejs20.x"
  filename         = data.archive_file.src_check_dms_status.output_path
  source_code_hash = data.archive_file.src_check_dms_status.output_base64sha256
  timeout          = 30

  environment {
    variables = {
      REPLICATION_TASK_ARN = module.lms_dms.replication_task_arn
    }
  }

  depends_on = [aws_cloudwatch_log_group.src_check_dms_status]
  tags       = local.common_tags
}

data "archive_file" "src_run_raw_crawler" {
  type        = "zip"
  source_dir  = "${path.module}/../lambda/src-run-raw-crawler"
  output_path = "${path.module}/lambda_builds/src-run-raw-crawler.zip"
}

resource "aws_cloudwatch_log_group" "src_run_raw_crawler" {
  name              = "/aws/lambda/${local.name_prefix}-src-run-raw-crawler"
  retention_in_days = 14
  tags              = local.common_tags
}

resource "aws_lambda_function" "src_run_raw_crawler" {
  function_name    = "${local.name_prefix}-src-run-raw-crawler"
  role             = aws_iam_role.lambda_pipeline.arn
  handler          = "index.handler"
  runtime          = "nodejs20.x"
  filename         = data.archive_file.src_run_raw_crawler.output_path
  source_code_hash = data.archive_file.src_run_raw_crawler.output_base64sha256
  timeout          = 30

  environment {
    variables = {
      CRAWLER_NAME = aws_glue_crawler.raw.name
    }
  }

  depends_on = [aws_cloudwatch_log_group.src_run_raw_crawler]
  tags       = local.common_tags
}

data "archive_file" "src_check_crawler_status" {
  type        = "zip"
  source_dir  = "${path.module}/../lambda/src-check-crawler-status"
  output_path = "${path.module}/lambda_builds/src-check-crawler-status.zip"
}

resource "aws_cloudwatch_log_group" "src_check_crawler_status" {
  name              = "/aws/lambda/${local.name_prefix}-src-check-crawler-status"
  retention_in_days = 14
  tags              = local.common_tags
}

resource "aws_lambda_function" "src_check_crawler_status" {
  function_name    = "${local.name_prefix}-src-check-crawler-status"
  role             = aws_iam_role.lambda_pipeline.arn
  handler          = "index.handler"
  runtime          = "nodejs20.x"
  filename         = data.archive_file.src_check_crawler_status.output_path
  source_code_hash = data.archive_file.src_check_crawler_status.output_base64sha256
  timeout          = 30

  environment {
    variables = {
      CRAWLER_NAME = aws_glue_crawler.raw.name
    }
  }

  depends_on = [aws_cloudwatch_log_group.src_check_crawler_status]
  tags       = local.common_tags
}

data "archive_file" "src_validate_raw" {
  type        = "zip"
  source_dir  = "${path.module}/../lambda/src-validate-raw"
  output_path = "${path.module}/lambda_builds/src-validate-raw.zip"
}

resource "aws_cloudwatch_log_group" "src_validate_raw" {
  name              = "/aws/lambda/${local.name_prefix}-src-validate-raw"
  retention_in_days = 14
  tags              = local.common_tags
}

resource "aws_lambda_function" "src_validate_raw" {
  function_name    = "${local.name_prefix}-src-validate-raw"
  role             = aws_iam_role.lambda_pipeline.arn
  handler          = "index.handler"
  runtime          = "nodejs20.x"
  filename         = data.archive_file.src_validate_raw.output_path
  source_code_hash = data.archive_file.src_validate_raw.output_base64sha256
  timeout          = 300
  memory_size      = 256

  environment {
    variables = {
      LAKEHOUSE_BUCKET = aws_s3_bucket.lakehouse-bucket.id
      RAW_DATABASE     = aws_glue_catalog_database.raw.name
      RAW_PREFIX       = local.raw_prefix
    }
  }

  depends_on = [aws_cloudwatch_log_group.src_validate_raw]
  tags       = local.common_tags
}

data "archive_file" "src_start_data_mart_glue_job" {
  type        = "zip"
  source_dir  = "${path.module}/../lambda/src-start-data-mart-glue-job"
  output_path = "${path.module}/lambda_builds/src-start-data-mart-glue-job.zip"
}

resource "aws_cloudwatch_log_group" "src_start_data_mart_glue_job" {
  name              = "/aws/lambda/${local.name_prefix}-src-start-data-mart-glue-job"
  retention_in_days = 14
  tags              = local.common_tags
}

resource "aws_lambda_function" "src_start_data_mart_glue_job" {
  function_name    = "${local.name_prefix}-src-start-data-mart-glue-job"
  role             = aws_iam_role.lambda_pipeline.arn
  handler          = "index.handler"
  runtime          = "nodejs20.x"
  filename         = data.archive_file.src_start_data_mart_glue_job.output_path
  source_code_hash = data.archive_file.src_start_data_mart_glue_job.output_base64sha256
  timeout          = 30

  depends_on = [aws_cloudwatch_log_group.src_start_data_mart_glue_job]
  tags       = local.common_tags
}

data "archive_file" "src_check_glue_job_status" {
  type        = "zip"
  source_dir  = "${path.module}/../lambda/src-check-glue-job-status"
  output_path = "${path.module}/lambda_builds/src-check-glue-job-status.zip"
}

resource "aws_cloudwatch_log_group" "src_check_glue_job_status" {
  name              = "/aws/lambda/${local.name_prefix}-src-check-glue-job-status"
  retention_in_days = 14
  tags              = local.common_tags
}

resource "aws_lambda_function" "src_check_glue_job_status" {
  function_name    = "${local.name_prefix}-src-check-glue-job-status"
  role             = aws_iam_role.lambda_pipeline.arn
  handler          = "index.handler"
  runtime          = "nodejs20.x"
  filename         = data.archive_file.src_check_glue_job_status.output_path
  source_code_hash = data.archive_file.src_check_glue_job_status.output_base64sha256
  timeout          = 30

  depends_on = [aws_cloudwatch_log_group.src_check_glue_job_status]
  tags       = local.common_tags
}

data "archive_file" "notify_failure" {
  type        = "zip"
  source_dir  = "${path.module}/../lambda/notify-failure"
  output_path = "${path.module}/lambda_builds/notify-failure.zip"
}

resource "aws_cloudwatch_log_group" "notify_failure" {
  name              = "/aws/lambda/${local.name_prefix}-notify-failure"
  retention_in_days = 14
  tags              = local.common_tags
}

resource "aws_lambda_function" "notify_failure" {
  function_name    = "${local.name_prefix}-notify-failure"
  role             = aws_iam_role.lambda_pipeline.arn
  handler          = "index.handler"
  runtime          = "nodejs20.x"
  filename         = data.archive_file.notify_failure.output_path
  source_code_hash = data.archive_file.notify_failure.output_base64sha256
  timeout          = 30

  environment {
    variables = {
      SNS_TOPIC_ARN = aws_sns_topic.etl_pipeline_notifications.arn
    }
  }

  depends_on = [aws_cloudwatch_log_group.notify_failure]
  tags       = local.common_tags
}

# ---------------------------------------------------------------------------
# IAM: one shared execution role, permissions scoped per action/resource.
# ---------------------------------------------------------------------------

data "aws_iam_policy_document" "lambda_assume" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "lambda_pipeline" {
  name               = "${local.name_prefix}-lambda-pipeline"
  description        = "Shared execution role for the 9 orchestration Lambdas driving the DMS/raw/layer2 Step Functions"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json

  tags = local.common_tags
}

resource "aws_iam_role_policy_attachment" "lambda_pipeline_logs" {
  role       = aws_iam_role.lambda_pipeline.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

data "aws_iam_policy_document" "lambda_pipeline" {
  statement {
    sid       = "DmsStartReplicationTask"
    actions   = ["dms:StartReplicationTask"]
    resources = [module.lms_dms.replication_task_arn]
  }

  statement {
    sid = "DmsDescribeReplicationTasks"
    # DescribeReplicationTasks does not support resource-level permissions.
    actions   = ["dms:DescribeReplicationTasks"]
    resources = ["*"]
  }

  statement {
    sid       = "GlueCrawler"
    actions   = ["glue:StartCrawler", "glue:GetCrawler"]
    resources = [aws_glue_crawler.raw.arn]
  }

  statement {
    sid     = "GlueGetRawTable"
    actions = ["glue:GetTable"]
    resources = [
      "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:catalog",
      "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:database/${aws_glue_catalog_database.raw.name}",
      "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:table/${aws_glue_catalog_database.raw.name}/*",
    ]
  }

  statement {
    sid     = "GlueJobRuns"
    actions = ["glue:StartJobRun", "glue:GetJobRun"]
    resources = [
      aws_glue_job.move_raw_tables.arn,
      aws_glue_job.build_layer2_dims_scd2.arn,
      aws_glue_job.build_layer2_facts.arn,
      aws_glue_job.build_etqa_facts.arn,
      aws_glue_job.build_layer2_facts_ctas.arn,
      aws_glue_job.publish_layer2_dims_postgres.arn,
      aws_glue_job.publish_layer2_facts_postgres.arn,
      aws_glue_job.publish_etqa_facts_postgres.arn,
    ]
  }

  statement {
    sid     = "S3RawArchiveObjects"
    actions = ["s3:GetObject", "s3:PutObject"]
    resources = [
      "${aws_s3_bucket.lakehouse-bucket.arn}/raw/*",
      "${aws_s3_bucket.lakehouse-bucket.arn}/archive/*",
    ]
  }

  statement {
    sid       = "S3ListRawArchive"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.lakehouse-bucket.arn]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["raw/*", "archive/*"]
    }
  }

  statement {
    sid       = "SnsPublishFailureNotifications"
    actions   = ["sns:Publish"]
    resources = [aws_sns_topic.etl_pipeline_notifications.arn]
  }
}

resource "aws_iam_role_policy" "lambda_pipeline" {
  name   = "${local.name_prefix}-lambda-pipeline"
  role   = aws_iam_role.lambda_pipeline.id
  policy = data.aws_iam_policy_document.lambda_pipeline.json
}
