# orchestration.tf
#
# The two Step Functions that drive the pipeline end to end, and the SNS
# topic notify-failure (lambdas.tf) publishes to on any failure. ASL
# definitions are rendered from infra/step_functions/*.asl.json.tftpl so the
# Lambda ARNs, Glue job names, and crawler name they reference always match
# what's actually deployed, instead of static strings that can drift (as the
# hand-built originals -- see the docx exports this was rebuilt from -- did,
# still pointing at a decommissioned account).
#
# dms_raw is the top-level state machine: archive raw/, run DMS, crawl,
# validate, then invoke src_improved_pipeline synchronously as its last real
# step. src_improved_pipeline is the nightly ETL: move_raw_tables through
# the layer2 dims/facts/publish jobs, in the order documented in the root
# CLAUDE.md.

resource "aws_sns_topic" "etl_pipeline_notifications" {
  name = "${local.name_prefix}-etl-pipeline-notifications"
  tags = local.common_tags
}

resource "aws_sns_topic_subscription" "etl_pipeline_notifications_email" {
  count     = var.notify_failure_email != "" ? 1 : 0
  topic_arn = aws_sns_topic.etl_pipeline_notifications.arn
  protocol  = "email"
  endpoint  = var.notify_failure_email
}

# ---------------------------------------------------------------------------
# src_improved_pipeline: the nightly ETL, invoked synchronously by dms_raw.
# Defined first since dms_raw's definition needs its ARN.
# ---------------------------------------------------------------------------

resource "aws_sfn_state_machine" "src_improved_pipeline" {
  name     = "${local.name_prefix}-src-improved-pipeline"
  role_arn = aws_iam_role.step_functions.arn

  definition = templatefile("${path.module}/step_functions/src_improved_pipeline.asl.json.tftpl", {
    start_glue_job_lambda_arn = aws_lambda_function.src_start_data_mart_glue_job.arn
    check_glue_job_lambda_arn = aws_lambda_function.src_check_glue_job_status.arn
    notify_failure_lambda_arn = aws_lambda_function.notify_failure.arn

    move_raw_tables_job_name               = aws_glue_job.move_raw_tables.name
    build_layer2_dims_scd2_job_name        = aws_glue_job.build_layer2_dims_scd2.name
    build_layer2_facts_job_name            = aws_glue_job.build_layer2_facts.name
    build_etqa_facts_job_name              = aws_glue_job.build_etqa_facts.name
    build_layer2_facts_ctas_job_name       = aws_glue_job.build_layer2_facts_ctas.name
    publish_layer2_dims_postgres_job_name  = aws_glue_job.publish_layer2_dims_postgres.name
    publish_layer2_facts_postgres_job_name = aws_glue_job.publish_layer2_facts_postgres.name
    publish_etqa_facts_postgres_job_name   = aws_glue_job.publish_etqa_facts_postgres.name
  })

  tags = local.common_tags
}

resource "aws_sfn_state_machine" "dms_raw" {
  name     = "${local.name_prefix}-dms-raw"
  role_arn = aws_iam_role.step_functions.arn

  definition = templatefile("${path.module}/step_functions/dms_raw.asl.json.tftpl", {
    archive_raw_lambda_arn          = aws_lambda_function.src_archive_raw.arn
    start_dms_lambda_arn            = aws_lambda_function.src_start_dms.arn
    check_dms_status_lambda_arn     = aws_lambda_function.src_check_dms_status.arn
    run_raw_crawler_lambda_arn      = aws_lambda_function.src_run_raw_crawler.arn
    check_crawler_status_lambda_arn = aws_lambda_function.src_check_crawler_status.arn
    validate_raw_lambda_arn         = aws_lambda_function.src_validate_raw.arn
    notify_failure_lambda_arn       = aws_lambda_function.notify_failure.arn

    raw_crawler_name        = aws_glue_crawler.raw.name
    child_state_machine_arn = aws_sfn_state_machine.src_improved_pipeline.arn
  })

  tags = local.common_tags
}

# ---------------------------------------------------------------------------
# IAM: the role both state machines run as.
# ---------------------------------------------------------------------------

data "aws_iam_policy_document" "step_functions_assume" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["states.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "step_functions" {
  name               = "${local.name_prefix}-step-functions"
  description        = "Execution role for dms_raw and src_improved_pipeline: invokes the 9 orchestration Lambdas and, for dms_raw, starts/tracks the nested src_improved_pipeline execution"
  assume_role_policy = data.aws_iam_policy_document.step_functions_assume.json

  tags = local.common_tags
}

data "aws_iam_policy_document" "step_functions" {
  statement {
    sid     = "InvokeOrchestrationLambdas"
    actions = ["lambda:InvokeFunction"]
    resources = [
      aws_lambda_function.src_archive_raw.arn,
      aws_lambda_function.src_start_dms.arn,
      aws_lambda_function.src_check_dms_status.arn,
      aws_lambda_function.src_run_raw_crawler.arn,
      aws_lambda_function.src_check_crawler_status.arn,
      aws_lambda_function.src_validate_raw.arn,
      aws_lambda_function.src_start_data_mart_glue_job.arn,
      aws_lambda_function.src_check_glue_job_status.arn,
      aws_lambda_function.notify_failure.arn,
    ]
  }

  # dms_raw invokes src_improved_pipeline via states:startExecution.sync,
  # which needs StartExecution on the child state machine plus
  # Describe/StopExecution on its own executions (AWS's .sync polling
  # pattern) and the EventBridge managed-rule permissions documented at
  # https://docs.aws.amazon.com/step-functions/latest/dg/connect-stepfunctions.html
  statement {
    sid       = "StartNestedPipeline"
    actions   = ["states:StartExecution"]
    resources = [aws_sfn_state_machine.src_improved_pipeline.arn]
  }

  statement {
    sid     = "TrackNestedPipelineExecution"
    actions = ["states:DescribeExecution", "states:StopExecution"]
    resources = [
      "arn:aws:states:${var.aws_region}:${data.aws_caller_identity.current.account_id}:execution:${local.name_prefix}-src-improved-pipeline:*"
    ]
  }

  statement {
    sid     = "SyncCallbackEventRule"
    actions = ["events:PutTargets", "events:PutRule", "events:DescribeRule"]
    resources = [
      "arn:aws:events:${var.aws_region}:${data.aws_caller_identity.current.account_id}:rule/StepFunctionsGetEventsForStepFunctionsExecutionRule"
    ]
  }
}

resource "aws_iam_role_policy" "step_functions" {
  name   = "${local.name_prefix}-step-functions"
  role   = aws_iam_role.step_functions.id
  policy = data.aws_iam_policy_document.step_functions.json
}
