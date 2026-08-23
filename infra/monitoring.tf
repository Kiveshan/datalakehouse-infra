# monitoring.tf
#
# Custom CloudWatch metrics for the ETL pipeline, a dashboard summarizing
# them, and SNS alarms on Step Function / Glue failures.
#
# Metric sources:
#   - RecordsProcessed, TablesProcessed, TablesFailed, TablesSkipped,
#     JobDurationSeconds: emitted by move_raw_tables.py (transform.tf) into
#     the custom "ETLPipeline" namespace, dimensioned by JobName. TablesFailed
#     matters on its own: process_single_table() catches per-table errors so
#     one bad table doesn't fail the whole Glue job, which means the job can
#     report SUCCEEDED to Step Functions while individual tables silently
#     failed -- see move_raw_tables_table_failures below.
#   - TablesValidated, ValidationFailures: emitted by src-validate-raw
#     (lambda/src-validate-raw/index.js) into the same namespace. This
#     validates table structure (S3 data present, Glue table has columns),
#     not row-level content, so ValidationFailures counts failed table
#     checks, not failed rows.
#   - Pipeline duration and error rate use Step Functions' own AWS/States
#     metrics (ExecutionTime, ExecutionsFailed, ExecutionsSucceeded) rather
#     than reinventing them -- Step Functions already reports these
#     accurately per state machine.

locals {
  metrics_namespace = "ETLPipeline"
}

# ---------------------------------------------------------------------------
# Alarms: any Glue job failure inside either state machine routes to
# "Send Failure Notification" then a Fail state (see step_functions/*.tftpl),
# so a single ExecutionsFailed alarm per state machine covers both Step
# Function failures and Glue job failures that Step Functions observes.
# ---------------------------------------------------------------------------

resource "aws_cloudwatch_metric_alarm" "dms_raw_execution_failed" {
  alarm_name          = "${local.name_prefix}-dms-raw-execution-failed"
  alarm_description   = "dms_raw state machine (archive/DMS/crawl/validate + nightly ETL) had a failed execution"
  namespace           = "AWS/States"
  metric_name         = "ExecutionsFailed"
  dimensions          = { StateMachineArn = aws_sfn_state_machine.dms_raw.arn }
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.etl_pipeline_notifications.arn]
  ok_actions          = [aws_sns_topic.etl_pipeline_notifications.arn]
  tags                = local.common_tags
}

resource "aws_cloudwatch_metric_alarm" "src_improved_pipeline_execution_failed" {
  alarm_name          = "${local.name_prefix}-src-improved-pipeline-execution-failed"
  alarm_description   = "src_improved_pipeline state machine (nightly ETL: raw -> dims -> facts -> publish) had a failed execution, including any Glue job failure in the chain"
  namespace           = "AWS/States"
  metric_name         = "ExecutionsFailed"
  dimensions          = { StateMachineArn = aws_sfn_state_machine.src_improved_pipeline.arn }
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.etl_pipeline_notifications.arn]
  ok_actions          = [aws_sns_topic.etl_pipeline_notifications.arn]
  tags                = local.common_tags
}

# move_raw_tables can report Glue job SUCCEEDED to Step Functions while
# individual tables failed underneath it (see the comment above), so that
# failure mode needs its own alarm on the custom metric.
resource "aws_cloudwatch_metric_alarm" "move_raw_tables_table_failures" {
  alarm_name          = "${local.name_prefix}-move-raw-tables-table-failures"
  alarm_description   = "move_raw_tables Glue job completed but one or more tables failed to process (silent per-table failure)"
  namespace           = local.metrics_namespace
  metric_name         = "TablesFailed"
  dimensions          = { JobName = aws_glue_job.move_raw_tables.name }
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.etl_pipeline_notifications.arn]
  tags                = local.common_tags
}

# ---------------------------------------------------------------------------
# Dashboard
# ---------------------------------------------------------------------------

resource "aws_cloudwatch_dashboard" "etl_pipeline" {
  dashboard_name = "${local.name_prefix}-etl-pipeline"

  dashboard_body = jsonencode({
    widgets = [
      {
        type   = "text"
        x      = 0
        y      = 0
        width  = 24
        height = 1
        properties = {
          markdown = "# ETL Pipeline — ${var.environment}\nRecords processed, pipeline duration, validation failures, and error rate across dms_raw and src_improved_pipeline."
        }
      },
      {
        type   = "metric"
        x      = 0
        y      = 1
        width  = 12
        height = 6
        properties = {
          title   = "Records Processed per Run"
          view    = "timeSeries"
          stacked = false
          region  = var.aws_region
          period  = 86400
          stat    = "Sum"
          metrics = [
            [local.metrics_namespace, "RecordsProcessed", "JobName", aws_glue_job.move_raw_tables.name]
          ]
        }
      },
      {
        type   = "metric"
        x      = 12
        y      = 1
        width  = 12
        height = 6
        properties = {
          title  = "Pipeline Duration (seconds)"
          view   = "timeSeries"
          region = var.aws_region
          period = 86400
          stat   = "Average"
          metrics = [
            [{ expression = "m1d/1000", label = "dms_raw", id = "e1d" }],
            ["AWS/States", "ExecutionTime", "StateMachineArn", aws_sfn_state_machine.dms_raw.arn, { id = "m1d", visible = false }],
            [{ expression = "m1s/1000", label = "src_improved_pipeline", id = "e1s" }],
            ["AWS/States", "ExecutionTime", "StateMachineArn", aws_sfn_state_machine.src_improved_pipeline.arn, { id = "m1s", visible = false }],
          ]
        }
      },
      {
        type   = "metric"
        x      = 0
        y      = 7
        width  = 12
        height = 6
        properties = {
          title   = "Raw Validation (src-validate-raw)"
          view    = "timeSeries"
          stacked = false
          region  = var.aws_region
          period  = 86400
          stat    = "Sum"
          metrics = [
            [local.metrics_namespace, "TablesValidated", { color = "#2ca02c" }],
            [local.metrics_namespace, "ValidationFailures", { color = "#d62728" }],
          ]
        }
      },
      {
        type   = "metric"
        x      = 12
        y      = 7
        width  = 12
        height = 6
        properties = {
          title  = "Pipeline Error Rate (%)"
          view   = "timeSeries"
          region = var.aws_region
          period = 86400
          stat   = "Sum"
          metrics = [
            [{ expression = "100*(f1/(f1+s1))", label = "dms_raw error rate", id = "e1" }],
            ["AWS/States", "ExecutionsFailed", "StateMachineArn", aws_sfn_state_machine.dms_raw.arn, { id = "f1", visible = false }],
            ["AWS/States", "ExecutionsSucceeded", "StateMachineArn", aws_sfn_state_machine.dms_raw.arn, { id = "s1", visible = false }],
            [{ expression = "100*(f2/(f2+s2))", label = "src_improved_pipeline error rate", id = "e2" }],
            ["AWS/States", "ExecutionsFailed", "StateMachineArn", aws_sfn_state_machine.src_improved_pipeline.arn, { id = "f2", visible = false }],
            ["AWS/States", "ExecutionsSucceeded", "StateMachineArn", aws_sfn_state_machine.src_improved_pipeline.arn, { id = "s2", visible = false }],
          ]
        }
      },
      {
        type   = "metric"
        x      = 0
        y      = 13
        width  = 12
        height = 6
        properties = {
          title   = "move_raw_tables: Table Outcomes"
          view    = "timeSeries"
          stacked = true
          region  = var.aws_region
          period  = 86400
          stat    = "Sum"
          metrics = [
            [local.metrics_namespace, "TablesProcessed", "JobName", aws_glue_job.move_raw_tables.name, { color = "#2ca02c" }],
            [local.metrics_namespace, "TablesFailed", "JobName", aws_glue_job.move_raw_tables.name, { color = "#d62728" }],
            [local.metrics_namespace, "TablesSkipped", "JobName", aws_glue_job.move_raw_tables.name, { color = "#ff7f0e" }],
          ]
        }
      },
      {
        type   = "alarm"
        x      = 12
        y      = 13
        width  = 12
        height = 6
        properties = {
          title = "Pipeline Failure Alarms"
          alarms = [
            aws_cloudwatch_metric_alarm.dms_raw_execution_failed.arn,
            aws_cloudwatch_metric_alarm.src_improved_pipeline_execution_failed.arn,
            aws_cloudwatch_metric_alarm.move_raw_tables_table_failures.arn,
          ]
        }
      },
    ]
  })
}
