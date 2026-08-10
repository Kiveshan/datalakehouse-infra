# schedule.tf
#
# EventBridge Scheduler that kicks off the pipeline automatically every night
# by starting dms_raw (orchestration.tf) -- the top-level state machine that
# archives raw/, runs DMS, crawls, validates, then invokes
# src_improved_pipeline itself. Nothing downstream needs its own schedule.

resource "aws_scheduler_schedule" "dms_raw_nightly" {
  name        = "${local.name_prefix}-dms-raw-nightly"
  description = "Starts the dms_raw state machine every night at 23:00 SAST"
  group_name  = "default"

  # af-south-1 (Cape Town) is Africa/Johannesburg, UTC+2 year-round -- no DST
  # to worry about, but naming the timezone explicitly keeps the schedule
  # correct at 23:00 local regardless.
  schedule_expression          = "cron(0 23 * * ? *)"
  schedule_expression_timezone = "Africa/Johannesburg"

  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn      = aws_sfn_state_machine.dms_raw.arn
    role_arn = aws_iam_role.scheduler_dms_raw.arn
    input    = "{}"
  }
}

data "aws_iam_policy_document" "scheduler_dms_raw_assume" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["scheduler.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "scheduler_dms_raw" {
  name               = "${local.name_prefix}-scheduler-dms-raw"
  description        = "Lets EventBridge Scheduler start dms_raw on its nightly schedule"
  assume_role_policy = data.aws_iam_policy_document.scheduler_dms_raw_assume.json

  tags = local.common_tags
}

data "aws_iam_policy_document" "scheduler_dms_raw" {
  statement {
    sid       = "StartDmsRaw"
    actions   = ["states:StartExecution"]
    resources = [aws_sfn_state_machine.dms_raw.arn]
  }
}

resource "aws_iam_role_policy" "scheduler_dms_raw" {
  name   = "${local.name_prefix}-scheduler-dms-raw"
  role   = aws_iam_role.scheduler_dms_raw.id
  policy = data.aws_iam_policy_document.scheduler_dms_raw.json
}
