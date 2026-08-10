# replication_task.tf

locals {
  # All components AWS enables by default, at default severity.
  dms_log_components = [
    "TRANSFORMATION", "SOURCE_UNLOAD", "IO", "TARGET_LOAD", "PERFORMANCE",
    "SOURCE_CAPTURE", "SORTER", "REST_SERVER", "VALIDATOR_EXT", "TARGET_APPLY",
    "TASK_MANAGER", "TABLES_MANAGER", "METADATA_MANAGER", "FILE_FACTORY",
    "COMMON", "ADDONS", "DATA_STRUCTURE", "COMMUNICATION", "FILE_TRANSFER",
  ]

  default_table_mappings = jsonencode({
    rules = [{
      rule-type   = "selection"
      rule-id     = "1"
      rule-name   = "include-all-tables"
      rule-action = "include"
      object-locator = {
        schema-name = var.source_schema_name
        table-name  = "%"
      }
    }]
  })
}

# To harden this against accidental teardown in a shared infra repo, add:
#
#   lifecycle {
#     prevent_destroy = true
#   }
#
# It is left off by default because prevent_destroy blocks `terraform destroy`
# for the WHOLE calling project, not just this resource, which is a surprising
# side effect to hand someone else's repo. `prevent_destroy` requires a literal,
# so it cannot be toggled by a variable.
resource "aws_dms_replication_task" "this" {
  replication_task_id = var.replication_task_id
  migration_type      = var.migration_type

  replication_instance_arn = aws_dms_replication_instance.this.replication_instance_arn
  source_endpoint_arn      = aws_dms_endpoint.source.endpoint_arn
  target_endpoint_arn      = aws_dms_s3_endpoint.target.endpoint_arn

  table_mappings = coalesce(var.table_mappings, local.default_table_mappings)

  start_replication_task = var.start_replication_task

  # Mirrors the original task settings. Logging.CloudWatchLogGroup and
  # Logging.CloudWatchLogStream are intentionally absent: DMS reports them as
  # read-only and rejects any write that includes them.
  replication_task_settings = jsonencode({
    Logging = {
      EnableLogging    = true
      EnableLogContext = true
      LogComponents = [
        for id in local.dms_log_components : {
          Id       = id
          Severity = "LOGGER_SEVERITY_DEFAULT"
        }
      ]
    }

    # TargetTablePrepMode = DROP_AND_CREATE means a run overwrites whatever is
    # already under the target prefix. Verify the prefix before first apply.
    FullLoadSettings = {
      CommitRate                      = 10000
      CreatePkAfterFullLoad           = false
      MaxFullLoadSubTasks             = 8
      StopTaskCachedChangesApplied    = false
      StopTaskCachedChangesNotApplied = false
      TargetTablePrepMode             = "DROP_AND_CREATE"
      TransactionConsistencyTimeout   = 600
    }

    TargetMetadata = {
      BatchApplyEnabled            = false
      FullLobMode                  = false
      InlineLobMaxSize             = 0
      LimitedSizeLobMode           = true
      LoadMaxFileSize              = 0
      LobChunkSize                 = 64
      LobMaxSize                   = 32
      ParallelApplyBufferSize      = 0
      ParallelApplyQueuesPerThread = 0
      ParallelApplyThreads         = 0
      ParallelLoadBufferSize       = 0
      ParallelLoadQueuesPerThread  = 0
      ParallelLoadThreads          = 0
      SupportLobs                  = true
      TargetSchema                 = ""
      TaskRecoveryTableEnabled     = false
    }

    ValidationSettings = {
      EnableValidation                 = true
      FailureMaxCount                  = 10000
      HandleCollationDiff              = false
      MaxKeyColumnSize                 = 8096
      PartitionSize                    = 10000
      RecordFailureDelayInMinutes      = 5
      RecordFailureDelayLimitInMinutes = 0
      RecordSuspendDelayInMinutes      = 30
      SkipLobColumns                   = false
      TableFailureMaxCount             = 1000
      ThreadCount                      = 5
      ValidationMode                   = "ROW_LEVEL"
      ValidationOnly                   = false
      ValidationPartialLobSize         = 0
      ValidationQueryCdcDelaySeconds   = 0
      ValidationS3Mask                 = 0
      ValidationS3Time                 = 0
    }

    ErrorBehavior = {
      ApplyErrorDeletePolicy                      = "IGNORE_RECORD"
      ApplyErrorEscalationCount                   = 0
      ApplyErrorEscalationPolicy                  = "LOG_ERROR"
      ApplyErrorFailOnTruncationDdl               = false
      ApplyErrorInsertPolicy                      = "LOG_ERROR"
      ApplyErrorUpdatePolicy                      = "LOG_ERROR"
      DataErrorEscalationCount                    = 0
      DataErrorEscalationPolicy                   = "SUSPEND_TABLE"
      DataErrorPolicy                             = "LOG_ERROR"
      DataMaskingErrorPolicy                      = "STOP_TASK"
      DataTruncationErrorPolicy                   = "LOG_ERROR"
      EventErrorPolicy                            = "IGNORE"
      FailOnNoTablesCaptured                      = true
      FailOnTransactionConsistencyBreached        = false
      FullLoadIgnoreConflicts                     = true
      RecoverableErrorCount                       = -1
      RecoverableErrorInterval                    = 5
      RecoverableErrorStopRetryAfterThrottlingMax = false
      RecoverableErrorThrottling                  = true
      RecoverableErrorThrottlingMax               = 1800
      TableErrorEscalationCount                   = 0
      TableErrorEscalationPolicy                  = "STOP_TASK"
      TableErrorPolicy                            = "SUSPEND_TABLE"
    }

    ChangeProcessingTuning = {
      BatchApplyMemoryLimit         = 500
      BatchApplyPreserveTransaction = true
      BatchApplyTimeoutMax          = 30
      BatchApplyTimeoutMin          = 1
      BatchSplitSize                = 0
      CommitTimeout                 = 1
      MemoryKeepTime                = 60
      MemoryLimitTotal              = 1024
      MinTransactionSize            = 1000
      RecoveryTimeout               = -1
      StatementCacheSize            = 50
    }

    ChangeProcessingDdlHandlingPolicy = {
      HandleSourceTableAltered   = true
      HandleSourceTableDropped   = true
      HandleSourceTableTruncated = true
    }

    ControlTablesSettings = {
      ControlSchema                 = ""
      FullLoadExceptionTableEnabled = false
      HistoryTableEnabled           = false
      HistoryTimeslotInMinutes      = 5
      StatusTableEnabled            = false
      SuspendedTablesTableEnabled   = false
      historyTimeslotInMinutes      = 5
    }

    StreamBufferSettings = {
      CtrlStreamBufferSizeInMB = 5
      StreamBufferCount        = 3
      StreamBufferSizeInMB     = 8
    }

    FailTaskWhenCleanTaskResourceFailed = false

    BeforeImageSettings        = null
    CharacterSetSettings       = null
    LoopbackPreventionSettings = null
    PostProcessingRules        = null
    TTSettings                 = null
  })

  tags = var.tags
}
