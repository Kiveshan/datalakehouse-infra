data "aws_caller_identity" "current" {}

output "raw_bucket_arn" {
  description = "The ARN of the raw S3 bucket"
  value       = aws_s3_bucket.lakehouse-bucket.arn
}

output "caller_identity" {
  description = "The AWS account ID of the caller"
  value       = data.aws_caller_identity.current.account_id
}

output "dms_replication_instance_public_ips" {
  description = <<-EOT
    Public IPs of the DMS replication instance.

    These must be added to the Azure MySQL server's firewall rules before the
    task can connect. A rebuilt instance gets new IPs, so re-check after any
    replacement. This step is outside Terraform and is the most common cause of
    a task failing on connection.
  EOT
  value       = module.lms_dms.replication_instance_public_ips
}

output "dms_replication_task_arn" {
  description = "ARN of the replication task. Start a run with: aws dms start-replication-task --replication-task-arn <this> --start-replication-task-type reload-target"
  value       = module.lms_dms.replication_task_arn
}

output "dms_target_prefix" {
  description = "Where DMS writes migrated tables. Overwritten in full on every run."
  value       = "s3://${aws_s3_bucket.lakehouse-bucket.id}/${var.dms_target_bucket_folder}/"
}

output "create_layer2_dims_job_name" {
  description = "Bootstrap job name. Run once for any newly mapped table: aws glue start-job-run --job-name <this>"
  value       = aws_glue_job.create_layer2_dims.name
}

output "build_layer2_dims_scd2_job_name" {
  description = "Nightly SCD2 merge job name. Run after a curated/ refresh: aws glue start-job-run --job-name <this>"
  value       = aws_glue_job.build_layer2_dims_scd2.name
}

output "build_layer2_facts_job_name" {
  description = "Builds fact_<x> Iceberg tables from dim_<x>. Run after build_layer2_dims_scd2: aws glue start-job-run --job-name <this>"
  value       = aws_glue_job.build_layer2_facts.name
}

output "build_layer2_facts_ctas_job_name" {
  description = "Builds fact_sdf_application/fact_wsp_submission via CTAS, then publishes those + fact_company to Postgres. Run after build_layer2_facts: aws glue start-job-run --job-name <this>"
  value       = aws_glue_job.build_layer2_facts_ctas.name
}

output "publish_layer2_dims_postgres_job_name" {
  description = "Mirrors dim_<x> to Postgres. Run after build_layer2_dims_scd2: aws glue start-job-run --job-name <this>"
  value       = aws_glue_job.publish_layer2_dims_postgres.name
}

output "publish_layer2_facts_postgres_job_name" {
  description = "Mirrors fact_<x> to Postgres with PK/FK constraints. Run after build_layer2_facts and publish_layer2_dims_postgres: aws glue start-job-run --job-name <this>"
  value       = aws_glue_job.publish_layer2_facts_postgres.name
}

output "build_etqa_facts_job_name" {
  description = "PLACEHOLDER join logic -- see infra/glue/scripts/build_etqa_facts.py header. Builds fact_certifylearners/fact_assessor_moderator. Run after build_layer2_dims_scd2: aws glue start-job-run --job-name <this>"
  value       = aws_glue_job.build_etqa_facts.name
}

output "publish_etqa_facts_postgres_job_name" {
  description = "Mirrors the manually-built ETQA facts to Postgres. Run after publish_layer2_dims_postgres: aws glue start-job-run --job-name <this>"
  value       = aws_glue_job.publish_etqa_facts_postgres.name
}

output "publish_postgres_endpoint" {
  description = "RDS endpoint mirroring the layer2 warehouse. Not publicly reachable -- only the Glue publish jobs' security group can connect. Credentials are in Secrets Manager (publish_postgres_secret_arn), never in plaintext."
  value       = aws_db_instance.publish_postgres.address
}

output "publish_postgres_secret_arn" {
  description = "Secrets Manager secret holding the publish-Postgres username/password: aws secretsmanager get-secret-value --secret-id <this>"
  value       = aws_secretsmanager_secret.publish_postgres.arn
}

output "dms_raw_state_machine_arn" {
  description = "Top-level Step Function: archive raw/, run DMS, crawl, validate, then invoke src_improved_pipeline. Start a run with: aws stepfunctions start-execution --state-machine-arn <this> --input '{}'"
  value       = aws_sfn_state_machine.dms_raw.arn
}

output "src_improved_pipeline_state_machine_arn" {
  description = "Nightly ETL Step Function invoked by dms_raw. Can also be run standalone against an already-validated raw/: aws stepfunctions start-execution --state-machine-arn <this> --input '{}'"
  value       = aws_sfn_state_machine.src_improved_pipeline.arn
}

output "etl_pipeline_notifications_topic_arn" {
  description = "SNS topic notify-failure publishes to on any pipeline failure. Subscribe with: aws sns subscribe --topic-arn <this> --protocol email --notification-endpoint <address> (or set notify_failure_email and re-apply)"
  value       = aws_sns_topic.etl_pipeline_notifications.arn
}
