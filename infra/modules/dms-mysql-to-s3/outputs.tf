# outputs.tf

output "source_endpoint_arn" {
  description = "ARN of the MySQL source endpoint."
  value       = aws_dms_endpoint.source.endpoint_arn
}

output "target_endpoint_arn" {
  description = "ARN of the S3 target endpoint."
  value       = aws_dms_s3_endpoint.target.endpoint_arn
}

output "replication_instance_arn" {
  description = "ARN of the replication instance."
  value       = aws_dms_replication_instance.this.replication_instance_arn
}

output "replication_instance_public_ips" {
  description = <<-EOT
    Public IPs of the replication instance.

    If the source is behind a firewall (e.g. Azure MySQL), these are the
    addresses that must be allowlisted before the task can connect.
  EOT
  value       = aws_dms_replication_instance.this.replication_instance_public_ips
}

output "replication_task_arn" {
  description = "ARN of the replication task."
  value       = aws_dms_replication_task.this.replication_task_arn
}
