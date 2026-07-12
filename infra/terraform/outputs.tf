# =============================================================================
# Outputs consumed by the other layers (Airflow Variables, CI, docs).
# =============================================================================
output "lakehouse_bucket" {
  description = "Medallion lakehouse bucket (SPEC §3)."
  value       = aws_s3_bucket.lakehouse.id
}

output "snapshot_bucket" {
  description = "Object-Lock (COMPLIANCE) snapshot bucket (SPEC §7)."
  value       = aws_s3_bucket.snapshot.id
}

output "glue_databases" {
  description = "Glue Catalog databases (SPEC §4)."
  value       = local.glue_databases
}

output "glue_job_names" {
  description = "Glue job names — must match the Airflow GLUE_JOBS dict."
  value       = { for k, v in local.glue_jobs : k => v.name }
}

output "athena_workgroup" {
  description = "Athena workgroup name."
  value       = aws_athena_workgroup.credit_decision.name
}

output "dq_alerts_topic_arn" {
  description = "SNS topic for DQ alerts (SPEC §8)."
  value       = aws_sns_topic.dq_alerts.arn
}

output "postgres_endpoint" {
  description = "Source PostgreSQL endpoint (identity spine)."
  value       = aws_db_instance.postgres.address
}

output "msk_cluster_arn" {
  description = "MSK cluster ARN for the CDC path."
  value       = aws_msk_cluster.cdc.arn
}
