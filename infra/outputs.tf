output "lake_bucket" {
  description = "S3 bucket holding Firehose output and the Iceberg warehouse."
  value       = module.lake.lake_bucket
}

output "artifacts_bucket" {
  description = "S3 bucket for the Glue entrypoint and the musicpods wheel."
  value       = module.lake.artifacts_bucket
}

output "delivery_stream_name" {
  description = "Kinesis Data Firehose delivery stream for listen events."
  value       = module.streaming.delivery_stream_name
}

output "state_machine_arn" {
  description = "Step Functions state machine running the daily pipeline."
  value       = module.orchestration.state_machine_arn
}

output "athena_workgroup" {
  description = "Athena workgroup used by dbt-athena."
  value       = module.catalog.athena_workgroup
}

output "glue_databases" {
  description = "Glue Data Catalog databases (raw Iceberg tables and dbt models)."
  value       = module.catalog.database_names
}

output "deploy_role_arn" {
  description = "IAM role that GitHub Actions assumes through OIDC (the AWS_DEPLOY_ROLE_ARN repository variable)."
  value       = module.github_oidc.role_arn
}

output "kms_key_arn" {
  description = "Customer-managed KMS key encrypting the lake, logs, catalog and topics."
  value       = module.kms.key_arn
}
