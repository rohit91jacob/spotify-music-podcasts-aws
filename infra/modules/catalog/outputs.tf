output "database_names" {
  description = "Glue database names by layer."
  value       = { for k, d in aws_glue_catalog_database.this : k => d.name }
}

output "athena_workgroup" {
  description = "Athena workgroup name."
  value       = aws_athena_workgroup.this.name
}

output "athena_workgroup_arn" {
  description = "Athena workgroup ARN."
  value       = aws_athena_workgroup.this.arn
}
