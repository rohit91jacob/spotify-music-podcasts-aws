output "lake_bucket" {
  description = "Lake bucket name."
  value       = aws_s3_bucket.data["lake"].id
}

output "lake_bucket_arn" {
  description = "Lake bucket ARN."
  value       = aws_s3_bucket.data["lake"].arn
}

output "athena_results_bucket" {
  description = "Athena query results bucket name."
  value       = aws_s3_bucket.data["athena-results"].id
}

output "athena_results_bucket_arn" {
  description = "Athena query results bucket ARN."
  value       = aws_s3_bucket.data["athena-results"].arn
}

output "artifacts_bucket" {
  description = "Artifacts bucket name (Glue entrypoint, wheel)."
  value       = aws_s3_bucket.data["artifacts"].id
}

output "artifacts_bucket_arn" {
  description = "Artifacts bucket ARN."
  value       = aws_s3_bucket.data["artifacts"].arn
}
