output "job_arns" {
  description = "Glue job ARNs by step name."
  value       = { for k, j in aws_glue_job.step : k => j.arn }
}

output "role_arn" {
  description = "IAM role used by the Glue jobs."
  value       = aws_iam_role.glue.arn
}
