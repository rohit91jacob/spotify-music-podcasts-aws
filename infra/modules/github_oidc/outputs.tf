output "role_arn" {
  description = "Deploy role ARN for aws-actions/configure-aws-credentials."
  value       = aws_iam_role.deploy.arn
}
