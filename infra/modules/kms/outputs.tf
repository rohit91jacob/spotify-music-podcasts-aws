output "key_arn" {
  description = "ARN of the lake KMS key."
  value       = aws_kms_key.lake.arn
}

output "alias_name" {
  description = "Alias of the lake KMS key."
  value       = aws_kms_alias.lake.name
}
