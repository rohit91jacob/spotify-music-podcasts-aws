output "budget_name" {
  description = "Monthly cost budget name."
  value       = aws_budgets_budget.monthly.name
}

output "alarm_arns" {
  description = "Pipeline health alarm ARNs."
  value = [
    aws_cloudwatch_metric_alarm.pipeline_failed.arn,
    aws_cloudwatch_metric_alarm.pipeline_not_run.arn,
    aws_cloudwatch_metric_alarm.firehose_freshness.arn,
  ]
}
