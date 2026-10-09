output "state_machine_arn" {
  description = "Pipeline state machine ARN."
  value       = aws_sfn_state_machine.pipeline.arn
}

output "alerts_topic_arn" {
  description = "SNS topic for pipeline and budget alerts."
  value       = aws_sns_topic.alerts.arn
}

output "schedule_arn" {
  description = "Daily EventBridge Scheduler schedule ARN."
  value       = aws_scheduler_schedule.daily.arn
}
