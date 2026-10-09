# Cost guardrail (monthly budget on the project tag) and pipeline health alarms.
# Note: the `project` tag must be activated as a cost allocation tag in Billing for the
# budget filter to match spend.

resource "aws_budgets_budget" "monthly" {
  name         = "${var.name}-monthly"
  budget_type  = "COST"
  limit_amount = tostring(var.monthly_budget_usd)
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  cost_filter {
    name   = "TagKeyValue"
    values = [format("user:project$%s", var.project)]
  }

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 80
    threshold_type             = "PERCENTAGE"
    notification_type          = "ACTUAL"
    subscriber_sns_topic_arns  = [var.alerts_topic_arn]
    subscriber_email_addresses = var.alert_emails
  }

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 100
    threshold_type             = "PERCENTAGE"
    notification_type          = "FORECASTED"
    subscriber_sns_topic_arns  = [var.alerts_topic_arn]
    subscriber_email_addresses = var.alert_emails
  }
}

resource "aws_cloudwatch_metric_alarm" "pipeline_failed" {
  alarm_name          = "${var.name}-pipeline-failed"
  alarm_description   = "A daily pipeline execution failed (see docs/runbook.md)."
  namespace           = "AWS/States"
  metric_name         = "ExecutionsFailed"
  dimensions          = { StateMachineArn = var.state_machine_arn }
  statistic           = "Sum"
  period              = 3600
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [var.alerts_topic_arn]
}

resource "aws_cloudwatch_metric_alarm" "pipeline_not_run" {
  alarm_name          = "${var.name}-pipeline-not-started"
  alarm_description   = "No pipeline execution started in 26 hours: the schedule may be broken."
  namespace           = "AWS/States"
  metric_name         = "ExecutionsStarted"
  dimensions          = { StateMachineArn = var.state_machine_arn }
  statistic           = "Sum"
  period              = 3600
  evaluation_periods  = 26
  datapoints_to_alarm = 26
  threshold           = 1
  comparison_operator = "LessThanThreshold"
  treat_missing_data  = "breaching"
  alarm_actions       = [var.alerts_topic_arn]
}

resource "aws_cloudwatch_metric_alarm" "firehose_freshness" {
  alarm_name          = "${var.name}-firehose-s3-freshness"
  alarm_description   = "Records waited more than 15 minutes for delivery to S3."
  namespace           = "AWS/Firehose"
  metric_name         = "DeliveryToS3.DataFreshness"
  dimensions          = { DeliveryStreamName = var.delivery_stream_name }
  statistic           = "Maximum"
  period              = 300
  evaluation_periods  = 3
  threshold           = 900
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [var.alerts_topic_arn]
}
