variable "name" {
  description = "Resource name prefix."
  type        = string
}

variable "project" {
  description = "Value of the project cost-allocation tag the budget filters on."
  type        = string
}

variable "monthly_budget_usd" {
  description = "Monthly budget in USD."
  type        = number
}

variable "alerts_topic_arn" {
  description = "SNS topic receiving alarm and budget notifications."
  type        = string
}

variable "alert_emails" {
  description = "Email subscribers for budget notifications."
  type        = list(string)
}

variable "state_machine_arn" {
  description = "Pipeline state machine ARN."
  type        = string
}

variable "delivery_stream_name" {
  description = "Firehose delivery stream name."
  type        = string
}
