variable "name" {
  description = "Resource name prefix (also the Glue job name prefix)."
  type        = string
}

variable "kms_key_arn" {
  description = "Lake KMS key."
  type        = string
}

variable "definition_path" {
  description = "Path of the ASL definition template shared with the local runner."
  type        = string
}

variable "glue_job_arns" {
  description = "Glue job ARNs by step name."
  type        = map(string)
}

variable "alert_emails" {
  description = "Email subscribers of the alerts topic."
  type        = list(string)
}

variable "schedule_expression" {
  description = "EventBridge Scheduler expression."
  type        = string
}

variable "log_retention_days" {
  description = "Retention of the execution log group."
  type        = number
}
