variable "region" {
  description = "AWS region for every resource."
  type        = string
  default     = "eu-west-1"
}

variable "environment" {
  description = "Deployment environment name (dev or prod)."
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "prod"], var.environment)
    error_message = "The environment must be dev or prod."
  }
}

variable "project" {
  description = "Project name; prefixes every resource name."
  type        = string
  default     = "musicpods"
}

variable "owner" {
  description = "Owner tag value (a person or team)."
  type        = string
  default     = "rohit91jacob"
}

variable "monthly_budget_usd" {
  description = "Monthly cost budget for resources tagged with this project. Alerts at 80 percent actual and 100 percent forecast."
  type        = number
  default     = 25
}

variable "alert_emails" {
  description = "Email addresses subscribed to pipeline and budget alerts (each must confirm)."
  type        = list(string)
  default     = []
}

variable "schedule_expression" {
  description = "EventBridge Scheduler expression for the daily pipeline run (UTC)."
  type        = string
  default     = "cron(15 6 * * ? *)"
}

variable "athena_bytes_scanned_cutoff" {
  description = "Per-query scan limit for the Athena workgroup in bytes (cost guardrail)."
  type        = number
  default     = 1073741824
}

variable "glue_max_workers" {
  description = "Glue workers per job (G.1X). Two is the minimum for Glue 6.0 Spark jobs."
  type        = number
  default     = 2
}

variable "glue_timeout_minutes" {
  description = "Per-job timeout so that a stuck job cannot run up cost."
  type        = number
  default     = 60
}

variable "log_retention_days" {
  description = "CloudWatch Logs retention in days."
  type        = number
  default     = 365
}

variable "raw_retention_days" {
  description = "Days before Firehose raw objects move to Infrequent Access. Iceberg holds the curated copy."
  type        = number
  default     = 30
}

variable "github_repository" {
  description = "The owner/name of the GitHub repository allowed to assume the deploy role through OIDC."
  type        = string
  default     = "rohit91jacob/spotify-music-podcasts-aws"
}

variable "github_environment" {
  description = "GitHub Actions environment whose jobs may assume the deploy role."
  type        = string
  default     = "aws-dev"
}

variable "create_github_oidc_provider" {
  description = "Create the account-wide GitHub OIDC provider (set false if it already exists)."
  type        = bool
  default     = true
}

variable "optional_source_secret_names" {
  description = "Secrets Manager secret names holding key-gated source credentials. Jobs skip a source when its secret is absent."
  type        = list(string)
  default     = ["musicpods/lastfm", "musicpods/podcastindex", "musicpods/spotify"]
}
