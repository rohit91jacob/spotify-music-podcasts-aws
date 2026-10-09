variable "name" {
  description = "Resource name prefix."
  type        = string
}

variable "steps" {
  description = "Pipeline step names; one Glue job each."
  type        = list(string)
}

variable "kms_key_arn" {
  description = "Lake KMS key."
  type        = string
}

variable "lake_bucket" {
  description = "Lake bucket name."
  type        = string
}

variable "lake_bucket_arn" {
  description = "Lake bucket ARN."
  type        = string
}

variable "artifacts_bucket" {
  description = "Artifacts bucket name."
  type        = string
}

variable "artifacts_bucket_arn" {
  description = "Artifacts bucket ARN."
  type        = string
}

variable "athena_results_bucket_arn" {
  description = "Athena results bucket ARN."
  type        = string
}

variable "athena_workgroup" {
  description = "Athena workgroup name."
  type        = string
}

variable "athena_workgroup_arn" {
  description = "Athena workgroup ARN."
  type        = string
}

variable "glue_database_names" {
  description = "Glue database names by layer."
  type        = map(string)
}

variable "firehose_stream_arn" {
  description = "Firehose delivery stream ARN (replay step)."
  type        = string
}

variable "firehose_stream_name" {
  description = "Firehose delivery stream name."
  type        = string
}

variable "max_workers" {
  description = "Workers per job."
  type        = number
}

variable "timeout_minutes" {
  description = "Job timeout in minutes."
  type        = number
}

variable "account_id" {
  description = "AWS account id."
  type        = string
}

variable "partition" {
  description = "AWS partition."
  type        = string
}

variable "region" {
  description = "AWS region."
  type        = string
}

variable "secret_names" {
  description = "Secrets Manager names of optional source credentials."
  type        = list(string)
}

variable "entrypoint_path" {
  description = "Local path of the Glue entrypoint script."
  type        = string
}

variable "wheel_filename" {
  description = "File name of the package wheel under s3://<artifacts>/wheels/ (published by the deploy workflow)."
  type        = string
  default     = "spotify_music_podcasts_aws-0.1.0-py3-none-any.whl"
}

variable "dbt_athena_version" {
  description = "dbt-athena-community version installed next to the wheel for the dbt-build step."
  type        = string
  default     = "1.11.1"
}
