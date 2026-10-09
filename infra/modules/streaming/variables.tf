variable "name" {
  description = "Resource name prefix."
  type        = string
}

variable "kms_key_arn" {
  description = "KMS key for stream, object and log encryption."
  type        = string
}

variable "lake_bucket_arn" {
  description = "Destination lake bucket ARN."
  type        = string
}

variable "log_retention_days" {
  description = "Retention of the delivery-error log group."
  type        = number
}
