variable "name" {
  description = "Resource name prefix."
  type        = string
}

variable "kms_key_arn" {
  description = "KMS key for SSE-KMS on the data buckets."
  type        = string
}

variable "raw_retention_days" {
  description = "Days before Firehose raw objects transition to STANDARD_IA."
  type        = number
}
