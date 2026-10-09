variable "name" {
  description = "Resource name prefix."
  type        = string
}

variable "project" {
  description = "Project name used for Glue database names."
  type        = string
}

variable "kms_key_arn" {
  description = "KMS key for catalog and query-result encryption."
  type        = string
}

variable "lake_bucket" {
  description = "Lake bucket holding the Iceberg warehouse."
  type        = string
}

variable "athena_results_bucket" {
  description = "Bucket for Athena query results."
  type        = string
}

variable "athena_bytes_scanned_cutoff" {
  description = "Per-query scan limit in bytes."
  type        = number
}
