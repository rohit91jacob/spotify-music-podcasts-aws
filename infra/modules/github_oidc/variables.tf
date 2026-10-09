variable "name" {
  description = "Resource name prefix."
  type        = string
}

variable "github_repository" {
  description = "The owner/name of the trusted repository."
  type        = string
}

variable "github_environment" {
  description = "GitHub environment whose jobs may assume the role."
  type        = string
}

variable "create_provider" {
  description = "Create the GitHub OIDC provider (it is account-wide)."
  type        = bool
}

variable "account_id" {
  description = "AWS account id."
  type        = string
}

variable "partition" {
  description = "AWS partition."
  type        = string
}

variable "artifacts_bucket_arn" {
  description = "Artifacts bucket the deploy job uploads to."
  type        = string
}

variable "kms_key_arn" {
  description = "Lake KMS key."
  type        = string
}

variable "state_machine_arn" {
  description = "Pipeline state machine the deploy job may start."
  type        = string
}
