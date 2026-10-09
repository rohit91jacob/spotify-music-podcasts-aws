variable "name" {
  description = "Resource name prefix."
  type        = string
}

variable "account_id" {
  description = "AWS account id."
  type        = string
}

variable "partition" {
  description = "AWS partition (aws, aws-cn, aws-us-gov)."
  type        = string
}

variable "region" {
  description = "AWS region."
  type        = string
}
