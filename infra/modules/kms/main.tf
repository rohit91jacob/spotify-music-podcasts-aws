# One customer-managed key for the lake: S3 objects, Firehose, Glue catalog/jobs, Athena
# results, CloudWatch Logs, SNS and Step Functions. Rotation is on.

data "aws_iam_policy_document" "key" {
  # checkov:skip=CKV_AWS_109:A key policy's Resource "*" means "this key"; admin is the account root, as AWS requires.
  # checkov:skip=CKV_AWS_111:Same: key policies cannot name the key's own ARN.
  # checkov:skip=CKV_AWS_356:Same: key policies cannot name the key's own ARN.
  statement {
    sid       = "AccountAdministration"
    actions   = ["kms:*"]
    resources = ["*"]
    principals {
      type        = "AWS"
      identifiers = ["arn:${var.partition}:iam::${var.account_id}:root"]
    }
  }

  statement {
    sid = "CloudWatchLogs"
    actions = [
      "kms:Encrypt",
      "kms:Decrypt",
      "kms:ReEncrypt*",
      "kms:GenerateDataKey*",
      "kms:DescribeKey",
    ]
    resources = ["*"]
    principals {
      type        = "Service"
      identifiers = ["logs.${var.region}.amazonaws.com"]
    }
    condition {
      test     = "ArnLike"
      variable = "kms:EncryptionContext:aws:logs:arn"
      values   = ["arn:${var.partition}:logs:${var.region}:${var.account_id}:log-group:*${var.name}*"]
    }
  }

  statement {
    sid       = "AlarmAndBudgetPublishersToEncryptedTopic"
    actions   = ["kms:Decrypt", "kms:GenerateDataKey*"]
    resources = ["*"]
    principals {
      type        = "Service"
      identifiers = ["cloudwatch.amazonaws.com", "budgets.amazonaws.com"]
    }
    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [var.account_id]
    }
  }
}

resource "aws_kms_key" "lake" {
  description             = "${var.name} lake encryption key"
  enable_key_rotation     = true
  rotation_period_in_days = 365
  deletion_window_in_days = 30
  policy                  = data.aws_iam_policy_document.key.json
}

resource "aws_kms_alias" "lake" {
  name          = "alias/${var.name}-lake"
  target_key_id = aws_kms_key.lake.key_id
}
