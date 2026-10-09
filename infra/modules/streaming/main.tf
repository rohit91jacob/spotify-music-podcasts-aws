# Kinesis Data Firehose (Direct PUT) -> S3 lake bucket, GZIP JSON Lines under
# firehose/listen-events/YYYY/MM/DD/HH/. The listen-events-to-iceberg Glue job commits those
# objects to the Iceberg table raw.listen_events (deduplicated on event_id).
# Firehose's native Iceberg destination is an alternative; this layout keeps the deployed path
# identical to the emulated one (moto / LocalStack) that the tests exercise.

data "aws_region" "current" {}
data "aws_caller_identity" "current" {}

resource "aws_cloudwatch_log_group" "firehose" {
  name              = "/aws/kinesisfirehose/${var.name}-listen-events"
  retention_in_days = var.log_retention_days
  kms_key_id        = var.kms_key_arn
}

resource "aws_cloudwatch_log_stream" "s3_delivery" {
  name           = "S3Delivery"
  log_group_name = aws_cloudwatch_log_group.firehose.name
}

data "aws_iam_policy_document" "assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["firehose.amazonaws.com"]
    }
    condition {
      test     = "StringEquals"
      variable = "sts:ExternalId"
      values   = [data.aws_caller_identity.current.account_id]
    }
  }
}

resource "aws_iam_role" "firehose" {
  name               = "${var.name}-firehose"
  assume_role_policy = data.aws_iam_policy_document.assume.json
}

data "aws_iam_policy_document" "firehose" {
  statement {
    sid = "WriteLakePrefixes"
    actions = [
      "s3:AbortMultipartUpload",
      "s3:GetBucketLocation",
      "s3:GetObject",
      "s3:ListBucket",
      "s3:ListBucketMultipartUploads",
      "s3:PutObject",
    ]
    resources = [
      var.lake_bucket_arn,
      "${var.lake_bucket_arn}/firehose/*",
    ]
  }

  statement {
    sid       = "EncryptWithLakeKey"
    actions   = ["kms:GenerateDataKey", "kms:Decrypt"]
    resources = [var.kms_key_arn]
    condition {
      test     = "StringEquals"
      variable = "kms:ViaService"
      values   = ["s3.${data.aws_region.current.region}.amazonaws.com"]
    }
  }

  statement {
    sid       = "DeliveryErrorLogs"
    actions   = ["logs:PutLogEvents"]
    resources = ["${aws_cloudwatch_log_group.firehose.arn}:*"]
  }
}

resource "aws_iam_role_policy" "firehose" {
  name   = "deliver-to-lake"
  role   = aws_iam_role.firehose.id
  policy = data.aws_iam_policy_document.firehose.json
}

resource "aws_kinesis_firehose_delivery_stream" "listen_events" {
  name        = "${var.name}-listen-events"
  destination = "extended_s3"

  server_side_encryption {
    enabled  = true
    key_type = "CUSTOMER_MANAGED_CMK"
    key_arn  = var.kms_key_arn
  }

  extended_s3_configuration {
    role_arn            = aws_iam_role.firehose.arn
    bucket_arn          = var.lake_bucket_arn
    prefix              = "firehose/listen-events/"
    error_output_prefix = "firehose/errors/!{firehose:error-output-type}/!{timestamp:yyyy/MM/dd}/"
    buffering_size      = 64
    buffering_interval  = 60
    compression_format  = "GZIP"
    kms_key_arn         = var.kms_key_arn

    cloudwatch_logging_options {
      enabled         = true
      log_group_name  = aws_cloudwatch_log_group.firehose.name
      log_stream_name = aws_cloudwatch_log_stream.s3_delivery.name
    }
  }
}
