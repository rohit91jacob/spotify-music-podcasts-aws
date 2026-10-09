# Buckets: lake (Firehose output + Iceberg warehouse), athena-results, artifacts (Glue code),
# and access-logs (S3 server access logs of the other three). All block public access,
# enforce TLS and bucket-owner object ownership; data buckets use SSE-KMS with the lake key.

locals {
  data_buckets = {
    lake           = "${var.name}-lake"
    athena-results = "${var.name}-athena-results"
    artifacts      = "${var.name}-artifacts"
  }
}

resource "aws_s3_bucket" "access_logs" {
  # checkov:skip=CKV_AWS_18:This is the access-log target bucket; logging it into itself would loop.
  # checkov:skip=CKV_AWS_144:Cross-region replication is not worth its cost for access logs of a demo lake.
  # checkov:skip=CKV2_AWS_62:No consumer needs event notifications for access logs.
  # checkov:skip=CKV_AWS_145:S3 server access log delivery only supports SSE-S3 on the target bucket.
  bucket = "${var.name}-access-logs"
}

resource "aws_s3_bucket" "data" {
  # checkov:skip=CKV_AWS_144:Every dataset is re-derivable from public sources; replication would double storage cost.
  # checkov:skip=CKV2_AWS_62:Processing is scheduled by EventBridge Scheduler, not triggered by object events.
  for_each = local.data_buckets
  bucket   = each.value
}

locals {
  all_buckets = merge(
    { for k, b in aws_s3_bucket.data : k => b },
    { access-logs = aws_s3_bucket.access_logs },
  )
}

# Companion resources index aws_s3_bucket.data[each.key] directly (rather than iterating a merged
# map) so that static analysis can link every setting to its bucket.
resource "aws_s3_bucket_public_access_block" "data" {
  for_each                = local.data_buckets
  bucket                  = aws_s3_bucket.data[each.key].id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_public_access_block" "access_logs" {
  bucket                  = aws_s3_bucket.access_logs.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_ownership_controls" "data" {
  for_each = local.data_buckets
  bucket   = aws_s3_bucket.data[each.key].id
  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_ownership_controls" "access_logs" {
  bucket = aws_s3_bucket.access_logs.id
  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_versioning" "data" {
  for_each = local.data_buckets
  bucket   = aws_s3_bucket.data[each.key].id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_versioning" "access_logs" {
  bucket = aws_s3_bucket.access_logs.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data" {
  for_each = local.data_buckets
  bucket   = aws_s3_bucket.data[each.key].id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = var.kms_key_arn
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "access_logs" {
  bucket = aws_s3_bucket.access_logs.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_logging" "data" {
  for_each      = local.data_buckets
  bucket        = aws_s3_bucket.data[each.key].id
  target_bucket = aws_s3_bucket.access_logs.id
  target_prefix = "${each.key}/"
}

resource "aws_s3_bucket_lifecycle_configuration" "lake" {
  bucket = aws_s3_bucket.data["lake"].id

  rule {
    id     = "firehose-raw-to-ia"
    status = "Enabled"
    filter {
      prefix = "firehose/"
    }
    transition {
      days          = var.raw_retention_days
      storage_class = "STANDARD_IA"
    }
    noncurrent_version_expiration {
      noncurrent_days = 30
    }
  }

  rule {
    id     = "warehouse-noncurrent"
    status = "Enabled"
    filter {
      prefix = "iceberg/"
    }
    # Iceberg snapshot expiry (the maintenance job) deletes old files; this cleans versions.
    noncurrent_version_expiration {
      noncurrent_days = 14
    }
  }

  rule {
    id     = "abort-incomplete-uploads"
    status = "Enabled"
    filter {}
    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "athena_results" {
  bucket = aws_s3_bucket.data["athena-results"].id
  rule {
    id     = "expire-query-results"
    status = "Enabled"
    filter {}
    expiration {
      days = 7
    }
    noncurrent_version_expiration {
      noncurrent_days = 1
    }
    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "artifacts" {
  bucket = aws_s3_bucket.data["artifacts"].id
  rule {
    id     = "old-builds"
    status = "Enabled"
    filter {}
    noncurrent_version_expiration {
      noncurrent_days = 30
    }
    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "access_logs" {
  bucket = aws_s3_bucket.access_logs.id
  rule {
    id     = "expire-logs"
    status = "Enabled"
    filter {}
    expiration {
      days = 90
    }
    noncurrent_version_expiration {
      noncurrent_days = 7
    }
    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }
}

data "aws_iam_policy_document" "tls_only" {
  for_each = local.all_buckets

  statement {
    sid       = "DenyInsecureTransport"
    effect    = "Deny"
    actions   = ["s3:*"]
    resources = [each.value.arn, "${each.value.arn}/*"]
    principals {
      type        = "*"
      identifiers = ["*"]
    }
    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }

  dynamic "statement" {
    for_each = each.key == "access-logs" ? [1] : []
    content {
      sid       = "S3ServerAccessLogs"
      actions   = ["s3:PutObject"]
      resources = ["${each.value.arn}/*"]
      principals {
        type        = "Service"
        identifiers = ["logging.s3.amazonaws.com"]
      }
      condition {
        test     = "ArnLike"
        variable = "aws:SourceArn"
        values   = [for b in aws_s3_bucket.data : b.arn]
      }
    }
  }
}

resource "aws_s3_bucket_policy" "all" {
  for_each = local.all_buckets
  bucket   = each.value.id
  policy   = data.aws_iam_policy_document.tls_only[each.key].json

  depends_on = [aws_s3_bucket_public_access_block.data, aws_s3_bucket_public_access_block.access_logs]
}
