# One Glue 6.0 job (Python 3.13) per pipeline step. Every job runs the same entrypoint
# (src/musicpods/glue/entrypoint.py) with --step <name>; the package wheel is installed with
# --additional-python-modules from the artifacts bucket (uploaded by the deploy workflow).
# The steps are plain Python (pyiceberg, DuckDB-free on AWS, dbt-athena); a Spark job type is
# used only because Glue's Python shell jobs stop at Python 3.9.

resource "aws_s3_object" "entrypoint" {
  bucket                 = var.artifacts_bucket
  key                    = "glue/entrypoint.py"
  source                 = var.entrypoint_path
  etag                   = filemd5(var.entrypoint_path)
  server_side_encryption = "aws:kms"
  kms_key_id             = var.kms_key_arn
}

resource "aws_glue_security_configuration" "this" {
  name = "${var.name}-glue"

  encryption_configuration {
    cloudwatch_encryption {
      cloudwatch_encryption_mode = "SSE-KMS"
      kms_key_arn                = var.kms_key_arn
    }
    job_bookmarks_encryption {
      job_bookmarks_encryption_mode = "CSE-KMS"
      kms_key_arn                   = var.kms_key_arn
    }
    s3_encryption {
      s3_encryption_mode = "SSE-KMS"
      kms_key_arn        = var.kms_key_arn
    }
  }
}

data "aws_iam_policy_document" "assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [var.account_id]
    }
  }
}

resource "aws_iam_role" "glue" {
  name               = "${var.name}-glue-jobs"
  assume_role_policy = data.aws_iam_policy_document.assume.json
}

locals {
  catalog_arn = "arn:${var.partition}:glue:${var.region}:${var.account_id}:catalog"
  database_arns = [
    for d in values(var.glue_database_names) : "arn:${var.partition}:glue:${var.region}:${var.account_id}:database/${d}"
  ]
  table_arns = [
    for d in values(var.glue_database_names) : "arn:${var.partition}:glue:${var.region}:${var.account_id}:table/${d}/*"
  ]
  secret_arns = [
    for s in var.secret_names : "arn:${var.partition}:secretsmanager:${var.region}:${var.account_id}:secret:${s}-??????"
  ]
}

data "aws_iam_policy_document" "glue" {
  # checkov:skip=CKV_AWS_356:cloudwatch:PutMetricData has no resource-level permissions; it is limited to the Glue namespace by condition.
  # checkov:skip=CKV_AWS_111:Same statement: PutMetricData only supports Resource "*".
  statement {
    sid       = "ListLakeBuckets"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [var.lake_bucket_arn, var.artifacts_bucket_arn, var.athena_results_bucket_arn]
  }

  statement {
    sid     = "ReadWriteLakeData"
    actions = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload"]
    resources = [
      "${var.lake_bucket_arn}/bronze/*",
      "${var.lake_bucket_arn}/state/*",
      "${var.lake_bucket_arn}/iceberg/*",
      "${var.athena_results_bucket_arn}/*",
    ]
  }

  statement {
    sid       = "ReadFirehoseOutput"
    actions   = ["s3:GetObject"]
    resources = ["${var.lake_bucket_arn}/firehose/*"]
  }

  statement {
    sid       = "ReadArtifacts"
    actions   = ["s3:GetObject"]
    resources = ["${var.artifacts_bucket_arn}/*"]
  }

  statement {
    sid       = "UseLakeKey"
    actions   = ["kms:Decrypt", "kms:Encrypt", "kms:GenerateDataKey", "kms:DescribeKey"]
    resources = [var.kms_key_arn]
  }

  statement {
    sid = "IcebergCatalog"
    actions = [
      "glue:GetDatabase",
      "glue:GetDatabases",
      "glue:GetTable",
      "glue:GetTables",
      "glue:CreateTable",
      "glue:UpdateTable",
      "glue:DeleteTable",
      "glue:GetPartitions",
    ]
    resources = concat([local.catalog_arn], local.database_arns, local.table_arns)
  }

  statement {
    sid = "AthenaForDbt"
    actions = [
      "athena:StartQueryExecution",
      "athena:GetQueryExecution",
      "athena:GetQueryResults",
      "athena:StopQueryExecution",
      "athena:GetWorkGroup",
    ]
    resources = [var.athena_workgroup_arn]
  }

  statement {
    sid       = "ReplayIntoFirehose"
    actions   = ["firehose:PutRecordBatch", "firehose:DescribeDeliveryStream"]
    resources = [var.firehose_stream_arn]
  }

  statement {
    sid       = "OptionalSourceSecrets"
    actions   = ["secretsmanager:GetSecretValue"]
    resources = local.secret_arns
  }

  statement {
    sid = "JobLogs"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents",
      "logs:AssociateKmsKey",
    ]
    resources = [
      "arn:${var.partition}:logs:${var.region}:${var.account_id}:log-group:/aws-glue/*",
      "arn:${var.partition}:logs:${var.region}:${var.account_id}:log-group:/aws-glue/*:*",
    ]
  }

  statement {
    sid       = "JobMetrics"
    actions   = ["cloudwatch:PutMetricData"]
    resources = ["*"]
    condition {
      test     = "StringEquals"
      variable = "cloudwatch:namespace"
      values   = ["Glue", "musicpods"]
    }
  }
}

resource "aws_iam_role_policy" "glue" {
  name   = "pipeline-steps"
  role   = aws_iam_role.glue.id
  policy = data.aws_iam_policy_document.glue.json
}

resource "aws_glue_job" "step" {
  for_each = toset(var.steps)

  name                   = "${var.name}-${each.key}"
  description            = "musicpods pipeline step ${each.key}"
  role_arn               = aws_iam_role.glue.arn
  glue_version           = "6.0"
  worker_type            = "G.1X"
  number_of_workers      = var.max_workers
  timeout                = var.timeout_minutes
  max_retries            = 0
  security_configuration = aws_glue_security_configuration.this.name

  command {
    name            = "glueetl"
    script_location = "s3://${var.artifacts_bucket}/${aws_s3_object.entrypoint.key}"
    python_version  = "3"
  }

  execution_property {
    max_concurrent_runs = 1
  }

  default_arguments = {
    "--step"                             = each.key
    "--lake_bucket"                      = var.lake_bucket
    "--firehose_stream"                  = var.firehose_stream_name
    "--athena_workgroup"                 = var.athena_workgroup
    "--additional-python-modules"        = "s3://${var.artifacts_bucket}/wheels/${var.wheel_filename},dbt-athena-community==${var.dbt_athena_version}"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
    "--job-language"                     = "python"
  }
}
