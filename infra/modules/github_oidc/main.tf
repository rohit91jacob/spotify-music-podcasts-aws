# GitHub Actions -> AWS through OIDC federation: no static access keys anywhere.
# Only jobs in the configured GitHub environment of this repository can assume the role, and
# the role can only publish build artifacts and start the pipeline. Infrastructure changes
# (terraform apply) are made by an operator, not by this role.

locals {
  oidc_url = "token.actions.githubusercontent.com"
}

resource "aws_iam_openid_connect_provider" "github" {
  count          = var.create_provider ? 1 : 0
  url            = "https://${local.oidc_url}"
  client_id_list = ["sts.amazonaws.com"]
}

locals {
  provider_arn = var.create_provider ? aws_iam_openid_connect_provider.github[0].arn : "arn:${var.partition}:iam::${var.account_id}:oidc-provider/${local.oidc_url}"
}

data "aws_iam_policy_document" "trust" {
  statement {
    actions = ["sts:AssumeRoleWithWebIdentity"]
    principals {
      type        = "Federated"
      identifiers = [local.provider_arn]
    }
    condition {
      test     = "StringEquals"
      variable = "${local.oidc_url}:aud"
      values   = ["sts.amazonaws.com"]
    }
    condition {
      test     = "StringEquals"
      variable = "${local.oidc_url}:sub"
      values   = ["repo:${var.github_repository}:environment:${var.github_environment}"]
    }
  }
}

resource "aws_iam_role" "deploy" {
  name                 = "${var.name}-github-deploy"
  assume_role_policy   = data.aws_iam_policy_document.trust.json
  max_session_duration = 3600
}

data "aws_iam_policy_document" "deploy" {
  statement {
    sid       = "PublishArtifacts"
    actions   = ["s3:PutObject", "s3:GetObject"]
    resources = ["${var.artifacts_bucket_arn}/wheels/*", "${var.artifacts_bucket_arn}/glue/*"]
  }

  statement {
    sid       = "EncryptArtifacts"
    actions   = ["kms:GenerateDataKey", "kms:Decrypt"]
    resources = [var.kms_key_arn]
  }

  statement {
    sid       = "StartAndWatchPipeline"
    actions   = ["states:StartExecution", "states:DescribeExecution"]
    resources = [var.state_machine_arn, "${replace(var.state_machine_arn, ":stateMachine:", ":execution:")}:*"]
  }
}

resource "aws_iam_role_policy" "deploy" {
  name   = "publish-and-run"
  role   = aws_iam_role.deploy.id
  policy = data.aws_iam_policy_document.deploy.json
}
