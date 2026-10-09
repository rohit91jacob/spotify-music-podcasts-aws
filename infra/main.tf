data "aws_caller_identity" "current" {}
data "aws_partition" "current" {}

locals {
  name       = "${var.project}-${var.environment}"
  account_id = data.aws_caller_identity.current.account_id
  partition  = data.aws_partition.current.partition

  tags = {
    project     = var.project
    environment = var.environment
    owner       = var.owner
    managed_by  = "terraform"
    repository  = var.github_repository
  }

  # Glue job names are "<name>-<step>"; the steps match musicpods.orchestration.steps.STEPS.
  glue_steps = [
    "ingest-listenbrainz",
    "ingest-musicbrainz",
    "ingest-podcasts",
    "ingest-optional",
    "replay-listens",
    "listen-events-to-iceberg",
    "build-raw-tables",
    "dbt-build",
    "maintenance",
  ]
}

module "kms" {
  source     = "./modules/kms"
  name       = local.name
  account_id = local.account_id
  partition  = local.partition
  region     = var.region
}

module "lake" {
  source             = "./modules/lake"
  name               = local.name
  kms_key_arn        = module.kms.key_arn
  raw_retention_days = var.raw_retention_days
}

module "catalog" {
  source                      = "./modules/catalog"
  name                        = local.name
  project                     = var.project
  kms_key_arn                 = module.kms.key_arn
  lake_bucket                 = module.lake.lake_bucket
  athena_results_bucket       = module.lake.athena_results_bucket
  athena_bytes_scanned_cutoff = var.athena_bytes_scanned_cutoff
}

module "streaming" {
  source             = "./modules/streaming"
  name               = local.name
  kms_key_arn        = module.kms.key_arn
  lake_bucket_arn    = module.lake.lake_bucket_arn
  log_retention_days = var.log_retention_days
}

module "glue_jobs" {
  source                    = "./modules/glue_jobs"
  name                      = local.name
  steps                     = local.glue_steps
  kms_key_arn               = module.kms.key_arn
  lake_bucket               = module.lake.lake_bucket
  lake_bucket_arn           = module.lake.lake_bucket_arn
  artifacts_bucket          = module.lake.artifacts_bucket
  artifacts_bucket_arn      = module.lake.artifacts_bucket_arn
  athena_results_bucket_arn = module.lake.athena_results_bucket_arn
  athena_workgroup          = module.catalog.athena_workgroup
  athena_workgroup_arn      = module.catalog.athena_workgroup_arn
  glue_database_names       = module.catalog.database_names
  firehose_stream_arn       = module.streaming.delivery_stream_arn
  firehose_stream_name      = module.streaming.delivery_stream_name
  max_workers               = var.glue_max_workers
  timeout_minutes           = var.glue_timeout_minutes
  account_id                = local.account_id
  partition                 = local.partition
  region                    = var.region
  secret_names              = var.optional_source_secret_names
  entrypoint_path           = "${path.root}/../src/musicpods/glue/entrypoint.py"
}

module "orchestration" {
  source              = "./modules/orchestration"
  name                = local.name
  kms_key_arn         = module.kms.key_arn
  definition_path     = "${path.root}/../src/musicpods/orchestration/pipeline.asl.json"
  glue_job_arns       = module.glue_jobs.job_arns
  alert_emails        = var.alert_emails
  schedule_expression = var.schedule_expression
  log_retention_days  = var.log_retention_days
}

module "monitoring" {
  source               = "./modules/monitoring"
  name                 = local.name
  project              = var.project
  monthly_budget_usd   = var.monthly_budget_usd
  alerts_topic_arn     = module.orchestration.alerts_topic_arn
  alert_emails         = var.alert_emails
  state_machine_arn    = module.orchestration.state_machine_arn
  delivery_stream_name = module.streaming.delivery_stream_name
}

module "github_oidc" {
  source               = "./modules/github_oidc"
  name                 = local.name
  github_repository    = var.github_repository
  github_environment   = var.github_environment
  create_provider      = var.create_github_oidc_provider
  account_id           = local.account_id
  partition            = local.partition
  artifacts_bucket_arn = module.lake.artifacts_bucket_arn
  kms_key_arn          = module.kms.key_arn
  state_machine_arn    = module.orchestration.state_machine_arn
}
