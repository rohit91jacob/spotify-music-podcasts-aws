# Glue Data Catalog (raw Iceberg tables + dbt schemas) and the Athena workgroup dbt uses.

locals {
  databases = {
    raw          = "${var.project}_raw"
    staging      = "${var.project}_staging"
    intermediate = "${var.project}_intermediate"
    marts        = "${var.project}_marts"
  }
}

resource "aws_glue_data_catalog_encryption_settings" "this" {
  data_catalog_encryption_settings {
    connection_password_encryption {
      return_connection_password_encrypted = true
      aws_kms_key_id                       = var.kms_key_arn
    }
    encryption_at_rest {
      catalog_encryption_mode = "SSE-KMS"
      sse_aws_kms_key_id      = var.kms_key_arn
    }
  }
}

resource "aws_glue_catalog_database" "this" {
  for_each     = local.databases
  name         = each.value
  description  = "${var.name} ${each.key} layer"
  location_uri = "s3://${var.lake_bucket}/iceberg/${each.value}.db/"
}

# raw.listen_events is created by the Firehose sink job on first run (pyiceberg, Glue catalog);
# declaring it here pins the schema up front. Iceberg partitioning (day(listened_at)) is hidden
# partitioning set in the table metadata by the job, not Glue partition keys.
resource "aws_glue_catalog_table" "listen_events" {
  name          = "listen_events"
  database_name = aws_glue_catalog_database.this["raw"].name
  table_type    = "EXTERNAL_TABLE"

  open_table_format_input {
    iceberg_input {
      metadata_operation = "CREATE"
      version            = "2"
    }
  }

  storage_descriptor {
    location = "s3://${var.lake_bucket}/iceberg/${local.databases["raw"]}.db/listen_events"

    dynamic "columns" {
      for_each = {
        event_id            = "string"
        user_key            = "string"
        listened_at         = "timestamp"
        inserted_at         = "timestamp"
        recording_msid      = "string"
        artist_name         = "string"
        track_name          = "string"
        release_name        = "string"
        recording_mbid      = "string"
        release_mbid        = "string"
        release_group_mbid  = "string"
        primary_artist_mbid = "string"
        artist_mbids        = "array<string>"
        duration_ms         = "bigint"
        played_ms           = "bigint"
        submission_client   = "string"
        music_service       = "string"
        ingested_at         = "timestamp"
      }
      content {
        name = columns.key
        type = columns.value
      }
    }
  }
}

resource "aws_athena_workgroup" "this" {
  name          = var.name
  description   = "dbt-athena and ad-hoc queries for ${var.name}"
  force_destroy = false

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true
    bytes_scanned_cutoff_per_query     = var.athena_bytes_scanned_cutoff

    engine_version {
      selected_engine_version = "Athena engine version 3"
    }

    result_configuration {
      output_location = "s3://${var.athena_results_bucket}/results/"
      encryption_configuration {
        encryption_option = "SSE_KMS"
        kms_key_arn       = var.kms_key_arn
      }
    }
  }
}
