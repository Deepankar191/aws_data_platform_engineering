# =============================================================================
# Glue — the three Catalog databases (SPEC §4) and one job per glue/ script.
# =============================================================================

# -----------------------------------------------------------------------------
# Catalog databases (SPEC §4). Athena queries the Delta/Parquet tables through these.
# -----------------------------------------------------------------------------
resource "aws_glue_catalog_database" "this" {
  for_each = local.glue_databases

  name        = each.value
  description = "Credit decision platform ${each.key} layer (SPEC §4)."

  # Point the database default at its medallion prefix so ad-hoc CTAS lands sanely.
  location_uri = "s3://${local.lakehouse_bucket}/${each.key == "bronze" ? local.bronze_prefix : (each.key == "silver" ? local.silver_prefix : local.gold_prefix)}/"
}

# -----------------------------------------------------------------------------
# Glue scripts bucket (the glue/**.py are synced here by CI; TF only owns the bucket).
# -----------------------------------------------------------------------------
resource "aws_s3_bucket" "glue_scripts" {
  bucket = local.glue_scripts_bucket
  tags   = merge(local.common_tags, { Name = local.glue_scripts_bucket, tier = "code" })
}

resource "aws_s3_bucket_server_side_encryption_configuration" "glue_scripts" {
  bucket = aws_s3_bucket.glue_scripts.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.platform.arn
    }
  }
}

resource "aws_s3_bucket_public_access_block" "glue_scripts" {
  bucket                  = aws_s3_bucket.glue_scripts.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# -----------------------------------------------------------------------------
# Glue jobs — one resource per glue/ script (SPEC §3). Glue 4.0, G.1X, metrics on.
# Job bookmarks enabled for batch/derived jobs; disabled for the CDC MERGE job.
# Names come from locals.glue_jobs and MUST match the Airflow GLUE_JOBS dict.
# -----------------------------------------------------------------------------
resource "aws_glue_job" "this" {
  for_each = local.glue_jobs

  name        = each.value.name
  description = each.value.description
  role_arn    = aws_iam_role.glue_job.arn

  glue_version      = var.glue_version
  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_number_of_workers

  # Fail fast: no automatic Glue-side retries — Airflow owns retry policy.
  max_retries = 0
  timeout     = 120 # minutes

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.glue_scripts.id}/${each.value.script}"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--job-bookmark-option"              = each.value.bookmark ? "job-bookmark-enable" : "job-bookmark-disable"
    "--enable-metrics"                   = "true"
    "--enable-observability-metrics"     = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-job-insights"              = "true"
    "--enable-spark-ui"                  = "true"
    "--spark-event-logs-path"            = "s3://${local.lakehouse_bucket}/${local.checkpoints_prefix}/spark-events/${each.value.name}/"
    "--TempDir"                          = "s3://${local.lakehouse_bucket}/${local.checkpoints_prefix}/glue-temp/${each.value.name}/"
    # Delta Lake support for silver/gold (SPEC §3).
    "--datalake-formats"                 = "delta"
    "--conf"                             = "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
    # Env + constants (SPEC §11) passed through to every script.
    "--ENV"                              = var.env
    "--LAKEHOUSE_BUCKET"                 = local.lakehouse_bucket
    "--SNAPSHOT_BUCKET"                  = local.snapshot_bucket
    "--MATCH_THRESHOLD"                  = "0.85"
    "--REVIEW_THRESHOLD"                 = "0.70"
    "--UNRESOLVED_SENTINEL"              = "UNRESOLVED"
    "--SNAPSHOT_RETENTION_YEARS"         = tostring(var.snapshot_retention_years)
    "--TZ"                               = "Asia/Dubai"
    "--DQ_ALERT_TOPIC_ARN"              = aws_sns_topic.dq_alerts.arn
  }

  # The snapshot writer additionally needs the write-only snapshot role's context;
  # it assumes aws_iam_role.snapshot_writer at runtime (see decision_snapshot script).
  execution_property {
    max_concurrent_runs = 1
  }

  tags = merge(local.common_tags, { Name = each.value.name })
}

# -----------------------------------------------------------------------------
# Catalog registration approach (SPEC §3):
#   * Silver + Gold are DELTA. The Glue jobs create/update the Delta tables and their
#     Glue catalog entries directly (DeltaCatalog + table_type=DELTA), so a crawler is
#     NOT used there — the athena/ddl/**/*.sql files are the reviewable equivalents.
#   * Bronze parquet (aecb/fraud/aml) uses partition projection (athena/ddl/bronze_layer/*_raw.sql),
#     so no crawler is needed for partition discovery either.
#   * The crawler below is OPTIONAL: it (re)discovers bronze schema drift on demand.
#     Left disabled from the DAG; run manually if an upstream source changes shape.
# -----------------------------------------------------------------------------
resource "aws_glue_crawler" "bronze_optional" {
  name          = "${local.name_prefix}-bronze-schema-drift"
  role          = aws_iam_role.glue_job.arn
  database_name = local.glue_databases.bronze
  description   = "Optional on-demand crawler to detect bronze source schema drift (SPEC §3). Not scheduled."

  schedule = null # on-demand only

  s3_target {
    path = "s3://${local.lakehouse_bucket}/${local.bronze_prefix}/aecb/"
  }
  s3_target {
    path = "s3://${local.lakehouse_bucket}/${local.bronze_prefix}/fraud/"
  }
  s3_target {
    path = "s3://${local.lakehouse_bucket}/${local.bronze_prefix}/aml/"
  }

  schema_change_policy {
    delete_behavior = "LOG"
    update_behavior = "LOG" # never silently mutate the catalog — log and let a human review
  }

  tags = merge(local.common_tags, { Name = "${local.name_prefix}-bronze-crawler" })
}
