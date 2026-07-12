# =============================================================================
# Athena — workgroup with an enforced results location (SPEC §3 _athena_results).
# =============================================================================
resource "aws_athena_workgroup" "credit_decision" {
  name        = "credit-decision-${var.env}"
  description = "Analyst + service workgroup for the credit decision platform (SPEC §3/§4)."
  state       = "ENABLED"

  configuration {
    enforce_workgroup_configuration    = true # clients cannot override results/encryption
    publish_cloudwatch_metrics_enabled = true
    bytes_scanned_cutoff_per_query     = 10 * 1024 * 1024 * 1024 # 10 GB guardrail per query

    result_configuration {
      output_location = "s3://${local.lakehouse_bucket}/${local.athena_results_prefix}/"
      encryption_configuration {
        encryption_option = "SSE_KMS"
        kms_key_arn       = aws_kms_key.platform.arn
      }
    }

    engine_version {
      selected_engine_version = "Athena engine version 3" # required for native Delta reads
    }
  }

  tags = merge(local.common_tags, { Name = "credit-decision-${var.env}" })
}

# Named-query stub so the workgroup ships with the analyst views/queries discoverable.
# (The actual view DDL lives in athena/views/ and is applied by CI, not stored here.)
resource "aws_athena_named_query" "daily_decision_summary" {
  name        = "v_daily_decision_summary"
  description = "Decisions/day x product with approval rate, avg fraud, AML hit rate (athena/views/)."
  database    = local.glue_databases.gold
  workgroup   = aws_athena_workgroup.credit_decision.name
  query       = "SELECT * FROM ${local.glue_databases.gold}.v_daily_decision_summary LIMIT 100;"
}
