# =============================================================================
# Derived names, tags, and the canonical Glue job list. Everything that other
# files reference by convention is computed here so names stay consistent.
# =============================================================================
locals {
  account_id = data.aws_caller_identity.current.account_id
  region     = var.aws_region

  name_prefix = "credit-decision-${var.env}"

  # SPEC §3 bucket convention: wio-credit-decision-<env>.
  lakehouse_bucket = coalesce(
    var.lakehouse_bucket_name,
    "wio-credit-decision-${var.env}"
  )
  snapshot_bucket = coalesce(
    var.snapshot_bucket_name,
    "wio-credit-decision-${var.env}-snapshots"
  )
  glue_scripts_bucket = coalesce(
    var.glue_scripts_bucket_name,
    "wio-credit-decision-${var.env}-glue-scripts"
  )

  # SPEC §3 medallion prefixes.
  bronze_prefix         = "bronze"
  silver_prefix         = "silver"
  gold_prefix           = "gold"
  checkpoints_prefix    = "_checkpoints"
  athena_results_prefix = "_athena_results"

  # SPEC §4 Glue Catalog databases.
  glue_databases = {
    bronze = "credit_bronze"
    silver = "credit_silver"
    gold   = "credit_gold"
  }

  retention_seconds_note = "Object Lock retention set in years via s3.tf (SPEC §7/§11)."

  # -- Canonical Glue jobs. Keys mirror the DAG task ids; the value.name mirrors the
  #    GLUE_JOBS dict in orchestration/airflow/dags/credit_decision_pipeline_dag.py.
  #    `bookmark = true` for the batch/derived jobs (SPEC §3 job bookmarks); the CDC
  #    silver job MERGEs instead, so bookmark is disabled there.
  glue_jobs = {
    bronze_to_silver_aecb = {
      name        = "credit_bronze_to_silver_aecb"
      script      = "silver_layer/aecb_to_silver.py"
      bookmark    = true
      description = "Parse+conform AECB bronze -> credit_silver.aecb_credit_report (SPEC §3)"
    }
    bronze_to_silver_fraud = {
      name        = "credit_bronze_to_silver_fraud"
      script      = "silver_layer/fraud_to_silver.py"
      bookmark    = true
      description = "Conform fraud bronze -> credit_silver.fraud_score (SPEC §3)"
    }
    bronze_to_silver_aml = {
      name        = "credit_bronze_to_silver_aml"
      script      = "silver_layer/aml_to_silver.py"
      bookmark    = true
      description = "Conform AML bronze -> credit_silver.aml_screening (SPEC §3)"
    }
    bronze_to_silver_customer_profile = {
      name        = "credit_bronze_to_silver_customer_profile"
      script      = "silver_layer/customer_profile_to_silver.py"
      bookmark    = false # CDC path uses Delta MERGE, not job bookmarks (SPEC §3)
      description = "Delta-MERGE customer_profile CDC -> credit_silver.customer_profile spine (SPEC §3/§6)"
    }
    build_customer_identity_xref = {
      name        = "credit_build_customer_identity_xref"
      script      = "silver_layer/build_customer_identity_xref.py"
      bookmark    = false
      description = "Golden-record identity resolution -> credit_silver.customer_identity_xref (SPEC §6)"
    }
    build_decision_input = {
      name        = "credit_build_decision_input"
      script      = "silver_layer/build_decision_input.py"
      bookmark    = false
      description = "Fuse sources -> credit_silver.decision_input + quarantine (SPEC §5/§8)"
    }
    write_decision_snapshots = {
      name        = "credit_write_decision_snapshots"
      script      = "silver_layer/write_decision_snapshots.py"
      bookmark    = false
      description = "Freeze immutable snapshots to Object-Lock bucket + index (SPEC §7)"
    }
    run_dq_scorecard = {
      name        = "credit_run_dq_scorecard"
      script      = "gold_layer/run_dq_scorecard_mart.py"
      bookmark    = false
      description = "Compute per-day DQ scorecard -> credit_gold.dq_scorecard_daily, alert SNS (SPEC §8)"
    }
    build_portfolio_monitoring_daily = {
      name        = "credit_build_portfolio_monitoring_daily"
      script      = "gold_layer/build_portfolio_monitoring_daily_mart.py"
      bookmark    = false
      description = "Build risk portfolio mart -> credit_gold.portfolio_monitoring_daily (SPEC §9)"
    }
  }

  common_tags = {
    domain      = "lending"
    sub_domain  = "credit_decisioning"
    environment = var.env
    managed_by  = "terraform"
    project     = "credit-decision-data-platform"
    owner       = "data-engineering"
  }
}
