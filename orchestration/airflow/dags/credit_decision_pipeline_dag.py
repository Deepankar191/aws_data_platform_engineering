"""Credit decision data platform — daily batch orchestration.

Single source of truth for shapes/paths/names: ``docs/SPEC.md``.

Pipeline order (SPEC section -> task):
  * §2/§3  landing sensors        -> wait_aecb_landing, wait_fraud_landing, wait_aml_landing
                                     (customer_profile is CONTINUOUS Kafka CDC — NOT sensed here,
                                      see orchestration/airflow/README.md "two cadences")
  * §3     bronze -> silver       -> bronze_to_silver_{aecb,fraud,aml,customer_profile}  (4 parallel Glue jobs)
  * §6     identity resolution    -> build_customer_identity_xref
  * §5     unified decision input -> build_decision_input
  * §7     immutable snapshots    -> write_decision_snapshots  (Object-Lock COMPLIANCE bucket)
  * §8     data-quality scorecard -> run_dq_scorecard
  * §9     portfolio mart         -> build_portfolio_monitoring_daily
  * §8     Soda checks            -> soda_scan
  * §3/§4  catalog refresh        -> athena_refresh  (Delta tables self-register; bronze parquet MSCK)

Incremental strategy (SPEC §3): batch Glue jobs (SFTP/API/webhook sources) use Glue **job
bookmarks**; the customer_profile CDC path uses **Delta MERGE**. See the README for detail.

Each env (dev/pre/prod) is a separate AWS account, so Glue job names are NOT env-suffixed;
the account boundary is the isolation. Region/conn come from Airflow env/connection config.
"""

from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.bash import BashOperator

# --- constants (SPEC §11) ----------------------------------------------------
TZ = "Asia/Dubai"  # GST, UTC+4
LOCAL_TZ = pendulum.timezone(TZ)

# Resolved from Airflow Variables at deploy time (one value per env account).
ENV = "{{ var.value.get('credit_env', 'dev') }}"
LAKEHOUSE_BUCKET = "{{ var.value.get('credit_lakehouse_bucket', 'wio-credit-decision-dev') }}"
AWS_CONN_ID = "aws_default"
AWS_REGION = "{{ var.value.get('aws_region', 'me-central-1') }}"

# ``ds`` is the logical date (YYYY-MM-DD) = the ingest_date partition we process.
INGEST_DATE = "{{ ds }}"

# --- Glue job names (must match infra/terraform/glue.tf locals.glue_jobs) -----
GLUE_JOBS = {
    "bronze_to_silver_aecb": "credit_bronze_to_silver_aecb",
    "bronze_to_silver_fraud": "credit_bronze_to_silver_fraud",
    "bronze_to_silver_aml": "credit_bronze_to_silver_aml",
    "bronze_to_silver_customer_profile": "credit_bronze_to_silver_customer_profile",
    "build_customer_identity_xref": "credit_build_customer_identity_xref",
    "build_decision_input": "credit_build_decision_input",
    "write_decision_snapshots": "credit_write_decision_snapshots",
    "run_dq_scorecard": "credit_run_dq_scorecard",
    "build_portfolio_monitoring_daily": "credit_build_portfolio_monitoring_daily",
}

# Arguments passed to every Glue job run. ``--enable-*`` mirror the job-level flags in
# glue.tf; ``--ENV`` / ``--INGEST_DATE`` let one script target the right account+partition.
COMMON_GLUE_ARGS = {
    "--ENV": ENV,
    "--INGEST_DATE": INGEST_DATE,
    "--LAKEHOUSE_BUCKET": LAKEHOUSE_BUCKET,
    "--enable-metrics": "true",
    "--enable-continuous-cloudwatch-log": "true",
    "--enable-job-insights": "true",
    "--job-language": "python",
}

default_args = {
    "owner": "deepankar",
    "depends_on_past": False,
    "email_on_failure": False,  # alerting is via SNS from the DQ job (SPEC §8), not Airflow email
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": pendulum.duration(minutes=30),
    "execution_timeout": pendulum.duration(hours=2),
}


def _glue_task(dag: DAG, key: str, extra_args: dict | None = None) -> GlueJobOperator:
    """Build a GlueJobOperator for a named pipeline step."""
    script_args = dict(COMMON_GLUE_ARGS)
    if extra_args:
        script_args.update(extra_args)
    return GlueJobOperator(
        task_id=key,
        job_name=GLUE_JOBS[key],
        script_args=script_args,
        aws_conn_id=AWS_CONN_ID,
        region_name=AWS_REGION,
        wait_for_completion=True,
        verbose=True,
        dag=dag,
    )


with DAG(
    dag_id="credit_decision_pipeline",
    description="Daily credit-decision lakehouse pipeline: bronze->silver->identity->"
    "decision_input->snapshots->DQ->portfolio mart (SPEC §3-§9).",
    schedule="30 5 * * *",  # 05:30 GST daily — after AECB (02:00), fraud (~06:15), AML (~05:00) land
    start_date=pendulum.datetime(2025, 4, 1, tz=LOCAL_TZ),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    dagrun_timeout=pendulum.duration(hours=6),
    tags=["credit-decisioning", "lending", "daily", "batch", "delta", "p0"],
) as dag:

    # -- §2/§3 landing sensors (batch sources only) ---------------------------
    # The customer_profile CDC sink is CONTINUOUS (Kafka Connect -> Delta) and is NOT
    # sensed here — its silver MERGE simply picks up whatever the stream has landed.
    wait_aecb_landing = S3KeySensor(
        task_id="wait_aecb_landing",
        bucket_name=LAKEHOUSE_BUCKET,
        bucket_key=f"bronze/aecb/ingest_date={INGEST_DATE}/_SUCCESS",
        aws_conn_id=AWS_CONN_ID,
        poke_interval=300,
        timeout=60 * 60 * 4,  # AECB SLA < 24h; give the daily batch 4h to appear
        mode="reschedule",
        soft_fail=False,
    )
    wait_fraud_landing = S3KeySensor(
        task_id="wait_fraud_landing",
        bucket_name=LAKEHOUSE_BUCKET,
        bucket_key=f"bronze/fraud/ingest_date={INGEST_DATE}/_SUCCESS",
        aws_conn_id=AWS_CONN_ID,
        poke_interval=180,
        timeout=60 * 60 * 2,
        mode="reschedule",
    )
    wait_aml_landing = S3KeySensor(
        task_id="wait_aml_landing",
        bucket_name=LAKEHOUSE_BUCKET,
        bucket_key=f"bronze/aml/ingest_date={INGEST_DATE}/_SUCCESS",
        aws_conn_id=AWS_CONN_ID,
        poke_interval=180,
        timeout=60 * 60 * 2,
        mode="reschedule",
    )
    # §2.1 the driving scoring-event feed — decision_input is built from it.
    wait_decisions_landing = S3KeySensor(
        task_id="wait_decisions_landing",
        bucket_name=LAKEHOUSE_BUCKET,
        bucket_key=f"bronze/decisions/ingest_date={INGEST_DATE}/_SUCCESS",
        aws_conn_id=AWS_CONN_ID,
        poke_interval=180,
        timeout=60 * 60 * 2,
        mode="reschedule",
    )

    # -- §3 bronze -> silver (4 parallel Glue jobs) ---------------------------
    b2s_aecb = _glue_task(dag, "bronze_to_silver_aecb")
    b2s_fraud = _glue_task(dag, "bronze_to_silver_fraud")
    b2s_aml = _glue_task(dag, "bronze_to_silver_aml")
    # CDC source: no landing sensor; Delta MERGE of the continuously-sinked stream.
    b2s_profile = _glue_task(dag, "bronze_to_silver_customer_profile")

    # -- §6 identity resolution / golden record -------------------------------
    build_xref = _glue_task(dag, "build_customer_identity_xref")

    # -- §5 unified decision input --------------------------------------------
    build_decision_input = _glue_task(dag, "build_decision_input")

    # -- §7 immutable snapshots (write-only to Object-Lock COMPLIANCE bucket) --
    write_snapshots = _glue_task(dag, "write_decision_snapshots")

    # -- §8 data-quality scorecard (writes dq_scorecard_daily, emits SNS alert) -
    run_dq = _glue_task(dag, "run_dq_scorecard")

    # -- §9 portfolio monitoring mart -----------------------------------------
    build_portfolio = _glue_task(dag, "build_portfolio_monitoring_daily")

    # -- §8 Soda Core checks (dq/soda/) ---------------------------------------
    # Independent assertion layer over the same tables; blocks catalog refresh on failure.
    soda_scan = BashOperator(
        task_id="soda_scan",
        bash_command=(
            "soda scan -d credit_lakehouse "
            "-c ${AIRFLOW_HOME}/dq/soda/configuration.yml "
            "${AIRFLOW_HOME}/dq/soda/checks/ "
            f"-v ENV={ENV} -v INGEST_DATE={INGEST_DATE}"
        ),
    )

    # -- §3/§4 catalog refresh ------------------------------------------------
    # Silver/Gold are Delta and self-register via the Glue jobs; bronze parquet uses
    # partition projection (no MSCK). This step runs a lightweight MSCK for any
    # non-projected bronze partitions and warms the Delta manifest for Athena.
    athena_refresh = BashOperator(
        task_id="athena_refresh",
        bash_command=(
            "python ${AIRFLOW_HOME}/orchestration/airflow/scripts/refresh_catalog.py "
            f"--env {ENV} --ingest-date {INGEST_DATE}"
        ),
    )

    # --- task graph (SPEC §3-§9 order) ---------------------------------------
    wait_aecb_landing >> b2s_aecb
    wait_fraud_landing >> b2s_fraud
    wait_aml_landing >> b2s_aml
    # b2s_profile has no upstream sensor (continuous CDC).

    # All four bronze->silver jobs fan in to identity resolution, then the linear
    # decision -> snapshot -> DQ -> mart -> soda -> catalog chain.
    [b2s_aecb, b2s_fraud, b2s_aml, b2s_profile] >> build_xref
    # decision_input needs both the golden record (build_xref) and the driver feed landed.
    [build_xref, wait_decisions_landing] >> build_decision_input
    chain(
        build_decision_input,
        write_snapshots,
        run_dq,
        build_portfolio,
        soda_scan,
        athena_refresh,
    )
