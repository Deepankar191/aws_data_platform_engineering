"""Delta-enabled Spark/Glue session bootstrap.

Two entry points:

* :func:`build_spark_session` — a plain, Delta-enabled ``SparkSession``. This is
  the EMR / local path and the one to point at when migrating off Glue (SPEC §3,
  "the same PySpark code runs on both").
* :func:`glue_bootstrap` — the AWS Glue 4.0 path. Returns ``(glue_context, spark,
  job)`` with Glue **job bookmarks** wired in for the batch S3 sources. ``awsglue``
  is imported lazily so this module also imports cleanly off-cluster.

Delta on Glue 4.0 additionally requires the job argument ``--datalake-formats delta``.
"""

import logging
import sys

from pyspark.sql import SparkSession

from common.constants import TZ

_LOG_FORMAT = "%(asctime)s %(levelname)s [%(name)s] %(message)s"


def get_logger(name: str) -> logging.Logger:
    """Structured (single-line, greppable) logger shared by all jobs."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter(_LOG_FORMAT))
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
    return logger


def _apply_delta_conf(builder: "SparkSession.Builder") -> "SparkSession.Builder":
    """Common Delta + timezone configuration shared by Glue and plain sessions."""
    return (
        builder
        # Delta SQL extensions + catalog so MERGE / time-travel / DDL work.
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # SPEC §10: timestamps are GST (UTC+4). Pin the session TZ so that
        # current_timestamp() and the "not in the future / older than 48h"
        # DQ comparisons evaluate in the same zone the data is stored in.
        .config("spark.sql.session.timeZone", TZ)
        # Safe, idempotent write behaviour.
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "false")
    )


def build_spark_session(app_name: str) -> SparkSession:
    """Plain Delta-enabled SparkSession (EMR / local).

    Registers the Glue Data Catalog as the Hive metastore so tables created here
    are visible to Athena, matching the Glue runtime behaviour.
    """
    builder = _apply_delta_conf(SparkSession.builder.appName(app_name))
    builder = builder.config(
        "spark.hadoop.hive.metastore.client.factory.class",
        "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
    ).enableHiveSupport()
    spark = builder.getOrCreate()
    get_logger(app_name).info("Built plain Delta SparkSession app=%s", app_name)
    return spark


def glue_bootstrap(app_name: str, args: dict):
    """AWS Glue 4.0 bootstrap. Returns ``(glue_context, spark, job)``.

    ``awsglue`` is imported lazily so importing this module never requires the
    Glue runtime. Callers pass the already-resolved ``args`` dict (which must
    include ``JOB_NAME``) and are responsible for calling ``job.commit()`` at the
    end so Glue **job bookmarks** advance for the batch S3 sources.
    """
    from awsglue.context import GlueContext  # noqa: WPS433 (lazy, Glue-only)
    from awsglue.job import Job
    from pyspark import SparkContext

    sc = SparkContext.getOrCreate()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session

    # Apply Delta + TZ config onto the live session.
    for key, value in {
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        "spark.sql.session.timeZone": TZ,
        "spark.sql.sources.partitionOverwriteMode": "dynamic",
    }.items():
        spark.conf.set(key, value)

    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)
    get_logger(app_name).info("Initialised Glue job=%s", args["JOB_NAME"])
    return glue_context, spark, job
