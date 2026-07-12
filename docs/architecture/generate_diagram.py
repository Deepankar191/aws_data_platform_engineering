#!/usr/bin/env python3
"""
Render the AWS deployment view of the Credit Decision Data Platform to a PNG.

Uses the `diagrams` (mingrammer) library, which in turn shells out to Graphviz
(`dot`). Both are optional: if either is missing, this script prints clear
installation instructions and exits 0 instead of crashing, so it is safe to run
in any environment (CI, a fresh laptop, etc.).

Output: docs/architecture/aws_deployment.png  (next to this script)

Usage:
    python3 generate_diagram.py
    # or, using the pinned venv:
    pip install -r requirements.txt && python3 generate_diagram.py

The Mermaid version of this same view lives in architecture.md and renders
without any of this tooling — this PNG is a convenience, not a dependency.
"""
from __future__ import annotations

import os
import shutil
import sys

OUTPUT_BASENAME = "aws_deployment"  # diagrams appends .png
HERE = os.path.dirname(os.path.abspath(__file__))
OUTPUT_PATH = os.path.join(HERE, f"{OUTPUT_BASENAME}.png")


def _fail_gracefully(reason: str, remedy: str) -> int:
    print("=" * 72)
    print("AWS deployment PNG was NOT generated.")
    print(f"Reason : {reason}")
    print(f"Remedy : {remedy}")
    print("-" * 72)
    print("The Mermaid diagram in architecture.md (§4 AWS deployment view)")
    print("renders the same content on GitHub / any Markdown viewer with no")
    print("extra tooling. This PNG is optional.")
    print("=" * 72)
    return 0  # graceful, non-fatal


def main() -> int:
    # 1. Graphviz `dot` must be on PATH for `diagrams` to render anything.
    if shutil.which("dot") is None:
        return _fail_gracefully(
            reason="Graphviz `dot` binary not found on PATH.",
            remedy=(
                "Install Graphviz: macOS `brew install graphviz`; "
                "Debian/Ubuntu `sudo apt-get install graphviz`; "
                "then re-run `python3 generate_diagram.py`."
            ),
        )

    # 2. The `diagrams` python package must be importable.
    try:
        from diagrams import Cluster, Diagram, Edge
        from diagrams.aws.analytics import Athena, Glue, GlueDataCatalog
        from diagrams.aws.analytics import ManagedStreamingForKafka as MSK
        from diagrams.aws.compute import ECS, Lambda
        from diagrams.aws.database import RDS
        from diagrams.aws.integration import SNS
        from diagrams.aws.management import Cloudwatch
        from diagrams.aws.network import APIGateway
        from diagrams.aws.security import IAM, KMS
        from diagrams.aws.storage import S3
        from diagrams.onprem.workflow import Airflow
    except ImportError as exc:  # pragma: no cover - depends on env
        return _fail_gracefully(
            reason=f"`diagrams` package not importable: {exc}",
            remedy="Run `pip install -r requirements.txt` (installs `diagrams`).",
        )

    graph_attr = {
        "fontsize": "20",
        "labelloc": "t",
        "pad": "0.4",
        "nodesep": "0.5",
        "ranksep": "0.8",
        "bgcolor": "white",
    }

    with Diagram(
        "Credit Decision Data Platform — AWS Deployment (me-central-1)",
        filename=os.path.join(HERE, OUTPUT_BASENAME),
        outformat="png",
        show=False,
        direction="TB",
        graph_attr=graph_attr,
    ):
        with Cluster("External"):
            # Represented as simple SNS-less nodes via API Gateway edge below;
            # external providers are drawn as the ingress points they hit.
            api = APIGateway("AML webhook\n(API Gateway)")

        with Cluster("VPC — private subnets (UAE region, data residency)"):
            lam = Lambda("Webhook receiver")
            rds = RDS("RDS PostgreSQL\n(identity spine)")

            with Cluster("CDC path"):
                dbz = MSK("MSK Connect\nDebezium source")
                msk = MSK("Amazon MSK\n(Kafka)")
                sink = MSK("MSK Connect\nS3 sink")

            with Cluster("Compute"):
                glue = Glue("Glue jobs (PySpark)\nbookmarks → EMR at scale")
                catalog = GlueDataCatalog("Glue Data Catalog\nbronze/silver/gold")

            meta = ECS("Metabase\n(ECS Fargate)")
            mwaa = Airflow("MWAA (Airflow)\norchestration")

        with Cluster("Amazon S3"):
            data = S3("wio-credit-decision-ENV\nbronze / silver / gold")
            lock = S3("Snapshot bucket\nObject Lock (7-yr WORM)")

        athena = Athena("Athena")
        sns = SNS("SNS — DQ alerts")
        cw = Cloudwatch("CloudWatch")
        iam = IAM("IAM")
        kms = KMS("KMS")

        # Ingestion
        api >> lam >> data
        rds >> Edge(label="WAL", style="dashed", color="darkorange") >> dbz
        dbz >> Edge(style="dashed", color="darkorange") >> msk
        msk >> Edge(style="dashed", color="darkorange") >> sink
        sink >> Edge(style="dashed", color="darkorange") >> data

        # Transform / catalog
        glue >> data
        glue >> catalog
        glue >> Edge(label="immutable JSON", color="firebrick") >> lock

        # Serve
        catalog >> athena
        data >> athena
        athena >> meta

        # Ops
        glue >> sns
        mwaa >> glue
        mwaa >> dbz
        glue >> cw
        lam >> cw

        # Governance (dotted)
        iam >> Edge(style="dotted", label="least-priv") >> glue
        kms >> Edge(style="dotted", label="SSE-KMS") >> data
        kms >> Edge(style="dotted") >> rds

    if os.path.isfile(OUTPUT_PATH) and os.path.getsize(OUTPUT_PATH) > 0:
        size = os.path.getsize(OUTPUT_PATH)
        print(f"OK: wrote {OUTPUT_PATH} ({size:,} bytes)")
        return 0

    return _fail_gracefully(
        reason="diagrams ran but no PNG was produced.",
        remedy="Check Graphviz install and write permissions on docs/architecture/.",
    )


if __name__ == "__main__":
    sys.exit(main())
