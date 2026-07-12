# Infra — Terraform

Provisions the AWS credit-decision data platform: the medallion S3 lakehouse, the
**Object-Lock compliance snapshot bucket** (SPEC §7 — the centrepiece), Glue databases +
jobs, the MSK/MSK-Connect CDC path, the source PostgreSQL RDS (identity spine), the Athena
workgroup, least-privilege IAM, and the DQ alert SNS topic.

Everything is driven by variables + `locals.tf`; there are **no hardcoded account ids or
secrets** (account id is read from `aws_caller_identity`; DB creds are RDS-managed in Secrets
Manager). Every resource carries `domain=lending`, `sub_domain=credit_decisioning` via
provider `default_tags`.

## File map

| File | Contents |
|---|---|
| `versions.tf` | Terraform + provider pins, partial S3 backend |
| `providers.tf` | AWS provider (default tags), caller/region data sources |
| `variables.tf` | env, region, bucket names, retention years, sizing knobs |
| `locals.tf` | derived names, common tags, **canonical Glue job list** |
| `kms.tf` | platform KMS key (SSE-KMS everywhere) |
| `s3.tf` | lakehouse bucket + **Object-Lock COMPLIANCE snapshot bucket (7y)** |
| `glue.tf` | 3 Catalog databases + one job per `glue/` script + optional crawler |
| `msk.tf` | MSK cluster + MSK Connect (Debezium source + Delta/S3 sink) wiring |
| `rds.tf` | source PostgreSQL with `rds.logical_replication=1` for Debezium |
| `athena.tf` | Athena workgroup (engine v3) + results location |
| `iam.tf` | Glue job role + **write-only snapshot-writer role** |
| `sns.tf` | DQ alert topic (SPEC §8) |
| `outputs.tf` | names/ARNs the Airflow + CI layers consume |

## Deploy per env (dev / pre / prod)

Each env is a **separate AWS account**. State is separated by backend config, values by
tfvars.

```bash
cd infra/terraform

# 1) init against the env's state backend (bucket/dynamodb differ per env)
terraform init -backend-config=backends/<env>.hcl

# 2) plan/apply with the env's variables
terraform plan  -var-file=env/<env>.tfvars
terraform apply -var-file=env/<env>.tfvars
```

Example `env/prod.tfvars`:

```hcl
env                          = "prod"
aws_region                   = "me-central-1"
snapshot_retention_years     = 7
glue_number_of_workers       = 10
msk_broker_instance_type     = "kafka.m5.2xlarge"
msk_number_of_broker_nodes   = 3
rds_instance_class           = "db.r6g.2xlarge"
vpc_id                       = "vpc-xxxxxxxx"
private_subnet_ids           = ["subnet-a", "subnet-b", "subnet-c"]
dq_alert_email_subscriptions = ["credit-dq-alerts@wio.io"]
```

> **Object Lock is set at bucket creation and is irreversible.** `snapshot_retention_years`
> is validated to be ≥ 7 (SPEC §11) and cannot be lowered. COMPLIANCE mode means not even
> the account root can delete or shorten a locked snapshot before its retain-until — this is
> intentional and is the regulatory guarantee. Test snapshot behaviour in `dev` first; a
> mistake in `prod` is not undoable for 7 years.

## The compliance centrepiece (SPEC §7)

- **Separate bucket** `wio-credit-decision-<env>-snapshots`, `object_lock_enabled = true`,
  versioning forced on.
- **Default retention:** `mode = COMPLIANCE`, `years = 7`. Applies to every PUT.
- **Writer is write-only:** `aws_iam_role.snapshot_writer` has `PutObject` +
  `PutObjectRetention` (COMPLIANCE only ever *extends*) and an explicit **Deny** on
  `DeleteObject*`, `BypassGovernanceRetention`, and `PutBucketObjectLockConfiguration`. It
  cannot even read snapshots back.
- **No lifecycle expiration** on the bucket (would fight the lock).
- The Delta index table `credit_silver.decision_input_snapshot` stores `content_sha256` so
  tampering is detectable; the S3 object is the legal record.

## Scale note: 10K → 100K decisions/day (SPEC §1)

Vertical-last: **scale Glue worker COUNT before worker TYPE**, and keep bookmarks doing the
incremental heavy-lifting so per-run volume stays roughly flat.

| Dimension | Launch (10K/day) | 100K/day | How |
|---|---|---|---|
| Glue `bronze_to_silver_*` | `G.1X` × 4 | `G.1X` × 10–12 | bump `glue_number_of_workers`; stay on G.1X (I/O-bound, not memory-bound) |
| Glue `build_decision_input` (fan-in join) | `G.1X` × 4 | `G.1X` × 12–16, or `G.2X` × 8 | this is the widest shuffle — first to need `G.2X` if skew appears |
| Glue `run_dq_scorecard` / marts | `G.1X` × 4 | `G.1X` × 6 | aggregation-light |
| MSK brokers | `m5.large` × 3 | `m5.2xlarge` × 3 (or × 6) | CDC volume grows with profile writes, not decisions; scale on partition lag |
| MSK topic partitions | 6 | 12–18 | raise `num.partitions` in `aws_msk_configuration` |
| RDS | `r6g.large` | `r6g.2xlarge`, add a read replica | logical replication load + app reads |
| Athena | workgroup w/ 10 GB/query guardrail | same + partition projection already on | Delta + projection keep scans bounded |

Migration path (SPEC §3): the same PySpark runs on EMR. When Glue DPU cost crosses EMR
break-even (~sustained many-hour daily runtime), lift the identical scripts onto a transient
EMR-on-EKS cluster; job bookmarks are replaced by Delta `MERGE` watermarks (the CDC job
already works this way).

## MSK Connect connectors

`msk.tf` provisions the cluster, config, security groups, and the MSK-Connect IAM role. The
two `aws_mskconnect_connector` resources (Debezium **source** on `public.customer_profile`,
Delta/S3 **sink** to `bronze/customer_profile/`) are the final apply once the custom-plugin
JAR ARNs exist in the code bucket — their exact config (plugin ARNs, worker config) is
documented inline in `msk.tf`.

## Notes

- `terraform fmt` / `validate` should pass on structure. A real `plan` needs concrete
  `vpc_id` + `private_subnet_ids` (networking is managed outside this stack) and the backend
  config for the target account.
- Glue job **names** in `locals.glue_jobs` are the single source of truth and must match the
  `GLUE_JOBS` dict in `orchestration/airflow/dags/credit_decision_pipeline_dag.py`.
