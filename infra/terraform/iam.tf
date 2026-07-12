# =============================================================================
# IAM — least-privilege-ish roles.
#   * glue_job        : read bronze + scripts, read/write silver+gold + checkpoints,
#                       Glue Catalog access, KMS use, SNS publish, assume snapshot_writer.
#   * snapshot_writer : WRITE-ONLY to the Object-Lock bucket (PutObject only) — no
#                       delete, no retention bypass. This is the compliance boundary (§7).
# =============================================================================

# -----------------------------------------------------------------------------
# Glue job execution role
# -----------------------------------------------------------------------------
data "aws_iam_policy_document" "glue_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "glue_job" {
  name               = "${local.name_prefix}-glue-job"
  assume_role_policy = data.aws_iam_policy_document.glue_assume.json
  tags               = merge(local.common_tags, { Name = "${local.name_prefix}-glue-job" })
}

# AWS-managed base for Glue service wiring (CloudWatch logs, ENIs for connections).
resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue_job.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

data "aws_iam_policy_document" "glue_job_inline" {
  # Read: bronze data + glue scripts.
  statement {
    sid     = "ReadBronzeAndScripts"
    effect  = "Allow"
    actions = ["s3:GetObject", "s3:GetObjectVersion", "s3:ListBucket"]
    resources = [
      aws_s3_bucket.lakehouse.arn,
      "${aws_s3_bucket.lakehouse.arn}/${local.bronze_prefix}/*",
      aws_s3_bucket.glue_scripts.arn,
      "${aws_s3_bucket.glue_scripts.arn}/*",
    ]
  }

  # Read/write: silver + gold + checkpoints/temp (Delta needs read+write+delete of
  # its own data/log files, but ONLY under silver/ gold/ _checkpoints/).
  statement {
    sid    = "ReadWriteSilverGoldCheckpoints"
    effect = "Allow"
    actions = [
      "s3:GetObject", "s3:GetObjectVersion", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"
    ]
    resources = [
      aws_s3_bucket.lakehouse.arn,
      "${aws_s3_bucket.lakehouse.arn}/${local.silver_prefix}/*",
      "${aws_s3_bucket.lakehouse.arn}/${local.gold_prefix}/*",
      "${aws_s3_bucket.lakehouse.arn}/${local.checkpoints_prefix}/*",
      "${aws_s3_bucket.lakehouse.arn}/${local.athena_results_prefix}/*",
    ]
  }

  # Glue Data Catalog: read+write the three credit_* databases and their tables.
  statement {
    sid    = "GlueCatalogAccess"
    effect = "Allow"
    actions = [
      "glue:GetDatabase", "glue:GetDatabases",
      "glue:GetTable", "glue:GetTables", "glue:GetPartition", "glue:GetPartitions",
      "glue:CreateTable", "glue:UpdateTable",
      "glue:BatchCreatePartition", "glue:CreatePartition", "glue:UpdatePartition", "glue:BatchGetPartition"
    ]
    resources = [
      "arn:aws:glue:${local.region}:${local.account_id}:catalog",
      "arn:aws:glue:${local.region}:${local.account_id}:database/credit_bronze",
      "arn:aws:glue:${local.region}:${local.account_id}:database/credit_silver",
      "arn:aws:glue:${local.region}:${local.account_id}:database/credit_gold",
      "arn:aws:glue:${local.region}:${local.account_id}:table/credit_bronze/*",
      "arn:aws:glue:${local.region}:${local.account_id}:table/credit_silver/*",
      "arn:aws:glue:${local.region}:${local.account_id}:table/credit_gold/*",
    ]
  }

  # KMS: use the platform key for SSE-KMS read/write.
  statement {
    sid    = "UsePlatformKmsKey"
    effect = "Allow"
    actions = [
      "kms:Decrypt", "kms:Encrypt", "kms:GenerateDataKey", "kms:DescribeKey"
    ]
    resources = [aws_kms_key.platform.arn]
  }

  # Publish DQ alerts (SPEC §8).
  statement {
    sid       = "PublishDqAlerts"
    effect    = "Allow"
    actions   = ["sns:Publish"]
    resources = [aws_sns_topic.dq_alerts.arn]
  }

  # The snapshot-writer Glue job assumes the write-only role at runtime (§7).
  statement {
    sid       = "AssumeSnapshotWriter"
    effect    = "Allow"
    actions   = ["sts:AssumeRole"]
    resources = [aws_iam_role.snapshot_writer.arn]
  }
}

resource "aws_iam_role_policy" "glue_job_inline" {
  name   = "${local.name_prefix}-glue-job-inline"
  role   = aws_iam_role.glue_job.id
  policy = data.aws_iam_policy_document.glue_job_inline.json
}

# -----------------------------------------------------------------------------
# Snapshot writer role — WRITE-ONLY to the Object-Lock bucket (SPEC §7).
# Deliberately: PutObject + PutObjectRetention(only to EXTEND, enforced by COMPLIANCE
# mode) — NO GetObject, NO DeleteObject, NO BypassGovernanceRetention, NO
# PutBucketObjectLockConfiguration. It cannot read back, alter, or remove a snapshot.
# -----------------------------------------------------------------------------
data "aws_iam_policy_document" "snapshot_writer_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
  # Also assumable by the glue_job role (the snapshot job runs under glue_job then
  # steps up to this narrow role for the actual PUTs).
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "AWS"
      identifiers = [aws_iam_role.glue_job.arn]
    }
  }
}

resource "aws_iam_role" "snapshot_writer" {
  name               = "${local.name_prefix}-snapshot-writer"
  assume_role_policy = data.aws_iam_policy_document.snapshot_writer_assume.json
  tags               = merge(local.common_tags, { Name = "${local.name_prefix}-snapshot-writer" })
}

data "aws_iam_policy_document" "snapshot_writer_inline" {
  statement {
    sid    = "WriteOnlyImmutableSnapshots"
    effect = "Allow"
    actions = [
      "s3:PutObject",          # write the snapshot.json
      "s3:PutObjectRetention", # stamp per-object retain-until (COMPLIANCE only extends)
      "s3:ListBucket"          # list its own prefix to build keys
    ]
    resources = [
      aws_s3_bucket.snapshot.arn,
      "${aws_s3_bucket.snapshot.arn}/*"
    ]
  }

  # Explicit self-deny of anything that could compromise immutability — belt and braces
  # over the bucket policy and COMPLIANCE mode.
  statement {
    sid    = "DenyMutationOfSnapshots"
    effect = "Deny"
    actions = [
      "s3:DeleteObject",
      "s3:DeleteObjectVersion",
      "s3:BypassGovernanceRetention",
      "s3:PutBucketObjectLockConfiguration"
    ]
    resources = [
      aws_s3_bucket.snapshot.arn,
      "${aws_s3_bucket.snapshot.arn}/*"
    ]
  }

  statement {
    sid       = "UseKmsForSnapshotEncryption"
    effect    = "Allow"
    actions   = ["kms:GenerateDataKey", "kms:Encrypt", "kms:DescribeKey"]
    resources = [aws_kms_key.platform.arn]
  }
}

resource "aws_iam_role_policy" "snapshot_writer_inline" {
  name   = "${local.name_prefix}-snapshot-writer-inline"
  role   = aws_iam_role.snapshot_writer.id
  policy = data.aws_iam_policy_document.snapshot_writer_inline.json
}
