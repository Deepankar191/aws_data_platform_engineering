# =============================================================================
# S3 — the lakehouse bucket (bronze/silver/gold) and the SEPARATE, Object-Locked
# snapshot bucket that is the compliance centrepiece (SPEC §3, §7).
# =============================================================================

# -----------------------------------------------------------------------------
# 1) Lakehouse bucket: bronze/ silver/ gold/ _checkpoints/ _athena_results/
#    Versioned, SSE-KMS, TLS-only, public access fully blocked.
# -----------------------------------------------------------------------------
resource "aws_s3_bucket" "lakehouse" {
  bucket = local.lakehouse_bucket
  tags   = merge(local.common_tags, { Name = local.lakehouse_bucket, tier = "lakehouse" })
}

resource "aws_s3_bucket_versioning" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.platform.arn
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "lakehouse" {
  bucket                  = aws_s3_bucket.lakehouse.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Create the medallion "folders" as zero-byte prefixes so the layout (SPEC §3) is
# visible immediately and IAM prefix policies resolve even before data lands.
resource "aws_s3_object" "lakehouse_prefixes" {
  for_each = toset([
    "${local.bronze_prefix}/aecb/",
    "${local.bronze_prefix}/fraud/",
    "${local.bronze_prefix}/aml/",
    "${local.bronze_prefix}/customer_profile/",
    "${local.silver_prefix}/",
    "${local.gold_prefix}/",
    "${local.checkpoints_prefix}/",
    "${local.athena_results_prefix}/",
  ])
  bucket       = aws_s3_bucket.lakehouse.id
  key          = each.value
  content_type = "application/x-directory"
}

# Lifecycle: expire non-current versions and clean up Athena results / checkpoints.
resource "aws_s3_bucket_lifecycle_configuration" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id

  rule {
    id     = "expire-noncurrent-versions"
    status = "Enabled"
    filter {}
    noncurrent_version_expiration {
      noncurrent_days = 90
    }
  }

  rule {
    id     = "expire-athena-results"
    status = "Enabled"
    filter {
      prefix = "${local.athena_results_prefix}/"
    }
    expiration {
      days = 30
    }
  }

  rule {
    id     = "abort-incomplete-mpu"
    status = "Enabled"
    filter {}
    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}

# TLS-only bucket policy.
resource "aws_s3_bucket_policy" "lakehouse_tls_only" {
  bucket = aws_s3_bucket.lakehouse.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "DenyInsecureTransport"
      Effect    = "Deny"
      Principal = "*"
      Action    = "s3:*"
      Resource = [
        aws_s3_bucket.lakehouse.arn,
        "${aws_s3_bucket.lakehouse.arn}/*"
      ]
      Condition = { Bool = { "aws:SecureTransport" = "false" } }
    }]
  })
}

# -----------------------------------------------------------------------------
# 2) SNAPSHOT bucket — the compliance centrepiece (SPEC §7).
#    A SEPARATE bucket with S3 Object Lock in COMPLIANCE mode and a 7-year default
#    retention. COMPLIANCE mode means NO principal — not even the account root —
#    can shorten retention, change the mode, or delete a locked object before its
#    retain-until date. This is what makes each decision snapshot a legal record
#    the UAE Central Bank regulator can trust.
#
#    Requirements/consequences (deliberate):
#      * object_lock_enabled MUST be set at bucket creation — it cannot be added later.
#      * Versioning is force-enabled (Object Lock requires it).
#      * The default retention below applies to EVERY object at PUT time; the Glue
#        snapshot writer may also stamp a per-object retain-until (created + 7y) —
#        whichever is later wins. The writer is WRITE-ONLY (see iam.tf): it can PUT
#        but has no DeleteObject / PutObjectRetention-bypass, and no lifecycle rule
#        may expire locked objects.
# -----------------------------------------------------------------------------
resource "aws_s3_bucket" "snapshot" {
  bucket              = local.snapshot_bucket
  object_lock_enabled = true # immutable-once-set; must be true at creation (SPEC §7)

  tags = merge(local.common_tags, {
    Name        = local.snapshot_bucket
    tier        = "compliance-immutable"
    data_class  = "regulatory-record"
    retention   = "${var.snapshot_retention_years}y-compliance-lock"
  })
}

# Object Lock requires versioning to be enabled.
resource "aws_s3_bucket_versioning" "snapshot" {
  bucket = aws_s3_bucket.snapshot.id
  versioning_configuration {
    status = "Enabled"
  }
}

# Default retention: COMPLIANCE mode, 7 years, applied to every PUT (SPEC §7/§11).
resource "aws_s3_bucket_object_lock_configuration" "snapshot" {
  bucket = aws_s3_bucket.snapshot.id

  rule {
    default_retention {
      mode  = "COMPLIANCE"
      years = var.snapshot_retention_years
    }
  }

  depends_on = [aws_s3_bucket_versioning.snapshot]
}

resource "aws_s3_bucket_server_side_encryption_configuration" "snapshot" {
  bucket = aws_s3_bucket.snapshot.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.platform.arn
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "snapshot" {
  bucket                  = aws_s3_bucket.snapshot.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# TLS-only + explicitly deny any attempt to weaken the lock or delete objects,
# defence-in-depth on top of COMPLIANCE mode.
resource "aws_s3_bucket_policy" "snapshot_guard" {
  bucket = aws_s3_bucket.snapshot.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "DenyInsecureTransport"
        Effect    = "Deny"
        Principal = "*"
        Action    = "s3:*"
        Resource = [
          aws_s3_bucket.snapshot.arn,
          "${aws_s3_bucket.snapshot.arn}/*"
        ]
        Condition = { Bool = { "aws:SecureTransport" = "false" } }
      },
      {
        Sid       = "DenyObjectLockWeakeningAndDeletes"
        Effect    = "Deny"
        Principal = "*"
        Action = [
          "s3:PutBucketObjectLockConfiguration",
          "s3:BypassGovernanceRetention",
          "s3:DeleteObjectVersion"
        ]
        Resource = [
          aws_s3_bucket.snapshot.arn,
          "${aws_s3_bucket.snapshot.arn}/*"
        ]
      }
    ]
  })
}

# NOTE: intentionally NO aws_s3_bucket_lifecycle_configuration on the snapshot bucket.
# A lifecycle expiration would conflict with the compliance lock and is a footgun; the
# 7-year (or later) legal hold is the only retention policy that governs these objects.
