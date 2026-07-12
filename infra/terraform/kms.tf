# =============================================================================
# Platform KMS key — SSE-KMS for all lakehouse + snapshot data at rest.
# =============================================================================
resource "aws_kms_key" "platform" {
  description             = "${local.name_prefix} platform key: S3 SSE-KMS, Glue, MSK, RDS, SNS"
  deletion_window_in_days = var.kms_deletion_window_days
  enable_key_rotation     = true

  tags = merge(local.common_tags, { Name = "${local.name_prefix}-kms" })
}

resource "aws_kms_alias" "platform" {
  name          = "alias/${local.name_prefix}"
  target_key_id = aws_kms_key.platform.key_id
}
