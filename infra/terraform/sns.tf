# =============================================================================
# SNS — DQ alert topic (SPEC §8). The Glue DQ job publishes here when a WARN
# threshold is breached or a must-pass check fails (quarantine event).
# =============================================================================
resource "aws_sns_topic" "dq_alerts" {
  name              = "${local.name_prefix}-dq-alerts"
  display_name      = "Credit Decision DQ Alerts (${var.env})"
  kms_master_key_id = aws_kms_key.platform.id

  tags = merge(local.common_tags, { Name = "${local.name_prefix}-dq-alerts" })
}

# Only the DQ job's role may publish; everyone else is denied by default.
resource "aws_sns_topic_policy" "dq_alerts" {
  arn = aws_sns_topic.dq_alerts.arn
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "AllowGlueDqJobPublish"
      Effect    = "Allow"
      Principal = { AWS = aws_iam_role.glue_job.arn }
      Action    = "sns:Publish"
      Resource  = aws_sns_topic.dq_alerts.arn
    }]
  })
}

resource "aws_sns_topic_subscription" "dq_alert_emails" {
  for_each = toset(var.dq_alert_email_subscriptions)

  topic_arn = aws_sns_topic.dq_alerts.arn
  protocol  = "email"
  endpoint  = each.value
}
