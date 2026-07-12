# =============================================================================
# MSK + MSK Connect — the CDC transport (SPEC §2/§3).
#   PostgreSQL --Debezium(source connector)--> MSK topics --S3 sink connector-->
#   bronze/customer_profile/ (Delta). Both connectors run on MSK Connect.
# =============================================================================

resource "aws_security_group" "msk" {
  name        = "${local.name_prefix}-msk-sg"
  description = "MSK brokers + MSK Connect workers (Debezium source, S3 sink)."
  vpc_id      = var.vpc_id

  # Intra-cluster + connector traffic (TLS broker port).
  ingress {
    description = "Kafka clients / connectors (TLS)"
    from_port   = 9094
    to_port     = 9094
    protocol    = "tcp"
    self        = true
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(local.common_tags, { Name = "${local.name_prefix}-msk-sg" })
}

# Broker log/config tuning: enable CDC-friendly retention and auto-topic behaviour off.
resource "aws_msk_configuration" "cdc" {
  kafka_versions = ["3.6.0"]
  name           = "${local.name_prefix}-cdc-config"

  server_properties = <<-PROPERTIES
    auto.create.topics.enable=false
    default.replication.factor=3
    min.insync.replicas=2
    num.partitions=6
    log.retention.hours=168
  PROPERTIES
}

resource "aws_msk_cluster" "cdc" {
  cluster_name           = "${local.name_prefix}-cdc"
  kafka_version          = "3.6.0"
  number_of_broker_nodes = var.msk_number_of_broker_nodes

  broker_node_group_info {
    instance_type   = var.msk_broker_instance_type
    client_subnets  = var.private_subnet_ids
    security_groups = [aws_security_group.msk.id]

    storage_info {
      ebs_storage_info {
        volume_size = 100
      }
    }
  }

  configuration_info {
    arn      = aws_msk_configuration.cdc.arn
    revision = aws_msk_configuration.cdc.latest_revision
  }

  encryption_info {
    encryption_at_rest_kms_key_arn = aws_kms_key.platform.arn
    encryption_in_transit {
      client_broker = "TLS"
      in_cluster    = true
    }
  }

  open_monitoring {
    prometheus {
      jmx_exporter {
        enabled_in_broker = true
      }
      node_exporter {
        enabled_in_broker = true
      }
    }
  }

  logging_info {
    broker_logs {
      cloudwatch_logs {
        enabled   = true
        log_group = aws_cloudwatch_log_group.msk.name
      }
    }
  }

  tags = merge(local.common_tags, { Name = "${local.name_prefix}-cdc" })
}

resource "aws_cloudwatch_log_group" "msk" {
  name              = "/aws/msk/${local.name_prefix}-cdc"
  retention_in_days = 30
  kms_key_id        = aws_kms_key.platform.arn
  tags              = local.common_tags
}

# -----------------------------------------------------------------------------
# MSK Connect — Debezium PostgreSQL SOURCE connector + S3/Delta SINK connector.
# -----------------------------------------------------------------------------
# The connector JARs (Debezium pgoutput plugin, Confluent/Delta S3 sink) are packaged
# as MSK Connect custom plugins and uploaded to the glue_scripts (code) bucket by CI.
# Referenced here as data-only notes; the plugin objects are created out-of-band.
#
#   Source: io.debezium.connector.postgresql.PostgresConnector
#     - plugin.name=pgoutput, slot.name=credit_profile_slot
#     - database.hostname = aws_db_instance.postgres.address
#     - table.include.list = public.customer_profile
#     - topic.prefix = credit.<env>  -> topic credit.<env>.public.customer_profile
#     - transforms=unwrap (ExtractNewRecordState) so the sink sees flat rows + op/lsn
#
#   Sink: Delta Lake / S3 sink
#     - topics = credit.<env>.public.customer_profile
#     - s3.bucket = local.lakehouse_bucket, path = bronze/customer_profile/
#     - format = delta, flush by size/time -> continuous micro-batches (SPEC §3)

resource "aws_iam_role" "msk_connect" {
  name = "${local.name_prefix}-msk-connect"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Action    = "sts:AssumeRole"
      Principal = { Service = "kafkaconnect.amazonaws.com" }
    }]
  })
  tags = merge(local.common_tags, { Name = "${local.name_prefix}-msk-connect" })
}

# The S3 sink writes ONLY to bronze/customer_profile/, plus KMS + Secrets read (DB creds).
data "aws_iam_policy_document" "msk_connect_inline" {
  statement {
    sid       = "SinkWriteBronzeCustomerProfile"
    effect    = "Allow"
    actions   = ["s3:PutObject", "s3:GetObject", "s3:ListBucket", "s3:DeleteObject"]
    resources = [
      aws_s3_bucket.lakehouse.arn,
      "${aws_s3_bucket.lakehouse.arn}/${local.bronze_prefix}/customer_profile/*",
    ]
  }
  statement {
    sid       = "UseKms"
    effect    = "Allow"
    actions   = ["kms:GenerateDataKey", "kms:Encrypt", "kms:Decrypt", "kms:DescribeKey"]
    resources = [aws_kms_key.platform.arn]
  }
  statement {
    sid       = "ReadDbSecret"
    effect    = "Allow"
    actions   = ["secretsmanager:GetSecretValue"]
    resources = [aws_db_instance.postgres.master_user_secret[0].secret_arn]
  }
}

resource "aws_iam_role_policy" "msk_connect_inline" {
  name   = "${local.name_prefix}-msk-connect-inline"
  role   = aws_iam_role.msk_connect.id
  policy = data.aws_iam_policy_document.msk_connect_inline.json
}

# NOTE ON CONNECTORS: aws_mskconnect_connector resources for the Debezium source and the
# Delta/S3 sink are intentionally kept as a documented follow-up rather than inlined,
# because they need the custom-plugin ARNs (uploaded by CI) and the worker config as
# concrete inputs. Wiring (roles, SGs, cluster, config) is fully provisioned above; the
# two connectors are the last `terraform apply` once the plugin artefacts exist. See
# README "MSK Connect connectors".
