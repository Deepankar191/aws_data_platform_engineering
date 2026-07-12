# =============================================================================
# Input variables. Per-env values live in env/<env>.tfvars (see README).
# No account ids or secrets here — those come from the environment / Secrets Manager.
# =============================================================================

variable "env" {
  description = "Deployment environment. Drives bucket names, sizing, and lock behaviour."
  type        = string
  validation {
    condition     = contains(["dev", "pre", "prod"], var.env)
    error_message = "env must be one of: dev, pre, prod."
  }
}

variable "aws_region" {
  description = "AWS region for all resources (UAE North / me-central-1 by default)."
  type        = string
  default     = "me-central-1"
}

variable "lakehouse_bucket_name" {
  description = "Medallion lakehouse bucket name. Defaults to the SPEC §3 convention wio-credit-decision-<env>."
  type        = string
  default     = ""
}

variable "snapshot_bucket_name" {
  description = "Dedicated Object-Lock bucket for immutable decision snapshots (SPEC §7). Defaults to <lakehouse>-snapshots."
  type        = string
  default     = ""
}

variable "snapshot_retention_years" {
  description = "S3 Object Lock retention for decision snapshots, years (SPEC §7/§11 = 7)."
  type        = number
  default     = 7
  validation {
    condition     = var.snapshot_retention_years >= 7
    error_message = "Regulatory minimum is 7 years (SPEC §11); do not lower."
  }
}

variable "glue_scripts_bucket_name" {
  description = "Bucket holding the Glue PySpark scripts (glue/**). Defaults to <lakehouse>-glue-scripts."
  type        = string
  default     = ""
}

variable "glue_version" {
  description = "AWS Glue version for all jobs."
  type        = string
  default     = "4.0"
}

variable "glue_worker_type" {
  description = "Glue worker type. G.1X at launch; scale worker COUNT before type (see README)."
  type        = string
  default     = "G.1X"
}

variable "glue_number_of_workers" {
  description = "Default worker count per Glue job at launch (10K decisions/day)."
  type        = number
  default     = 4
}

variable "msk_broker_instance_type" {
  description = "MSK broker instance type for the CDC path."
  type        = string
  default     = "kafka.m5.large"
}

variable "msk_number_of_broker_nodes" {
  description = "MSK broker node count (multiple of the number of client subnets / AZs)."
  type        = number
  default     = 3
}

variable "rds_instance_class" {
  description = "Source PostgreSQL RDS instance class (identity spine)."
  type        = string
  default     = "db.r6g.large"
}

variable "rds_allocated_storage_gb" {
  description = "Source PostgreSQL allocated storage in GB."
  type        = number
  default     = 100
}

variable "vpc_id" {
  description = "VPC for MSK/RDS/Glue connections. Supplied per env (networking managed elsewhere)."
  type        = string
  default     = ""
}

variable "private_subnet_ids" {
  description = "Private subnet ids (one per AZ) for MSK brokers, RDS, and Glue connections."
  type        = list(string)
  default     = []
}

variable "dq_alert_email_subscriptions" {
  description = "Email addresses subscribed to the DQ alert SNS topic (SPEC §8)."
  type        = list(string)
  default     = []
}

variable "kms_deletion_window_days" {
  description = "Deletion window for the platform KMS key."
  type        = number
  default     = 30
}
