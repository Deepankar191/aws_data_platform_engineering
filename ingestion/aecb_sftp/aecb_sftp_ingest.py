#!/usr/bin/env python3
"""AECB SFTP credit-report ingestion  (Source #1, SPEC.md §2).

Pulls new AECB XML credit-report batch files from the bureau's SFTP drop, parses
each <CreditReport>, normalises it, and writes Parquet to the bronze landing:

    s3://wio-credit-decision-${ENV}/bronze/aecb/ingest_date=YYYY-MM-DD/*.parquet

Design goals
------------
* Idempotent / incremental — a manifest (watermark) in S3 records every remote
  file already processed (by name + content hash), so re-runs pick up only new files.
* No hardcoded secrets — SFTP credentials and bucket come from env vars, which in
  prod are injected from SSM Parameter Store / Secrets Manager (see resolve_secret).
* Structured JSON logging on stdout for CloudWatch / ELK.

Normalised bronze record (source schema preserved + lineage):
    emirates_id            STRING   -- PII Level 1
    credit_score           INT
    total_outstanding_aed  DECIMAL(18,2)  (carried as Decimal)
    num_active_loans       INT
    report_ref             STRING
    report_date            DATE
    raw_xml                STRING   -- verbatim <CreditReport> element (audit / snapshot §7)
    record_hash            STRING   -- sha256(raw_xml)
    source_file            STRING
    source_system          STRING   = 'AECB'
    batch_id               STRING
    ingest_timestamp       TIMESTAMP (GST)
    ingest_date            STRING   (partition key)

Run:  python aecb_sftp_ingest.py
"""
from __future__ import annotations

import hashlib
import io
import json
import logging
import os
import posixpath
import sys
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Any, Iterable, Optional
from xml.etree import ElementTree as ET
from zoneinfo import ZoneInfo

import boto3
import paramiko
import pyarrow as pa
import pyarrow.parquet as pq

# --- Constants (SPEC.md §11) -------------------------------------------------
TZ = ZoneInfo("Asia/Dubai")  # GST, UTC+4
SOURCE_SYSTEM = "AECB"
AECB_NS = "urn:aecb:creditreport:v1"  # matches sample_data/aecb/*.xml
MANIFEST_KEY_TEMPLATE = "bronze/aecb/_manifest/processed_files.json"


# --- Structured logging ------------------------------------------------------
class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": datetime.now(TZ).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if getattr(record, "context", None):
            payload.update(record.context)  # type: ignore[arg-type]
        return json.dumps(payload)


def _build_logger() -> logging.Logger:
    logger = logging.getLogger("aecb_sftp_ingest")
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(_JsonFormatter())
        logger.addHandler(handler)
    logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))
    return logger


log = _build_logger()


def _log(level: int, message: str, **context: Any) -> None:
    log.log(level, message, extra={"context": context})


# --- Config ------------------------------------------------------------------
@dataclass(frozen=True)
class Config:
    env: str
    bucket: str
    sftp_host: str
    sftp_port: int
    sftp_username: str
    sftp_password: Optional[str]
    sftp_private_key: Optional[str]
    remote_dir: str
    file_glob_suffix: str = ".xml"
    aws_region: str = "me-central-1"


def resolve_secret(name: str, ssm_param_path: Optional[str] = None,
                   region: str = "me-central-1") -> Optional[str]:
    """Env-first secret resolution; falls back to SSM Parameter Store.

    Prod deployments set SFTP_PASSWORD_SSM_PATH / SFTP_PRIVATE_KEY_SSM_PATH to a
    SecureString parameter; local dev just exports the env var. No secret is ever
    committed to source.
    """
    if os.environ.get(name):
        return os.environ[name]
    if ssm_param_path:
        try:
            ssm = boto3.client("ssm", region_name=region)
            resp = ssm.get_parameter(Name=ssm_param_path, WithDecryption=True)
            return resp["Parameter"]["Value"]
        except Exception as exc:  # noqa: BLE001 - best-effort, logged
            _log(logging.WARNING, "ssm_secret_fetch_failed", param=ssm_param_path,
                 error=str(exc))
    return None


def load_config() -> Config:
    env = os.environ.get("ENV", "dev")
    region = os.environ.get("AWS_REGION", "me-central-1")
    return Config(
        env=env,
        bucket=os.environ.get("S3_BUCKET", f"wio-credit-decision-{env}"),
        sftp_host=os.environ.get("AECB_SFTP_HOST", "aecb-sftp.example.ae"),
        sftp_port=int(os.environ.get("AECB_SFTP_PORT", "22")),
        sftp_username=os.environ.get("AECB_SFTP_USERNAME", "wio_credit"),
        sftp_password=resolve_secret(
            "AECB_SFTP_PASSWORD",
            os.environ.get("AECB_SFTP_PASSWORD_SSM_PATH", f"/credit/{env}/aecb/sftp-password"),
            region,
        ),
        sftp_private_key=resolve_secret(
            "AECB_SFTP_PRIVATE_KEY",
            os.environ.get("AECB_SFTP_PRIVATE_KEY_SSM_PATH"),
            region,
        ),
        remote_dir=os.environ.get("AECB_SFTP_REMOTE_DIR", "/outbound/credit_reports"),
        aws_region=region,
    )


# --- Manifest / watermark ----------------------------------------------------
@dataclass
class Manifest:
    processed: dict[str, str] = field(default_factory=dict)  # filename -> sha256

    @classmethod
    def load(cls, s3, bucket: str) -> "Manifest":
        try:
            obj = s3.get_object(Bucket=bucket, Key=MANIFEST_KEY_TEMPLATE)
            data = json.loads(obj["Body"].read())
            return cls(processed=data.get("processed", {}))
        except s3.exceptions.NoSuchKey:
            return cls()
        except Exception as exc:  # noqa: BLE001
            _log(logging.WARNING, "manifest_load_failed_starting_empty", error=str(exc))
            return cls()

    def is_processed(self, filename: str, file_hash: str) -> bool:
        return self.processed.get(filename) == file_hash

    def mark(self, filename: str, file_hash: str) -> None:
        self.processed[filename] = file_hash

    def save(self, s3, bucket: str) -> None:
        body = json.dumps(
            {"processed": self.processed, "updated_at": datetime.now(TZ).isoformat()},
            indent=2,
        ).encode("utf-8")
        s3.put_object(Bucket=bucket, Key=MANIFEST_KEY_TEMPLATE, Body=body,
                      ContentType="application/json")


# --- XML parsing -------------------------------------------------------------
def _qn(tag: str) -> str:
    return f"{{{AECB_NS}}}{tag}"


def _text(elem: Optional[ET.Element]) -> Optional[str]:
    if elem is None or elem.text is None:
        return None
    return elem.text.strip()


def parse_reports(xml_bytes: bytes, source_file: str, batch_id: str,
                  ingest_ts: datetime, ingest_date: str) -> list[dict[str, Any]]:
    """Parse an AECB batch file into normalised bronze records (one per CreditReport)."""
    root = ET.fromstring(xml_bytes)
    records: list[dict[str, Any]] = []
    for report in root.findall(_qn("CreditReport")):
        raw_xml = ET.tostring(report, encoding="unicode")
        record_hash = hashlib.sha256(raw_xml.encode("utf-8")).hexdigest()
        subject = report.find(_qn("Subject"))
        emirates_id = _text(subject.find(_qn("EmiratesId"))) if subject is not None else None

        outstanding_raw = _text(report.find(_qn("TotalOutstandingAED")))
        credit_score_raw = _text(report.find(_qn("CreditScore")))
        active_loans_raw = _text(report.find(_qn("ActiveLoans")))

        records.append({
            "emirates_id": emirates_id,  # PII Level 1
            "credit_score": int(credit_score_raw) if credit_score_raw else None,
            # money as DECIMAL(18,2) semantics
            "total_outstanding_aed": Decimal(outstanding_raw).quantize(Decimal("0.01"))
            if outstanding_raw else None,
            "num_active_loans": int(active_loans_raw) if active_loans_raw else None,
            "report_ref": _text(report.find(_qn("ReportRef"))),
            "report_date": _text(report.find(_qn("ReportDate"))),
            "raw_xml": raw_xml,
            "record_hash": record_hash,
            "source_file": source_file,
            "source_system": SOURCE_SYSTEM,
            "batch_id": batch_id,
            "ingest_timestamp": ingest_ts,
            "ingest_date": ingest_date,
        })
    return records


# --- Parquet + S3 ------------------------------------------------------------
def _arrow_schema() -> pa.Schema:
    return pa.schema([
        ("emirates_id", pa.string()),
        ("credit_score", pa.int32()),
        ("total_outstanding_aed", pa.decimal128(18, 2)),
        ("num_active_loans", pa.int32()),
        ("report_ref", pa.string()),
        ("report_date", pa.string()),
        ("raw_xml", pa.string()),
        ("record_hash", pa.string()),
        ("source_file", pa.string()),
        ("source_system", pa.string()),
        ("batch_id", pa.string()),
        ("ingest_timestamp", pa.timestamp("us", tz="Asia/Dubai")),
        ("ingest_date", pa.string()),
    ])


def write_parquet_to_s3(s3, bucket: str, records: list[dict[str, Any]],
                        ingest_date: str, source_file: str) -> str:
    table = pa.Table.from_pylist(records, schema=_arrow_schema())
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    buf.seek(0)
    stem = posixpath.splitext(posixpath.basename(source_file))[0]
    key = f"bronze/aecb/ingest_date={ingest_date}/{stem}_{uuid.uuid4().hex[:8]}.parquet"
    s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue(),
                  ContentType="application/octet-stream")
    return f"s3://{bucket}/{key}"


# --- SFTP --------------------------------------------------------------------
def open_sftp(cfg: Config) -> tuple[paramiko.SFTPClient, paramiko.Transport]:
    transport = paramiko.Transport((cfg.sftp_host, cfg.sftp_port))
    if cfg.sftp_private_key:
        pkey = paramiko.RSAKey.from_private_key(io.StringIO(cfg.sftp_private_key))
        transport.connect(username=cfg.sftp_username, pkey=pkey)
    elif cfg.sftp_password:
        transport.connect(username=cfg.sftp_username, password=cfg.sftp_password)
    else:
        raise RuntimeError(
            "No SFTP credential resolved. Set AECB_SFTP_PASSWORD/AECB_SFTP_PRIVATE_KEY "
            "or the corresponding *_SSM_PATH."
        )
    sftp = paramiko.SFTPClient.from_transport(transport)
    return sftp, transport


def list_remote_files(sftp: paramiko.SFTPClient, remote_dir: str, suffix: str) -> list[str]:
    return sorted(f for f in sftp.listdir(remote_dir) if f.endswith(suffix))


# --- Orchestration -----------------------------------------------------------
def run(cfg: Optional[Config] = None) -> dict[str, Any]:
    cfg = cfg or load_config()
    ingest_ts = datetime.now(TZ)
    ingest_date = ingest_ts.strftime("%Y-%m-%d")
    batch_id = f"aecb-{ingest_ts.strftime('%Y%m%dT%H%M%S')}-{uuid.uuid4().hex[:8]}"

    s3 = boto3.client("s3", region_name=cfg.aws_region)
    manifest = Manifest.load(s3, cfg.bucket)

    _log(logging.INFO, "aecb_ingest_start", env=cfg.env, bucket=cfg.bucket,
         remote_dir=cfg.remote_dir, batch_id=batch_id)

    sftp, transport = open_sftp(cfg)
    files_processed = 0
    records_written = 0
    outputs: list[str] = []
    try:
        for filename in list_remote_files(sftp, cfg.remote_dir, cfg.file_glob_suffix):
            remote_path = posixpath.join(cfg.remote_dir, filename)
            with sftp.open(remote_path, "rb") as fh:
                xml_bytes = fh.read()
            file_hash = hashlib.sha256(xml_bytes).hexdigest()

            if manifest.is_processed(filename, file_hash):
                _log(logging.INFO, "aecb_file_skipped_already_processed", file=filename)
                continue

            records = parse_reports(xml_bytes, filename, batch_id, ingest_ts, ingest_date)
            if not records:
                _log(logging.WARNING, "aecb_file_no_reports", file=filename)
                manifest.mark(filename, file_hash)
                continue

            s3_uri = write_parquet_to_s3(s3, cfg.bucket, records, ingest_date, filename)
            manifest.mark(filename, file_hash)
            files_processed += 1
            records_written += len(records)
            outputs.append(s3_uri)
            _log(logging.INFO, "aecb_file_ingested", file=filename, records=len(records),
                 s3_uri=s3_uri)
    finally:
        sftp.close()
        transport.close()

    manifest.save(s3, cfg.bucket)
    summary = {
        "batch_id": batch_id,
        "files_processed": files_processed,
        "records_written": records_written,
        "outputs": outputs,
    }
    _log(logging.INFO, "aecb_ingest_complete", **summary)
    return summary


if __name__ == "__main__":
    run()
