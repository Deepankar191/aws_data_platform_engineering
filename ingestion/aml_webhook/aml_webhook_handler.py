#!/usr/bin/env python3
"""AML / PEP screening webhook handler  (Source #3, SPEC.md §2).

AWS Lambda behind API Gateway (proxy integration). The AML provider POSTs a
screening callback; this handler:

  1. Verifies an HMAC-SHA256 signature header  (X-Aml-Signature) over the raw body.
  2. Parses + normalises the JSON callback.
  3. Writes one Parquet object per screening to the bronze landing, idempotent on
     screening_ref:

       s3://wio-credit-decision-${ENV}/bronze/aml/ingest_date=YYYY-MM-DD/screening_ref=<ref>/part.parquet

Storage choice (documented per task): each callback is a single small record, so
we write **one Parquet object per callback keyed by screening_ref**. The object key
is deterministic, so a duplicate delivery (AML providers retry) overwrites the same
key rather than creating a duplicate row -> idempotent on screening_ref. A downstream
Glue/Delta compaction merges these small files in silver; this keeps the webhook path
stateless and dependency-light. (Alternative: buffer newline-JSON and batch-convert to
Parquet — rejected here to keep exactly-one-object-per-ref idempotency simple.)

Normalised bronze record:
    full_name       STRING          -- PII Level 2
    date_of_birth   STRING (DATE)   -- PII Level 2
    aml_status      STRING          -- CLEAR | HIT | PENDING
    is_pep          BOOLEAN
    screening_ref   STRING          -- idempotency key
    screened_at     TIMESTAMP (GST)
    raw_json        STRING          -- verbatim callback (audit / snapshot §7)
    record_hash     STRING          -- sha256(raw_json)
    source_system   STRING          = 'AML'
    ingest_timestamp TIMESTAMP (GST)
    ingest_date     STRING          (partition key)
"""
from __future__ import annotations

import hashlib
import hmac
import io
import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any, Optional
from zoneinfo import ZoneInfo

import boto3
import pyarrow as pa
import pyarrow.parquet as pq

# --- Constants (SPEC.md §11) -------------------------------------------------
TZ = ZoneInfo("Asia/Dubai")  # GST, UTC+4
SOURCE_SYSTEM = "AML"
SIGNATURE_HEADER = "x-aml-signature"  # API Gateway lowercases header keys
VALID_AML_STATUS = {"CLEAR", "HIT", "PENDING"}

_s3 = boto3.client("s3", region_name=os.environ.get("AWS_REGION", "me-central-1"))


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
    logger = logging.getLogger("aml_webhook_handler")
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(_JsonFormatter())
        logger.addHandler(handler)
    logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))
    return logger


log = _build_logger()


def _log(level: int, message: str, **context: Any) -> None:
    log.log(level, message, extra={"context": context})


# --- Secrets -----------------------------------------------------------------
def _hmac_secret() -> str:
    """Resolve the HMAC signing secret. Env-first (local); SSM SecureString in prod.

    Never hardcoded. SAM wires AML_WEBHOOK_HMAC_SECRET from an SSM parameter.
    """
    secret = os.environ.get("AML_WEBHOOK_HMAC_SECRET")
    if secret:
        return secret
    ssm_path = os.environ.get("AML_WEBHOOK_HMAC_SECRET_SSM_PATH")
    if ssm_path:
        ssm = boto3.client("ssm", region_name=os.environ.get("AWS_REGION", "me-central-1"))
        return ssm.get_parameter(Name=ssm_path, WithDecryption=True)["Parameter"]["Value"]
    raise RuntimeError("AML webhook HMAC secret not configured")


# --- Signature verification --------------------------------------------------
def _get_header(headers: Optional[dict[str, str]], name: str) -> Optional[str]:
    if not headers:
        return None
    lname = name.lower()
    for key, value in headers.items():
        if key.lower() == lname:
            return value
    return None


def verify_signature(raw_body: str, provided_signature: Optional[str], secret: str) -> bool:
    if not provided_signature:
        return False
    expected = hmac.new(secret.encode("utf-8"), raw_body.encode("utf-8"),
                        hashlib.sha256).hexdigest()
    # accept optional "sha256=" prefix some providers use
    candidate = provided_signature.split("=", 1)[-1].strip()
    return hmac.compare_digest(expected, candidate)


# --- Normalisation -----------------------------------------------------------
def _parse_screened_at(value: Optional[str], fallback: datetime) -> datetime:
    if not value:
        return fallback
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(TZ)


def normalise(callback: dict[str, Any], ingest_ts: datetime, ingest_date: str) -> dict[str, Any]:
    raw_json = json.dumps(callback, sort_keys=True, separators=(",", ":"))
    record_hash = hashlib.sha256(raw_json.encode("utf-8")).hexdigest()
    return {
        "full_name": callback.get("full_name"),       # PII Level 2
        "date_of_birth": callback.get("date_of_birth"),  # PII Level 2
        "aml_status": callback.get("aml_status"),
        "is_pep": bool(callback.get("is_pep")) if callback.get("is_pep") is not None else None,
        "screening_ref": callback.get("screening_ref"),
        "screened_at": _parse_screened_at(callback.get("screened_at"), ingest_ts),
        "raw_json": raw_json,
        "record_hash": record_hash,
        "source_system": SOURCE_SYSTEM,
        "ingest_timestamp": ingest_ts,
        "ingest_date": ingest_date,
    }


def _arrow_schema() -> pa.Schema:
    return pa.schema([
        ("full_name", pa.string()),
        ("date_of_birth", pa.string()),
        ("aml_status", pa.string()),
        ("is_pep", pa.bool_()),
        ("screening_ref", pa.string()),
        ("screened_at", pa.timestamp("us", tz="Asia/Dubai")),
        ("raw_json", pa.string()),
        ("record_hash", pa.string()),
        ("source_system", pa.string()),
        ("ingest_timestamp", pa.timestamp("us", tz="Asia/Dubai")),
        ("ingest_date", pa.string()),
    ])


def write_parquet_to_s3(bucket: str, record: dict[str, Any]) -> str:
    table = pa.Table.from_pylist([record], schema=_arrow_schema())
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    buf.seek(0)
    # Deterministic key -> idempotent on screening_ref (SPEC.md idempotency requirement).
    key = (
        f"bronze/aml/ingest_date={record['ingest_date']}"
        f"/screening_ref={record['screening_ref']}/part.parquet"
    )
    _s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue(),
                   ContentType="application/octet-stream")
    return f"s3://{bucket}/{key}"


# --- Validation --------------------------------------------------------------
def _validate(record: dict[str, Any]) -> Optional[str]:
    if not record.get("screening_ref"):
        return "missing screening_ref"
    if record.get("aml_status") not in VALID_AML_STATUS:
        return f"invalid aml_status: {record.get('aml_status')}"
    if not record.get("full_name") or not record.get("date_of_birth"):
        return "missing full_name or date_of_birth"
    return None


# --- API Gateway response helper ---------------------------------------------
def _response(status: int, body: dict[str, Any]) -> dict[str, Any]:
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body),
    }


# --- Lambda entrypoint -------------------------------------------------------
def handler(event: dict[str, Any], context: Any = None) -> dict[str, Any]:  # noqa: ARG001
    bucket = os.environ.get("S3_BUCKET", f"wio-credit-decision-{os.environ.get('ENV', 'dev')}")
    ingest_ts = datetime.now(TZ)
    ingest_date = ingest_ts.strftime("%Y-%m-%d")

    raw_body = event.get("body") or ""
    if event.get("isBase64Encoded"):
        import base64
        raw_body = base64.b64decode(raw_body).decode("utf-8")

    signature = _get_header(event.get("headers"), SIGNATURE_HEADER)
    try:
        secret = _hmac_secret()
    except RuntimeError as exc:
        _log(logging.ERROR, "aml_secret_unconfigured", error=str(exc))
        return _response(500, {"result": "FAILURE", "error": "server misconfiguration"})

    if not verify_signature(raw_body, signature, secret):
        _log(logging.WARNING, "aml_signature_rejected")
        return _response(401, {"result": "FAILURE", "error": "invalid signature"})

    try:
        payload = json.loads(raw_body)
    except json.JSONDecodeError as exc:
        _log(logging.WARNING, "aml_invalid_json", error=str(exc))
        return _response(400, {"result": "FAILURE", "error": "invalid JSON body"})

    # Accept either a single callback object or a batch array.
    callbacks = payload if isinstance(payload, list) else [payload]
    written: list[str] = []
    rejected: list[dict[str, str]] = []
    for callback in callbacks:
        record = normalise(callback, ingest_ts, ingest_date)
        problem = _validate(record)
        if problem:
            _log(logging.WARNING, "aml_record_rejected",
                 screening_ref=record.get("screening_ref"), reason=problem)
            rejected.append({"screening_ref": record.get("screening_ref"), "reason": problem})
            continue
        s3_uri = write_parquet_to_s3(bucket, record)
        written.append(s3_uri)
        _log(logging.INFO, "aml_record_written",
             screening_ref=record["screening_ref"], aml_status=record["aml_status"],
             is_pep=record["is_pep"], s3_uri=s3_uri)

    if rejected and not written:
        return _response(400, {"result": "FAILURE", "rejected": rejected})
    return _response(200, {
        "result": "SUCCESS",
        "written": len(written),
        "rejected": rejected,
    })


if __name__ == "__main__":
    # Local smoke test: sign and process the sample AML callbacks.
    import pathlib

    os.environ.setdefault("AML_WEBHOOK_HMAC_SECRET", "local-only-not-a-real-secret")
    os.environ.setdefault("S3_BUCKET", "wio-credit-decision-dev")
    sample = pathlib.Path(__file__).resolve().parents[2] / "sample_data" / "aml" / "aml_callbacks_20250401.json"
    body = sample.read_text() if sample.exists() else "[]"
    sig = hmac.new(b"local-only-not-a-real-secret", body.encode(), hashlib.sha256).hexdigest()
    fake_event = {"body": body, "headers": {"X-Aml-Signature": sig}, "isBase64Encoded": False}
    print("Signature valid:", verify_signature(body, sig, "local-only-not-a-real-secret"))
    # NOTE: the S3 write will only succeed with valid AWS credentials + bucket.
    print("Parsed callbacks:", len(json.loads(body)))
