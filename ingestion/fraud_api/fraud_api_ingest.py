#!/usr/bin/env python3
"""Fraud provider REST-API ingestion  (Source #2, SPEC.md §2).

Polls the fraud-scoring provider's REST API for events scored since a stored
watermark, follows pagination, retries transient failures with exponential
backoff, normalises each event and writes Parquet to the bronze landing:

    s3://wio-credit-decision-${ENV}/bronze/fraud/ingest_date=YYYY-MM-DD/*.parquet

The watermark (max scored_at consumed so far) is persisted in S3 so the next run
is incremental and idempotent:

    s3://wio-credit-decision-${ENV}/bronze/fraud/_watermark/watermark.json

Expected API response shape (see sample_data/fraud/fraud_events_20250401.json for
the element shape; the poller wraps them in a page envelope):

    GET {FRAUD_API_BASE_URL}/v1/scoring-events?scored_since=<iso>&cursor=<c>&limit=<n>
    -> { "events": [ {event_id, phone, email, fraud_score, fraud_decision, scored_at}, ... ],
         "next_cursor": "<opaque|null>", "has_more": true|false }

Normalised bronze record:
    phone           STRING          -- PII Level 2 (E.164)
    email           STRING          -- PII Level 2
    fraud_score     DECIMAL(5,4)    -- 0.0000-1.0000
    fraud_decision  STRING          -- APPROVE | REVIEW | DECLINE
    event_id        STRING
    scored_at       TIMESTAMP (GST)
    raw_json        STRING          -- verbatim event JSON (audit / snapshot §7)
    record_hash     STRING          -- sha256(raw_json)
    source_system   STRING          = 'FRAUD'
    batch_id        STRING
    ingest_timestamp TIMESTAMP (GST)
    ingest_date     STRING          (partition key)

Run:  python fraud_api_ingest.py
"""
from __future__ import annotations

import hashlib
import io
import json
import logging
import os
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Iterator, Optional
from zoneinfo import ZoneInfo

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import requests

# --- Constants (SPEC.md §11) -------------------------------------------------
TZ = ZoneInfo("Asia/Dubai")  # GST, UTC+4
SOURCE_SYSTEM = "FRAUD"
WATERMARK_KEY = "bronze/fraud/_watermark/watermark.json"
DEFAULT_PAGE_LIMIT = 500
# On first run with no watermark, look back this far (SLA is < 1h; a day is safe).
INITIAL_LOOKBACK_ISO = "1970-01-01T00:00:00+04:00"
MAX_RETRIES = 5
BACKOFF_BASE_SECONDS = 1.0
BACKOFF_CAP_SECONDS = 30.0
RETRYABLE_STATUS = {429, 500, 502, 503, 504}


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
    logger = logging.getLogger("fraud_api_ingest")
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
    api_base_url: str
    api_key: Optional[str]
    page_limit: int
    aws_region: str = "me-central-1"
    request_timeout_s: int = 20


def resolve_secret(name: str, ssm_param_path: Optional[str], region: str) -> Optional[str]:
    """Env-first secret resolution; SSM Parameter Store fallback. No secrets in code."""
    if os.environ.get(name):
        return os.environ[name]
    if ssm_param_path:
        try:
            ssm = boto3.client("ssm", region_name=region)
            resp = ssm.get_parameter(Name=ssm_param_path, WithDecryption=True)
            return resp["Parameter"]["Value"]
        except Exception as exc:  # noqa: BLE001
            _log(logging.WARNING, "ssm_secret_fetch_failed", param=ssm_param_path,
                 error=str(exc))
    return None


def load_config() -> Config:
    env = os.environ.get("ENV", "dev")
    region = os.environ.get("AWS_REGION", "me-central-1")
    return Config(
        env=env,
        bucket=os.environ.get("S3_BUCKET", f"wio-credit-decision-{env}"),
        api_base_url=os.environ.get("FRAUD_API_BASE_URL", "https://fraud.example.com").rstrip("/"),
        api_key=resolve_secret(
            "FRAUD_API_KEY",
            os.environ.get("FRAUD_API_KEY_SSM_PATH", f"/credit/{env}/fraud/api-key"),
            region,
        ),
        page_limit=int(os.environ.get("FRAUD_API_PAGE_LIMIT", str(DEFAULT_PAGE_LIMIT))),
        aws_region=region,
    )


# --- Watermark ---------------------------------------------------------------
def load_watermark(s3, bucket: str) -> str:
    try:
        obj = s3.get_object(Bucket=bucket, Key=WATERMARK_KEY)
        data = json.loads(obj["Body"].read())
        return data.get("scored_at_watermark", INITIAL_LOOKBACK_ISO)
    except s3.exceptions.NoSuchKey:
        return INITIAL_LOOKBACK_ISO
    except Exception as exc:  # noqa: BLE001
        _log(logging.WARNING, "watermark_load_failed_using_initial", error=str(exc))
        return INITIAL_LOOKBACK_ISO


def save_watermark(s3, bucket: str, scored_at_iso: str, batch_id: str) -> None:
    body = json.dumps({
        "scored_at_watermark": scored_at_iso,
        "batch_id": batch_id,
        "updated_at": datetime.now(TZ).isoformat(),
    }, indent=2).encode("utf-8")
    s3.put_object(Bucket=bucket, Key=WATERMARK_KEY, Body=body,
                  ContentType="application/json")


# --- HTTP with retry/backoff -------------------------------------------------
def _sleep_backoff(attempt: int) -> None:
    delay = min(BACKOFF_CAP_SECONDS, BACKOFF_BASE_SECONDS * (2 ** attempt))
    # deterministic jitter component keeps tests reproducible while spreading load
    jitter = (uuid.uuid4().int % 1000) / 1000.0
    time.sleep(delay + jitter)


def _get_with_retry(session: requests.Session, url: str, params: dict[str, Any],
                    headers: dict[str, str], timeout: int) -> dict[str, Any]:
    last_exc: Optional[Exception] = None
    for attempt in range(MAX_RETRIES):
        try:
            resp = session.get(url, params=params, headers=headers, timeout=timeout)
            if resp.status_code in RETRYABLE_STATUS:
                _log(logging.WARNING, "fraud_api_retryable_status",
                     status=resp.status_code, attempt=attempt)
                _sleep_backoff(attempt)
                continue
            resp.raise_for_status()
            return resp.json()
        except (requests.ConnectionError, requests.Timeout) as exc:
            last_exc = exc
            _log(logging.WARNING, "fraud_api_transient_error",
                 error=str(exc), attempt=attempt)
            _sleep_backoff(attempt)
    raise RuntimeError(f"Fraud API GET failed after {MAX_RETRIES} attempts: {last_exc}")


def poll_events(cfg: Config, scored_since: str) -> Iterator[dict[str, Any]]:
    """Yield raw event dicts across all pages since the watermark."""
    session = requests.Session()
    headers = {"Accept": "application/json"}
    if cfg.api_key:
        headers["Authorization"] = f"Bearer {cfg.api_key}"
    url = f"{cfg.api_base_url}/v1/scoring-events"
    cursor: Optional[str] = None
    page = 0
    while True:
        params: dict[str, Any] = {"scored_since": scored_since, "limit": cfg.page_limit}
        if cursor:
            params["cursor"] = cursor
        body = _get_with_retry(session, url, params, headers, cfg.request_timeout_s)
        events = body.get("events", [])
        _log(logging.INFO, "fraud_api_page_fetched", page=page, count=len(events))
        yield from events
        if not body.get("has_more") or not body.get("next_cursor"):
            break
        cursor = body["next_cursor"]
        page += 1


# --- Normalisation -----------------------------------------------------------
def _parse_scored_at(value: str) -> datetime:
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(TZ)


def normalise(event: dict[str, Any], batch_id: str, ingest_ts: datetime,
              ingest_date: str) -> dict[str, Any]:
    raw_json = json.dumps(event, sort_keys=True, separators=(",", ":"))
    record_hash = hashlib.sha256(raw_json.encode("utf-8")).hexdigest()
    score = event.get("fraud_score")
    return {
        "phone": event.get("phone"),  # PII Level 2
        "email": event.get("email"),  # PII Level 2
        # fraud_score DECIMAL(5,4) 0.0000-1.0000
        "fraud_score": Decimal(str(score)).quantize(Decimal("0.0001"))
        if score is not None else None,
        "fraud_decision": event.get("fraud_decision"),
        "event_id": event.get("event_id"),
        "scored_at": _parse_scored_at(event["scored_at"]) if event.get("scored_at") else None,
        "raw_json": raw_json,
        "record_hash": record_hash,
        "source_system": SOURCE_SYSTEM,
        "batch_id": batch_id,
        "ingest_timestamp": ingest_ts,
        "ingest_date": ingest_date,
    }


def _arrow_schema() -> pa.Schema:
    return pa.schema([
        ("phone", pa.string()),
        ("email", pa.string()),
        ("fraud_score", pa.decimal128(5, 4)),
        ("fraud_decision", pa.string()),
        ("event_id", pa.string()),
        ("scored_at", pa.timestamp("us", tz="Asia/Dubai")),
        ("raw_json", pa.string()),
        ("record_hash", pa.string()),
        ("source_system", pa.string()),
        ("batch_id", pa.string()),
        ("ingest_timestamp", pa.timestamp("us", tz="Asia/Dubai")),
        ("ingest_date", pa.string()),
    ])


def write_parquet_to_s3(s3, bucket: str, records: list[dict[str, Any]],
                        ingest_date: str, batch_id: str) -> str:
    table = pa.Table.from_pylist(records, schema=_arrow_schema())
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    buf.seek(0)
    key = f"bronze/fraud/ingest_date={ingest_date}/{batch_id}.parquet"
    s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue(),
                  ContentType="application/octet-stream")
    return f"s3://{bucket}/{key}"


# --- Orchestration -----------------------------------------------------------
def run(cfg: Optional[Config] = None) -> dict[str, Any]:
    cfg = cfg or load_config()
    ingest_ts = datetime.now(TZ)
    ingest_date = ingest_ts.strftime("%Y-%m-%d")
    batch_id = f"fraud-{ingest_ts.strftime('%Y%m%dT%H%M%S')}-{uuid.uuid4().hex[:8]}"

    s3 = boto3.client("s3", region_name=cfg.aws_region)
    scored_since = load_watermark(s3, cfg.bucket)
    _log(logging.INFO, "fraud_ingest_start", env=cfg.env, bucket=cfg.bucket,
         scored_since=scored_since, batch_id=batch_id)

    records: list[dict[str, Any]] = []
    max_scored_at = scored_since
    seen_event_ids: set[str] = set()
    for event in poll_events(cfg, scored_since):
        event_id = event.get("event_id")
        if event_id and event_id in seen_event_ids:
            continue  # idempotent within the run
        if event_id:
            seen_event_ids.add(event_id)
        rec = normalise(event, batch_id, ingest_ts, ingest_date)
        records.append(rec)
        if rec["scored_at"] is not None:
            scored_iso = rec["scored_at"].isoformat()
            if scored_iso > max_scored_at:
                max_scored_at = scored_iso

    output = None
    if records:
        output = write_parquet_to_s3(s3, cfg.bucket, records, ingest_date, batch_id)
        save_watermark(s3, cfg.bucket, max_scored_at, batch_id)
    else:
        _log(logging.INFO, "fraud_ingest_no_new_events")

    summary = {
        "batch_id": batch_id,
        "records_written": len(records),
        "new_watermark": max_scored_at,
        "output": output,
    }
    _log(logging.INFO, "fraud_ingest_complete", **summary)
    return summary


if __name__ == "__main__":
    run()
