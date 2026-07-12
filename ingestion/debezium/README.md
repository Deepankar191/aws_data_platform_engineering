# Customer-profile CDC (Debezium → Kafka → S3)

Source #4 in [`docs/SPEC.md` §2](../../docs/SPEC.md): the internal PostgreSQL
`customer_profile` table is the **identity spine** (`internal_customer_uuid` is the
canonical id). Changes are captured as CDC and landed to the bronze zone so the
silver identity-resolution job can build the golden record (§6).

```
Postgres (wal_level=logical)
   │  logical decoding (pgoutput)
   ▼
Debezium PostgreSQL source connector   ── topic ──►  credit.public.customer_profile
   │                                                        │
   │  (Kafka Connect worker)                                ▼
   ▼                                          Confluent S3 sink connector
Kafka (topic prefix: credit)                          │  Parquet, time-partitioned
                                                       ▼
                                    s3://wio-credit-decision-${ENV}/bronze/customer_profile/
                                              ingest_date=YYYY-MM-DD/*.parquet
```

## Files

| File | Purpose |
|---|---|
| `postgres-source-connector.json` | Debezium source: CDC on `public.customer_profile`, topic prefix `credit`, `snapshot.mode=initial`, `decimal.handling.mode=string`, tombstones off, JSON **with schema**. |
| `s3-sink-connector.json` | Confluent S3 sink: writes the CDC topic to `bronze/customer_profile/` as Parquet, time-based partitioning on `ingest_date`, `flush.size`, `schema.compatibility=BACKWARD`. |
| `register-connectors.sh` | `envsubst` + `curl` to upsert both connectors via the Connect REST API. |
| `docker-compose.yml` | Local Postgres (logical replication) + Zookeeper + Kafka + Kafka Connect (Debezium image) + MinIO (S3 stand-in). |
| `connect-secrets/pg.properties` | **Local-only** DB creds read by the worker's `FileConfigProvider` — keeps credentials out of the connector JSON. |

## Key connector settings (and why)

**Source (`postgres-source-connector.json`)**
- `topic.prefix: credit` → topic `credit.public.customer_profile`.
- `snapshot.mode: initial` → one-time full snapshot of existing rows, then streaming.
- `decimal.handling.mode: string` → `monthly_income_aed` (`NUMERIC(18,2)`) is emitted as an
  exact decimal string, never a lossy double — preserves the `DECIMAL(18,2)` money contract (SPEC §10).
- `tombstones.on.delete: false` → deletes emit a delete event but **no** null-value tombstone.
- `key/value.converter.schemas.enable: true` → payloads carry the Connect schema (the sink
  needs it to write typed Parquet).
- `plugin.name: pgoutput` → native logical decoding, no extra Postgres extension.
- DB credentials via `${file:/kafka/secrets/pg.properties:...}` (FileConfigProvider) — **no secrets in the JSON**.
- `REPLICA IDENTITY FULL` is set on the table by the seed SQL so update/delete events carry the full "before" image.

**Sink (`s3-sink-connector.json`)**
- `format.class: ...parquet.ParquetFormat`, `parquet.codec: snappy`.
- `TimeBasedPartitioner`, `partition.duration.ms: 86400000`, `path.format: 'ingest_date'=yyyy-MM-dd`,
  `timezone: Asia/Dubai` → partitions match the bronze layout `ingest_date=YYYY-MM-DD/`.
- `RegexRouter` rewrites the topic `credit.public.customer_profile` → `customer_profile`, and
  `topics.dir: bronze`, so the final prefix is exactly `bronze/customer_profile/…`.
- `flush.size: 1000` + `rotate.schedule.interval.ms: 900000` → flush by row count or every 15 min
  (bounds latency for the <15m CDC freshness SLA in SPEC §8).
- `schema.compatibility: BACKWARD` → the sink tolerates additive schema evolution from the source.
- `store.url: ${S3_STORE_URL}` → points at MinIO locally; leave empty for real AWS S3.

## Run it locally

Prereqs: Docker + Docker Compose.

```bash
cd ingestion/debezium

# 1. Start Postgres (logical replication), Kafka, Connect, MinIO, and create the bucket.
docker compose up -d

# 2. Seed the identity spine (7 sample customers).
docker compose exec -T postgres psql -U credit_app -d credit \
  < ../../sample_data/postgres/customer_profile_seed.sql

# 3. Register both connectors (source then sink).
ENV=dev AWS_REGION=me-central-1 S3_STORE_URL=http://minio:9000 ./register-connectors.sh

# 4. Watch the CDC topic (initial snapshot = 7 rows).
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 \
  --topic credit.public.customer_profile --from-beginning --max-messages 7

# 5. Generate a live change and see it flow (uncomment an UPDATE in the seed file,
#    or run one directly), then inspect MinIO:
docker compose exec postgres psql -U credit_app -d credit -c \
  "UPDATE customer_profile SET kyc_completed = TRUE, updated_at = now()
     WHERE internal_customer_uuid = '44444444-4444-4444-8444-444444444444';"

# 6. Confirm Parquet landed in bronze (MinIO console: http://localhost:9001).
docker compose exec minio-init /usr/bin/mc ls -r local/wio-credit-decision-dev/bronze/customer_profile/
```

Tear down: `docker compose down -v`.

## How this maps to AWS (prod)

| Local (docker-compose) | AWS production |
|---|---|
| Postgres container | **Amazon RDS/Aurora PostgreSQL**, `rds.logical_replication=1` in the parameter group |
| Zookeeper + Kafka | **Amazon MSK** (topic prefix `credit`; `credit.public.customer_profile`) |
| Kafka Connect (Debezium image) | **MSK Connect** — same `postgres-source-connector.json` config, plugin uploaded as a custom plugin (Debezium Postgres) |
| Confluent S3 sink container plugin | **MSK Connect** connector using the Confluent S3 sink custom plugin, writing to the real `s3://wio-credit-decision-${ENV}` bucket |
| MinIO | **Amazon S3** — drop `store.url`; the connector uses the MSK Connect worker's IAM role for S3 access |
| `FileConfigProvider` + `connect-secrets/pg.properties` | **AWS Secrets Manager config provider** — swap `${file:...}` for `${secretsmanager:credit/${ENV}/rds/customer_profile:username}` etc.; the worker role is granted `secretsmanager:GetSecretValue` |
| local IAM env creds | MSK Connect **execution role** (S3 write to the bucket, Secrets Manager read, MSK cluster access) |

The connector JSONs are environment-agnostic: `${ENV}`, `${AWS_REGION}`, `${S3_STORE_URL}` are
filled at registration time (`register-connectors.sh` locally; the MSK Connect connector
configuration in Terraform for prod — see `infra/terraform`). Because the bronze CDC output is
Delta-compatible Parquet partitioned by `ingest_date`, the downstream Glue silver job reads it
identically whether it was produced by local MinIO or prod MSK Connect + S3.
