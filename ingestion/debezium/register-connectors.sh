#!/usr/bin/env bash
# =============================================================================
# Register the CDC connectors on the Kafka Connect REST API.
#   1. Debezium PostgreSQL source  -> topic credit.public.customer_profile
#   2. Confluent S3 sink           -> s3://wio-credit-decision-${ENV}/bronze/customer_profile/
#
# Runtime placeholders (${ENV}, ${AWS_REGION}, ${S3_STORE_URL}) in the sink config
# are substituted from the environment via envsubst before POSTing — no secrets
# live in the JSON (DB credentials are read by the worker via FileConfigProvider).
#
# Usage:
#   ENV=dev AWS_REGION=me-central-1 S3_STORE_URL=http://minio:9000 \
#     ./register-connectors.sh
# =============================================================================
set -euo pipefail

CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
ENV="${ENV:-dev}"
AWS_REGION="${AWS_REGION:-me-central-1}"
# For local MinIO set http://minio:9000; for real AWS S3 leave empty.
S3_STORE_URL="${S3_STORE_URL:-}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

export ENV AWS_REGION S3_STORE_URL

echo "Waiting for Kafka Connect at ${CONNECT_URL} ..."
until curl -sf "${CONNECT_URL}/connectors" >/dev/null; do
  sleep 3
  echo "  ... still waiting for Connect REST API"
done
echo "Kafka Connect is up."

post_connector() {
  local file="$1"
  local name
  name="$(python3 -c "import json,sys; print(json.load(open(sys.argv[1]))['name'])" "${file}")"
  echo "Registering connector '${name}' from $(basename "${file}") ..."
  # PUT /connectors/{name}/config is an idempotent upsert and takes ONLY the config
  # map, so extract .config from the {name, config} wrapper first. envsubst then fills
  # ${ENV}/${AWS_REGION}/${S3_STORE_URL} (the source config contains none of these).
  python3 -c "import json,sys; print(json.dumps(json.load(open(sys.argv[1]))['config']))" "${file}" \
    | envsubst \
    | curl -sS -X PUT \
        -H "Content-Type: application/json" \
        --data @- \
        "${CONNECT_URL}/connectors/${name}/config" | python3 -m json.tool
  echo
}

# Source first (creates the topic), then the sink.
post_connector "${SCRIPT_DIR}/postgres-source-connector.json"
post_connector "${SCRIPT_DIR}/s3-sink-connector.json"

echo "Current connector status:"
curl -sS "${CONNECT_URL}/connectors?expand=status" | python3 -m json.tool
