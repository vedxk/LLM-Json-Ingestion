#!/usr/bin/env bash
# Fires the sample payloads at a locally-running instance of the ingest service.
# Usage:
#   ./scripts/seed_webhooks.sh                 # one of each
#   BASE_URL=http://localhost:8000 ./scripts/seed_webhooks.sh
#   REPEAT=5 ./scripts/seed_webhooks.sh        # fire each payload N times to
#                                              # exercise dedup and rate limits

set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:8000}"
REPEAT="${REPEAT:-1}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PAYLOADS="${SCRIPT_DIR}/sample_payloads"

post() {
  local vendor="$1"
  local file="$2"
  local idem="${3:-}"
  local idem_header=()
  if [[ -n "$idem" ]]; then
    idem_header=(-H "Idempotency-Key: $idem")
  fi
  echo "── POST /webhooks/${vendor} (${file##*/})"
  curl -sS -X POST "${BASE_URL}/webhooks/${vendor}" \
    -H "Content-Type: application/json" \
    ${idem_header[@]+"${idem_header[@]}"} \
    --data-binary "@${file}" \
    -w "\nHTTP %{http_code}  time=%{time_total}s\n" \
    || echo "  (curl failed)"
  echo
}

for _ in $(seq 1 "$REPEAT"); do
  post maersk           "${PAYLOADS}/maersk_shipment.json"        "maersk-evt-$(date +%s%N)"
  post fedex            "${PAYLOADS}/fedex_delivered.json"        "fedex-evt-$(date +%s%N)"
  post fedex            "${PAYLOADS}/fedex_exception.json"
  post acme_invoicing   "${PAYLOADS}/acme_invoice.json"           "acme-evt-$(date +%s%N)"
  post generic          "${PAYLOADS}/unclassified.json"
done
