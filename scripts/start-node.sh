#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
"$ROOT/scripts/build.sh" >/dev/null
cd "$ROOT"

ID="${1:-n1}"
PORT="${2:-8001}"
CLUSTER="${CLUSTER:-n1=http://127.0.0.1:8001,n2=http://127.0.0.1:8002,n3=http://127.0.0.1:8003}"

java -cp build/classes raftkv.server.NodeMain \
  --id "$ID" \
  --url "http://127.0.0.1:$PORT" \
  --cluster "$CLUSTER" \
  --data-dir "work/data/$ID" \
  --snapshot-threshold "${SNAPSHOT_THRESHOLD:-50}"

