#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
"$ROOT/scripts/build.sh" >/dev/null
cd "$ROOT"

NODES="${NODES:-http://127.0.0.1:8001,http://127.0.0.1:8002,http://127.0.0.1:8003}"
java -cp build/classes raftkv.client.ClientMain --nodes "$NODES" "$@"

