#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
"$ROOT/scripts/build.sh" >/dev/null
cd "$ROOT"

java -cp build/classes raftkv.tool.BenchmarkMain "$@"

