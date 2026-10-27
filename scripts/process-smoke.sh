#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_DIR"
if [[ -n "${RAFTKV_CLASSPATH:-}" ]]; then
    smoke_classpath="$RAFTKV_CLASSPATH"
else
    bash scripts/build.sh
    smoke_classpath="build/classes"
fi
exec java -cp "$smoke_classpath" raftkv.tool.ProcessClusterCheck "$@"
