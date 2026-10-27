#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_DIR"

echo "Checking protocol, persistence, and cluster regressions"
bash scripts/test.sh

echo "Building the Java 17-compatible delivery package"
bash scripts/package.sh

echo "Checking the delivery JAR in three separate JVMs and a forced leader-process exit"
RAFTKV_CLASSPATH=build/dist/raftkv.jar bash scripts/process-smoke.sh

echo "Verified: regression tests, process recovery, and JAR packaging"
