#!/usr/bin/env bash
set -euo pipefail
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_DIR"
exec bash scripts/maven.sh "-Draft.seed.count=${RAFT_SEED_COUNT:-3}" test "$@"
