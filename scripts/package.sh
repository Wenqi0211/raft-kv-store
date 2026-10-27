#!/usr/bin/env bash
set -euo pipefail
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_DIR"
bash scripts/build.sh
mkdir -p build/dist
jar --create --file build/dist/raftkv.jar --main-class raftkv.server.NodeMain -C target/classes .
echo "Packaged legacy HTTP node JAR: build/dist/raftkv.jar"
echo "The gRPC entry point requires its Maven runtime classpath; use scripts/grpc-benchmark.sh."
