#!/usr/bin/env bash
set -euo pipefail
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_DIR"
bash scripts/maven.sh -DskipTests compile
mkdir -p build/classes
cp -R target/classes/. build/classes/
echo "Built Java 17-compatible classes in target/classes and build/classes"
