#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_DIR"
export MAVEN_CMD="${RAFTKV_MAVEN:-${MAVEN_CMD:-}}"
export MAVEN_REPO="${RAFTKV_MAVEN_REPOSITORY:-${MAVEN_REPO:-}}"
bash scripts/maven.sh -q -DskipTests compile \
  org.apache.maven.plugins:maven-dependency-plugin:3.8.1:build-classpath \
  -Dmdep.outputFile=target/runtime-classpath.txt -DincludeScope=runtime
runtime_classpath="target/classes:$(cat target/runtime-classpath.txt)"
exec java -cp "$runtime_classpath" raftkv.tool.GrpcBenchmarkMain "$@"
