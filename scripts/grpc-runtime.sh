#!/usr/bin/env bash
# Source this helper. Build once before starting multiple terminals.
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_DIR"
if [[ ! -f target/runtime-classpath.txt || ! -f target/classes/raftkv/server/GrpcNodeMain.class ]]; then
  bash scripts/maven.sh -q -DskipTests compile \
    org.apache.maven.plugins:maven-dependency-plugin:3.8.1:build-classpath \
    -Dmdep.outputFile=target/runtime-classpath.txt -DincludeScope=runtime
fi
runtime_classpath="target/classes:$(cat target/runtime-classpath.txt)"
