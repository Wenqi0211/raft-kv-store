#!/usr/bin/env bash
set -euo pipefail
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_DIR"
if [[ -n "${MAVEN_CMD:-}" ]]; then
  maven_binary="$MAVEN_CMD"
elif command -v mvn >/dev/null 2>&1; then
  maven_binary="$(command -v mvn)"
elif [[ -x "$PROJECT_DIR/../order-fulfillment-platform/.tooling/apache-maven-3.9.11/bin/mvn" ]]; then
  maven_binary="$PROJECT_DIR/../order-fulfillment-platform/.tooling/apache-maven-3.9.11/bin/mvn"
else
  echo "Maven 3.9+ is required. Set MAVEN_CMD to its executable path." >&2
  exit 1
fi
if [[ -n "${MAVEN_REPO:-}" ]]; then
  dependency_cache="$MAVEN_REPO"
elif [[ -d "$PROJECT_DIR/../order-fulfillment-platform/.tooling/m2" ]]; then
  dependency_cache="$PROJECT_DIR/../order-fulfillment-platform/.tooling/m2"
else
  dependency_cache="$PROJECT_DIR/.m2"
fi
exec "$maven_binary" -B "-Dmaven.repo.local=$dependency_cache" "$@"
