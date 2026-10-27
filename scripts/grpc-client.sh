#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/grpc-runtime.sh"
exec java -cp "$runtime_classpath" raftkv.client.GrpcClientMain "$@"
