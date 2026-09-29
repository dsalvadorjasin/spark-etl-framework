#!/usr/bin/env bash
# Stop the local EMR mock: NodeManager, ResourceManager, moto_server.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=env.sh
. "${SCRIPT_DIR}/env.sh"

export HADOOP_CONF_DIR
"${HADOOP_HOME}/bin/yarn" --daemon stop nodemanager 2>/dev/null || true
"${HADOOP_HOME}/bin/yarn" --daemon stop resourcemanager 2>/dev/null || true
rm -f "${RUNTIME_DIR}/nm.pid" "${RUNTIME_DIR}/rm.pid"

if [ -f "${RUNTIME_DIR}/moto.pid" ] && kill -0 "$(cat "${RUNTIME_DIR}/moto.pid")" 2>/dev/null; then
  kill "$(cat "${RUNTIME_DIR}/moto.pid")" || true
  rm -f "${RUNTIME_DIR}/moto.pid"
fi
echo "Mock stopped."
