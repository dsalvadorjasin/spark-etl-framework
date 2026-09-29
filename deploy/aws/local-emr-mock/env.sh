#!/usr/bin/env bash
# Shared environment for the local EMR mock harness (moto S3 + single-node YARN).
# Source this file: . env.sh
# Requires AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_DEFAULT_REGION to be
# exported by the caller, e.g.:
#   export AWS_ACCESS_KEY_ID=mock AWS_SECRET_ACCESS_KEY=mock AWS_DEFAULT_REGION=us-east-2

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"

export SPARK_HOME="${SPARK_HOME:-$HOME/opt/spark-3.5.7-bin-hadoop3}"
export HADOOP_HOME="${HADOOP_HOME:-$HOME/opt/hadoop-3.3.4}"
export MOCK_S3_PORT="${MOCK_S3_PORT:-5000}"
export BUCKET="${BUCKET:-spark-etl-migration-demo}"
export REGION="${REGION:-us-east-2}"
export S3_ENDPOINT="${S3_ENDPOINT:-http://localhost:${MOCK_S3_PORT}}"
export RUNTIME_DIR="${RUNTIME_DIR:-$HOME/.local-emr-mock}"
export HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-${RUNTIME_DIR}/conf}"
export JAVA_HOME="${JAVA_HOME:-$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")}"

# moto_server binary: defaults to the dedicated venv created by docs, overridable.
export MOTO_SERVER="${MOTO_SERVER:-$HOME/opt/moto-venv/bin/moto_server}"

# AWS CLI binary (mock endpoint only). Falls back to PATH.
export AWS_CLI="${AWS_CLI:-aws}"

# Java 17 module opens required by Spark 3.5 / Hadoop 3.3 on JDK 17.
export JAVA17_OPENS="--add-opens=java.base/java.lang=ALL-UNNAMED \
--add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
--add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
--add-opens=java.base/java.io=ALL-UNNAMED \
--add-opens=java.base/java.net=ALL-UNNAMED \
--add-opens=java.base/java.nio=ALL-UNNAMED \
--add-opens=java.base/java.util=ALL-UNNAMED \
--add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED \
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
--add-opens=java.base/sun.nio.cs=ALL-UNNAMED \
--add-opens=java.base/sun.security.action=ALL-UNNAMED \
--add-opens=java.base/sun.util.calendar=ALL-UNNAMED \
--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"

for v in AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_DEFAULT_REGION; do
  if [ -z "${!v:-}" ]; then
    echo "ERROR: $v is not exported. Run:" >&2
    echo "  export AWS_ACCESS_KEY_ID=mock AWS_SECRET_ACCESS_KEY=mock AWS_DEFAULT_REGION=${REGION}" >&2
    return 1 2>/dev/null || exit 1
  fi
done

mkdir -p "${RUNTIME_DIR}" "${HADOOP_CONF_DIR}"
