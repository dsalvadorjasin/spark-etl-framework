#!/usr/bin/env bash
# Run pipeline_fileRead-fileWrite.xml on-prem (local Spark, local filesystem).
# Stages the test fixtures into /tmp/events, submits the pipeline with
# conf/application-onprem.conf, then copies /tmp/events/features to
# ${BASELINE_DIR:-$HOME/baseline/local_output}/features.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

SPARK_HOME="${SPARK_HOME:-$HOME/opt/spark-3.5.7-bin-hadoop3}"
BASELINE_DIR="${BASELINE_DIR:-$HOME/baseline/local_output}"
JAR="$(ls -1 "${REPO_ROOT}"/target/*-jar-with-dependencies.jar | head -1)"

# Java 17 module opens required by Spark 3.5 on JDK 17.
JAVA17_OPENS="--add-opens=java.base/java.lang=ALL-UNNAMED \
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

echo "Staging input data -> /tmp/events/data, scripts -> /tmp/events/scripts"
rm -rf /tmp/events/features /tmp/staging/events
mkdir -p /tmp/events/data /tmp/events/scripts
cp -r "${REPO_ROOT}"/src/test/resources/data/* /tmp/events/data/
cp -r "${REPO_ROOT}"/src/test/resources/scripts/* /tmp/events/scripts/

"${SPARK_HOME}/bin/spark-submit" \
  --class com.qwshen.etl.Launcher \
  --conf "spark.driver.extraJavaOptions=${JAVA17_OPENS}" \
  --conf "spark.executor.extraJavaOptions=${JAVA17_OPENS}" \
  "${JAR}" \
  --pipeline-def "${REPO_ROOT}/src/test/resources/pipelines/pipeline_fileRead-fileWrite.xml" \
  --application-conf "${REPO_ROOT}/conf/application-onprem.conf"

echo "Copying /tmp/events/features -> ${BASELINE_DIR}/features"
rm -rf "${BASELINE_DIR}/features"
mkdir -p "${BASELINE_DIR}"
cp -r /tmp/events/features "${BASELINE_DIR}/features"
echo "Done: ${BASELINE_DIR}/features"
