#!/usr/bin/env bash
# Execute the committed EMR step JSON (deploy/aws/emr-steps-fileRead-fileWrite.json)
# against the local mock: rewrites s3://->s3a://, appends the mock overlay conf to
# --files, injects the fs.s3a endpoint/credential overrides as spark-submit --conf
# options, runs spark-submit on the single-node YARN, waits for the application to
# finish, saves yarn logs, then downloads the output and lists the staging dir.
#
# Usage: run-step.sh [step-json]   (default: ../emr-steps-fileRead-fileWrite.json)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=env.sh
. "${SCRIPT_DIR}/env.sh"

STEP_JSON="${1:-${SCRIPT_DIR}/../emr-steps-fileRead-fileWrite.json}"
MOCK_CONF="${SCRIPT_DIR}/application-mock.conf"
export HADOOP_CONF_DIR
export PATH="${HADOOP_HOME}/bin:${SPARK_HOME}/bin:${PATH}"

aws_s3() { "${AWS_CLI}" --endpoint-url "${S3_ENDPOINT}" --region "${REGION}" s3 "$@"; }

echo "Clearing s3://${BUCKET}/v1/output/ and v1/staging/"
aws_s3 rm --recursive "s3://${BUCKET}/v1/output/" 2>/dev/null || true
aws_s3 rm --recursive "s3://${BUCKET}/v1/staging/" 2>/dev/null || true

# --- Rewrite Args from the EMR step JSON ----------------------------------
mapfile -t ARGS < <(jq -r '.[0].Args[]' "${STEP_JSON}")

NEW_ARGS=()
i=0
while [ $i -lt ${#ARGS[@]} ]; do
  a="${ARGS[$i]}"
  case "$a" in
    --files)
      NEW_ARGS+=("$a")
      i=$((i+1))
      files="${ARGS[$i]//s3:\/\//s3a:\/\/}"
      NEW_ARGS+=("${files},${MOCK_CONF}")
      ;;
    --application-conf)
      NEW_ARGS+=("$a")
      i=$((i+1))
      NEW_ARGS+=("application-cloud.conf,application-mock.conf")
      ;;
    *)
      NEW_ARGS+=("${a//s3:\/\//s3a:\/\/}")
      ;;
  esac
  i=$((i+1))
done

# Find the primary jar arg (first arg ending in .jar) and inject options before it.
CONFS=(
  --conf "spark.hadoop.fs.s3a.endpoint=${S3_ENDPOINT}"
  --conf "spark.hadoop.fs.s3a.path.style.access=true"
  --conf "spark.hadoop.fs.s3a.connection.ssl.enabled=false"
  --conf "spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.EnvironmentVariableCredentialsProvider"
  --conf "spark.hadoop.fs.s3a.endpoint.region=${REGION}"
  --conf "spark.yarn.appMasterEnv.AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}"
  --conf "spark.yarn.appMasterEnv.AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}"
  --conf "spark.yarn.appMasterEnv.AWS_DEFAULT_REGION=${AWS_DEFAULT_REGION}"
  --conf "spark.executorEnv.AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}"
  --conf "spark.executorEnv.AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}"
  --conf "spark.executorEnv.AWS_DEFAULT_REGION=${AWS_DEFAULT_REGION}"
  --conf "spark.driver.extraJavaOptions=${JAVA17_OPENS}"
  --conf "spark.executor.extraJavaOptions=${JAVA17_OPENS}"
  --conf "spark.yarn.submit.waitAppCompletion=false"
)

FINAL_ARGS=()
inserted=0
for a in "${NEW_ARGS[@]}"; do
  if [ $inserted -eq 0 ] && [[ "$a" == *.jar ]]; then
    FINAL_ARGS+=("${CONFS[@]}")
    inserted=1
  fi
  FINAL_ARGS+=("$a")
done
[ $inserted -eq 0 ] && { echo "No .jar arg found in step JSON"; exit 1; }

echo "=== spark-submit ==="
SUBMIT_LOG="${RUNTIME_DIR}/submit-$(date +%Y%m%d%H%M%S).log"
"${SPARK_HOME}/bin/spark-submit" "${FINAL_ARGS[@]}" 2>&1 | tee "${SUBMIT_LOG}"

APP_ID="$(grep -oE 'application_[0-9]+_[0-9]+' "${SUBMIT_LOG}" | head -1)"
[ -z "${APP_ID}" ] && { echo "Could not determine YARN application id"; exit 1; }
echo "Application id: ${APP_ID}"

# --- Poll application status ----------------------------------------------
FINAL_STATUS="UNKNOWN"; STATE="UNKNOWN"
while true; do
  status="$("${HADOOP_HOME}/bin/yarn" application -status "${APP_ID}" 2>/dev/null || true)"
  STATE="$(echo "${status}" | awk -F'[ :]+' '/State/ {print $NF}' | head -1)"
  FINAL_STATUS="$(echo "${status}" | awk -F'[ :]+' '/Final-State/ {print $NF}' | head -1)"
  [ -z "${FINAL_STATUS}" ] && FINAL_STATUS="${FINAL_STATUS:-UNDEFINED}"
  case "${STATE}" in
    FINISHED|FAILED|KILLED) break ;;
  esac
  sleep 5
done

"${HADOOP_HOME}/bin/yarn" logs -applicationId "${APP_ID}" > "${RUNTIME_DIR}/yarn-${APP_ID}.log" 2>&1 || true

START_TS="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
cat > "${RUNTIME_DIR}/last-run.env" <<EOF
APP_ID=${APP_ID}
STATE=${STATE}
FINAL_STATUS=${FINAL_STATUS}
SUBMIT_LOG=${SUBMIT_LOG}
YARN_LOG=${RUNTIME_DIR}/yarn-${APP_ID}.log
FINISHED_AT=${START_TS}
EOF

echo "Final status: ${FINAL_STATUS} (state ${STATE})"
if [ "${FINAL_STATUS}" != "SUCCEEDED" ]; then
  echo "Application did not succeed; logs: ${RUNTIME_DIR}/yarn-${APP_ID}.log"
  exit 1
fi

# --- Download output + verify staging -------------------------------------
CLOUD_OUTPUT_DIR="${CLOUD_OUTPUT_DIR:-$HOME/cloud_output}"
echo "Downloading s3://${BUCKET}/v1/output/ -> ${CLOUD_OUTPUT_DIR}"
rm -rf "${CLOUD_OUTPUT_DIR}"
mkdir -p "${CLOUD_OUTPUT_DIR}"
aws_s3 cp --recursive "s3://${BUCKET}/v1/output/" "${CLOUD_OUTPUT_DIR}"

echo "--- staging objects under v1/staging/events ---"
aws_s3 ls --recursive "s3://${BUCKET}/v1/staging/events" || true
echo "Done."
