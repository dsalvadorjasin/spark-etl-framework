#!/usr/bin/env bash
# Upload inputs, scripts, jar, pipeline XML and application-cloud.conf to the
# mock S3 bucket, mirroring playbook steps 5 and 8 against ${S3_ENDPOINT}.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=env.sh
. "${SCRIPT_DIR}/env.sh"

JAR="$(ls -1 "${REPO_ROOT}"/target/*-jar-with-dependencies.jar | head -1)"

aws_s3() { "${AWS_CLI}" --endpoint-url "${S3_ENDPOINT}" --region "${REGION}" s3 "$@"; }

echo "Uploading input data -> s3://${BUCKET}/v1/input/"
aws_s3 cp --recursive "${REPO_ROOT}/src/test/resources/data/users"  "s3://${BUCKET}/v1/input/users"
aws_s3 cp --recursive "${REPO_ROOT}/src/test/resources/data/train"  "s3://${BUCKET}/v1/input/train"
aws_s3 cp --recursive "${REPO_ROOT}/src/test/resources/data/events" "s3://${BUCKET}/v1/input/events"

echo "Uploading scripts -> s3://${BUCKET}/v1/scripts/"
aws_s3 cp --recursive "${REPO_ROOT}/src/test/resources/scripts" "s3://${BUCKET}/v1/scripts/"

echo "Uploading jar -> s3://${BUCKET}/v1/lib/"
aws_s3 cp "${JAR}" "s3://${BUCKET}/v1/lib/"

echo "Uploading pipeline XML -> s3://${BUCKET}/v1/pipelines/"
aws_s3 cp "${REPO_ROOT}/src/test/resources/pipelines/pipeline_fileRead-fileWrite.xml" \
          "s3://${BUCKET}/v1/pipelines/"

echo "Uploading application-cloud.conf -> s3://${BUCKET}/v1/conf/"
aws_s3 cp "${REPO_ROOT}/conf/application-cloud.conf" "s3://${BUCKET}/v1/conf/"

echo "--- bucket contents ---"
aws_s3 ls --recursive "s3://${BUCKET}/"
