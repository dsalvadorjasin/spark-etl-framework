#!/usr/bin/env bash
# Start the local EMR mock: moto S3 + single-node YARN (ResourceManager + NodeManager).
# Creates the demo bucket, generates core-site.xml/yarn-site.xml, drops the AWS
# jars onto the Spark/YARN classpaths, and waits until the NM registers.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=env.sh
. "${SCRIPT_DIR}/env.sh"

export PATH="${HADOOP_HOME}/bin:${HADOOP_HOME}/sbin:${SPARK_HOME}/bin:${PATH}"

# --- moto S3 -------------------------------------------------------------
if [ ! -f "${RUNTIME_DIR}/moto.pid" ] || ! kill -0 "$(cat "${RUNTIME_DIR}/moto.pid")" 2>/dev/null; then
  echo "Starting moto_server on port ${MOCK_S3_PORT}"
  "${MOTO_SERVER}" -H 127.0.0.1 -p "${MOCK_S3_PORT}" \
    >"${RUNTIME_DIR}/moto.log" 2>&1 &
  echo $! > "${RUNTIME_DIR}/moto.pid"
fi
for i in $(seq 1 30); do
  curl -sf "http://127.0.0.1:${MOCK_S3_PORT}/" >/dev/null 2>&1 && break
  sleep 1
  [ "$i" = 30 ] && { echo "moto_server failed to start; see ${RUNTIME_DIR}/moto.log"; exit 1; }
done

echo "Creating bucket s3://${BUCKET} (idempotent)"
"${AWS_CLI}" --endpoint-url "${S3_ENDPOINT}" s3 mb "s3://${BUCKET}" 2>/dev/null || true

# --- Hadoop configs -------------------------------------------------------
mkdir -p "${HADOOP_CONF_DIR}" "${RUNTIME_DIR}/logs" "${RUNTIME_DIR}/yarn-logs" \
         "${RUNTIME_DIR}/nm-local-dirs" "${RUNTIME_DIR}/remote-app-logs"

cat > "${HADOOP_CONF_DIR}/core-site.xml" <<EOF
<configuration>
  <property><name>fs.defaultFS</name><value>file:///</value></property>
  <property><name>hadoop.tmp.dir</name><value>${RUNTIME_DIR}/tmp</value></property>
</configuration>
EOF

cat > "${HADOOP_CONF_DIR}/yarn-site.xml" <<EOF
<configuration>
  <property><name>yarn.resourcemanager.hostname</name><value>127.0.0.1</value></property>
  <property><name>yarn.nodemanager.aux-services</name><value>mapreduce_shuffle</value></property>
  <property><name>yarn.nodemanager.aux-services.mapreduce_shuffle.class</name><value>org.apache.hadoop.mapred.ShuffleHandler</value></property>
  <property><name>yarn.nodemanager.resource.memory-mb</name><value>24576</value></property>
  <property><name>yarn.nodemanager.resource.cpu-vcores</name><value>8</value></property>
  <property><name>yarn.scheduler.maximum-allocation-mb</name><value>12288</value></property>
  <property><name>yarn.nodemanager.local-dirs</name><value>${RUNTIME_DIR}/nm-local-dirs</value></property>
  <property><name>yarn.log-aggregation-enable</name><value>true</value></property>
  <property><name>yarn.nodemanager.remote-app-log-dir</name><value>${RUNTIME_DIR}/remote-app-logs</value></property>
  <property><name>yarn.nodemanager.log-dirs</name><value>${RUNTIME_DIR}/yarn-logs</value></property>
  <property><name>yarn.nodemanager.env-whitelist</name><value>JAVA_HOME,HADOOP_COMMON_HOME,HADOOP_HDFS_HOME,HADOOP_CONF_DIR,HADOOP_YARN_HOME,HADOOP_HOME,PATH,LANG,TZ,AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY,AWS_DEFAULT_REGION</value></property>
  <property><name>yarn.nodemanager.vmem-check-enabled</name><value>false</value></property>
  <property><name>yarn.nodemanager.pmem-check-enabled</name><value>false</value></property>
</configuration>
EOF

cat > "${HADOOP_CONF_DIR}/capacity-scheduler.xml" <<EOF
<configuration>
  <property><name>yarn.scheduler.capacity.root.queues</name><value>default</value></property>
  <property><name>yarn.scheduler.capacity.root.default.capacity</name><value>100</value></property>
  <property><name>yarn.scheduler.capacity.root.default.maximum-capacity</name><value>100</value></property>
  <property><name>yarn.scheduler.capacity.root.default.user-limit-factor</name><value>1</value></property>
  <property><name>yarn.scheduler.capacity.root.default.state</name><value>RUNNING</value></property>
</configuration>
EOF

# --- AWS jars on Spark + YARN classpath (idempotent) ----------------------
for j in hadoop-aws-3.3.4.jar aws-java-sdk-bundle-1.12.262.jar; do
  for dest in "${SPARK_HOME}/jars" "${HADOOP_HOME}/share/hadoop/common/lib"; do
    if [ ! -f "${dest}/${j}" ]; then
      cp "$HOME/opt/${j}" "${dest}/${j}"
      echo "Installed ${j} -> ${dest}"
    fi
  done
done

# --- YARN daemons ----------------------------------------------------------
YARN_OPTS="${JAVA17_OPENS}"
export YARN_RESOURCEMANAGER_OPTS="${YARN_OPTS}"
export YARN_NODEMANAGER_OPTS="${YARN_OPTS}"
export HADOOP_OPTS="${HADOOP_OPTS:-} ${YARN_OPTS}"

if [ ! -f "${RUNTIME_DIR}/rm.pid" ] || ! kill -0 "$(cat "${RUNTIME_DIR}/rm.pid")" 2>/dev/null; then
  echo "Starting ResourceManager"
  "${HADOOP_HOME}/bin/yarn" --daemon start resourcemanager
  pgrep -f 'ResourceManager' > "${RUNTIME_DIR}/rm.pid"
fi
if [ ! -f "${RUNTIME_DIR}/nm.pid" ] || ! kill -0 "$(cat "${RUNTIME_DIR}/nm.pid")" 2>/dev/null; then
  echo "Starting NodeManager"
  "${HADOOP_HOME}/bin/yarn" --daemon start nodemanager
  pgrep -f 'NodeManager' > "${RUNTIME_DIR}/nm.pid"
fi

echo "Waiting for NodeManager to register..."
for i in $(seq 1 60); do
  state="$("${HADOOP_HOME}/bin/yarn" node -list 2>/dev/null | grep -c RUNNING || true)"
  [ "${state}" -ge 1 ] && { "${HADOOP_HOME}/bin/yarn" node -list; echo "YARN is up."; exit 0; }
  sleep 2
done
echo "NodeManager did not register; check ${HADOOP_HOME}/logs/"
exit 1
