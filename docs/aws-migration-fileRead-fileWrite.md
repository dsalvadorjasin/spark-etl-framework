# AWS migration: pipeline_fileRead-fileWrite

Migration of the `pipeline_fileRead-fileWrite.xml` job (`event-consolidation`) from the
local/on-prem file paths to S3 + EMR, with a local mock mode (moto S3 + single-node
YARN) for exercising the EMR step definition without AWS.

## Path mapping

| On-prem (`conf/application-onprem.conf`) | Cloud (`conf/application-cloud.conf`) |
|---|---|
| `/tmp/events/data/users` | `s3a://spark-etl-migration-demo/v1/input/users` |
| `/tmp/events/data/events` | `s3a://spark-etl-migration-demo/v1/input/events` |
| `/tmp/events/data/train` | `s3a://spark-etl-migration-demo/v1/input/train` |
| `/tmp/events/features` | `s3a://spark-etl-migration-demo/v1/output/features` |
| `/tmp/events/scripts` | `.` (container working dir, via `--files`) |
| `--staging-uri` `/tmp/staging/events` | `s3a://spark-etl-migration-demo/v1/staging/events` |
| `/tmp/metrics/events` | `s3a://.../v1/metrics/events` (only if metrics enabled) |

## Files added

- `conf/application-onprem.conf` — local paths, `spark.master=local[*]`; secrets via `${?VAR}`.
- `conf/application-cloud.conf` — s3a paths, S3A filesystem settings, `scripts_uri="."`
  (the EMR step ships conf/pipeline/SQL via `spark-submit --files`, so files land in the
  container working directory and are referenced by basename).
- `deploy/aws/emr-steps-fileRead-fileWrite.json` — the `aws emr add-steps` / `create-cluster --steps`
  definition; stays runnable on real EMR (plain `s3://` URIs, no endpoint overrides).
- `deploy/aws/local-emr-mock/` — mock harness: `env.sh`, `start.sh`, `stop.sh`, `upload.sh`,
  `run-step.sh`, plus `application-mock.conf`, an overlay layered after the cloud conf via
  `--application-conf application-cloud.conf,application-mock.conf` (ArgumentParser merges
  comma-separated confs; later file wins). It repoints `fs.s3a.endpoint`/`ssl`/credentials at
  the moto endpoint.
- `deploy/onprem/run-fileRead-fileWrite.sh` — on-prem runner producing a baseline output copy.

### Why `spark.hadoop.fs.s3a.*` keys appear outside `hadoopConfiguration`

HOCON cannot hold the same key both as a value and as an object: `fs.s3a.fast.upload = true`
conflicts with `fs.s3a.fast.upload.buffer`, and `fs.s3a.endpoint` conflicts with
`fs.s3a.endpoint.region`. The scalar is therefore kept as a `spark.hadoop.*` entry while the
`fs.s3a.*` object keys live under `application.runtime.hadoopConfiguration` (each entry is
rewritten to `spark.hadoop.<key>` by `Launcher.createSparkSession`).

## Secrets

- No credentials are stored in any file. On-prem confs use `${?DB_PASSWORD}` /
  `${?FLIGHT_PASSWORD}` / `${?DECRYPTION_KEY}` — optional substitutions that simply
  disappear when unset; **this job does not need any of them** (no JDBC/Flight/encrypted
  variables are exercised).
- Cloud S3 access uses the EMR instance-profile role
  (`InstanceProfileCredentialsProvider`); mock mode uses `EnvironmentVariableCredentialsProvider`
  with `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`/`AWS_DEFAULT_REGION` exported by the caller
  and propagated to the YARN app via `spark.yarn.appMasterEnv.AWS_*` /
  `spark.executorEnv.AWS_*`.

## Metrics logging

Metrics logging is **disabled** in the pipeline XML (`<metrics-logging enabled="false">`).
To enable it in the cloud, pass both
`--metrics-logging-uri s3a://spark-etl-migration-demo/v1/metrics/events` and
`--metrics-logging-actions "load users,load train,transform users-train"`.

## Running on-prem

```bash
./deploy/onprem/run-fileRead-fileWrite.sh   # writes ${BASELINE_DIR:-$HOME/baseline/local_output}/features
```
It stages `src/test/resources/data` and `scripts` into `/tmp/events`, spark-submits the jar
in local mode (`spark.master` comes from the conf) with Java 17 `--add-opens`, and copies
`/tmp/events/features` to the baseline dir.

Validation test:
```bash
JAVA_TOOL_OPTIONS='<java17 add-opens>' \
  mvn test -Dsuites=com.qwshen.etl.test.migration.FileReadFileWriteValidationTest
```

## Running on real AWS

Upload (playbook step 8):
```bash
aws s3 cp --recursive src/test/resources/data/users  s3://spark-etl-migration-demo/v1/input/users
aws s3 cp --recursive src/test/resources/data/train  s3://spark-etl-migration-demo/v1/input/train
aws s3 cp --recursive src/test/resources/data/events s3://spark-etl-migration-demo/v1/input/events
aws s3 cp --recursive src/test/resources/scripts     s3://spark-etl-migration-demo/v1/scripts/
aws s3 cp target/*-jar-with-dependencies.jar         s3://spark-etl-migration-demo/v1/lib/
aws s3 cp src/test/resources/pipelines/pipeline_fileRead-fileWrite.xml s3://spark-etl-migration-demo/v1/pipelines/
aws s3 cp conf/application-cloud.conf                s3://spark-etl-migration-demo/v1/conf/
```

Option A — new cluster (Spark 3.5 ⇒ EMR 7.x):
```bash
aws emr create-cluster --release-label emr-7.x   # placeholder: pick the EMR 7.x release available in us-east-2
  --applications Name=Spark \
  --instance-type m5.xlarge --instance-count 3 \   # 1 master + 2 core
  --use-default-roles --auto-terminate \
  --log-uri s3://spark-etl-migration-demo/v1/logs/ \
  --steps file://deploy/aws/emr-steps-fileRead-fileWrite.json
```
Option B — existing cluster: `aws emr add-steps --cluster-id <id> --steps file://deploy/aws/emr-steps-fileRead-fileWrite.json`.
Monitor with `aws emr describe-step` / `describe-cluster`; driver logs land under `--log-uri`
or `yarn logs -applicationId <app>` on the master node.

## Running in local mock mode (moto S3 + single-node YARN)

```bash
python3 -m venv ~/opt/moto-venv && ~/opt/moto-venv/bin/pip install 'moto[server]'   # once
export AWS_ACCESS_KEY_ID=mock AWS_SECRET_ACCESS_KEY=mock AWS_DEFAULT_REGION=us-east-2
cd deploy/aws/local-emr-mock
./start.sh     # moto on :5000, bucket, hadoop conf, RM+NM; waits for 1 RUNNING node
./upload.sh    # uploads inputs/scripts/jar/xml/conf to s3://spark-etl-migration-demo
./run-step.sh  # runs deploy/aws/emr-steps-fileRead-fileWrite.json on YARN, downloads
               # output to ${CLOUD_OUTPUT_DIR:-$HOME/cloud_output}
./stop.sh
```
`start.sh` writes `core-site.xml`/`yarn-site.xml` into `~/.local-emr-mock/conf`, installs
`hadoop-aws-3.3.4.jar` + `aws-java-sdk-bundle-1.12.262.jar` into `$SPARK_HOME/jars` and
`$HADOOP_HOME/share/hadoop/common/lib` (idempotent). The harness uses
`${MOTO_SERVER:-~/opt/moto-venv/bin/moto_server}` — point `MOTO_SERVER` elsewhere if needed.

**Limitation:** mock mode validates the cloud config, `--files` distribution, cluster-mode
driver startup, S3A I/O and the step definition, but **not** IAM roles, EMR's classpath/EMRFS,
or real region/endpoint behaviour.

## Comparing outputs

```bash
JAVA_TOOL_OPTIONS='<java17 add-opens> \
  -Dcompare.local.dir=$HOME/baseline/local_output/features \
  -Dcompare.cloud.dir=$HOME/cloud_output/features \
  -Dcompare.report.file=$HOME/migration-report/comparison-report.md \
  -Dcompare.execution.file=$HOME/migration-report/execution.txt' \
  mvn test -Dsuites=com.qwshen.etl.test.migration.FileReadFileWriteComparisonTest
```
System properties must travel via `JAVA_TOOL_OPTIONS` (scalatest-maven-plugin runs in a forked
JVM and does not forward `mvn -D`). Without the three required `compare.*` props the test cancels.

## Data findings (fixtures unchanged)

- `train.csv`: trailer `TRL|0000028` declares 28 records but the file only has 27 data rows —
  the flat-file reader does not cross-check, so the discrepancy is tolerated.
- `train.txt` is fixed-length; the `user:1-9` field keeps leading spaces, so only the single
  9-digit user id (`111840914`) matches `users.csv` ��� 6 rows join, 22 rows carry null
  user_id/gender/birthyear. This is pre-existing framework behaviour preserved by the migration.
