# End-to-end deployment runbook

## 0. Choose the correct host

Run this on a disposable AWS EC2 Linux instance, not the resource-constrained
laptop. The instance runs the complete self-hosted demo: Apache Kafka KRaft,
Confluent Schema Registry, Flink, ClickHouse and Redis. The fixture is bounded;
do not copy the full Taobao dataset to the repository.

This runbook deploys the committed self-hosted Docker Compose profile. It does
not use Confluent Cloud, Amazon MSK or ClickHouse Cloud. See
[operations](OPERATIONS.md) for the security and teardown boundary.

The manual SSH path below is the primary deployment procedure. The
optional GitHub Actions + SSH release path is documented in
[AWS_GITHUB_ACTIONS.md](AWS_GITHUB_ACTIONS.md); enable it only after choosing
a safe network-access model for the GitHub runner.

## 1. Launch and secure the EC2 host

Use these names exactly throughout the AWS Console, GitHub Environment and
shell commands. They are labels only; changing them is safe if every later
reference is changed consistently.

| Object | Required name/value | Where it is used |
| --- | --- | --- |
| EC2 security group | `taobao-streaming-ssh` | Selected during instance launch. |
| EC2 key pair | `taobao-streaming-admin` | Manual workstation-to-EC2 SSH only. |
| Downloaded private-key file | `taobao-streaming-admin.pem` | Keep only on the workstation; never commit it. |
| EC2 `Name` tag | `taobao-streaming-host` | Identifies the single runtime host in the EC2 console. |
| Elastic IP `Name` tag | `taobao-streaming-eip` | Stable address associated to the runtime host. |
| Linux login user | `ubuntu` | Default user of the selected Ubuntu AMI. |
| Host release directory | `~/taobao-streaming` | Used by the GitHub deployment script and release metadata. |
| GitHub Environment | `aws-demo` | Optional protected deployment environment. |
| GitHub deploy key basename | `taobao-streaming-github-deploy` | Separate key used only by GitHub Actions. |

### 1.1 Create the security group

Before launching the instance, open **EC2 → Security Groups → Create security
group** in the VPC you will use. Create `taobao-streaming-ssh` with this
inbound rule:

| Type | Protocol | Port | Source | Reason |
| --- | --- | --- | --- | --- |
| SSH | TCP | 22 | **My IP** (`YOUR.PUBLIC.IP/32`) | Manual administration only |

Keep the default outbound allow-all rule so the host can download operating
system packages, source and container images. Do **not** add inbound rules for
Kafka, Schema Registry, Flink, ClickHouse, Redis, PostgreSQL, Kafka Connect or
the API.

Then open **EC2 → Instances → Launch instances**, select the security group
above. Use the bounded configuration for contract/recovery checks and the
full-replay storage configuration when processing the complete
`UserBehavior.csv`. Both remain single-host demonstrations, not production
high availability.

| EC2 console setting | Value | Reason |
| --- | --- | --- |
| Region | `ap-southeast-2` (Sydney) | Matches this project's selected region. |
| AMI | Ubuntu Server 24.04 LTS, x86_64 | Matches the Docker/bootstrap commands and runtime images. |
| Instance type | `m6i.xlarge` — 4 vCPU, 16 GiB RAM | Core has one Kafka broker/partition, Flink parallelism 1 and two TaskManager slots. |
| Fallback | `m7i.xlarge` — 4 vCPU, 16 GiB RAM | Use only if `m6i.xlarge` is unavailable in the chosen AZ. |
| Root volume (bounded fixture) | 80 GiB, encrypted `gp3`, default 3,000 IOPS / 125 MiB/s | Enough for the fixture and recovery experiment. |
| Root volume (full replay) | 200 GiB, encrypted `gp3`, default 3,000 IOPS / 125 MiB/s | Allows for Docker images, Kafka input/output retention, ClickHouse data, checkpoints, logs and evidence. |
| Network | Default VPC public subnet, or an equivalent public subnet with an Internet Gateway | The host needs outbound Internet for package, image and Git downloads. |
| Public address | One Elastic IP associated directly with the instance | Keeps the SSH/CI hostname stable; do not create a NAT Gateway. |
| Key pair | Create and select `taobao-streaming-admin`, ED25519, `.pem` format | Used only for manual administration. GitHub Actions uses a separate deploy key. |

In the **Name and tags** section of the launch form, set:

```text
Name = taobao-streaming-host
```

Save the downloaded private key as `taobao-streaming-admin.pem` in a protected
workstation directory. On macOS/Linux, restrict it before the first connection:

```bash
chmod 400 taobao-streaming-admin.pem
```

Do not use free-tier-sized instances. A `t3.xlarge` may work for casual
experimentation but is not the supported E2E target because its sustained CPU
and memory headroom are less predictable. Your recorded 8-vCPU Standard
On-Demand quota permits this 4-vCPU host; your 50-TiB gp3 quota is sufficient
for either volume. Verify the **EC2-VPC Elastic IPs** quota has at least
one available address; the “Elastic IP per NAT gateway” quota is unrelated.

The source file size alone does not describe runtime storage: Kafka retains the
Avro input and JSON materialization topics while ClickHouse, Redis, Docker and
Flink checkpoints use the same volume. A full replay is also a state-capacity
experiment because deduplication retains one entry per distinct `event_id` for
`FLINK_DEDUP_RETENTION_HOURS` (168 by default). Use 200 GiB for that run. Keep
the `m6i.xlarge` initially because the active input has one Kafka partition and
Flink parallelism is one; increasing CPU without changing those boundaries is
not a meaningful scale test. Full-source capacity remains **NOT VERIFIED**
until evidence is captured.

After launch, retain at least 20 GiB free for Docker rebuilds, checkpoints and
E2E evidence:

```bash
df -h /
docker system df
```

Do **not** open 9092, 8081, 8082, 8123, 6379, 5432 or 8083 to the Internet.
The Compose file binds host ports to `127.0.0.1`; inspect Flink, ClickHouse and
the API with SSH tunnels instead. AWS security groups are the instance firewall
and AWS recommends limiting SSH to the required source IP. [AWS security group
guide](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/creating-security-group.html)

For GitHub Actions deployment, complete the dedicated deploy-key, pinned
host-key and protected environment setup in
[AWS_GITHUB_ACTIONS.md](AWS_GITHUB_ACTIONS.md).

Important: the `/32` rule makes the manual path secure, but a GitHub-hosted
runner has no single fixed source IP. Therefore do not expect the SSH workflow
to work under this rule until you approve a runner/network design. Do **not**
open port 22 to `0.0.0.0/0` merely to enable CI deployment.

### 1.2 Associate and name the Elastic IP

After the instance state becomes `Running`, open **EC2 → Elastic IPs → Allocate
Elastic IP address**. Select the allocated address, choose **Actions →
Associate Elastic IP address**, then select `taobao-streaming-host`. Add this
tag to the Elastic IP:

```text
Name = taobao-streaming-eip
```

Use the resulting public IPv4 address as `EC2_PUBLIC_IP` in the commands below.
It is the value later used for the optional GitHub `EC2_HOST` variable.

### 1.3 Connect from the workstation

```bash
ssh -i taobao-streaming-admin.pem ubuntu@EC2_PUBLIC_IP
```

## 2. Bootstrap Docker and build prerequisites

On the EC2 host, install Git, Make, Python, Maven and a JDK capable of compiling the
project's Java 11 target when using the manual checks path. Ubuntu 24.04's JDK
17 is suitable for that build target. The automated release builds the
multi-stage Docker image and does not require Maven on the host. Install Docker
Engine and the Compose plugin from Docker's official APT repository:

```bash
sudo apt-get update
sudo apt-get install -y ca-certificates curl git make maven openjdk-17-jdk openssl \
  python-is-python3 python3 python3-venv
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg \
  -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc
sudo tee /etc/apt/sources.list.d/docker.sources >/dev/null <<EOF
Types: deb
URIs: https://download.docker.com/linux/ubuntu
Suites: $(. /etc/os-release && echo "${UBUNTU_CODENAME:-$VERSION_CODENAME}")
Components: stable
Architectures: $(dpkg --print-architecture)
Signed-By: /etc/apt/keyrings/docker.asc
EOF
sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
sudo usermod -aG docker "$USER"
```

Disconnect and reconnect so the Docker group applies, then verify:

```bash
docker version
docker compose version
java -version
mvn -version
```

These commands follow Docker's current Ubuntu/Compose installation guidance.
[Docker Engine on Ubuntu](https://docs.docker.com/engine/install/ubuntu/)

## 3. Optional SSH tunnels for local-only service ports

Keep the EC2 security group closed and create tunnels from your workstation
when you need a UI or API:

```bash
ssh -i taobao-streaming-admin.pem \
  -L 8082:127.0.0.1:8082 \
  -L 8123:127.0.0.1:8123 \
  -L 8000:127.0.0.1:8000 \
  ubuntu@EC2_PUBLIC_IP
```

Open `http://localhost:8082` only through this tunnel after the stack starts.

## 4. Obtain and inspect the release candidate

```bash
git clone https://github.com/mtoanng/Kafka-Flink-ClickHouse-Pipeline.git
cd Kafka-Flink-ClickHouse-Pipeline
git checkout main
git status --short
cp .env.example .env
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e ".[api,kafka]" ruff
```

`git status --short` must be empty before deployment. Edit only `.env`; do not
commit it. Keep the local defaults for this Compose profile. `KAFKA_SOURCE_BOUNDED`
must remain `false` for the normal start-then-replay sequence. Keep the virtual
environment active for every later `make`, `python` or replay/verifier command.
In a new SSH session, return to the repository and run
`source .venv/bin/activate` again.

The repository `.env.example` is the self-hosted configuration. If an older
`.env` exists, overwrite it with this file before editing; do not carry forward
Cloud-host variables such as `CLICKHOUSE_HOST`, port `8443`, or database
`f1_telemetry`:

```bash
cp .env.example .env
openssl rand -hex 24
nano .env
```

Replace only `CLICKHOUSE_PASSWORD=local-clickhouse` with the generated value.
Keep `CLICKHOUSE_ENDPOINT=http://localhost:8123` and
`CLICKHOUSE_DATABASE=taobao_behavior`. The core profile does not need
PostgreSQL credentials; leave the catalog variables unused until you
explicitly start the optional catalog profile.

## 5. Run credential-independent gates

```bash
make checks
```

Expected: Python contracts, lint/format, compilation, connector packaging and
Compose rendering all succeed. These checks deliberately do not start Docker
services.

## 6. Build and start the core

```bash
STARTUP_TIMEOUT_SECONDS=180 bash scripts/start.sh
```

The script packages the connector bundle, starts Kafka, Schema Registry,
ClickHouse, Redis, the Redis materializer, Flink JobManager/TaskManager,
creates topics, registers Avro and submits one detached SQL/PyFlink job.

Verify the control plane before sending data:

```bash
docker compose -f infra/docker-compose.yml --profile core ps
curl -fsS http://localhost:8082/jobs/overview
curl -fsS http://localhost:8082/taskmanagers
```

Expected: one `RUNNING` job and at least one TaskManager. If not, stop here and
inspect logs; do not replay input into a failed job.

## 7. Run the bounded core E2E fixture

```bash
REPLAY_RUN_IDS=golden-a,golden-b bash scripts/replay.sh
PYTHONPATH=producer/src python scripts/verify.py
```

The verifier independently compares canonical raw events, one-minute metrics,
quality evidence and Redis active-cart state with committed fixture outputs.
It fails with a labelled expected/actual diff.

If the run is successful, capture an uninterrupted snapshot for recovery:

```bash
PYTHONPATH=producer/src python scripts/verify.py \
  --snapshot artifacts/uninterrupted.json --snapshot-only
```

The current late-event fixture expectation is a live verification item because
the active pipeline uses periodic built-in watermarks. Record the actual
result; do not alter golden data merely to make a service run pass.

## 8. Verify a completed checkpoint

Wait at least `FLINK_CHECKPOINT_INTERVAL_MS` after replay, then:

```bash
job_id="$(curl -fsS http://localhost:8082/jobs/overview | \
  python -c 'import json,sys; print(json.load(sys.stdin)["jobs"][0]["jid"])')"
curl -fsS "http://localhost:8082/jobs/$job_id/checkpoints"
```

The response must show a completed checkpoint before attempting recovery.

## 9. Optional catalog CDC extension

Start it only after the core verifier passes:

### Bounded fixture catalog

The PostgreSQL bootstrap contains five products (100-104) matching the
committed fixture:

```bash
docker compose -f infra/docker-compose.yml --profile core --profile catalog \
  up -d postgres kafka-connect
POSTGRES_PASSWORD=local-catalog python scripts/register_connector.py
bash scripts/update_catalog.sh
PYTHONPATH=producer/src python scripts/verify.py --with-catalog
```

### Full Taobao dataset catalog

`UserBehavior.csv` contains identifiers and behavior only; it does not contain
product names or prices. The generator therefore preserves every valid
item and creates clearly labelled deterministic synthetic names/prices. It
streams category observations into a temporary SQLite index, so Python memory
does not grow with the number of products. When an item appears under multiple
source categories, the most frequently observed category wins; ties use the
lowest category ID. The manifest records both this policy and the number of
ambiguous products.

Keep the downloaded dataset outside Git at `data/UserBehavior.csv`, then run:

```bash
PYTHONPATH=producer/src python -m taobao_catalog data/UserBehavior.csv
cat artifacts/product_catalog_manifest.json
docker compose -f infra/docker-compose.yml --profile catalog up -d postgres
bash scripts/load_product_catalog.sh \
  artifacts/product_catalog.csv artifacts/product_catalog_manifest.json
docker compose -f infra/docker-compose.yml --profile core --profile catalog \
  up -d kafka-connect
POSTGRES_PASSWORD=local-catalog python scripts/register_connector.py
```

The loader replaces the five-row seed in one PostgreSQL transaction and fails
unless PostgreSQL's row count equals `manifest.unique_products`. It must run
before the first Debezium connector registration so the initial snapshot is
complete. Reconcile all three stages:

```bash
python - <<'PY'
import json
from pathlib import Path
manifest = json.loads(Path("artifacts/product_catalog_manifest.json").read_text())
assert manifest["source_rows"] > 0
assert manifest["unique_products"] > 0
print(manifest)
PY
docker compose -f infra/docker-compose.yml --profile catalog exec -T postgres \
  psql -U catalog -d catalog -tAc 'SELECT count(*) FROM product_catalog'
curl -fsS 'http://localhost:8123/?database=taobao_behavior' \
  --data-binary 'SELECT count() FROM product_catalog_current_canonical'
```

The manifest count, PostgreSQL count and ClickHouse canonical count must match.
This repository does not commit the large source file, so full-dataset coverage
is `NOT VERIFIED` until these commands are executed on the deployment host and
their output is retained as evidence.

This path replicates current catalog state directly from the flattened
Debezium topic through ClickHouse Kafka Engine. It bypasses Flink because it
does not enrich behavior history or change the metric source-category grain.

## 10. Optional HTTP/API extension

```bash
docker compose -f infra/docker-compose.yml --profile core --profile api up -d api
curl -fsS http://localhost:8000/health
curl -fsS 'http://localhost:8000/v1/users/1/cart'
curl -fsS 'http://localhost:8000/v1/products/trending?minutes=15&as_of_ms=1511658120000'
```

To publish through HTTP, omit `event_id`; the API recomputes it from stable
source fields:

```bash
curl -fsS -X POST http://localhost:8000/v1/events \
  -H 'content-type: application/json' \
  -d '{"user_id":9,"item_id":900,"category_id":90,"behavior_type":"cart","event_time_ms":1511658120000,"source_sequence":900,"replay_run_id":"http-demo"}'
```

## 11. Recovery experiment

Run this only on the disposable host after saving the baseline snapshot and
confirming a completed checkpoint:

```bash
RECOVERY_TEST_CONFIRM=YES \
FLINK_JOB_ID="$job_id" \
BASELINE_SNAPSHOT=artifacts/uninterrupted.json \
bash scripts/recovery_test.sh
```

Expected: the recovered canonical snapshot equals the uninterrupted snapshot.
This is not yet verified by repository evidence.

## 12. Collect evidence and teardown

Save command output, Flink checkpoint JSON and canonical snapshots under
`docs/evidence/final-e2e/` before teardown. That directory is intentionally
empty until a real run occurs.

```bash
mkdir -p docs/evidence/final-e2e
cp artifacts/uninterrupted.json docs/evidence/final-e2e/
curl -fsS "http://localhost:8082/jobs/$job_id/checkpoints" \
  > docs/evidence/final-e2e/checkpoints.json
make stop
```

`make stop` retains volumes. Follow [operations](OPERATIONS.md) only if you
intend to delete local runtime data.
