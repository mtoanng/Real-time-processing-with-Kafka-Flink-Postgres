# End-to-end deployment runbook

## 0. Choose the correct host

Run this on a disposable AWS EC2 Linux instance, not the resource-constrained
laptop. The instance runs the complete self-hosted demo: Apache Kafka KRaft,
Apicurio Schema Registry, Flink, ClickHouse and Redis. The fixture is bounded;
do not copy the full Taobao dataset to the repository.

This runbook deploys the committed self-hosted Docker Compose profile. It does
not use Confluent Cloud, Amazon MSK or ClickHouse Cloud. See
[operations](OPERATIONS.md) for the security and teardown boundary.

The preferred repeatable release path is the GitHub Actions + SSH workflow
documented in [AWS_GITHUB_ACTIONS.md](AWS_GITHUB_ACTIONS.md). The manual host
steps below remain the bootstrap, troubleshooting and verification path.

## 1. Provision and secure the EC2 host

The preferred provisioning route is the committed Terraform configuration in
[`infra/terraform`](../infra/terraform), not a hand-created console instance.
Follow [Terraform on AWS](TERRAFORM_AWS.md) through its bootstrap section, then
return here at step 3. Terraform creates only the single host and its network
prerequisites; it does not start the streaming stack or run a replay.

Use this exact **bounded-fixture E2E configuration** in the AWS EC2 console.
It is sized for the committed fixture and recovery experiment, not for a
throughput benchmark, production high availability, or an unmeasured full
`UserBehavior.csv` replay.

| Console setting | Required value | Why |
| --- | --- | --- |
| Region | `ap-southeast-2` (Sydney) | Keeps the deployment in the region used for this project. |
| AMI | Ubuntu Server 24.04 LTS, x86_64 | Supported by the Docker installation commands below and by the x86_64 images. |
| Instance type | `m6i.xlarge` - 4 vCPU, 16 GiB RAM | The core Compose profile has one Kafka broker/partition, Flink parallelism 1 and two TaskManager slots. This is sufficient for the committed fixture E2E run. |
| Fallback instance | `m7i.xlarge` - 4 vCPU, 16 GiB RAM | Use only if the primary type is unavailable in the selected AZ. |
| Root EBS volume | `80 GiB`, `gp3`, default `3,000 IOPS` and `125 MiB/s` | Enough for Ubuntu, current Docker images/build cache, bounded Kafka/ClickHouse volumes and Flink checkpoints. |
| Public IPv4 / Elastic IP | One stable public address | Required for workstation SSH and the GitHub Environment host variable. Associate an EC2 Elastic IP directly; do not create a NAT Gateway. |
| Key pair | One new ED25519 key pair | Manual bootstrap/admin access only. The GitHub deployment uses a separate deploy key. |

Do not use a free-tier-sized instance. A `t3.xlarge` may work for ad-hoc
experimentation but is not the prescribed E2E target because sustained CPU and
memory headroom are less predictable. Your recorded `Standard On-Demand` quota
of 8 vCPUs permits this 4-vCPU host (and leaves 4 vCPUs available). Your 50-TiB
gp3 quota is sufficient for this 80-GiB volume. gp3's included
baseline is 3,000 IOPS and 125 MiB/s, which is sufficient for this bounded
demo. [AWS gp3 performance](https://docs.aws.amazon.com/ebs/latest/userguide/general-purpose.html)

After launch, verify free disk before cloning the repository. Keep at least
20 GiB free for image rebuilds and checkpoint/replay evidence:

```bash
df -h /
docker system df
```

### Full 2-GB source replay is a separate capacity experiment

Do not choose a larger host merely because the source CSV is about 2 GB; raw
file size is not the controlling memory value. The current job retains a
deduplication state entry for each distinct `event_id` for
`FLINK_DEDUP_RETENTION_HOURS` (default: 168 hours). A fast full replay can
therefore retain far more state than the CSV size suggests. The full-source
catalog section later in this runbook generates catalog metadata only; it does
not certify a full-source core replay.

Run the bounded E2E fixture on `m6i.xlarge` first. If you later approve a
full-source replay, capture Flink state size, container memory and disk use,
then resize to `m6i.2xlarge` / 32 GiB only if those measurements require it.
Until that experiment is recorded, full-source capacity is **NOT VERIFIED**.

Create a security group with this baseline. The application ports stay private
because Compose binds them to `127.0.0.1`.

| Port | Source | Reason |
| --- | --- | --- |
| TCP 22 | Your current public IP as `/32` | SSH administration only |

Do **not** open 9092, 8081, 8082, 8123, 6379, 5432 or 8083 to the Internet.
The Compose file binds host ports to `127.0.0.1`; inspect Flink, ClickHouse and
the API with SSH tunnels instead. AWS security groups are the instance firewall
and AWS recommends limiting SSH to the required source IP. [AWS security group
guide](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/creating-security-group.html)

For GitHub Actions deployment, complete the dedicated deploy-key, pinned
host-key and protected environment setup in
[AWS_GITHUB_ACTIONS.md](AWS_GITHUB_ACTIONS.md).

Important: a GitHub-hosted runner does not have one fixed source IP. Therefore
the current SSH-based Actions workflow cannot reach an instance whose port 22
is restricted only to your `/32`. Keep the `/32` rule for manual deployment.
Before enabling GitHub-hosted deployment, choose and document one access model:

1. temporarily allow the GitHub runner ranges on TCP 22 for a short-lived demo;
   or
2. replace the hosted-runner SSH hop with a separately approved private runner
   or AWS Systems Manager design.

Do not open TCP 22 to `0.0.0.0/0` merely to make the workflow pass. AWS
recommends restricting remote access to specific trusted addresses or ranges.
[AWS security-group guidance](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/creating-security-group.html)

Connect from your workstation:

```bash
chmod 400 taobao-demo.pem
ssh -i taobao-demo.pem ubuntu@EC2_PUBLIC_DNS
```

## 2. Bootstrap Docker and build prerequisites

On the EC2 host, install Git, Python, Maven and a JDK capable of compiling the
project's Java 11 target when using the manual checks path. Ubuntu 24.04's JDK
17 is suitable for that build target. The automated release builds the
multi-stage Docker image and does not require Maven on the host. Install Docker
Engine and the Compose plugin from Docker's official APT repository:

```bash
sudo apt-get update
sudo apt-get install -y ca-certificates curl git maven openjdk-17-jdk python3 awscli
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
aws --version
```

These commands follow Docker's current Ubuntu/Compose installation guidance.
[Docker Engine on Ubuntu](https://docs.docker.com/engine/install/ubuntu/)

## 3. Optional SSH tunnels for local-only service ports

Keep the EC2 security group closed and create tunnels from your workstation
when you need a UI or API:

```bash
ssh -i taobao-demo.pem \
  -L 8082:127.0.0.1:8082 \
  -L 8123:127.0.0.1:8123 \
  -L 8000:127.0.0.1:8000 \
  ubuntu@EC2_PUBLIC_DNS
```

Open `http://localhost:8082` only through this tunnel after the stack starts.

## 4. Obtain and inspect the release candidate

```bash
git clone https://github.com/mtoanng/Kafka-Flink-ClickHouse-Pipeline.git
cd Kafka-Flink-ClickHouse-Pipeline
git checkout refactor/python-sql-preserve-architecture
git status --short
```

If the host was created with Terraform, link its separately provisioned runtime
configuration instead of creating a second `.env` file:

```bash
ln -sfn "$HOME/taobao-streaming/shared/.env" .env
```

For the manual-console route only, use `cp .env.example .env` and edit that
local file. In both cases, `git status --short` must be empty before
deployment. Do not commit `.env`. Keep the local defaults for this Compose
profile. `KAFKA_SOURCE_BOUNDED` must remain `false` for the normal
start-then-replay sequence.

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
`(item_id, category_id)` and creates clearly labelled deterministic synthetic
names/prices. It streams the source into a temporary SQLite index, so Python
memory does not grow with the number of products. Conflicting categories for
one item fail closed.

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

This branch replicates current catalog state. It does not enrich behavior
history or change the metric source-category grain.

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
