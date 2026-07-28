# AWS disposable cloud demo (deprecated legacy draft)

> **NOT ACTIVE / NOT VERIFIED.** This document describes the former
> API/Redis/Product-CDC runtime and does not apply to the SQL-first core release.
> Use `docs/RUNBOOK.md`. A replacement cloud runbook requires separate approval
> after the local core path has real deployment evidence.

This guide deploys the verified code shape to one disposable Ubuntu EC2 host.
Kafka and Schema Registry remain in Confluent Cloud. PostgreSQL, Kafka Connect,
Flink, ClickHouse, Redis, and the API run as seven containers on EC2.

This is a portfolio cloud demo, not a production deployment. Do not expose the
API or Flink UI publicly; use SSH tunnels. Cloud execution remains
`NOT VERIFIED` until the exact verification commands below pass.

## 1. Create Confluent Cloud resources

Create one Confluent Cloud environment and Kafka cluster in an AWS region near
the EC2 region. Create a service account for the demo, then create:

1. one Kafka cluster API key and secret;
2. one separate Schema Registry API key and secret;
3. the following single-partition topics:

```text
user-behavior-events
product-catalog-cdc
product-catalog-connect-config
product-catalog-connect-offsets
product-catalog-connect-status
```

Set `cleanup.policy=compact` for the three `product-catalog-connect-*` internal
topics. The behavior and catalog topics retain ordinary delete-based retention.
For a disposable demo, grant the service account permission to read, write,
describe, and create topics on this cluster. Remove the credentials and cluster
after teardown.

Kafka and Schema Registry credentials are different resource-scoped keys.
Confluent documents the required SASL_SSL client settings and separate Schema
Registry key at:

- <https://docs.confluent.io/cloud/current/cp-component/clients-cloud-config.html>
- <https://docs.confluent.io/cloud/current/get-started/schema-registry.html>

Record these values without committing them:

```text
Kafka bootstrap endpoint
Kafka API key
Kafka API secret
Schema Registry HTTPS endpoint
Schema Registry API key
Schema Registry API secret
```

## 2. Build a deployment archive locally

From this working tree:

```bash
python -m pip install -e ".[api,kafka]"
make checks
make deployment-package
```

If GNU Make is unavailable:

```bash
python -m scripts.package_deployment
```

Output:

```text
artifacts/taobao-aws-demo.tar.gz
```

The archive includes current tracked and untracked source changes. It excludes
`.env`, Git metadata, the raw Taobao dataset, caches, `target`, and other build
outputs.

## 3. Launch the EC2 host

In the AWS EC2 console:

1. Choose Ubuntu Server 24.04 LTS, x86_64.
2. Use `m7i.xlarge` or another instance with at least 4 vCPU and 16 GiB RAM.
3. Allocate at least 50 GiB gp3 storage.
4. Create or select an SSH key pair.
5. Create a security group with one inbound rule:

```text
SSH TCP 22 from your current public IP/32
```

Do not add inbound rules for 5432, 6379, 8000, 8082, 8083, 8123, or 9000.
Docker publishes these ports only on EC2 loopback.

AWS documents security groups as the instance firewall and recommends limiting
SSH to a specific address range:

- <https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/creating-security-group.html>
- <https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-instance-launch-parameters.html>

Wait until the instance passes both status checks.

## 4. Install host prerequisites

Connect:

```bash
chmod 400 taobao-demo.pem
ssh -i taobao-demo.pem ubuntu@EC2_PUBLIC_DNS
```

Install Java, Maven, Python, and basic tools:

```bash
sudo apt-get update
sudo apt-get install -y \
  ca-certificates curl git make maven openjdk-11-jdk \
  python3-pip python3-venv
```

Install Docker from Docker's official Ubuntu repository:

```bash
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg \
  -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc

. /etc/os-release
echo \
  "Types: deb
URIs: https://download.docker.com/linux/ubuntu
Suites: ${UBUNTU_CODENAME:-$VERSION_CODENAME}
Components: stable
Architectures: $(dpkg --print-architecture)
Signed-By: /etc/apt/keyrings/docker.asc" |
  sudo tee /etc/apt/sources.list.d/docker.sources >/dev/null

sudo apt-get update
sudo apt-get install -y \
  containerd.io docker-buildx-plugin docker-ce docker-ce-cli \
  docker-compose-plugin
sudo usermod -aG docker ubuntu
```

Log out and reconnect so the Docker group applies:

```bash
exit
ssh -i taobao-demo.pem ubuntu@EC2_PUBLIC_DNS
docker version
docker compose version
```

The upstream installation procedure is maintained at
<https://docs.docker.com/engine/install/ubuntu/>.

## 5. Transfer and unpack the application

From the local machine:

```bash
scp -i taobao-demo.pem \
  artifacts/taobao-aws-demo.tar.gz \
  ubuntu@EC2_PUBLIC_DNS:/home/ubuntu/
```

On EC2:

```bash
tar -xzf taobao-aws-demo.tar.gz
cd taobao-aws-demo
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e ".[api,kafka]"
cp .env.example .env
chmod 600 .env
```

## 6. Configure `.env`

Edit the file:

```bash
nano .env
```

Set these values:

```dotenv
RUNTIME_PROFILE=cloud-demo

KAFKA_BOOTSTRAP_SERVERS=<Confluent bootstrap host:9092>
KAFKA_SECURITY_PROTOCOL=SASL_SSL
KAFKA_SASL_MECHANISM=PLAIN
KAFKA_SASL_USERNAME=<Kafka API key>
KAFKA_SASL_PASSWORD=<Kafka API secret>
KAFKA_CONNECT_REPLICATION_FACTOR=3

SCHEMA_REGISTRY_URL=https://<Confluent Schema Registry endpoint>
SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO=<SR API key>:<SR API secret>

POSTGRES_PASSWORD=<new random password>
CLICKHOUSE_PASSWORD=<new random password>

KAFKA_SOURCE_BOUNDED=true
EVENT_API_URL=http://localhost:8000/v1/events
```

Generate local database passwords without printing them into shell history:

```bash
openssl rand -base64 32
```

Place each generated value into `.env`, clear the terminal, and do not copy the
result of `docker compose config` into tickets or evidence because resolved
container environment values include secrets.

## 7. Run preflight

```bash
make cloud-preflight
docker compose \
  -f infra/docker-compose.yml \
  --profile cloud-demo \
  config --services
```

Expected services:

```text
api
clickhouse
flink-jobmanager
flink-taskmanager
kafka-connect
postgres
redis
```

`kafka` and `schema-registry` must not appear.

## 8. Start the cloud demo

```bash
make start
```

This command:

1. validates cloud endpoints, credentials, and `.env` permissions;
2. packages the Flink JAR;
3. starts seven containers;
4. waits for Confluent Schema Registry and registers the Avro schema;
5. waits for Kafka Connect and registers the PostgreSQL connector;
6. waits for the API health endpoint.

Inspect:

```bash
docker compose \
  -f infra/docker-compose.yml \
  --profile cloud-demo \
  ps

curl -fsS http://localhost:8000/health
curl -fsS http://localhost:8083/connectors/product-catalog-postgres/status
```

All containers should be running or healthy. The connector task state must be
`RUNNING`.

## 9. Replay and verify exact outputs

```bash
make replay
make verify
```

`make replay` sends both replay attempts through HTTP, performs catalog updates,
waits for the exact catalog state in Confluent Kafka, submits the bounded Flink
job, and waits up to 180 seconds for `FINISHED`.

`make verify` must report:

```text
rows=11
metrics=5
quality=14
catalog=5
```

It also requires the exact Redis cart and API trending fixture, not only row
counts. Save command output as deployment evidence only after removing
hostnames, account identifiers, and credentials.

## 10. Access the API and Flink UI safely

From the local machine, keep this SSH tunnel open:

```bash
ssh -i taobao-demo.pem \
  -L 8000:127.0.0.1:8000 \
  -L 8082:127.0.0.1:8082 \
  ubuntu@EC2_PUBLIC_DNS
```

Then access locally:

```bash
curl http://localhost:8000/v1/users/1/cart
curl \
  "http://localhost:8000/v1/products/trending?minutes=15&as_of_ms=1511658120000"
```

Flink UI:

```text
http://localhost:8082
```

Do not expose these endpoints directly to the Internet. Public deployment
would require a separately approved authentication and TLS boundary.

## 11. Recovery experiment

First capture the successful bounded baseline:

```bash
PYTHONPATH=producer/src \
python scripts/verify.py \
  --snapshot artifacts/uninterrupted.json \
  --snapshot-only
```

Change `KAFKA_SOURCE_BOUNDED=false` in `.env`, then submit without waiting for
the unbounded job to finish:

```bash
WAIT_FOR_JOB_FINISH=false make replay
```

Copy the printed `FLINK_JOB_ID`, wait for a completed checkpoint in the Flink
UI, then run:

```bash
RECOVERY_TEST_CONFIRM=YES \
FLINK_JOB_ID=<job-id> \
BASELINE_SNAPSHOT=artifacts/uninterrupted.json \
make recovery-test
```

Restore `KAFKA_SOURCE_BOUNDED=true` afterward. Recovery is verified only if the
script reports exact equality after restarting the TaskManager.

## 12. Rollback

The code rollback boundary is the deployment archive:

```bash
make stop
mv taobao-aws-demo taobao-aws-demo.failed
tar -xzf <previous-taobao-aws-demo.tar.gz>
cp taobao-aws-demo.failed/.env taobao-aws-demo/.env
cd taobao-aws-demo
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e ".[api,kafka]"
make start
```

Named Docker volumes remain intact. This rollback does not reverse incompatible
database schema changes; no such migration is part of this phase.

## 13. Troubleshooting

### Preflight rejects cloud configuration

Do not bypass it. Verify the external endpoints, SASL_SSL/PLAIN settings,
separate Schema Registry key, non-default database passwords, replication
factor, and `chmod 600 .env`.

### Kafka Connect is not `RUNNING`

```bash
curl -fsS http://localhost:8083/connectors/product-catalog-postgres/status
docker compose \
  -f infra/docker-compose.yml \
  --profile cloud-demo \
  logs kafka-connect
```

Authentication errors normally indicate a Kafka API key, ACL, or SASL setting.
Snapshot errors normally indicate PostgreSQL publication or replication-slot
permissions.

### Schema registration returns 401/403

Confirm that `SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO` contains the separate
Schema Registry key and secret, not the Kafka cluster key.

### Flink job does not finish

```bash
docker compose \
  -f infra/docker-compose.yml \
  --profile cloud-demo \
  logs flink-jobmanager flink-taskmanager
```

Check Confluent topic names, API Kafka delivery errors, ClickHouse health, and
available memory.

### EC2 becomes unresponsive

Stop the instance and resize it to at least 16 GiB RAM. Do not add swap and
claim that the original sizing passed.

## 14. Stop and teardown

Preserve volumes while stopping:

```bash
make stop
```

After evidence has been copied and the host is disposable, remove local
containers and volumes:

```bash
docker compose \
  -f infra/docker-compose.yml \
  --profile cloud-demo \
  down --volumes --remove-orphans
```

Then:

1. terminate the EC2 instance;
2. delete its EBS volume if it was not removed automatically;
3. delete the security group and key pair if dedicated to the demo;
4. delete Confluent API keys;
5. delete the disposable Confluent topics/cluster/environment.

Confirm billing dashboards show no remaining EC2, EBS, or Confluent resources.
