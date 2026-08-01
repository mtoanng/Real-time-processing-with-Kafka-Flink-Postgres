# GitHub Actions deployment to AWS EC2

This repository uses a small CI/CD shape for a bounded single-host deployment:

```text
pull request
-> tests, lint, Maven connector package, Compose render

push main or manual dispatch
-> the same checks
-> protected GitHub Environment
-> SSH
-> exact Git SHA on EC2
-> build SHA-tagged Docker images
-> Docker Compose start and health checks
```

There is no image registry, AWS access key, Terraform apply, Systems Manager or
deployment framework in this path.

## 1. Prepare the EC2 deploy user

Use the normal non-root Ubuntu user and add it to the Docker group:

```bash
sudo usermod -aG docker ubuntu
```

Reconnect, then confirm the user can run:

```bash
docker version
docker compose version
git --version
curl --version
```

Create a dedicated deployment key on an operator machine. Do not reuse a
personal key:

```bash
ssh-keygen -t ed25519 -C github-actions-taobao-deploy \
  -f taobao-streaming-github-deploy
```

Append `taobao-streaming-github-deploy.pub` to the EC2 user's
`~/.ssh/authorized_keys`. The private file becomes the GitHub Environment
secret `EC2_SSH_PRIVATE_KEY`.

## 2. Pin the server host key

Obtain the SSH host key:

```bash
ssh-keyscan -H EC2_PUBLIC_IP
```

Verify its fingerprint through an independent trusted connection:

```bash
ssh-keygen -lf /etc/ssh/ssh_host_ed25519_key.pub
```

Store the verified `ssh-keyscan` output as the GitHub Environment secret
`EC2_SSH_KNOWN_HOSTS`. The workflow uses strict host-key checking and does not
learn a new key during deployment.

## 3. Provision runtime configuration once

On EC2:

```bash
install -d -m 700 "$HOME/taobao-streaming/shared"
curl -fsSL \
  https://raw.githubusercontent.com/mtoanng/Kafka-Flink-ClickHouse-Pipeline/main/.env.example \
  -o "$HOME/taobao-streaming/shared/.env"
chmod 600 "$HOME/taobao-streaming/shared/.env"
nano "$HOME/taobao-streaming/shared/.env"
```

Replace demonstration passwords. Keep these internal Docker-network values:

```text
KAFKA_BOOTSTRAP_SERVERS=kafka:29092
SCHEMA_REGISTRY_URL=http://schema-registry:8081
```

Do not put `.env` in GitHub. It remains outside every source release.

## 4. Configure the protected GitHub environment

Create the GitHub Environment `aws-demo`. If available for the repository,
restrict deployment branches to `main` and require a reviewer.

Add variables:

| Variable | Example |
| --- | --- |
| `EC2_HOST` | The `taobao-streaming-eip` public IPv4 address, for example `203.0.113.10` |
| `EC2_USER` | `ubuntu` |

Add secrets:

| Secret | Content |
| --- | --- |
| `EC2_SSH_PRIVATE_KEY` | Complete dedicated OpenSSH private key |
| `EC2_SSH_KNOWN_HOSTS` | Verified hashed host-key line |

No AWS access key is required.

## 5. Network rule

The GitHub-hosted runner must be able to reach TCP 22 on EC2. GitHub-hosted
runner source ranges are not a single stable address, so this simplicity has a
real security trade-off.

For a short-lived demonstration, keep key authentication only, disable SSH
password login, stop the instance when unused, and restrict the security group
as tightly as the chosen runner setup permits. If a permanent tightly
restricted inbound rule is required, use a self-hosted runner with known egress
or migrate later to OIDC plus Systems Manager; neither is added to this
repository now.

Never expose Kafka, Flink, ClickHouse, Redis, PostgreSQL or Kafka Connect ports
to the Internet.

## 6. Deploy

Normal path:

```text
merge to main
-> Actions
-> CI and deploy
-> checks
-> aws-demo approval
-> deploy
```

A manual dispatch of `CI and deploy` is also supported for a selected Git ref.

The remote script:

1. validates the full 40-character commit SHA;
2. checks out that SHA under
   `~/taobao-streaming/releases/<git-sha>`;
3. links `~/taobao-streaming/shared/.env`;
4. builds `taobao-flink-runtime:<git-sha>` and
   `taobao-app-runtime:<git-sha>`;
5. starts the core and waits for Flink, Schema Registry and a running job;
6. advances `~/taobao-streaming/current` only after success.

Inspect the deployed release:

```bash
cat "$HOME/taobao-streaming/shared/current-release"
readlink -f "$HOME/taobao-streaming/current"
```

Then run replay, verification and recovery from
`~/taobao-streaming/current` using [RUNBOOK.md](RUNBOOK.md).

## 7. Rollback

Manually dispatch the workflow from a known-good commit or tag. The same checks
run, and the host rebuilds and starts the exact older release. Named Docker
volumes remain stable because Compose declares a fixed project name.

This is suitable for a bounded, single-EC2 deployment. It is not
the preferred credential or network model for a permanent production fleet.
