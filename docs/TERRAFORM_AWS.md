# Terraform provisioning for AWS EC2

Terraform provisions the disposable **host**, not the application. The
application remains the existing self-hosted Docker Compose deployment:

```text
Terraform
-> VPC + public subnet + Internet Gateway + SSH-only security group
-> Elastic IP + Ubuntu EC2 + Docker bootstrap
-> existing Compose / Flink / Kafka / ClickHouse / Redis deployment
```

It deliberately creates no NAT Gateway, load balancer, MSK, RDS, ECR, ECS,
Kubernetes, managed Redis, or managed ClickHouse service.

## What Terraform creates

| Resource | Configuration |
| --- | --- |
| VPC and public subnet | Dedicated `10.42.0.0/16` VPC and `10.42.1.0/24` subnet. |
| Internet Gateway and route | Public Internet access for package, GitHub and image downloads. |
| Security group | TCP 22 only from `admin_ssh_cidr`; all application ports remain private. |
| EC2 | Ubuntu 24.04 x86_64, `m6i.xlarge` by default: 4 vCPU and 16 GiB RAM. |
| EBS | One encrypted 80-GiB gp3 root disk, 3,000 IOPS and 125 MiB/s. |
| Elastic IP | One stable public address associated directly with EC2. |
| Bootstrap | Docker Engine, Compose plugin, Git, Python and AWS CLI; no application secrets. |

The size is for the committed bounded fixture, core profile and recovery
experiment. Full `UserBehavior.csv` capacity remains **NOT VERIFIED** because
deduplication state is per distinct `event_id`, not proportional only to the
CSV file size.

## Prerequisites

1. Install Terraform 1.6 or newer and AWS CLI on your workstation.
2. Authenticate the AWS CLI as an IAM principal permitted to manage EC2, VPC,
   EBS, security groups, Elastic IPs and key pairs in `ap-southeast-2`.
3. Confirm these Service Quotas have at least one available unit:

   - EC2 Standard On-Demand vCPUs: 4;
   - EBS gp3 storage: 80 GiB;
   - VPCs, Internet Gateways and Elastic IPs: 1 each.

4. Create a dedicated local ED25519 administration key; do not reuse the
   separate GitHub Actions deploy key.

```powershell
ssh-keygen -t ed25519 -f $HOME\.ssh\taobao-demo -C taobao-demo-admin
curl.exe https://checkip.amazonaws.com
```

Use the returned public address with `/32` as `admin_ssh_cidr`.

## Configure Terraform

```powershell
Set-Location infra\terraform
Copy-Item terraform.tfvars.example terraform.tfvars
notepad terraform.tfvars
```

Set the actual workstation address and public key:

```hcl
aws_region           = "ap-southeast-2"
project_name         = "taobao-streaming-demo"
instance_type        = "m6i.xlarge"
root_volume_size_gib = 80
admin_ssh_cidr       = "YOUR.PUBLIC.IP/32"
admin_ssh_public_key = "ssh-ed25519 AAAA..."
```

`terraform.tfvars` and state files are ignored by Git. Do not place a private
key, AWS secret, `.env` content or GitHub secret in Terraform variables.

## Review before creating anything

```powershell
terraform init
terraform fmt -check -recursive
terraform validate
terraform plan -out tfplan
```

Inspect the plan. It should contain only the VPC, subnet, route/IGW, security
group, key pair, EC2 instance, Elastic IP and EIP association listed above.

## Create the host

This command creates billable AWS resources. Run it yourself only after the
plan is correct:

```powershell
terraform apply tfplan
terraform output
```

Connect using the emitted `ssh_command`, then wait for bootstrap:

```bash
cloud-init status --wait
sudo tail -n 200 /var/log/taobao-bootstrap.log
docker version
docker compose version
```

The Terraform bootstrap creates `~/taobao-streaming/shared/` but intentionally
does not create `.env`, because that file contains local runtime passwords.
Provision it once on the host:

```bash
curl -fsSL \
  https://raw.githubusercontent.com/mtoanng/Kafka-Flink-ClickHouse-Pipeline/main/.env.example \
  -o "$HOME/taobao-streaming/shared/.env"
chmod 600 "$HOME/taobao-streaming/shared/.env"
nano "$HOME/taobao-streaming/shared/.env"
```

Replace the demonstration passwords. Keep these internal Compose values:

```text
KAFKA_BOOTSTRAP_SERVERS=kafka:29092
SCHEMA_REGISTRY_URL=http://schema-registry:8080/apis/ccompat/v7
```

Next, return to [the E2E runbook](RUNBOOK.md) at step 3 for SSH tunnels,
application startup, replay, verification and recovery evidence.

## GitHub Actions deployment boundary

Terraform outputs a stable `public_ip`; use it as the `EC2_HOST` GitHub
Environment variable after verifying the SSH host key. The existing Actions
workflow deploys application code only; it never runs Terraform.

The Terraform security group deliberately permits SSH only from
`admin_ssh_cidr`. A GitHub-hosted runner therefore cannot deploy through this
rule because its source address is not fixed. Keep manual SSH deployment as the
secure default. Before enabling GitHub-hosted SSH deployment, approve a
separate runner/network access design; do not widen port 22 to the Internet.
See [GitHub Actions deployment](AWS_GITHUB_ACTIONS.md).

## Teardown

Copy E2E evidence first. Then stop the Compose stack and destroy exactly the
Terraform-managed resources:

```bash
cd "$HOME/taobao-streaming/current"
make stop
```

```powershell
Set-Location infra\terraform
terraform destroy
```

`terraform destroy` deletes the EC2 host, its root volume and its Elastic IP
association. Because the root volume has `delete_on_termination = true`, copy
evidence before destroying it.
