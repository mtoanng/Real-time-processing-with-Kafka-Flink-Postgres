provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project   = var.project_name
      ManagedBy = "Terraform"
      Purpose   = "bounded-streaming-demo"
    }
  }
}

data "aws_ami" "ubuntu" {
  most_recent = true
  owners      = ["099720109477"] # Canonical, the Ubuntu publisher account.

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-amd64-server-*"]
  }

  filter {
    name   = "architecture"
    values = ["x86_64"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

resource "aws_vpc" "demo" {
  cidr_block           = "10.42.0.0/16"
  enable_dns_hostnames = true
  enable_dns_support   = true

  tags = {
    Name = "${var.project_name}-vpc"
  }
}

resource "aws_internet_gateway" "demo" {
  vpc_id = aws_vpc.demo.id

  tags = {
    Name = "${var.project_name}-igw"
  }
}

resource "aws_subnet" "public" {
  vpc_id                  = aws_vpc.demo.id
  cidr_block              = "10.42.1.0/24"
  map_public_ip_on_launch = false

  tags = {
    Name = "${var.project_name}-public"
  }
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.demo.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.demo.id
  }

  tags = {
    Name = "${var.project_name}-public"
  }
}

resource "aws_route_table_association" "public" {
  subnet_id      = aws_subnet.public.id
  route_table_id = aws_route_table.public.id
}

resource "aws_security_group" "instance" {
  name_prefix = "${var.project_name}-ssh-"
  description = "SSH-only administration for the self-hosted streaming demonstration"
  vpc_id      = aws_vpc.demo.id

  ingress {
    description = "SSH from the named administrator workstation only"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [var.admin_ssh_cidr]
  }

  egress {
    description = "Package, GitHub and container image downloads"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "${var.project_name}-ssh"
  }
}

resource "aws_key_pair" "admin" {
  key_name_prefix = "${var.project_name}-admin-"
  public_key      = var.admin_ssh_public_key

  tags = {
    Name = "${var.project_name}-admin"
  }
}

resource "aws_instance" "demo" {
  ami                         = data.aws_ami.ubuntu.id
  instance_type               = var.instance_type
  subnet_id                   = aws_subnet.public.id
  vpc_security_group_ids      = [aws_security_group.instance.id]
  key_name                    = aws_key_pair.admin.key_name
  associate_public_ip_address = false
  user_data                   = templatefile("${path.module}/user_data.sh.tftpl", {})
  user_data_replace_on_change = true

  root_block_device {
    delete_on_termination = true
    encrypted             = true
    volume_type           = "gp3"
    volume_size           = var.root_volume_size_gib
    iops                  = 3000
    throughput            = 125
  }

  metadata_options {
    http_endpoint = "enabled"
    http_tokens   = "required"
  }

  tags = {
    Name = "${var.project_name}-host"
  }
}

resource "aws_eip" "demo" {
  domain = "vpc"

  tags = {
    Name = "${var.project_name}-eip"
  }
}

resource "aws_eip_association" "demo" {
  allocation_id = aws_eip.demo.id
  instance_id   = aws_instance.demo.id
}
