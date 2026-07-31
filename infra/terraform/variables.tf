variable "aws_region" {
  description = "AWS region for the disposable single-host demonstration."
  type        = string
  default     = "ap-southeast-2"
}

variable "project_name" {
  description = "Prefix for all Terraform-managed resource names and tags."
  type        = string
  default     = "taobao-streaming-demo"

  validation {
    condition     = can(regex("^[a-z0-9-]+$", var.project_name))
    error_message = "project_name may contain only lowercase letters, numbers and hyphens."
  }
}

variable "instance_type" {
  description = "EC2 size for the bounded-fixture core profile."
  type        = string
  default     = "m6i.xlarge"
}

variable "root_volume_size_gib" {
  description = "Encrypted gp3 root-volume size. 80 GiB is the bounded-fixture target."
  type        = number
  default     = 80

  validation {
    condition     = var.root_volume_size_gib >= 40
    error_message = "Use at least 40 GiB so Docker images, state and evidence have room."
  }
}

variable "admin_ssh_cidr" {
  description = "Trusted workstation IPv4 CIDR allowed to administer the instance over SSH, normally one public address with /32."
  type        = string

  validation {
    condition     = can(cidrnetmask(var.admin_ssh_cidr))
    error_message = "admin_ssh_cidr must be a valid IPv4 CIDR block."
  }
}

variable "admin_ssh_public_key" {
  description = "Public ED25519 key used for initial Ubuntu administration. Never put the private key in tfvars."
  type        = string
  sensitive   = true

  validation {
    condition     = startswith(var.admin_ssh_public_key, "ssh-ed25519 ")
    error_message = "Use an ED25519 OpenSSH public key beginning with 'ssh-ed25519 '."
  }
}
