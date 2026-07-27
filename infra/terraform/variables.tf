variable "aws_region" {
  description = "AWS region for the temporary EC2 demo host."
  type        = string
  default     = "ap-southeast-1"
}

variable "plan_only" {
  description = "Skip AWS credential/account discovery for an offline refresh-free plan."
  type        = bool
  default     = false
}

variable "vpc_cidr" {
  description = "CIDR for the temporary E2E VPC."
  type        = string
  default     = "10.42.0.0/16"
}

variable "public_subnet_cidr" {
  description = "CIDR for the disposable EC2 public subnet."
  type        = string
  default     = "10.42.1.0/24"
}

variable "availability_zone" {
  description = "Single availability zone for the bounded demo host."
  type        = string
  default     = "ap-southeast-1a"
}

variable "ami_id" {
  description = "Amazon Linux 2023 AMI ID selected for aws_region."
  type        = string
  nullable    = false
}

variable "instance_type" {
  description = "Small disposable EC2 instance type."
  type        = string
  default     = "t3.small"
}

variable "key_name" {
  description = "Existing EC2 key pair name; no private key is stored here."
  type        = string
  nullable    = false
}

variable "ssh_cidr" {
  description = "Single approved operator CIDR allowed to reach SSH."
  type        = string
  nullable    = false
}

variable "root_volume_size_gb" {
  description = "Encrypted root volume size for bounded Docker/Flink artifacts."
  type        = number
  default     = 40

  validation {
    condition     = var.root_volume_size_gb >= 20
    error_message = "root_volume_size_gb must be at least 20 GB."
  }
}

variable "flink_version" {
  description = "Apache Flink distribution version installed on the temporary host."
  type        = string
  default     = "1.20.2"
}

variable "flink_download_url" {
  description = "HTTPS URL for the pinned Flink binary distribution."
  type        = string
  default     = "https://archive.apache.org/dist/flink/flink-1.20.2/flink-1.20.2-bin-scala_2.12.tgz"
}

variable "flink_sha256" {
  description = "SHA-256 for flink_download_url; empty disables checksum enforcement."
  type        = string
  default     = ""
}

variable "application_jar_url" {
  description = "HTTPS URL for the versioned shaded Taobao Flink application JAR."
  type        = string
  default     = ""
}

variable "application_jar_sha256" {
  description = "SHA-256 for application_jar_url; empty disables checksum enforcement."
  type        = string
  default     = ""
}

variable "runtime_bundle_url" {
  description = "HTTPS URL for a repository runtime bundle containing Compose and scripts."
  type        = string
  default     = ""
}

variable "runtime_profile" {
  description = "Initial runtime profile loaded by the host bootstrap."
  type        = string
  default     = "core"

  validation {
    condition = contains(
      ["core", "serving", "cdc", "observability"],
      var.runtime_profile
    )
    error_message = "runtime_profile must be core, serving, cdc, or observability."
  }
}

variable "auto_start_runtime" {
  description = "Start Compose and submit Flink after bootstrap when artifacts/configuration exist."
  type        = bool
  default     = false
}

variable "enable_confluent_resources" {
  description = "Enable Confluent Cloud resources in a credentialed operator plan."
  type        = bool
  default     = false
}

variable "confluent_environment_name" {
  description = "Confluent Cloud environment display name."
  type        = string
  default     = "taobao-cloud-e2e"
}

variable "confluent_cluster_name" {
  description = "Confluent Cloud Basic Kafka cluster display name."
  type        = string
  default     = "taobao-basic"
}

variable "confluent_cloud_region" {
  description = "Confluent Cloud AWS region."
  type        = string
  default     = "ap-southeast-1"
}

variable "confluent_cloud_api_key" {
  description = "Injected Confluent Cloud API key; never commit."
  type        = string
  sensitive   = true
  default     = null
}

variable "confluent_cloud_api_secret" {
  description = "Injected Confluent Cloud API secret; never commit."
  type        = string
  sensitive   = true
  default     = null
}

variable "confluent_kafka_api_key" {
  description = "Injected Kafka API key for topic/ACL resources."
  type        = string
  sensitive   = true
  default     = null
}

variable "confluent_kafka_api_secret" {
  description = "Injected Kafka API secret for topic/ACL resources."
  type        = string
  sensitive   = true
  default     = null
}

variable "confluent_schema_registry_cluster_id" {
  description = "Existing Schema Registry cluster ID from Confluent Cloud."
  type        = string
  default     = null
}

variable "confluent_schema_registry_rest_endpoint" {
  description = "Existing Schema Registry REST endpoint from Confluent Cloud."
  type        = string
  default     = null
}

variable "confluent_schema_registry_api_key" {
  description = "Injected Schema Registry API key."
  type        = string
  sensitive   = true
  default     = null
}

variable "confluent_schema_registry_api_secret" {
  description = "Injected Schema Registry API secret."
  type        = string
  sensitive   = true
  default     = null
}
