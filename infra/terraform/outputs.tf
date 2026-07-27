output "demo_instance_id" {
  description = "Temporary EC2 instance ID for operator inventory."
  value       = aws_instance.demo_host.id
}

output "demo_instance_public_ip" {
  description = "Temporary EC2 public IP for SSH; value exists only after apply."
  value       = aws_eip.demo.public_ip
}

output "demo_vpc_id" {
  description = "Temporary E2E VPC ID."
  value       = aws_vpc.demo.id
}

output "confluent_kafka_bootstrap_endpoint" {
  description = "Confluent Cloud bootstrap endpoint when resources are enabled."
  value       = var.enable_confluent_resources ? confluent_kafka_cluster.basic[0].bootstrap_endpoint : null
}
