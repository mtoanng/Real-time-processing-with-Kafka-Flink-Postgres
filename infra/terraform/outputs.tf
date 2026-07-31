output "instance_id" {
  description = "EC2 instance identifier for console inspection and teardown."
  value       = aws_instance.demo.id
}

output "public_ip" {
  description = "Stable Elastic IP for SSH and the GitHub deployment host variable."
  value       = aws_eip.demo.public_ip
}

output "ssh_command" {
  description = "Initial administration command. Replace the local key-file path."
  value       = "ssh -i /path/to/taobao-demo.pem ubuntu@${aws_eip.demo.public_ip}"
}

output "bootstrap_log_command" {
  description = "Inspect cloud-init bootstrap progress after the first SSH connection."
  value       = "sudo tail -n 200 /var/log/taobao-bootstrap.log"
}
