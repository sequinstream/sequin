output "sequin_url" {
  description = "URL to access your Sequin application"
  value       = "http://${data.terraform_remote_state.infra.outputs.alb_dns_name}"
}

output "admin_password" {
  description = "Auto-generated admin password for Sequin"
  value       = random_password.admin_password.result
  sensitive   = true
}