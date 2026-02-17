variable "environment" { type = string }
variable "location" { type = string }
variable "resource_group_name" { type = string }
variable "key_vault_name" { type = string }
variable "tenant_id" { type = string }
variable "vnet_subnet_id" {
  type    = string
  default = ""
}
variable "allowed_ips" {
  type        = list(string)
  default     = []
  description = "List of IP addresses allowed to access Key Vault (for deployment/CI-CD)"
}
variable "tags" { type = map(string) }
