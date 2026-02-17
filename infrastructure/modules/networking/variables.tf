variable "environment" { type = string }
variable "location" { type = string }
variable "resource_group_name" { type = string }
variable "vnet_address_space" { type = list(string) }
variable "tags" { type = map(string) }

variable "allowed_ingress_ips" {
  description = "List of IP addresses/CIDR blocks allowed to access AKS ingress (HTTPS)"
  type        = list(string)
  default     = ["0.0.0.0/0"]  # Default: allow all (override in prod with corporate IPs)
}
