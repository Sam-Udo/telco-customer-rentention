variable "domain_name" {
  type        = string
  description = "Custom domain name (e.g., telco-churn.xyz)"
}

variable "resource_group_name" {
  type        = string
  description = "Resource group name for DNS zone"
}

variable "location" {
  type        = string
  description = "Azure region for the resource group"
}

variable "create_placeholder_record" {
  type        = bool
  default     = false
  description = "Whether to create placeholder A record (updated by deploy script)"
}

variable "tags" {
  type        = map(string)
  description = "Tags to apply to DNS resources"
}
