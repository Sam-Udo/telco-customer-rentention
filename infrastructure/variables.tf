variable "subscription_id" {
  type        = string
  description = "Azure subscription ID"
}

variable "environment" {
  type        = string
  description = "Environment: dev, staging, or prod"
  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Environment must be one of: dev, staging, prod"
  }
}

variable "location" {
  type        = string
  description = "Azure region for all resources"
  default     = "uksouth"
}

variable "resource_prefix" {
  type        = string
  description = "Prefix for all resource names (e.g., 'telco-churn')"
  default     = "telco-churn"
}

variable "storage_prefix" {
  type        = string
  description = "Prefix for storage account name (no hyphens, max 24 chars)"
  default     = "telcochurnsa"
}

variable "domain_name" {
  type        = string
  description = "Custom domain name for ingress (e.g., telco-churn.xyz). Leave empty to skip DNS setup."
  default     = ""
  validation {
    condition     = var.domain_name == "" || can(regex("^[a-z0-9][a-z0-9.-]+\\.[a-z]{2,}$", var.domain_name))
    error_message = "domain_name must be a valid domain (e.g., example.com) or empty."
  }
}

variable "vnet_address_space" {
  type        = list(string)
  description = "VNet address space CIDR"
  default     = ["10.0.0.0/16"]
}

variable "databricks_sku" {
  type        = string
  description = "Databricks workspace SKU: standard or premium"
  default     = "premium"
}

variable "metastore_id" {
  type        = string
  description = "Existing Unity Catalog metastore ID (shared across environments)"
  default     = ""
  validation {
    condition     = var.metastore_id == "" || can(regex("^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", var.metastore_id))
    error_message = "metastore_id must be a valid UUID or empty."
  }
}

variable "allowed_ips" {
  type        = list(string)
  description = "IP addresses allowed to access Key Vault (set per environment or via TF_VAR)"
  default     = []
}

variable "aks_kubernetes_version" {
  type        = string
  description = "Kubernetes version for AKS cluster"
  default     = "1.29"
}

variable "aks_system_vm_size" {
  type        = string
  description = "VM size for AKS system node pool"
  default     = "Standard_DS2_v2"
}

variable "aks_system_min_nodes" {
  type        = number
  description = "Minimum nodes in AKS system pool"
  default     = 1
}

variable "aks_system_max_nodes" {
  type        = number
  description = "Maximum nodes in AKS system pool"
  default     = 3
}

variable "aks_workload_vm_size" {
  type        = string
  description = "VM size for AKS workload node pool (API + Dashboard pods)"
  default     = "Standard_DS3_v2"
}

variable "aks_workload_min_nodes" {
  type        = number
  description = "Minimum nodes in AKS workload pool"
  default     = 1
}

variable "aks_workload_max_nodes" {
  type        = number
  description = "Maximum nodes in AKS workload pool"
  default     = 5
}
