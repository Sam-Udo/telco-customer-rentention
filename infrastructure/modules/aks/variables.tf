variable "environment" {
  type        = string
  description = "Environment: dev, staging, or prod"
}

variable "location" {
  type        = string
  description = "Azure region"
}

variable "resource_group_name" {
  type        = string
  description = "Resource group for AKS cluster"
}

variable "cluster_name" {
  type        = string
  description = "AKS cluster name"
}

variable "kubernetes_version" {
  type        = string
  description = "Kubernetes version"
  default     = "1.29"
}

variable "system_node_vm_size" {
  type        = string
  description = "VM size for system node pool"
  default     = "Standard_DS2_v2"
}

variable "system_node_min_count" {
  type        = number
  description = "Minimum nodes in system pool"
  default     = 1
}

variable "system_node_max_count" {
  type        = number
  description = "Maximum nodes in system pool"
  default     = 3
}

variable "workload_node_vm_size" {
  type        = string
  description = "VM size for workload node pool (API + Dashboard)"
  default     = "Standard_DS3_v2"
}

variable "workload_node_min_count" {
  type        = number
  description = "Minimum nodes in workload pool"
  default     = 1
}

variable "workload_node_max_count" {
  type        = number
  description = "Maximum nodes in workload pool"
  default     = 5
}

variable "aks_subnet_id" {
  type        = string
  description = "Existing subnet ID for AKS nodes"
  default     = ""
}

variable "aks_subnet_cidr" {
  type        = string
  description = "CIDR for AKS subnet (if creating new)"
  default     = ""
}

variable "create_aks_subnet" {
  type        = bool
  description = "Whether to create a new subnet for AKS"
  default     = false
}

variable "vnet_resource_group_name" {
  type        = string
  description = "Resource group of the VNet (for subnet creation)"
  default     = ""
}

variable "vnet_name" {
  type        = string
  description = "VNet name (for subnet creation)"
  default     = ""
}

variable "service_cidr" {
  type        = string
  description = "Kubernetes service CIDR"
  default     = "10.0.0.0/16"
}

variable "dns_service_ip" {
  type        = string
  description = "Kubernetes DNS service IP (must be within service_cidr)"
  default     = "10.0.0.10"
}

variable "acr_id" {
  type        = string
  description = "ACR resource ID for AcrPull role assignment"
}

variable "log_analytics_workspace_id" {
  type        = string
  description = "Log Analytics workspace ID for Container Insights"
}

variable "key_vault_id" {
  type        = string
  description = "Key Vault resource ID for workload identity RBAC"
}

variable "storage_account_id" {
  type        = string
  description = "Storage Account resource ID for workload identity RBAC"
}

variable "k8s_namespace" {
  type        = string
  description = "Kubernetes namespace for workload identity federation"
  default     = "telco-churn"
}

variable "tags" {
  type        = map(string)
  description = "Common tags"
  default     = {}
}
