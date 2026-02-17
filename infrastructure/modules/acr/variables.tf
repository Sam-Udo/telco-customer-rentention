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
  description = "Resource group for ACR"
}

variable "acr_name" {
  type        = string
  description = "Container registry name (globally unique, alphanumeric only)"
}

variable "tags" {
  type        = map(string)
  description = "Common tags"
  default     = {}
}
