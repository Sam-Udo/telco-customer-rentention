variable "storage_account_name" {
  type        = string
  description = "Name for the shared landing storage account"
}

variable "resource_group_name" {
  type        = string
  description = "Resource group for shared landing storage"
}

variable "location" {
  type        = string
  description = "Azure region"
}

variable "tags" {
  type        = map(string)
  description = "Resource tags"
}
