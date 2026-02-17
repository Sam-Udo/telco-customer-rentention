variable "environment" { type = string }
variable "location" { type = string }
variable "resource_group_name" { type = string }
variable "log_analytics_name" { type = string }
variable "databricks_workspace_id" { type = string }
variable "storage_account_id" { type = string }
variable "key_vault_id" { type = string }
variable "tags" { type = map(string) }
