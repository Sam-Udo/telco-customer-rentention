variable "environment" { type = string }
variable "location" { type = string }
variable "catalog_name" { type = string }
variable "metastore_id" { type = string }
variable "workspace_id" { type = string }
variable "access_connector_id" { type = string }
variable "storage_account_name" { type = string }
variable "storage_container" { type = string }
variable "landing_storage_account_name" {
  type        = string
  description = "Shared landing storage account name for raw data"
}
variable "databricks_workspace_url" { type = string }
