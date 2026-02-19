variable "access_connector_name" { type = string }
variable "resource_group_name" { type = string }
variable "location" { type = string }
variable "storage_account_id" { type = string }
variable "landing_storage_account_id" {
  type        = string
  description = "Shared landing storage account ID (for read-only access to raw data)"
}
variable "tags" { type = map(string) }
