variable "environment" { type = string }
variable "location" { type = string }
variable "resource_group_name" { type = string }
variable "workspace_name" { type = string }
variable "sku" {
  type    = string
  default = "premium"
}
variable "vnet_id" { type = string }
variable "public_subnet_id" { type = string }
variable "private_subnet_id" { type = string }
variable "public_subnet_nsg_id" { type = string }
variable "private_subnet_nsg_id" { type = string }
variable "storage_account_id" { type = string }
variable "storage_account_name" { type = string }
variable "key_vault_id" { type = string }
variable "tags" { type = map(string) }
