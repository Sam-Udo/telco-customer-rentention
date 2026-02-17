variable "environment" { type = string }
variable "location" { type = string }
variable "resource_group_name" { type = string }
variable "storage_account_name" { type = string }
variable "vnet_subnet_id" {
  type    = string
  default = ""
}
variable "tags" { type = map(string) }
