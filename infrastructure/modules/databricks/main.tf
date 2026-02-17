# Databricks workspace with VNet injection, service principal, and Key Vault secret storage

resource "azurerm_resource_group" "databricks" {
  name     = var.resource_group_name
  location = var.location
  tags     = var.tags
}

resource "azurerm_databricks_workspace" "main" {
  name                        = var.workspace_name
  resource_group_name         = azurerm_resource_group.databricks.name
  location                    = var.location
  sku                         = var.sku
  managed_resource_group_name = "${var.workspace_name}-managed-rg"

  custom_parameters {
    virtual_network_id                                   = var.vnet_id
    public_subnet_name                                   = split("/", var.public_subnet_id)[length(split("/", var.public_subnet_id)) - 1]
    private_subnet_name                                  = split("/", var.private_subnet_id)[length(split("/", var.private_subnet_id)) - 1]
    public_subnet_network_security_group_association_id  = var.public_subnet_nsg_id
    private_subnet_network_security_group_association_id = var.private_subnet_nsg_id
    no_public_ip                                         = var.environment == "prod" ? true : false
    storage_account_name                                 = "dbxsa${replace(var.workspace_name, "-", "")}"
  }

  tags = var.tags
}

resource "azuread_application" "databricks_sp" {
  display_name = "sp-databricks-${var.environment}"
}

resource "azuread_service_principal" "databricks_sp" {
  client_id = azuread_application.databricks_sp.client_id
}

resource "azuread_service_principal_password" "databricks_sp" {
  service_principal_id = azuread_service_principal.databricks_sp.id
  end_date_relative    = "8760h"
}

resource "azurerm_role_assignment" "sp_workspace_contributor" {
  scope                = azurerm_databricks_workspace.main.id
  role_definition_name = "Contributor"
  principal_id         = azuread_service_principal.databricks_sp.object_id
}

resource "azurerm_role_assignment" "sp_storage_contributor" {
  scope                = var.storage_account_id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azuread_service_principal.databricks_sp.object_id
}

resource "azurerm_key_vault_secret" "sp_client_id" {
  name         = "databricks-sp-client-id"
  value        = azuread_application.databricks_sp.client_id
  key_vault_id = var.key_vault_id
}

resource "azurerm_key_vault_secret" "sp_client_secret" {
  name         = "databricks-sp-client-secret"
  value        = azuread_service_principal_password.databricks_sp.value
  key_vault_id = var.key_vault_id
}

output "workspace_id" {
  value = azurerm_databricks_workspace.main.id
}

output "workspace_number_id" {
  value = azurerm_databricks_workspace.main.workspace_id
}

output "workspace_url" {
  value = "https://${azurerm_databricks_workspace.main.workspace_url}"
}

output "managed_resource_group" {
  value = azurerm_databricks_workspace.main.managed_resource_group_name
}

output "resource_group_name" {
  value = azurerm_resource_group.databricks.name
}
