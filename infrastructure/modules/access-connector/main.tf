# Module: Azure Databricks Access Connector
# Required for Unity Catalog managed identity authentication

resource "azurerm_databricks_access_connector" "unity_catalog" {
  name                = var.access_connector_name
  resource_group_name = var.resource_group_name
  location            = var.location

  identity {
    type = "SystemAssigned"
  }

  tags = var.tags
}

# Grant Storage Blob Data Contributor to Access Connector for Unity Catalog storage
resource "azurerm_role_assignment" "unity_catalog_storage" {
  scope                = var.storage_account_id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_databricks_access_connector.unity_catalog.identity[0].principal_id
}

# Grant access to the shared landing storage account
# Requires Contributor (not Reader) because Databricks external location
# creation writes a validation file to prove access.
resource "azurerm_role_assignment" "landing_storage_contributor" {
  scope                = var.landing_storage_account_id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_databricks_access_connector.unity_catalog.identity[0].principal_id
}

# Azure RBAC propagation takes up to 60s — wait before downstream resources
# (e.g. Databricks external locations) try to use the role assignments.
resource "time_sleep" "rbac_propagation" {
  create_duration = "60s"

  depends_on = [
    azurerm_role_assignment.unity_catalog_storage,
    azurerm_role_assignment.landing_storage_contributor,
  ]
}

output "access_connector_id" {
  value = azurerm_databricks_access_connector.unity_catalog.id
}

output "identity_principal_id" {
  value = azurerm_databricks_access_connector.unity_catalog.identity[0].principal_id
}
