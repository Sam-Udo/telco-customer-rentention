resource "azurerm_user_assigned_identity" "api" {
  name                = "mi-churn-api-${var.environment}"
  location            = var.location
  resource_group_name = azurerm_resource_group.aks.name
  tags                = var.tags
}

resource "azurerm_federated_identity_credential" "api" {
  name                = "fc-churn-api-${var.environment}"
  resource_group_name = azurerm_resource_group.aks.name
  parent_id           = azurerm_user_assigned_identity.api.id
  audience            = ["api://AzureADTokenExchange"]
  issuer              = azurerm_kubernetes_cluster.main.oidc_issuer_url
  subject             = "system:serviceaccount:${var.k8s_namespace}:churn-api-sa"
}

resource "azurerm_role_assignment" "api_keyvault_reader" {
  scope                = var.key_vault_id
  role_definition_name = "Key Vault Secrets User"
  principal_id         = azurerm_user_assigned_identity.api.principal_id
}

resource "azurerm_role_assignment" "api_storage_reader" {
  scope                = var.storage_account_id
  role_definition_name = "Storage Blob Data Reader"
  principal_id         = azurerm_user_assigned_identity.api.principal_id
}

resource "azurerm_user_assigned_identity" "dashboard" {
  name                = "mi-churn-dashboard-${var.environment}"
  location            = var.location
  resource_group_name = azurerm_resource_group.aks.name
  tags                = var.tags
}

resource "azurerm_federated_identity_credential" "dashboard" {
  name                = "fc-churn-dashboard-${var.environment}"
  resource_group_name = azurerm_resource_group.aks.name
  parent_id           = azurerm_user_assigned_identity.dashboard.id
  audience            = ["api://AzureADTokenExchange"]
  issuer              = azurerm_kubernetes_cluster.main.oidc_issuer_url
  subject             = "system:serviceaccount:${var.k8s_namespace}:churn-dashboard-sa"
}

resource "azurerm_role_assignment" "dashboard_storage_reader" {
  scope                = var.storage_account_id
  role_definition_name = "Storage Blob Data Reader"
  principal_id         = azurerm_user_assigned_identity.dashboard.principal_id
}

output "api_managed_identity_client_id" {
  description = "Client ID for API workload identity"
  value       = azurerm_user_assigned_identity.api.client_id
}

output "dashboard_managed_identity_client_id" {
  description = "Client ID for Dashboard workload identity"
  value       = azurerm_user_assigned_identity.dashboard.client_id
}
