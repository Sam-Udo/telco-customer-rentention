
# Module: Key Vault

# Azure Key Vault for storing:
#   - Databricks service principal credentials
#   - Storage account keys
#   - Any API keys or secrets needed by the pipeline

data "azurerm_client_config" "current" {}

resource "azurerm_resource_group" "security" {
  name     = var.resource_group_name
  location = var.location
  tags     = var.tags
}

resource "azurerm_key_vault" "main" {
  name                       = var.key_vault_name
  location                   = var.location
  resource_group_name        = azurerm_resource_group.security.name
  tenant_id                  = var.tenant_id
  sku_name                   = "standard"
  purge_protection_enabled   = var.environment == "prod" ? true : false
  soft_delete_retention_days = var.environment == "prod" ? 90 : 7
  enable_rbac_authorization  = true

  network_acls {
    default_action = var.environment == "prod" ? "Deny" : "Allow"
    bypass         = "AzureServices"
    ip_rules       = var.allowed_ips
  }

  tags = var.tags
}

#  Grant current user/SP Key Vault Secrets Officer role ──
# Required for Terraform to write secrets during deployment
resource "azurerm_role_assignment" "kv_secrets_officer" {
  scope                = azurerm_key_vault.main.id
  role_definition_name = "Key Vault Secrets Officer"
  principal_id         = data.azurerm_client_config.current.object_id

  depends_on = [azurerm_key_vault.main]
}

resource "time_sleep" "wait_for_rbac" {
  depends_on      = [azurerm_role_assignment.kv_secrets_officer]
  create_duration = "60s"
}

#  Private Endpoint ──
resource "azurerm_private_endpoint" "key_vault" {
  name                = "pe-${var.key_vault_name}"
  location            = var.location
  resource_group_name = azurerm_resource_group.security.name
  subnet_id           = var.vnet_subnet_id

  private_service_connection {
    name                           = "psc-${var.key_vault_name}"
    private_connection_resource_id = azurerm_key_vault.main.id
    subresource_names              = ["vault"]
    is_manual_connection           = false
  }

  tags = var.tags
}

#  Outputs ──
output "key_vault_id" {
  value      = azurerm_key_vault.main.id
  depends_on = [time_sleep.wait_for_rbac]
}

output "key_vault_name" {
  value = azurerm_key_vault.main.name
}

output "key_vault_uri" {
  value = azurerm_key_vault.main.vault_uri
}
