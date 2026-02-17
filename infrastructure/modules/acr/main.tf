
# Module: Azure Container Registry (ACR)

# Stores Docker images for the API and Dashboard services.
# Premium SKU in prod for geo-replication; Basic in dev.

resource "azurerm_resource_group" "acr" {
  name     = var.resource_group_name
  location = var.location
  tags     = var.tags
}

resource "azurerm_container_registry" "main" {
  name                = var.acr_name
  resource_group_name = azurerm_resource_group.acr.name
  location            = var.location
  sku                 = var.environment == "prod" ? "Premium" : "Basic"
  admin_enabled       = false

  dynamic "georeplications" {
    for_each = var.environment == "prod" ? ["ukwest"] : []
    content {
      location = georeplications.value
      tags     = var.tags
    }
  }

  tags = var.tags
}

#  Outputs ──
output "acr_id" {
  value = azurerm_container_registry.main.id
}

output "acr_login_server" {
  value = azurerm_container_registry.main.login_server
}

output "acr_name" {
  value = azurerm_container_registry.main.name
}
