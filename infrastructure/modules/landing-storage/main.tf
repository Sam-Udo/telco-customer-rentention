# Module: Shared Landing Storage
# Single ADLS Gen2 account for raw source data — shared across all environments.
# Created once (by DEV infra run), read by all environments.

resource "azurerm_resource_group" "landing" {
  name     = var.resource_group_name
  location = var.location
  tags     = var.tags
}

resource "azurerm_storage_account" "landing" {
  name                     = var.storage_account_name
  resource_group_name      = azurerm_resource_group.landing.name
  location                 = var.location
  account_tier             = "Standard"
  account_replication_type = "GRS"
  account_kind             = "StorageV2"
  is_hns_enabled           = true
  min_tls_version          = "TLS1_2"

  blob_properties {
    delete_retention_policy {
      days = 30
    }
    container_delete_retention_policy {
      days = 30
    }
  }

  tags = var.tags
}

resource "azurerm_storage_container" "landing" {
  name                  = "landing"
  storage_account_name  = azurerm_storage_account.landing.name
  container_access_type = "private"
}

output "storage_account_id" {
  value = azurerm_storage_account.landing.id
}

output "storage_account_name" {
  value = azurerm_storage_account.landing.name
}

output "landing_container_name" {
  value = azurerm_storage_container.landing.name
}
