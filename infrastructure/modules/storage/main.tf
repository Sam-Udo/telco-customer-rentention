
# Module: Storage (ADLS Gen2)

# Creates Azure Data Lake Storage Gen2 with containers for:
#   - landing: Raw source files (Bronze input)
#   - delta:   Delta Lake tables (Bronze/Silver/Gold)
#   - unity-catalog: Unity Catalog managed storage
#   - mlflow:  MLflow artifact storage

resource "azurerm_resource_group" "storage" {
  name     = var.resource_group_name
  location = var.location
  tags     = var.tags
}

resource "azurerm_storage_account" "datalake" {
  name                     = var.storage_account_name
  resource_group_name      = azurerm_resource_group.storage.name
  location                 = var.location
  account_tier             = "Standard"
  account_replication_type = var.environment == "prod" ? "GRS" : "LRS"
  account_kind             = "StorageV2"
  is_hns_enabled           = true # Hierarchical namespace = ADLS Gen2
  min_tls_version          = "TLS1_2"

  blob_properties {
    delete_retention_policy {
      days = var.environment == "prod" ? 30 : 7
    }
    container_delete_retention_policy {
      days = var.environment == "prod" ? 30 : 7
    }
  }

  tags = var.tags
}

# Containers (landing is in the shared landing-storage module)
resource "azurerm_storage_container" "delta" {
  name                  = "delta"
  storage_account_name  = azurerm_storage_account.datalake.name
  container_access_type = "private"
}

resource "azurerm_storage_container" "unity_catalog" {
  name                  = "unity-catalog"
  storage_account_name  = azurerm_storage_account.datalake.name
  container_access_type = "private"
}

resource "azurerm_storage_container" "mlflow" {
  name                  = "mlflow-artifacts"
  storage_account_name  = azurerm_storage_account.datalake.name
  container_access_type = "private"
}

resource "azurerm_storage_container" "checkpoints" {
  name                  = "checkpoints"
  storage_account_name  = azurerm_storage_account.datalake.name
  container_access_type = "private"
}

#  Private Endpoint ──
resource "azurerm_private_endpoint" "storage" {
  name                = "pe-${var.storage_account_name}"
  location            = var.location
  resource_group_name = azurerm_resource_group.storage.name
  subnet_id           = var.vnet_subnet_id

  private_service_connection {
    name                           = "psc-${var.storage_account_name}"
    private_connection_resource_id = azurerm_storage_account.datalake.id
    subresource_names              = ["dfs"]
    is_manual_connection           = false
  }

  tags = var.tags
}

#  Outputs ──
output "storage_account_id" {
  value = azurerm_storage_account.datalake.id
}

output "storage_account_name" {
  value = azurerm_storage_account.datalake.name
}

output "delta_container_name" {
  value = azurerm_storage_container.delta.name
}

output "unity_catalog_container_name" {
  value = azurerm_storage_container.unity_catalog.name
}

output "storage_primary_dfs_endpoint" {
  value = azurerm_storage_account.datalake.primary_dfs_endpoint
}
