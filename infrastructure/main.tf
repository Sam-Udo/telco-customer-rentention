terraform {
  required_version = ">= 1.5.0"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.85"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.35"
    }
    azuread = {
      source  = "hashicorp/azuread"
      version = "~> 2.47"
    }
    time = {
      source  = "hashicorp/time"
      version = "~> 0.9"
    }
  }

  backend "azurerm" {}
}

provider "azurerm" {
  features {
    key_vault {
      purge_soft_delete_on_destroy = false
    }
  }
  subscription_id = var.subscription_id
}

provider "azuread" {}

provider "databricks" {
  host                        = module.databricks.workspace_url
  azure_workspace_resource_id = module.databricks.workspace_id
}

data "azurerm_client_config" "current" {}

data "azuread_client_config" "current" {}

module "networking" {
  source = "./modules/networking"

  environment         = var.environment
  location            = var.location
  resource_group_name = "${var.resource_prefix}-networking-${var.environment}"
  vnet_address_space  = var.vnet_address_space
  tags                = local.common_tags
}

module "storage" {
  source = "./modules/storage"

  environment          = var.environment
  location             = var.location
  resource_group_name  = "${var.resource_prefix}-data-${var.environment}"
  storage_account_name = "${var.storage_prefix}${var.environment}"
  vnet_subnet_id       = module.networking.private_endpoint_subnet_id
  tags                 = local.common_tags
}

# Shared landing storage — raw source data (created once, read by all environments)
module "landing_storage" {
  source = "./modules/landing-storage"
  count  = var.create_landing_storage ? 1 : 0

  storage_account_name = var.landing_storage_account_name
  resource_group_name  = "${var.resource_prefix}-landing"
  location             = var.location
  tags                 = local.common_tags
}

# Data source to reference the shared landing storage when not creating it
data "azurerm_storage_account" "landing" {
  count = var.create_landing_storage ? 0 : 1

  name                = var.landing_storage_account_name
  resource_group_name = "${var.resource_prefix}-landing"
}

module "key_vault" {
  source = "./modules/key-vault"

  environment         = var.environment
  location            = var.location
  resource_group_name = "${var.resource_prefix}-security-${var.environment}"
  key_vault_name      = var.environment == "prod" ? "${var.resource_prefix}-vault-${var.environment}" : "${var.resource_prefix}-kv-${var.environment}"
  tenant_id           = data.azurerm_client_config.current.tenant_id
  vnet_subnet_id      = module.networking.private_endpoint_subnet_id
  allowed_ips         = var.allowed_ips
  tags                = local.common_tags
}

module "databricks" {
  source = "./modules/databricks"

  environment           = var.environment
  location              = var.location
  resource_group_name   = "${var.resource_prefix}-dbx-${var.environment}"
  workspace_name        = "${var.resource_prefix}-dbx-${var.environment}"
  sku                   = var.databricks_sku
  vnet_id               = module.networking.vnet_id
  public_subnet_id      = module.networking.databricks_public_subnet_id
  private_subnet_id     = module.networking.databricks_private_subnet_id
  public_subnet_nsg_id  = module.networking.databricks_public_nsg_id
  private_subnet_nsg_id = module.networking.databricks_private_nsg_id
  storage_account_id    = module.storage.storage_account_id
  storage_account_name  = module.storage.storage_account_name
  key_vault_id          = module.key_vault.key_vault_id
  tags                  = local.common_tags
}

module "access_connector" {
  source = "./modules/access-connector"

  access_connector_name      = "${var.resource_prefix}-access-connector-${var.environment}"
  resource_group_name        = module.databricks.resource_group_name
  location                   = var.location
  storage_account_id         = module.storage.storage_account_id
  landing_storage_account_id = local.landing_storage_account_id
  tags                       = local.common_tags

  depends_on = [module.databricks]
}

module "unity_catalog" {
  source = "./modules/unity-catalog"

  environment                  = var.environment
  location                     = var.location
  catalog_name                 = var.environment == "prod" ? "uk_telecoms" : "uk_telecoms_${var.environment}"
  metastore_id                 = var.metastore_id
  workspace_id                 = module.databricks.workspace_number_id
  access_connector_id          = module.access_connector.access_connector_id
  storage_account_name         = module.storage.storage_account_name
  storage_container            = module.storage.unity_catalog_container_name
  landing_storage_account_name = local.landing_storage_account_name
  databricks_workspace_url     = module.databricks.workspace_url

  depends_on = [module.databricks, module.access_connector]
}

module "monitoring" {
  source = "./modules/monitoring"

  environment             = var.environment
  location                = var.location
  resource_group_name     = "${var.resource_prefix}-monitoring-${var.environment}"
  log_analytics_name      = "${var.resource_prefix}-la-${var.environment}"
  databricks_workspace_id = module.databricks.workspace_id
  storage_account_id      = module.storage.storage_account_id
  key_vault_id            = module.key_vault.key_vault_id
  tags                    = local.common_tags
}

module "acr" {
  source = "./modules/acr"

  environment         = var.environment
  location            = var.location
  resource_group_name = "${var.resource_prefix}-acr-${var.environment}"
  acr_name            = "${var.storage_prefix}cr${var.environment}"
  tags                = local.common_tags
}

module "aks" {
  source = "./modules/aks"

  environment         = var.environment
  location            = var.location
  resource_group_name = "${var.resource_prefix}-aks-${var.environment}"
  cluster_name        = "${var.resource_prefix}-aks-${var.environment}"
  kubernetes_version  = var.aks_kubernetes_version

  system_node_vm_size     = var.aks_system_vm_size
  system_node_min_count   = var.aks_system_min_nodes
  system_node_max_count   = var.aks_system_max_nodes
  workload_node_vm_size   = var.aks_workload_vm_size
  workload_node_min_count = var.aks_workload_min_nodes
  workload_node_max_count = var.aks_workload_max_nodes

  aks_subnet_id  = module.networking.aks_subnet_id
  service_cidr   = "172.16.0.0/16"
  dns_service_ip = "172.16.0.10"

  acr_id                     = module.acr.acr_id
  log_analytics_workspace_id = module.monitoring.log_analytics_workspace_id
  storage_account_id         = module.storage.storage_account_id
  key_vault_id               = module.key_vault.key_vault_id

  tags = local.common_tags

  depends_on = [module.networking, module.acr, module.monitoring, module.storage, module.key_vault]
}

module "dns" {
  source = "./modules/dns"
  count  = var.domain_name != "" ? 1 : 0

  domain_name               = var.domain_name
  resource_group_name       = "rg-${var.resource_prefix}-networking-${var.environment}"
  location                  = var.location
  create_placeholder_record = false
  tags                      = local.common_tags
}

locals {
  common_tags = {
    project     = "telco-churn"
    environment = var.environment
    managed_by  = "terraform"
    team        = "data-engineering"
    cost_center = "analytics"
  }

  landing_storage_account_id   = var.create_landing_storage ? module.landing_storage[0].storage_account_id : data.azurerm_storage_account.landing[0].id
  landing_storage_account_name = var.landing_storage_account_name
}

output "landing_storage_account_name" {
  value       = local.landing_storage_account_name
  description = "Shared landing storage account name (raw data)"
}

output "databricks_workspace_url" {
  value       = module.databricks.workspace_url
  description = "Databricks workspace URL"
}

output "storage_account_name" {
  value       = module.storage.storage_account_name
  description = "ADLS Gen2 storage account name"
}

output "unity_catalog_name" {
  value       = module.unity_catalog.catalog_name
  description = "Unity Catalog name for this environment"
}

output "key_vault_name" {
  value       = module.key_vault.key_vault_name
  description = "Key Vault name for this environment"
}

output "log_analytics_workspace_id" {
  value       = module.monitoring.log_analytics_workspace_id
  description = "Log Analytics workspace ID"
}

output "acr_login_server" {
  value       = module.acr.acr_login_server
  description = "ACR login server URL"
}

output "aks_cluster_name" {
  value       = module.aks.cluster_name
  description = "AKS cluster name"
}

output "aks_cluster_fqdn" {
  value       = module.aks.cluster_fqdn
  description = "AKS cluster FQDN"
}

output "dns_nameservers" {
  value       = var.domain_name != "" ? module.dns[0].name_servers : []
  description = "Azure DNS nameservers for domain delegation"
}

output "acr_name" {
  value       = module.acr.acr_name
  description = "ACR name (for docker login and push)"
}

output "aks_resource_group_name" {
  value       = module.aks.resource_group_name
  description = "AKS resource group name (for az aks get-credentials)"
}
