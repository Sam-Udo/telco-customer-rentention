resource "azurerm_resource_group" "aks" {
  name     = var.resource_group_name
  location = var.location
  tags     = var.tags
}

resource "azurerm_kubernetes_cluster" "main" {
  name                = var.cluster_name
  location            = var.location
  resource_group_name = azurerm_resource_group.aks.name
  dns_prefix          = var.cluster_name
  kubernetes_version  = var.kubernetes_version

  default_node_pool {
    name                = "system"
    vm_size             = var.system_node_vm_size
    min_count           = var.system_node_min_count
    max_count           = var.system_node_max_count
    enable_auto_scaling = true
    os_disk_size_gb     = 50
    vnet_subnet_id      = var.aks_subnet_id

    node_labels = {
      "nodepool-type" = "system"
      "environment"   = var.environment
    }
  }

  identity {
    type = "SystemAssigned"
  }

  network_profile {
    network_plugin    = "azure"
    network_policy    = "azure"
    load_balancer_sku = "standard"
    service_cidr      = var.service_cidr
    dns_service_ip    = var.dns_service_ip
  }

  oms_agent {
    log_analytics_workspace_id = var.log_analytics_workspace_id
  }

  oidc_issuer_enabled       = true
  workload_identity_enabled = true

  tags = var.tags
}

resource "azurerm_kubernetes_cluster_node_pool" "workload" {
  name                  = "workload"
  kubernetes_cluster_id = azurerm_kubernetes_cluster.main.id
  vm_size               = var.workload_node_vm_size
  min_count             = var.workload_node_min_count
  max_count             = var.workload_node_max_count
  enable_auto_scaling   = true
  os_disk_size_gb       = 50
  vnet_subnet_id        = var.aks_subnet_id

  node_labels = {
    "nodepool-type" = "workload"
    "environment"   = var.environment
  }

  node_taints = [
    "workload=churn:NoSchedule"
  ]

  tags = var.tags
}

resource "azurerm_role_assignment" "aks_acr_pull" {
  scope                = var.acr_id
  role_definition_name = "AcrPull"
  principal_id         = azurerm_kubernetes_cluster.main.kubelet_identity[0].object_id
}

resource "azurerm_subnet" "aks" {
  count                = var.create_aks_subnet ? 1 : 0
  name                 = "snet-aks-${var.environment}"
  resource_group_name  = var.vnet_resource_group_name
  virtual_network_name = var.vnet_name
  address_prefixes     = [var.aks_subnet_cidr]
}

output "cluster_id" {
  value = azurerm_kubernetes_cluster.main.id
}

output "cluster_name" {
  value = azurerm_kubernetes_cluster.main.name
}

output "cluster_fqdn" {
  value = azurerm_kubernetes_cluster.main.fqdn
}

output "kube_config" {
  value     = azurerm_kubernetes_cluster.main.kube_config_raw
  sensitive = true
}

output "kubelet_identity_object_id" {
  value = azurerm_kubernetes_cluster.main.kubelet_identity[0].object_id
}

output "aks_subnet_id" {
  value = var.create_aks_subnet ? azurerm_subnet.aks[0].id : var.aks_subnet_id
}

output "resource_group_name" {
  value = azurerm_resource_group.aks.name
}
