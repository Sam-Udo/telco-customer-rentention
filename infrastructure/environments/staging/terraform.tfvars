# Staging environment
subscription_id    = "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx" # Set via TF_VAR_subscription_id
environment        = "staging"
location           = "uksouth"
resource_prefix    = "telco-churn"
storage_prefix     = "telcochurnsa"
vnet_address_space = ["10.2.0.0/16"]
databricks_sku     = "premium"
metastore_id       = "" # Set via TF_VAR_metastore_id

# AKS node pool sizing
aks_kubernetes_version = "1.29"
aks_system_vm_size     = "Standard_DS2_v2"
aks_system_min_nodes   = 1
aks_system_max_nodes   = 3
aks_workload_vm_size   = "Standard_DS3_v2"
aks_workload_min_nodes = 2
aks_workload_max_nodes = 5
