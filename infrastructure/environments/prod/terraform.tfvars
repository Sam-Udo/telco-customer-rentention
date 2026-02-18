# Production environment
subscription_id    = "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx" # Set via TF_VAR_subscription_id
environment        = "prod"
location           = "uksouth"
resource_prefix    = "telco-churn"
storage_prefix     = "telcochurnsa"
domain_name        = "telco-churn.xyz"
vnet_address_space = ["10.3.0.0/16"]
databricks_sku     = "premium"
metastore_id       = "0ff81767-3ec8-4fa1-b5d2-2680da343f16"

allowed_ips = ["151.240.254.33", "86.145.28.123"]

# AKS node pool sizing
aks_kubernetes_version = "1.32"
aks_system_vm_size     = "Standard_D2als_v7"
aks_system_min_nodes   = 1
aks_system_max_nodes   = 2
aks_workload_vm_size   = "Standard_D2als_v7"
aks_workload_min_nodes = 1
aks_workload_max_nodes = 2
