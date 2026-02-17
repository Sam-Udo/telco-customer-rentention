# Terraform remote state — PRODUCTION
resource_group_name  = "rg-telco-churn-tfstate"
storage_account_name = "sttelcochurntfstate"
container_name       = "tfstate"
key                  = "prod.terraform.tfstate"
