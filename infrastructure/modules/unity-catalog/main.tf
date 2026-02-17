terraform {
  required_providers {
    databricks = {
      source = "databricks/databricks"
    }
  }
}

resource "databricks_metastore" "this" {
  count = var.metastore_id == "" ? 1 : 0

  name          = "telco-churn-metastore-${var.environment}"
  region        = var.location
  storage_root  = "abfss://${var.storage_container}@${var.storage_account_name}.dfs.core.windows.net/"
  force_destroy = false
}

resource "databricks_metastore_assignment" "this" {
  workspace_id         = var.workspace_id
  metastore_id         = var.metastore_id != "" ? var.metastore_id : databricks_metastore.this[0].id
  default_catalog_name = "main"
}

locals {
  metastore_id = var.metastore_id != "" ? var.metastore_id : databricks_metastore.this[0].id
}

resource "databricks_storage_credential" "external" {
  name = "telco-churn-storage-${var.environment}"

  azure_managed_identity {
    access_connector_id = var.access_connector_id
  }
  comment = "Storage credential for telco-churn ${var.environment} data lake"

  depends_on = [databricks_metastore_assignment.this]
}

resource "databricks_external_location" "datalake" {
  name            = "telco-churn-datalake-${var.environment}"
  url             = "abfss://${var.storage_container}@${var.storage_account_name}.dfs.core.windows.net/"
  credential_name = databricks_storage_credential.external.name
  comment         = "ADLS Gen2 storage for telco-churn ${var.environment}"
}

resource "databricks_catalog" "main" {
  name         = var.catalog_name
  comment      = "Telco Customer Churn — ${var.environment} environment"
  storage_root = "abfss://${var.storage_container}@${var.storage_account_name}.dfs.core.windows.net/catalogs/${var.catalog_name}"

  properties = {
    environment = var.environment
    project     = "telco-churn"
  }

  depends_on = [databricks_external_location.datalake]
}

resource "databricks_schema" "bronze" {
  catalog_name = databricks_catalog.main.name
  name         = "bronze"
  comment      = "Raw data — no transformations. Immutable audit layer."
}

resource "databricks_schema" "silver" {
  catalog_name = databricks_catalog.main.name
  name         = "silver"
  comment      = "Cleaned, typed, validated data. Quality gates applied."
}

resource "databricks_schema" "gold" {
  catalog_name = databricks_catalog.main.name
  name         = "gold"
  comment      = "Feature-engineered, model-ready data. Feature Store registered."
}

resource "databricks_schema" "ml" {
  catalog_name = databricks_catalog.main.name
  name         = "ml"
  comment      = "ML models, experiments, scoring outputs, monitoring metrics."
}

output "catalog_name" {
  value = databricks_catalog.main.name
}

output "schemas" {
  value = {
    bronze = databricks_schema.bronze.name
    silver = databricks_schema.silver.name
    gold   = databricks_schema.gold.name
    ml     = databricks_schema.ml.name
  }
}
