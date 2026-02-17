resource "azurerm_resource_group" "monitoring" {
  name     = var.resource_group_name
  location = var.location
  tags     = var.tags
}

resource "azurerm_log_analytics_workspace" "main" {
  name                = var.log_analytics_name
  location            = var.location
  resource_group_name = azurerm_resource_group.monitoring.name
  sku                 = "PerGB2018"
  retention_in_days   = var.environment == "prod" ? 90 : 30

  tags = var.tags
}

resource "azurerm_monitor_diagnostic_setting" "databricks" {
  name                       = "diag-databricks-${var.environment}"
  target_resource_id         = var.databricks_workspace_id
  log_analytics_workspace_id = azurerm_log_analytics_workspace.main.id

  enabled_log {
    category = "dbfs"
  }
  enabled_log {
    category = "clusters"
  }
  enabled_log {
    category = "accounts"
  }
  enabled_log {
    category = "jobs"
  }
  enabled_log {
    category = "notebook"
  }
  enabled_log {
    category = "sqlPermissions"
  }
  enabled_log {
    category = "unityCatalog"
  }
}

resource "azurerm_monitor_diagnostic_setting" "storage" {
  name                       = "diag-storage-${var.environment}"
  target_resource_id         = "${var.storage_account_id}/blobServices/default"
  log_analytics_workspace_id = azurerm_log_analytics_workspace.main.id

  enabled_log {
    category = "StorageRead"
  }
  enabled_log {
    category = "StorageWrite"
  }
  enabled_log {
    category = "StorageDelete"
  }

  metric {
    category = "Transaction"
  }
}

resource "azurerm_monitor_diagnostic_setting" "key_vault" {
  name                       = "diag-keyvault-${var.environment}"
  target_resource_id         = var.key_vault_id
  log_analytics_workspace_id = azurerm_log_analytics_workspace.main.id

  enabled_log {
    category = "AuditEvent"
  }

  metric {
    category = "AllMetrics"
  }
}

# Disabled: query validation fails until Databricks starts emitting logs — enable in Portal after first job runs
resource "azurerm_monitor_scheduled_query_rules_alert_v2" "job_failures" {
  count               = 0
  name                = "alert-databricks-job-failures"
  location            = var.location
  resource_group_name = azurerm_resource_group.monitoring.name

  scopes               = [azurerm_log_analytics_workspace.main.id]
  severity             = 2
  window_duration      = "PT1H"
  evaluation_frequency = "PT15M"

  criteria {
    query = <<-QUERY
      DatabricksJobs
      | where actionName_s == "runFailed"
      | where TimeGenerated > ago(1h)
    QUERY

    time_aggregation_method = "Count"
    operator                = "GreaterThan"
    threshold               = 0

    failing_periods {
      minimum_failing_periods_to_trigger_alert = 1
      number_of_evaluation_periods             = 1
    }

    dimension {
      name     = "jobId_s"
      operator = "Include"
      values   = ["*"]
    }
  }

  description = "Alert when Databricks jobs fail in the telco-churn pipeline"
  enabled     = true

  tags = var.tags
}

output "log_analytics_workspace_id" {
  value = azurerm_log_analytics_workspace.main.id
}

output "log_analytics_workspace_name" {
  value = azurerm_log_analytics_workspace.main.name
}
