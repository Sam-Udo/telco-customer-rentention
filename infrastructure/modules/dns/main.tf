# Module: Azure DNS Zone
# Creates DNS zone for custom domain with automated delegation

resource "azurerm_resource_group" "dns" {
  name     = var.resource_group_name
  location = var.location
  tags     = var.tags
}

resource "azurerm_dns_zone" "main" {
  name                = var.domain_name
  resource_group_name = azurerm_resource_group.dns.name
  tags                = var.tags
}

# A record for root domain (will be updated by deploy script with ingress IP)
# Initial placeholder - deploy script will update this
resource "azurerm_dns_a_record" "root" {
  count               = var.create_placeholder_record ? 1 : 0
  name                = "@"
  zone_name           = azurerm_dns_zone.main.name
  resource_group_name = azurerm_resource_group.dns.name
  ttl                 = 300
  records             = ["0.0.0.0"]  # Placeholder - updated by deploy script
  tags                = var.tags
}

# Outputs
output "dns_zone_id" {
  value       = azurerm_dns_zone.main.id
  description = "DNS Zone resource ID"
}

output "dns_zone_name" {
  value       = azurerm_dns_zone.main.name
  description = "DNS Zone name (domain)"
}

output "resource_group_name" {
  value       = azurerm_resource_group.dns.name
  description = "DNS resource group name"
}

output "name_servers" {
  value       = azurerm_dns_zone.main.name_servers
  description = "List of Azure nameservers for domain delegation"
}
