
# Module: Networking

# Creates VNet with subnets for:
#   - Databricks public (host) subnet
#   - Databricks private (container) subnet
#   - Private endpoints subnet (storage, key vault)
#   - NSGs with Databricks-required rules

resource "azurerm_resource_group" "networking" {
  name     = var.resource_group_name
  location = var.location
  tags     = var.tags
}

#  Virtual Network ──
resource "azurerm_virtual_network" "main" {
  name                = "vnet-telco-churn-${var.environment}"
  location            = var.location
  resource_group_name = azurerm_resource_group.networking.name
  address_space       = var.vnet_address_space
  tags                = var.tags
}

#  Databricks Public (Host) Subnet ──
resource "azurerm_subnet" "databricks_public" {
  name                 = "snet-dbx-public-${var.environment}"
  resource_group_name  = azurerm_resource_group.networking.name
  virtual_network_name = azurerm_virtual_network.main.name
  address_prefixes     = [cidrsubnet(var.vnet_address_space[0], 8, 1)] # /24

  delegation {
    name = "databricks-delegation"
    service_delegation {
      name = "Microsoft.Databricks/workspaces"
      actions = [
        "Microsoft.Network/virtualNetworks/subnets/join/action",
        "Microsoft.Network/virtualNetworks/subnets/prepareNetworkPolicies/action",
        "Microsoft.Network/virtualNetworks/subnets/unprepareNetworkPolicies/action",
      ]
    }
  }
}

#  Databricks Private (Container) Subnet ──
resource "azurerm_subnet" "databricks_private" {
  name                 = "snet-dbx-private-${var.environment}"
  resource_group_name  = azurerm_resource_group.networking.name
  virtual_network_name = azurerm_virtual_network.main.name
  address_prefixes     = [cidrsubnet(var.vnet_address_space[0], 8, 2)] # /24

  delegation {
    name = "databricks-delegation"
    service_delegation {
      name = "Microsoft.Databricks/workspaces"
      actions = [
        "Microsoft.Network/virtualNetworks/subnets/join/action",
        "Microsoft.Network/virtualNetworks/subnets/prepareNetworkPolicies/action",
        "Microsoft.Network/virtualNetworks/subnets/unprepareNetworkPolicies/action",
      ]
    }
  }
}

#  Private Endpoints Subnet ──
resource "azurerm_subnet" "private_endpoints" {
  name                 = "snet-private-endpoints-${var.environment}"
  resource_group_name  = azurerm_resource_group.networking.name
  virtual_network_name = azurerm_virtual_network.main.name
  address_prefixes     = [cidrsubnet(var.vnet_address_space[0], 8, 3)] # /24
}

#  AKS Node Subnet ──
resource "azurerm_subnet" "aks" {
  name                 = "snet-aks-${var.environment}"
  resource_group_name  = azurerm_resource_group.networking.name
  virtual_network_name = azurerm_virtual_network.main.name
  address_prefixes     = [cidrsubnet(var.vnet_address_space[0], 8, 4)] # /24
}

#  NSG: Databricks Public Subnet ──
resource "azurerm_network_security_group" "databricks_public" {
  name                = "nsg-dbx-public-${var.environment}"
  location            = var.location
  resource_group_name = azurerm_resource_group.networking.name
  tags                = var.tags
}

resource "azurerm_subnet_network_security_group_association" "databricks_public" {
  subnet_id                 = azurerm_subnet.databricks_public.id
  network_security_group_id = azurerm_network_security_group.databricks_public.id
}

#  NSG: Databricks Private Subnet ──
resource "azurerm_network_security_group" "databricks_private" {
  name                = "nsg-dbx-private-${var.environment}"
  location            = var.location
  resource_group_name = azurerm_resource_group.networking.name
  tags                = var.tags
}

resource "azurerm_subnet_network_security_group_association" "databricks_private" {
  subnet_id                 = azurerm_subnet.databricks_private.id
  network_security_group_id = azurerm_network_security_group.databricks_private.id
}

#  NSG: AKS Subnet ──
resource "azurerm_network_security_group" "aks" {
  name                = "nsg-aks-${var.environment}"
  location            = var.location
  resource_group_name = azurerm_resource_group.networking.name
  tags                = var.tags

  # ─ Inbound Rules ───

  # Allow HTTPS from known IPs (restrict in production)
  security_rule {
    name                       = "AllowHTTPSFromKnownIPs"
    priority                   = 100
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     = "443"
    source_address_prefixes    = var.allowed_ingress_ips  # Configured per environment
    destination_address_prefix = "*"
    description                = "Allow HTTPS ingress from whitelisted corporate IPs"
  }

  # Allow HTTP from known IPs (for HTTP → HTTPS redirect)
  security_rule {
    name                       = "AllowHTTPFromKnownIPs"
    priority                   = 110
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     = "80"
    source_address_prefixes    = var.allowed_ingress_ips
    destination_address_prefix = "*"
    description                = "Allow HTTP for redirect to HTTPS"
  }

  # Allow Azure Load Balancer health probes
  security_rule {
    name                       = "AllowAzureLoadBalancer"
    priority                   = 120
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "*"
    source_port_range          = "*"
    destination_port_range     = "*"
    source_address_prefix      = "AzureLoadBalancer"
    destination_address_prefix = "*"
    description                = "Allow Azure Load Balancer health probes"
  }

  # Allow inter-VNet communication (AKS nodes, private endpoints)
  security_rule {
    name                       = "AllowVNetInbound"
    priority                   = 130
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "*"
    source_port_range          = "*"
    destination_port_range     = "*"
    source_address_prefix      = "VirtualNetwork"
    destination_address_prefix = "VirtualNetwork"
    description                = "Allow VNet-to-VNet communication"
  }

  # Deny all other inbound traffic
  security_rule {
    name                       = "DenyAllInbound"
    priority                   = 1000
    direction                  = "Inbound"
    access                     = "Deny"
    protocol                   = "*"
    source_port_range          = "*"
    destination_port_range     = "*"
    source_address_prefix      = "*"
    destination_address_prefix = "*"
    description                = "Deny all inbound traffic not explicitly allowed"
  }

  # ─ Outbound Rules ───

  # Allow outbound to Internet (for package downloads, ACR pulls, monitoring)
  security_rule {
    name                       = "AllowInternetOutbound"
    priority                   = 100
    direction                  = "Outbound"
    access                     = "Allow"
    protocol                   = "*"
    source_port_range          = "*"
    destination_port_range     = "*"
    source_address_prefix      = "*"
    destination_address_prefix = "Internet"
    description                = "Allow outbound to Internet for ACR, monitoring, updates"
  }

  # Allow outbound to VNet (for private endpoints, ACR via private link)
  security_rule {
    name                       = "AllowVNetOutbound"
    priority                   = 110
    direction                  = "Outbound"
    access                     = "Allow"
    protocol                   = "*"
    source_port_range          = "*"
    destination_port_range     = "*"
    source_address_prefix      = "VirtualNetwork"
    destination_address_prefix = "VirtualNetwork"
    description                = "Allow VNet-to-VNet communication"
  }
}

resource "azurerm_subnet_network_security_group_association" "aks" {
  subnet_id                 = azurerm_subnet.aks.id
  network_security_group_id = azurerm_network_security_group.aks.id
}

#  Outputs ──
output "vnet_id" {
  value = azurerm_virtual_network.main.id
}

output "databricks_public_subnet_id" {
  value = azurerm_subnet.databricks_public.id
}

output "databricks_private_subnet_id" {
  value = azurerm_subnet.databricks_private.id
}

output "private_endpoint_subnet_id" {
  value = azurerm_subnet.private_endpoints.id
}

output "databricks_public_nsg_id" {
  value = azurerm_network_security_group.databricks_public.id
}

output "databricks_private_nsg_id" {
  value = azurerm_network_security_group.databricks_private.id
}

output "aks_subnet_id" {
  value = azurerm_subnet.aks.id
}

output "aks_nsg_id" {
  value = azurerm_network_security_group.aks.id
}

output "vnet_name" {
  value = azurerm_virtual_network.main.name
}

output "vnet_resource_group_name" {
  value = azurerm_resource_group.networking.name
}
