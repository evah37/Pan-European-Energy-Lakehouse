# ──────────────────────────────────────────────────────────────────
# Pan-European Energy Lakehouse - Azure Infrastructure
# ──────────────────────────────────────────────────────────────────

terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.80"
    }
  }
  required_version = ">= 1.2.0"
}

provider "azurerm" {
  features {}
  skip_provider_registration = true
}

# Resource Group
resource "azurerm_resource_group" "energy" {
  name     = var.resource_group_name
  location = var.location

  tags = {
    project     = "Pan-European-Energy-Lakehouse"
    environment = var.environment
    managed_by  = "terraform"
  }
}

# Network (VNet + Subnet + NSG)
resource "azurerm_virtual_network" "energy_vnet" {
  name                = "vnet-energy-lakehouse"
  address_space       = ["10.0.0.0/16"]
  location            = azurerm_resource_group.energy.location
  resource_group_name = azurerm_resource_group.energy.name
}

resource "azurerm_subnet" "energy_subnet" {
  name                 = "snet-energy-vm"
  resource_group_name  = azurerm_resource_group.energy.name
  virtual_network_name = azurerm_virtual_network.energy_vnet.name
  address_prefixes     = ["10.0.1.0/24"]
}

resource "azurerm_network_security_group" "energy_nsg" {
  name                = "nsg-energy-lakehouse"
  location            = azurerm_resource_group.energy.location
  resource_group_name = azurerm_resource_group.energy.name

  # SSH access
  security_rule {
    name                       = "AllowSSH"
    priority                   = 100
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     = "22"
    source_address_prefix      = var.allowed_ip
    destination_address_prefix = "*"
  }

  # Airflow WebUI
  security_rule {
    name                       = "AllowAirflow"
    priority                   = 200
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     = "8080"
    source_address_prefix      = var.allowed_ip
    destination_address_prefix = "*"
  }

  # Streamlit Dashboard
  security_rule {
    name                       = "AllowStreamlit"
    priority                   = 300
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     = "8501"
    source_address_prefix      = var.allowed_ip
    destination_address_prefix = "*"
  }
}

# Public IP & NIC
resource "azurerm_public_ip" "energy_pip" {
  name                = "pip-energy-vm"
  location            = azurerm_resource_group.energy.location
  resource_group_name = azurerm_resource_group.energy.name
  allocation_method   = "Static"
  sku                 = "Standard"
}

resource "azurerm_network_interface" "energy_nic" {
  name                = "nic-energy-vm"
  location            = azurerm_resource_group.energy.location
  resource_group_name = azurerm_resource_group.energy.name

  ip_configuration {
    name                          = "internal"
    subnet_id                     = azurerm_subnet.energy_subnet.id
    private_ip_address_allocation = "Dynamic"
    public_ip_address_id          = azurerm_public_ip.energy_pip.id
  }
}

resource "azurerm_network_interface_security_group_association" "energy_nic_nsg" {
  network_interface_id      = azurerm_network_interface.energy_nic.id
  network_security_group_id = azurerm_network_security_group.energy_nsg.id
}

# Virtual Machine (runs Docker Compose)
resource "azurerm_linux_virtual_machine" "energy_vm" {
  name                  = "vm-energy-lakehouse"
  location              = azurerm_resource_group.energy.location
  resource_group_name   = azurerm_resource_group.energy.name
  size                  = var.vm_size
  admin_username        = var.admin_username
  network_interface_ids = [azurerm_network_interface.energy_nic.id]

  admin_ssh_key {
    username   = var.admin_username
    public_key = file(var.ssh_public_key_path)
  }

  os_disk {
    caching              = "ReadWrite"
    storage_account_type = "Standard_LRS"
    disk_size_gb         = 64
  }

  source_image_reference {
    publisher = "Canonical"
    offer     = "0001-com-ubuntu-server-jammy"
    sku       = "22_04-lts-gen2"
    version   = "latest"
  }

  custom_data = base64encode(<<-EOF
    #!/bin/bash
    set -e

    echo "======================== START SETUP ========================"
    sudo apt-get -y update
    sudo apt-get -y install ca-certificates curl gnupg lsb-release make

    # Install Docker
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg \
      | sudo gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg

    echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] \
      https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" \
      | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

    sudo apt-get -y update
    sudo apt-get -y install docker-ce docker-ce-cli containerd.io docker-compose-plugin
    sudo chmod 666 /var/run/docker.sock
    sudo usermod -aG docker ${var.admin_username}

    # Clone & Launch
    cd /home/${var.admin_username}
    git clone ${var.repo_url} Pan-European-Energy-Lakehouse
    cd Pan-European-Energy-Lakehouse
    make up

    echo "======================== END SETUP ========================"
  EOF
  )

  tags = {
    project     = "Pan-European-Energy-Lakehouse"
    environment = var.environment
  }
}

# Azure Data Lake Storage Gen2
resource "azurerm_storage_account" "energy_lake" {
  name                     = var.storage_account_name
  resource_group_name      = azurerm_resource_group.energy.name
  location                 = azurerm_resource_group.energy.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"
  is_hns_enabled           = true # Hierarchical Namespace = ADLS Gen2

  tags = {
    project     = "Pan-European-Energy-Lakehouse"
    environment = var.environment
    layer       = "data-lake"
  }
}

# Data Lake file systems (containers)
resource "azurerm_storage_data_lake_gen2_filesystem" "bronze" {
  name               = "bronze"
  storage_account_id = azurerm_storage_account.energy_lake.id
}

resource "azurerm_storage_data_lake_gen2_filesystem" "silver" {
  name               = "silver"
  storage_account_id = azurerm_storage_account.energy_lake.id
}

resource "azurerm_storage_data_lake_gen2_filesystem" "gold" {
  name               = "gold"
  storage_account_id = azurerm_storage_account.energy_lake.id
}

# Budget Alert
resource "azurerm_consumption_budget_resource_group" "energy_budget" {
  name              = "budget-energy-monthly"
  resource_group_id = azurerm_resource_group.energy.id
  amount            = 30
  time_grain        = "Monthly"

  time_period {
    start_date = "2026-01-01T00:00:00Z"
    end_date   = "2028-12-31T00:00:00Z"
  }

  notification {
    enabled        = true
    threshold      = 80
    operator       = "GreaterThan"
    threshold_type = "Forecasted"

    contact_emails = [var.alert_email]
  }
}
