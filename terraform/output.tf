# Azure Outputs

output "resource_group_name" {
  description = "The name of the resource group"
  value       = azurerm_resource_group.energy.name
}

output "vm_public_ip" {
  description = "Public IP address of the VM"
  value       = azurerm_public_ip.energy_pip.ip_address
}

output "airflow_url" {
  description = "URL to access the Airflow Web UI"
  value       = "http://${azurerm_public_ip.energy_pip.ip_address}:8080"
}

output "streamlit_url" {
  description = "URL to access the Streamlit Dashboard"
  value       = "http://${azurerm_public_ip.energy_pip.ip_address}:8501"
}

output "adls_gen2_endpoint" {
  description = "ADLS Gen2 primary DFS endpoint"
  value       = azurerm_storage_account.energy_lake.primary_dfs_endpoint
}

output "storage_account_name" {
  description = "Name of the ADLS Gen2 Storage Account"
  value       = azurerm_storage_account.energy_lake.name
}
