# Azure Configuration Variables

variable "location" {
  description = "Azure region for all resources"
  type        = string
  default     = "westeurope"
}

variable "resource_group_name" {
  description = "Name of the Azure Resource Group"
  type        = string
  default     = "rg-pan-euro-energy"
}

variable "environment" {
  description = "Deployment environment (dev, staging, production)"
  type        = string
  default     = "dev"
}

# VM Configuration

variable "vm_size" {
  description = "Azure VM size (Standard_B2s for dev, Standard_D4s_v3 for prod)"
  type        = string
  default     = "Standard_B2s"
}

variable "admin_username" {
  description = "Admin username for the VM"
  type        = string
  default     = "energyadmin"
}

variable "ssh_public_key_path" {
  description = "Path to SSH public key for VM access"
  type        = string
  default     = "~/.ssh/id_rsa.pub"
}

variable "allowed_ip" {
  description = "IP address allowed to access VM (CIDR format, e.g. '1.2.3.4/32')"
  type        = string
  default     = "*"
}

# Storage

variable "storage_account_name" {
  description = "Azure Storage Account name (must be globally unique, lowercase, no hyphens)"
  type        = string
  default     = "stpaneuroenergy"
}

# Notifications

variable "alert_email" {
  description = "Email address for budget alerts"
  type        = string
  default     = "[EMAIL_ADDRESS]"
}

# Repository

variable "repo_url" {
  description = "Git repository URL to clone on the VM"
  type        = string
  default     = "https://github.com/evah37/Pan-European-Energy-Lakehouse.git"
}
