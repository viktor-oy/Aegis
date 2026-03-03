variable "kubeconfig_path" {
  description = "Path to the kubeconfig used for the local validation cluster."
  type        = string
  default     = "~/.kube/config"
}

