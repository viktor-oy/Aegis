variable "environment" {
  type        = string
  description = "The deployment environment"
  default     = "local"
}

variable "node_count" {
  type        = number
  description = "Number of nodes in the kind cluster"
  default     = 1
}
