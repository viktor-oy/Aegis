output "namespace" {
  value       = kubernetes_namespace.aegis_system.metadata[0].name
  description = "The namespace created for Aegis"
}
