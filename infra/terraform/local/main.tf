terraform {
  required_version = "= 1.12.2"

  required_providers {
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "2.37.1"
    }
    helm = {
      source  = "hashicorp/helm"
      version = "3.0.2"
    }
  }
}

provider "kubernetes" {
  config_path = var.kubeconfig_path
}

provider "helm" {
  kubernetes = {
    config_path = var.kubeconfig_path
  }
}

resource "kubernetes_namespace" "aegis" {
  metadata {
    name = "aegis-system"
  }
}

resource "helm_release" "aegis" {
  name       = "aegis"
  namespace  = kubernetes_namespace.aegis.metadata[0].name
  chart      = "${path.module}/../../helm/aegis"
  wait       = false
  dependency_update = false
}

