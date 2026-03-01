terraform {
  required_version = ">= 1.1.0"
  required_providers {
    kind = {
      source  = "tehcyx/kind"
      version = "~> 0.7.0"
    }
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.11"
    }
    null = {
      source  = "hashicorp/null"
      version = "~> 3.2"
    }
  }
}

provider "kind" {}

resource "kind_cluster" "aegis" {
  name       = terraform.workspace == "default" ? "aegis" : "aegis-${terraform.workspace}"
  node_image = "kindest/node:v1.34.8"
  kind_config {
    kind        = "Cluster"
    api_version = "kind.x-k8s.io/v1alpha4"
    containerd_config_patches = [
      <<-TOML
      [plugins."io.containerd.grpc.v1.cri".registry.mirrors."localhost:5001"]
        endpoint = ["http://kind-registry:5000"]
      TOML
    ]
    node {
      role = "control-plane"
    }
  }
}

provider "kubernetes" {
  host                   = kind_cluster.aegis.endpoint
  client_certificate     = kind_cluster.aegis.client_certificate
  client_key             = kind_cluster.aegis.client_key
  cluster_ca_certificate = kind_cluster.aegis.cluster_ca_certificate
}

resource "kubernetes_namespace" "aegis_system" {
  metadata {
    name = "aegis-system"
  }
  depends_on = [kind_cluster.aegis]
}

resource "null_resource" "registry_setup" {
  triggers = {
    cluster_id = kind_cluster.aegis.id
  }

  provisioner "local-exec" {
    command = <<EOT
      echo "Starting local registry..."
      if ! docker inspect kind-registry > /dev/null 2>&1; then
        docker run -d --restart=always -p 127.0.0.1:5001:5000 --name kind-registry registry:2.8.3
      else
        echo "Registry already running."
      fi
      echo "Connecting registry to kind network..."
      docker network connect kind kind-registry 2>/dev/null || true
      echo "Annotating nodes for local registry discovery..."
      cluster_name="${terraform.workspace == "default" ? "aegis" : "aegis-${terraform.workspace}"}"
      for node in $(kind get nodes --name "$cluster_name"); do
        kubectl annotate node "$node" tilt.dev/registry=localhost:5001 --overwrite
      done
    EOT
  }
}

resource "null_resource" "tilt_restart_hook" {
  triggers = {
    cluster_id = kind_cluster.aegis.id
  }

  provisioner "local-exec" {
    command = "pkill tilt || true"
  }

  depends_on = [null_resource.registry_setup]
}
