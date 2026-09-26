terraform {
  required_version = ">= 1.1.0"
  required_providers {
    kind = {
      source  = "tehcyx/kind"
      version = "~> 0.7.0"
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
      extra_mounts {
        host_path      = pathexpand("~/.aegis-ollama-cache")
        container_path = "/root/.ollama"
      }
    }

    dynamic "node" {
      for_each = range(var.node_count > 1 ? var.node_count - 1 : 0)
      content {
        role = "worker"
        extra_mounts {
          host_path      = pathexpand("~/.aegis-ollama-cache")
          container_path = "/root/.ollama"
        }
      }
    }
  }
}

# Hack / Gotcha (inotify limits): We run ~15 microservices concurrently during testing, so Tilt tries to stream logs for all of them at once. 
# This instantly blows past the default Linux fs.inotify.max_user_instances limit (128) on the Kind node.
# Instead of forcing everyone to manually bump sysctl limits on their Mac or Docker VM, this provisioner sneaks in a dynamic sysctl hack 
# directly inside the Kind node container before handing off to Tilt.
resource "null_resource" "inotify_hack" {
  depends_on = [kind_cluster.aegis]
  triggers = {
    cluster_id = kind_cluster.aegis.id
  }

  provisioner "local-exec" {
    command = <<-EOT
      docker ps -q -f name="^${kind_cluster.aegis.name}-" | xargs -I {} docker exec {} sysctl -w fs.inotify.max_user_watches=524288 fs.inotify.max_user_instances=512
    EOT
  }
}

resource "null_resource" "aegis_system_namespace" {
  depends_on = [kind_cluster.aegis]
  triggers = {
    cluster_id = kind_cluster.aegis.id
  }

  provisioner "local-exec" {
    command = "kubectl create namespace aegis-system --context kind-${kind_cluster.aegis.name} --dry-run=client -o yaml | kubectl apply -f -"
  }
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
        kubectl --context "kind-$cluster_name" annotate node "$node" tilt.dev/registry=localhost:5001 --overwrite
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
