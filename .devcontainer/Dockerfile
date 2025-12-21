ARG PYTHON_VERSION=3.13.13
FROM python:${PYTHON_VERSION}-slim-bookworm

ARG GO_VERSION=1.25.7
ARG KUBECTL_VERSION=1.34.8
ARG HELM_VERSION=4.1.3
ARG KUSTOMIZE_VERSION=5.8.1
ARG TERRAFORM_VERSION=1.12.2
ARG KIND_VERSION=0.29.0
ARG TILT_VERSION=0.33.22
ARG PROTOC_VERSION=29.5

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
      bash=5.2.15-2+b9 \
      ca-certificates=20230311+deb12u1 \
      curl=7.88.1-10+deb12u8 \
      git=1:2.39.5-0+deb12u1 \
      make=4.3-4.1 \
      unzip=6.0-28 \
      xz-utils=5.4.1-1 \
    && rm -rf /var/lib/apt/lists/*

RUN useradd -m -s /bin/bash vscode

RUN curl -fsSL "https://go.dev/dl/go${GO_VERSION}.linux-amd64.tar.gz" -o /tmp/go.tgz \
    && tar -C /usr/local -xzf /tmp/go.tgz \
    && rm /tmp/go.tgz

ENV PATH="/usr/local/go/bin:/home/vscode/go/bin:${PATH}"

RUN curl -fsSL "https://dl.k8s.io/release/v${KUBECTL_VERSION}/bin/linux/amd64/kubectl" -o /usr/local/bin/kubectl \
    && chmod +x /usr/local/bin/kubectl \
    && curl -fsSL "https://get.helm.sh/helm-v${HELM_VERSION}-linux-amd64.tar.gz" -o /tmp/helm.tgz \
    && tar -C /tmp -xzf /tmp/helm.tgz \
    && mv /tmp/linux-amd64/helm /usr/local/bin/helm \
    && rm -rf /tmp/helm.tgz /tmp/linux-amd64 \
    && curl -fsSL "https://github.com/kubernetes-sigs/kustomize/releases/download/kustomize%2Fv${KUSTOMIZE_VERSION}/kustomize_v${KUSTOMIZE_VERSION}_linux_amd64.tar.gz" -o /tmp/kustomize.tgz \
    && tar -C /usr/local/bin -xzf /tmp/kustomize.tgz \
    && rm /tmp/kustomize.tgz

RUN curl -fsSL "https://releases.hashicorp.com/terraform/${TERRAFORM_VERSION}/terraform_${TERRAFORM_VERSION}_linux_amd64.zip" -o /tmp/terraform.zip \
    && unzip /tmp/terraform.zip -d /usr/local/bin \
    && rm /tmp/terraform.zip \
    && curl -fsSL "https://kind.sigs.k8s.io/dl/v${KIND_VERSION}/kind-linux-amd64" -o /usr/local/bin/kind \
    && chmod +x /usr/local/bin/kind \
    && curl -fsSL "https://github.com/tilt-dev/tilt/releases/download/v${TILT_VERSION}/tilt.${TILT_VERSION}.linux.x86_64.tar.gz" -o /tmp/tilt.tgz \
    && tar -C /usr/local/bin -xzf /tmp/tilt.tgz tilt \
    && rm /tmp/tilt.tgz

RUN curl -fsSL "https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/protoc-${PROTOC_VERSION}-linux-x86_64.zip" -o /tmp/protoc.zip \
    && unzip /tmp/protoc.zip -d /usr/local \
    && rm /tmp/protoc.zip \
    && go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.0 \
    && go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1

WORKDIR /workspace/aegis

