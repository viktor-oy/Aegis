# Aegis Version Ledger

Project window: 2025-12-14 through 2026-05-13.

This ledger records the pinned versions used by the repository. Pins avoid floating tags and were chosen from versions available no later than 2026-05-13.

| Area | Pin | Used in | Release/date note |
| --- | --- | --- | --- |
| Go | `1.25.7` | `.tool-versions`, Go images | Go 1.25 was released in August 2025 and remained a supported stable line during the project. |
| Python | `3.13.13` | `.tool-versions`, Python images | Python.org lists Python 3.13.13 with an April 7, 2026 release date. |
| Kubernetes | `1.34.8` | Kind/Kubectl guidance | Kubernetes 1.34 was released August 27, 2025; 1.34.8 was available on May 12, 2026. |
| Helm | `4.1.3` | Devcontainer, docs | Helm 4 was released November 12, 2025 and was the current stable major line during the project. |
| Kustomize | `5.8.1` | Devcontainer, docs | Kustomize v5.8.1 was available before project close. |
| Terraform | `1.12.2` | Devcontainer, optional IaC | Pinned to an available 1.12 patch during the project window. |
| Kafka | `4.1.1` | Helm values, local K8s | Apache download indexes show Kafka 4.1.1 artifacts dated November 12, 2025. |
| Redis | `8.0.2` | Helm values, local K8s | Redis release index lists `redis-8.0.2.tar.gz` dated May 27, 2025. |
| PostgreSQL | `18.2` | Helm values, local K8s | PostgreSQL 18 GA was released September 25, 2025; 18.2 was available in February 2026. |
| MinIO | `RELEASE.2025-04-22T22-12-26Z` | Helm values | Last widely published community image suitable for the MVP S3-compatible local target. |
| OpenTelemetry Collector | `0.151.0` | Observability manifests | Collector release metadata shows v0.151.0 before the project close; v0.153.0 landed after the project window. |
| KEDA | `2.19.0` | ScaledObject manifests | KEDA 2.19.0 was released February 2, 2026. |
| Grafana Loki | `3.6.11` | Optional LGTM values | Loki 3.6.11 was available May 13, 2026. |

Dependency policy:

- Do not use `latest` image tags.
- Do not upgrade a dependency past the commit date that introduces it.
- Runtime service code uses small adapter interfaces, so client libraries can be upgraded without changing deterministic domain logic.
- Generated protobuf outputs must be reproducible from the pinned `protoc` and plugin versions documented in `.devcontainer/devcontainer.json`.

