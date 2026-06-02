FROM golang:1.25.5-bookworm AS build

WORKDIR /workspace
COPY go.mod go.sum ./
RUN go mod download
COPY gen ./gen
COPY services/control-plane ./services/control-plane
COPY services/pkg ./services/pkg
RUN go build -o /out/aegis-control-plane ./services/control-plane

FROM gcr.io/distroless/base-debian12:nonroot

COPY --from=build /out/aegis-control-plane /aegis-control-plane

# Runtime environment variable declarations (values supplied at deploy time via
# Kubernetes Secrets/ConfigMaps or docker run -e). Documented here for reference.
ENV AEGIS_CP_ID=
ENV AEGIS_CP_ADDRESS=
ENV AEGIS_GRPC_ADDRESS=
ENV AEGIS_REDIS_ADDR=
ENV AEGIS_KAFKA_BROKERS=
ENV AEGIS_QUEUE_SIZE=
ENV AEGIS_TRACE_FILE=
ENV AEGIS_DEBUG=

USER nonroot:nonroot
EXPOSE 50051
ENTRYPOINT ["/aegis-control-plane"]
# Force rebuild
