FROM golang:1.25.7-bookworm AS build

WORKDIR /workspace
COPY go.mod ./
COPY services/sink-workers ./services/sink-workers
RUN go build -o /out/aegis-sink-worker ./services/sink-workers/cmd/aegis-sink-worker

FROM gcr.io/distroless/base-debian12:nonroot

COPY --from=build /out/aegis-sink-worker /aegis-sink-worker
USER nonroot:nonroot
ENTRYPOINT ["/aegis-sink-worker"]

