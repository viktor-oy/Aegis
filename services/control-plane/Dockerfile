FROM golang:1.25.7-bookworm AS build

WORKDIR /workspace
COPY go.mod ./
COPY services/control-plane ./services/control-plane
RUN go build -o /out/aegis-control-plane ./services/control-plane/cmd/aegis-control-plane

FROM gcr.io/distroless/base-debian12:nonroot

COPY --from=build /out/aegis-control-plane /aegis-control-plane
USER nonroot:nonroot
EXPOSE 8080 50051
ENTRYPOINT ["/aegis-control-plane"]

