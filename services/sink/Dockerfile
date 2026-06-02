# syntax=docker/dockerfile:1
FROM golang:1.25 AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY gen ./gen
COPY services/sink ./services/sink
COPY services/pkg ./services/pkg
# Build the sink binary
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /bin/aegis-sink ./services/sink

FROM gcr.io/distroless/static-debian12:latest

# Documented configuration variables
ENV AEGIS_DEBUG=""
ENV AEGIS_KAFKA_BROKERS=""
ENV AEGIS_FILE_SINK_ENABLED=""
ENV AEGIS_FILE_SINK_PATH=""
ENV AEGIS_EMAIL_SINK_ENABLED=""
ENV AEGIS_SMTP_HOST=""
ENV AEGIS_SMTP_PORT=""
ENV AEGIS_SMTP_USER=""
ENV AEGIS_SMTP_PASS=""
ENV AEGIS_SMTP_TO=""
ENV AEGIS_SMTP_FROM=""

COPY --from=builder /bin/aegis-sink /aegis-sink
ENTRYPOINT ["/aegis-sink"]
