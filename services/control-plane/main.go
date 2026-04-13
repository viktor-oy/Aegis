package main

import (
	"context"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"runtime"
	"runtime/trace"
	"strconv"
	"strings"
	"syscall"
	"time"

	aegisv1 "github.com/aegis/aegis/gen/go/aegis/v1"
	"github.com/aegis/aegis/services/control-plane/internal/hashring"
	"github.com/aegis/aegis/services/control-plane/internal/incident"
	"github.com/aegis/aegis/services/control-plane/internal/kafka"
	"github.com/aegis/aegis/services/control-plane/internal/membership"
	"github.com/aegis/aegis/services/control-plane/internal/server"
	"github.com/aegis/aegis/services/control-plane/internal/state"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
)

type localDiagnostics struct{}

func (localDiagnostics) TriggerDiagnostics(_ context.Context, req state.DiagnosticRequest) (state.DiagnosticBundle, error) {
	return state.DiagnosticBundle{
		WorkerID:         req.WorkerID,
		IncidentID:       req.IncidentID,
		CollectedAt:      time.Now().UTC(),
		DiagnosticStatus: "partial",
		Payload: map[string]any{
			"note": "local MVP diagnostic adapter; production uses gRPC AgentDiagnostics",
		},
		CorrelationID: req.CorrelationID,
	}, nil
}

func main() {
	if traceFile := os.Getenv("AEGIS_TRACE_FILE"); traceFile != "" {
		f, err := os.Create(traceFile)
		if err != nil {
			slog.Error("failed to create trace file", "error", err)
			os.Exit(1)
		}
		defer f.Close()
		if err := trace.Start(f); err != nil {
			slog.Error("failed to start trace", "error", err)
			os.Exit(1)
		}
		defer trace.Stop()
	}

	// Setup structured logging to stdout.
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	id := requireEnv("AEGIS_CP_ID")
	address := requireEnv("AEGIS_CP_ADDRESS")
	grpcAddress := requireEnv("AEGIS_GRPC_ADDRESS")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	redisAddr := requireEnv("AEGIS_REDIS_ADDR")
	kafkaBrokers := requireEnv("AEGIS_KAFKA_BROKERS")

	store := membership.NewRedisStore(redisAddr)
	member := hashring.Member{ID: id, Address: address}
	now := time.Now().UTC()
	if err := store.Register(context.Background(), member, 30*time.Second, now); err != nil {
		slog.Error("failed to register CP member", "error", err)
		os.Exit(1)
	}
	members, err := store.ActiveMembers(context.Background(), now)
	if err != nil {
		slog.Error("failed to find any active member which is needed to build the hashring", "error", err)
		os.Exit(1)
	}

	ring, err := hashring.New(members, 128)
	if err != nil {
		slog.Error("failed to build hash ring", "error", err)
		os.Exit(1)
	}

	queueSizeStr := os.Getenv("AEGIS_QUEUE_SIZE")
	queueSize, err := strconv.Atoi(queueSizeStr)
	if err != nil {
		queueSize = 256
	}

	publisher := kafka.NewKafkaPublisher(strings.Split(kafkaBrokers, ","))
	manager := incident.NewManager(store, publisher, localDiagnostics{}, "aegis-control-plane/"+id)
	cp := server.New(id, address, ring, manager, queueSize, 15*time.Second)

	// Multiconcurrency for telemetry ingestion.
	numWorkers := runtime.NumCPU()
	for i := 0; i < numWorkers; i++ {
		go telemetryLoop(ctx, cp)
	}

	// Background heartbeat expiry loop.
	go heartbeatLoop(ctx, cp)

	// Background membership refresh and ring rebuild loop.
	go membershipLoop(ctx, member, store, cp)

	// gRPC server.
	lis, err := net.Listen("tcp", grpcAddress)
	if err != nil {
		slog.Error("failed to listen", "address", grpcAddress, "error", err)
		os.Exit(1)
	}

	grpcServer := grpc.NewServer()
	aegisv1.RegisterControlPlaneTelemetryServer(grpcServer, server.NewGRPCServer(cp))
	if os.Getenv("AEGIS_DEBUG") == "true" {
		reflection.Register(grpcServer)
	}

	// gRPC health service for Kubernetes probes (k8s 1.24+ supports native gRPC health checks).
	// TODO: impl fallback for older k8s or other envs(e.g. docker compose)
	healthServer := health.NewServer()
	healthpb.RegisterHealthServer(grpcServer, healthServer)
	healthServer.SetServingStatus("aegis.v1.ControlPlaneTelemetry", healthpb.HealthCheckResponse_SERVING)

	// Graceful shutdown on SIGTERM / SIGINT.
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
		<-sig
		slog.Info("shutting down gRPC server")
		healthServer.SetServingStatus("aegis.v1.ControlPlaneTelemetry", healthpb.HealthCheckResponse_NOT_SERVING)
		grpcServer.GracefulStop()
		cancel()
	}()

	slog.Info("aegis control-plane listening", "id", id, "address", grpcAddress, "protocol", "gRPC")
	if err := grpcServer.Serve(lis); err != nil {
		slog.Error("gRPC server exited", "error", err)
		os.Exit(1)
	}
}

func telemetryLoop(ctx context.Context, cp *server.ControlPlane) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for {
				processed, err := cp.ProcessOne(ctx)
				if err != nil {
					slog.Error("telemetry process error", "error", err)
					break
				}
				if !processed {
					break
				}
			}
		}
	}
}

func heartbeatLoop(ctx context.Context, cp *server.ControlPlane) {
	const heartbeatFallback = 5 * time.Second

	// heartbeatTimer fires at the next worker deadline (or at fallback if idle).
	nextDeadline, hasDeadline := cp.NextHeartbeatDeadline()
	var heartbeatDelay time.Duration
	if hasDeadline {
		heartbeatDelay = time.Until(nextDeadline.DeadlineAt)
		if heartbeatDelay < 0 {
			heartbeatDelay = 0
		}
	} else {
		heartbeatDelay = heartbeatFallback
	}
	heartbeatTimer := time.NewTimer(heartbeatDelay)
	defer heartbeatTimer.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-heartbeatTimer.C:
			now := time.Now().UTC()
			if err := cp.ExpireHeartbeats(ctx, now); err != nil {
				slog.Error("heartbeat expiry error", "error", err)
			}

			// Reset the timer to fire at the updated min-heap root deadline.
			nextDeadline, hasDeadline = cp.NextHeartbeatDeadline()
			if hasDeadline {
				heartbeatDelay = time.Until(nextDeadline.DeadlineAt)
				if heartbeatDelay < 0 {
					heartbeatDelay = 0
				}
			} else {
				heartbeatDelay = heartbeatFallback
			}
			heartbeatTimer.Reset(heartbeatDelay)
		}
	}
}

func membershipLoop(ctx context.Context, member hashring.Member, store membership.Store, cp *server.ControlPlane) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			now := time.Now().UTC()
			_ = store.Refresh(ctx, member, 30*time.Second, now)
			members, err := store.ActiveMembers(ctx, now)
			if err == nil {
				ring, err := hashring.New(members, 128)
				if err == nil {
					cp.UpdateRing(ring)
				}
			}
		}
	}
}

func requireEnv(key string) string {
	value := os.Getenv(key)
	if value == "" {
		slog.Error("required environment variable missing", "key", key)
		os.Exit(1)
	}
	return value
}
