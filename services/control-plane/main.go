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
	"sync"
	"syscall"
	"time"

	aegisv1 "github.com/aegis/aegis/gen/go/aegis/v1"
	"github.com/aegis/aegis/services/control-plane/internal/hashring"
	"github.com/aegis/aegis/services/control-plane/internal/incident"
	"github.com/aegis/aegis/services/control-plane/internal/kafka"
	"github.com/aegis/aegis/services/control-plane/internal/membership"
	"github.com/aegis/aegis/services/control-plane/internal/server"
	"github.com/aegis/aegis/services/control-plane/internal/state"
	"github.com/aegis/aegis/services/pkg/logger"
	kafkago "github.com/segmentio/kafka-go"
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
			slog.Error("failed to create trace file", "component", "MAIN", "event", "TRACE_ERR", "error", err)
			os.Exit(1)
		}
		defer f.Close()
		if err := trace.Start(f); err != nil {
			slog.Error("failed to start trace", "component", "MAIN", "event", "TRACE_ERR", "error", err)
			os.Exit(1)
		}
		defer trace.Stop()
	}

	// Setup structured logging to stdout.
	dbg := strings.ToLower(os.Getenv("AEGIS_DEBUG"))
	logger.Setup(dbg == "true" || dbg == "1")

	id := requireEnv("AEGIS_CP_ID")
	address := requireEnv("AEGIS_CP_ADDRESS")
	grpcAddress := requireEnv("AEGIS_GRPC_ADDRESS")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	etcdUrls := os.Getenv("AEGIS_ETCD_ENDPOINTS")
	if etcdUrls == "" {
		etcdUrls = "http://localhost:2379"
	}
	kafkaBrokers := requireEnv("AEGIS_KAFKA_BROKERS")
	store, err := membership.NewEtcdStore(strings.Split(etcdUrls, ","))
	if err != nil {
		slog.Error("failed to connect to etcd", "error", err)
		os.Exit(1)
	}
	member := hashring.Member{ID: id, Address: address}
	now := time.Now().UTC()
	if err := store.Register(context.Background(), member, 30*time.Second, now); err != nil {
		slog.Error("failed to register CP member", "component", "MAIN", "event", "CP_REGISTER_ERR", "error", err)
		os.Exit(1)
	}
	members, err := store.ActiveMembers(context.Background(), now)
	if err != nil {
		slog.Error("failed to find any active member which is needed to build the hashring", "component", "MAIN", "event", "CP_DISCOVERY_ERR", "error", err)
		os.Exit(1)
	}

	ring, err := hashring.New(members, 128)
	if err != nil {
		slog.Error("failed to build hash ring", "component", "MAIN", "event", "RING_BUILD_ERR", "error", err)
		os.Exit(1)
	}

	queueSizeStr := os.Getenv("AEGIS_QUEUE_SIZE")
	queueSize, err := strconv.Atoi(queueSizeStr)
	if err != nil {
		queueSize = 256
	}

	incidentIDTumblingWindowStr := os.Getenv("AEGIS_INCIDENT_ID_TUMBLING_WINDOW")
	incidentIDTumblingWindow, err := time.ParseDuration(incidentIDTumblingWindowStr)
	if err != nil || incidentIDTumblingWindow <= 0 {
		incidentIDTumblingWindow = time.Minute
	}

	heartbeatExpiryStr := os.Getenv("AEGIS_HEARTBEAT_EXPIRY_SECONDS")
	heartbeatExpiry, err := strconv.Atoi(heartbeatExpiryStr)
	if err != nil || heartbeatExpiry <= 0 {
		heartbeatExpiry = 15
	}

	publisher := kafka.NewKafkaPublisher(strings.Split(kafkaBrokers, ","))
	manager := incident.NewManager(store, publisher, localDiagnostics{}, state.CreateProducerRef(id), incidentIDTumblingWindow)
	cp := server.New(id, address, ring, manager, queueSize, time.Duration(heartbeatExpiry)*time.Second)

	var wg sync.WaitGroup

	// Multiconcurrency for telemetry ingestion.
	numWorkers := runtime.NumCPU()
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			telemetryLoop(ctx, cp)
		}()
	}

	// Background heartbeat expiry loop.
	wg.Add(1)
	go func() {
		defer wg.Done()
		heartbeatLoop(ctx, cp)
	}()

	// Background FSM consumer loop for downstream pipeline state tracking.
	fsmConsumer := incident.NewFSMConsumer(store, publisher, state.CreateProducerRef(id))
	groupID := os.Getenv("AEGIS_FSM_GROUP_ID")
	if groupID == "" {
		groupID = "aegis-cp-fsm-group"
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		fsmConsumerLoop(ctx, fsmConsumer, strings.Split(kafkaBrokers, ","), groupID)
	}()

	// Background FSM indefinite state watchdog for stuck incident and corrupt marker alerting.
	watchdog := incident.NewWatchdog(store, publisher, state.CreateProducerRef(id), id)
	wg.Add(1)
	go func() {
		defer wg.Done()
		watchdog.Start(ctx)
	}()

	// Background membership refresh and ring rebuild loop.
	wg.Add(1)
	go func() {
		defer wg.Done()
		membershipLoop(ctx, member, store, cp, watchdog)
	}()

	// gRPC server.
	lis, err := net.Listen("tcp", grpcAddress)
	if err != nil {
		slog.Error("failed to listen", "component", "GRPC_SERVER", "event", "LISTEN_ERR", "address", grpcAddress, "error", err)
		os.Exit(1)
	}

	var serverOpts []grpc.ServerOption
	if sizeStr := os.Getenv("AEGIS_GRPC_MAX_MSG_SIZE"); sizeStr != "" {
		if size, err := strconv.Atoi(sizeStr); err == nil {
			serverOpts = append(serverOpts, grpc.MaxRecvMsgSize(size))
		}
	}
	grpcServer := grpc.NewServer(serverOpts...)
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
		slog.Info("shutting down gRPC server", "component", "GRPC_SERVER", "event", "SHUTDOWN")
		healthServer.SetServingStatus("aegis.v1.ControlPlaneTelemetry", healthpb.HealthCheckResponse_NOT_SERVING)
		grpcServer.GracefulStop()
		cancel()
		wg.Wait()
	}()

	slog.Info("aegis control-plane listening", "component", "GRPC_SERVER", "event", "STARTING", "id", id, "address", grpcAddress, "protocol", "gRPC")
	if err := grpcServer.Serve(lis); err != nil {
		slog.Error("gRPC server exited", "component", "GRPC_SERVER", "event", "EXIT_ERR", "error", err)
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
					slog.Error("telemetry process error", "component", "TELEMETRY_WORKER", "event", "PROCESS_ERR", "error", err)
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
			monotonicNow := time.Now()
			if err := cp.ExpireHeartbeats(ctx, monotonicNow); err != nil {
				slog.Error("heartbeat expiry error", "component", "HEARTBEAT_WORKER", "event", "EXPIRY_ERR", "error", err)
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

func membershipLoop(ctx context.Context, member hashring.Member, store membership.Store, cp *server.ControlPlane, watchdog *incident.Watchdog) {
	intervalStr := os.Getenv("AEGIS_MEMBERSHIP_INTERVAL")
	interval := 10 * time.Second
	if intervalStr != "" {
		if d, err := time.ParseDuration(intervalStr); err == nil {
			interval = d
		}
	}
	ticker := time.NewTicker(interval)
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
					if watchdog != nil {
						watchdog.UpdateRing(ring)
					}
				}
			}
		}
	}
}

func requireEnv(key string) string {
	value := os.Getenv(key)
	if value == "" {
		slog.Error("required environment variable missing", "component", "MAIN", "event", "ENV_ERR", "key", key)
		os.Exit(1)
	}
	return value
}

func fsmConsumerLoop(ctx context.Context, consumer *incident.FSMConsumer, brokers []string, groupID string) {
	dialer := &net.Dialer{
		Timeout:   10 * time.Second,
		DualStack: true,
		KeepAlive: 30 * time.Second,
	}

	topics := []string{
		incident.TopicPostmortemGenerated,
		incident.TopicDeliveryStatus,
		incident.TopicDeliveryDLQ,
	}

	for _, topic := range topics {
		go func(t string) {
			reader := kafkago.NewReader(kafkago.ReaderConfig{
				Brokers:     brokers,
				Topic:       t,
				GroupID:     groupID,
				StartOffset: kafkago.FirstOffset,
				Dialer: &kafkago.Dialer{
					Timeout:   dialer.Timeout,
					DualStack: dialer.DualStack,
					KeepAlive: dialer.KeepAlive,
				},
			})
			defer reader.Close()

			for {
				m, err := reader.FetchMessage(ctx)
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					slog.Warn("read message error", "component", "FSM_CONSUMER", "event", "KAFKA_READ_ERR", "topic", t, "error", err)
					time.Sleep(500 * time.Millisecond)
					continue
				}
				for {
					if err := consumer.ConsumeEvent(ctx, m.Topic, m.Partition, m.Offset, m.Value); err != nil {
						slog.Error("consume event error. Retrying in 5s...", "component", "FSM_CONSUMER", "event", "CONSUME_ERR", "topic", m.Topic, "error", err)
						time.Sleep(5 * time.Second)
						continue
					}
					break
				}
				if err := reader.CommitMessages(ctx, m); err != nil {
					slog.Error("Failed to commit kafka offset", "component", "KAFKA", "event", "COMMIT_ERR", "topic", t, "error", err)
				}
			}
		}(topic)
	}

	<-ctx.Done()
}
