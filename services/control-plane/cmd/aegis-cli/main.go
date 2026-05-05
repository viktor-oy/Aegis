package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/aegis/aegis/services/control-plane/internal/incident"
	"github.com/aegis/aegis/services/control-plane/internal/kafka"
	"github.com/aegis/aegis/services/control-plane/internal/membership"
	"github.com/aegis/aegis/services/control-plane/internal/state"
)

func main() {
	if len(os.Args) < 2 {
		printUsageAndExit()
	}

	subcommand := os.Args[1]
	if subcommand != "resolve" {
		fmt.Fprintf(os.Stderr, "unknown subcommand: %s\n", subcommand)
		printUsageAndExit()
	}

	resolveFlags := flag.NewFlagSet("resolve", flag.ExitOnError)
	fixCorruptFSM := resolveFlags.Bool("fix-corrupt-fsm", false, "Reset FSM state to HEALTHY after clearing stuck/corrupt flags")
	force := resolveFlags.Bool("force", false, "Ignore transition validation errors and forcefully overwrite state in Redis")
	targetStateStr := resolveFlags.String("state", "", "Force a specific target state (must be used with --force)")
	redisAddr := resolveFlags.String("redis-addr", getEnv("AEGIS_REDIS_ADDR", "localhost:6379"), "Redis server address")
	kafkaBrokersStr := resolveFlags.String("kafka-brokers", getEnv("AEGIS_KAFKA_BROKERS", "localhost:9092"), "Kafka broker addresses (comma-separated)")

	_ = resolveFlags.Parse(os.Args[2:])
	args := resolveFlags.Args()
	if len(args) < 2 {
		fmt.Fprintf(os.Stderr, "error: resolve requires <worker_id> and <error_type>\n")
		resolveFlags.Usage()
		os.Exit(1)
	}

	workerID := args[0]
	errorType := args[1]

	var validStates = []state.WorkerHealthState{
		state.WorkerHealthy,
		state.WorkerSuspected,
		state.WorkerDiagnosticsTriggered,
		state.WorkerDiagnosticsCollected,
		state.WorkerPostmortemRequested,
		state.WorkerPostmortemGenerated,
		state.WorkerDeliveryInProgress,
		state.WorkerDelivered,
		state.WorkerDeliveryFailed,
		state.WorkerResolved,
	}

	if *targetStateStr != "" {
		if !*force {
			fmt.Fprintf(os.Stderr, "error: --state requires --force to be used\n")
			os.Exit(1)
		}
		
		isValid := false
		for _, s := range validStates {
			if string(s) == *targetStateStr {
				isValid = true
				break
			}
		}
		if !isValid {
			fmt.Fprintf(os.Stderr, "error: invalid state string: %s\n", *targetStateStr)
			os.Exit(1)
		}

		fmt.Printf("WARNING: You are forcefully setting the state to %s.\nAre you sure you want to proceed? [y/N]: ", *targetStateStr)
		var response string
		_, err := fmt.Scanln(&response)
		if err != nil || strings.ToLower(strings.TrimSpace(response)) != "y" {
			fmt.Println("Aborting.")
			os.Exit(0)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store := membership.NewRedisStore(*redisAddr)
	pub := kafka.NewKafkaPublisher(strings.Split(*kafkaBrokersStr, ","))

	err := RunResolve(ctx, store, pub, workerID, errorType, *fixCorruptFSM, *force, *targetStateStr, time.Now().UTC())
	if err != nil {
		slog.Error("resolve failed", "error", err)
		os.Exit(1)
	}

	fmt.Printf("Successfully resolved incident for worker=%s error_type=%s (fix_corrupt_fsm=%v, force=%v, state=%s)\n", workerID, errorType, *fixCorruptFSM, *force, *targetStateStr)
}

func printUsageAndExit() {
	fmt.Fprintf(os.Stderr, "Usage: aegis-cli <subcommand> [flags] [args...]\n")
	fmt.Fprintf(os.Stderr, "Subcommands:\n")
	fmt.Fprintf(os.Stderr, "  resolve <worker_id> <error_type> [--fix-corrupt-fsm] [--force] [--state <STATE>]\n")
	os.Exit(1)
}

func getEnv(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}

// RunResolve executes the SRE administrative resolve workflow.
func RunResolve(ctx context.Context, store incident.Store, pub kafka.Publisher, workerID, errorType string, fixCorruptFSM, force bool, targetStateStr string, now time.Time) error {
	if workerID == "" || errorType == "" {
		return errors.New("worker_id and error_type are required")
	}

	existing, err := store.GetWorkerState(ctx, workerID, errorType)
	if err != nil {
		return fmt.Errorf("get worker state: %w", err)
	}

	if fixCorruptFSM && (existing == nil || existing.CurrentState == "") {
		return errors.New("cannot fix corrupt FSM: no existing state found for this worker")
	}

	fromState := state.WorkerHealthy
	incidentID := fmt.Sprintf("manual-resolve-%s-%d", workerID, now.Unix())
	correlationID := fmt.Sprintf("corr-cli-%d", now.UnixNano())

	if existing != nil && existing.CurrentState != "" {
		fromState = state.WorkerHealthState(existing.CurrentState)
		if existing.IncidentID != "" {
			incidentID = existing.IncidentID
		}
		if existing.CorrelationID != "" {
			correlationID = existing.CorrelationID
		}
	}

	toState := state.WorkerResolved
	if fixCorruptFSM {
		toState = state.WorkerHealthy
	}
	if targetStateStr != "" && force {
		toState = state.WorkerHealthState(targetStateStr)
	}

	if !force && !fixCorruptFSM && fromState != toState {
		if !incident.ValidTransition(fromState, toState) {
			return fmt.Errorf("illegal FSM transition from %s to %s; use --force to override", fromState, toState)
		}
	}

	// Clear any DLQ marker in Redis
	if err := store.DeleteDLQMarker(ctx, workerID, errorType); err != nil {
		return fmt.Errorf("delete dlq marker: %w", err)
	}

	// Set authoritative Redis state
	st := membership.WorkerState{
		WorkerID:      workerID,
		ErrorType:     errorType,
		IncidentID:    incidentID,
		CurrentState:  string(toState),
		CorrelationID: correlationID,
		UpdatedAt:     now,
	}
	if err := store.SetWorkerState(ctx, st); err != nil {
		return fmt.Errorf("set worker state: %w", err)
	}

	// Publish repair tombstone event to DLQ topic
	if pub != nil {
		inc := state.Incident{
			IncidentID:    incidentID,
			WorkerID:      workerID,
			FailureType:   state.FailureType(errorType),
			State:         toState,
			CorrelationID: correlationID,
		}
		causationID := fmt.Sprintf("%s:%s", workerID, errorType)
		env := kafka.NewEnvelope("aegis.cp.corrupt-fsm.repair", inc, "aegis-cli", causationID, map[string]any{
			"action":          "resolve",
			"fix_corrupt_fsm": fixCorruptFSM,
			"force":           force,
			"new_state":       string(toState),
			"from_state":      string(fromState),
		}, now)
		_ = pub.Publish(ctx, incident.TopicCorruptFSMDLQ, env)
	}

	return nil
}
