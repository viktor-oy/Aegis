package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aegis/aegis/services/control-plane/internal/incident"
	"github.com/aegis/aegis/services/control-plane/internal/kafka"
	"github.com/aegis/aegis/services/control-plane/internal/membership"
	"github.com/aegis/aegis/services/control-plane/internal/state"
	"github.com/aegis/aegis/services/pkg/logger"
)

func main() {
	if len(os.Args) < 2 {
		printUsageAndExit()
	}

	verbose := false
	nonInteractive := false
	for _, arg := range os.Args {
		if arg == "--verbose" || arg == "-v" {
			verbose = true
		}
		if arg == "--non-interactive" {
			nonInteractive = true
		}
	}

	dbg := strings.ToLower(os.Getenv("AEGIS_DEBUG"))
	debugEnabled := verbose || dbg == "true" || dbg == "1"

	if nonInteractive {
		logger.Setup(debugEnabled)
	} else {
		if debugEnabled {
			slog.SetLogLoggerLevel(slog.LevelDebug)
		}
	}

	subcommand := os.Args[1]
	if subcommand != "resolve" {
		fmt.Fprintf(os.Stderr, "unknown subcommand: %s\n", subcommand)
		printUsageAndExit()
	}

	resolveFlags := flag.NewFlagSet("resolve", flag.ExitOnError)
	fixCorruptFSM := resolveFlags.Bool("fix-corrupt-fsm", false, "Reset FSM state to HEALTHY after clearing stuck/corrupt flags")
	force := resolveFlags.Bool("force", false, "Ignore transition validation errors and forcefully overwrite state in etcd")
	fixAll := resolveFlags.Bool("fix-all", false, "Reset FSM state to HEALTHY and forcefully overwrite state (combines --fix-corrupt-fsm and --force)")
	targetStateStr := resolveFlags.String("state", "", "Force a specific target state (must be used with --force)")
	usecase := resolveFlags.String("usecase", getEnv("AEGIS_USECASE", "local"), "Aegis usecase: local, intg-test, scenario")
	etcdUrlsStr := resolveFlags.String("etcd-urls", "", "Etcd server URLs (comma-separated, default inferred from --usecase)")
	kafkaBrokersStr := resolveFlags.String("kafka-brokers", "", "Kafka broker addresses (comma-separated, default inferred from --usecase)")
	yes := resolveFlags.Bool("yes", false, "Bypass all confirmation prompts (assume yes)")
	_ = resolveFlags.Bool("non-interactive", false, "Use structured JSON/Dev logging instead of default interactive logging")
	_ = resolveFlags.Bool("verbose", false, "Enable verbose/debug logging (can also use -v)")
	_ = resolveFlags.Bool("v", false, "Enable verbose/debug logging (shorthand for --verbose)")

	// Go's flag package stops parsing at the first positional argument.
	// To make the CLI SRE-friendly (allowing flags at the end of the command),
	// we separate flags from positional args manually before parsing.
	var flagsOnly []string
	var positionals []string

	for _, arg := range os.Args[2:] {
		if strings.HasPrefix(arg, "-") {
			flagsOnly = append(flagsOnly, arg)
			if arg == "--state" || arg == "-state" || arg == "--etcd-urls" || arg == "-etcd-urls" || arg == "--kafka-brokers" || arg == "-kafka-brokers" || arg == "--usecase" || arg == "-usecase" {
				// We'll let flag.Parse handle the actual value parsing, this is just to prevent breaking positionals
			}
		} else {
			// If the previous argument was a flag that requires a value, this is its value, not a positional.
			if len(flagsOnly) > 0 {
				lastFlag := flagsOnly[len(flagsOnly)-1]
				if lastFlag == "--state" || lastFlag == "-state" || lastFlag == "--etcd-urls" || lastFlag == "-etcd-urls" || lastFlag == "--kafka-brokers" || lastFlag == "-kafka-brokers" || lastFlag == "--usecase" || lastFlag == "-usecase" {
					flagsOnly = append(flagsOnly, arg)
					continue
				}
			}
			positionals = append(positionals, arg)
		}
	}

	_ = resolveFlags.Parse(flagsOnly)

	var portsData map[string]map[string]any
	portsJSONFound := false
	if dir, err := os.Getwd(); err == nil {
		for dir != "/" && dir != "." {
			data, err := os.ReadFile(filepath.Join(dir, "scripts", "ports.json"))
			if err == nil {
				if err := json.Unmarshal(data, &portsData); err == nil {
					portsJSONFound = true
				}
				break
			}
			parent := filepath.Dir(dir)
			if parent == dir {
				break
			}
			dir = parent
		}
	}

	if *etcdUrlsStr == "" {
		if envUrls := os.Getenv("AEGIS_ETCD_URLS"); envUrls != "" {
			*etcdUrlsStr = envUrls
		} else if portsJSONFound && portsData[*usecase] != nil {
			if port, ok := portsData[*usecase]["etcd"].(float64); ok {
				*etcdUrlsStr = fmt.Sprintf("http://localhost:%d", int(port))
			}
		}
		if *etcdUrlsStr == "" {
			fmt.Fprintf(os.Stderr, "error: could not determine etcd urls (check scripts/ports.json or AEGIS_ETCD_URLS)\n")
			os.Exit(1)
		}
	}

	if *kafkaBrokersStr == "" {
		if envBrokers := os.Getenv("AEGIS_KAFKA_BROKERS"); envBrokers != "" {
			*kafkaBrokersStr = envBrokers
		} else if portsJSONFound && portsData[*usecase] != nil {
			if port, ok := portsData[*usecase]["kafka_bootstrap"].(float64); ok {
				*kafkaBrokersStr = fmt.Sprintf("localhost:%d", int(port))
			}
		}
		if *kafkaBrokersStr == "" {
			fmt.Fprintf(os.Stderr, "error: could not determine kafka address (check scripts/ports.json or AEGIS_KAFKA_BROKERS)\n")
			os.Exit(1)
		}
	}

	if len(positionals) < 2 {
		fmt.Fprintf(os.Stderr, "error: resolve requires <worker_id> and <error_type>\n")
		resolveFlags.Usage()
		os.Exit(1)
	}

	workerID := positionals[0]
	errorType := positionals[1]

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

	if *fixAll {
		*fixCorruptFSM = true
		*force = true
		*targetStateStr = "HEALTHY"
	}

	if *targetStateStr != "" && !*force {
		fmt.Fprintf(os.Stderr, "error: --state requires --force to be used\n")
		os.Exit(1)
	}

	if *targetStateStr != "" {
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
	}

	if !*yes {
		if *fixAll {
			fmt.Printf("WARNING: You are about to completely wipe and reset the FSM state for this worker to HEALTHY.\nThis is a destructive disaster recovery operation.\nType 'RESOLVE' to confirm: ")
			var response string
			_, err := fmt.Scanln(&response)
			if err != nil || strings.TrimSpace(response) != "RESOLVE" {
				fmt.Println("Aborting.")
				os.Exit(0)
			}
		} else if *force {
			msg := "WARNING: You are forcefully overriding safety checks."
			if *targetStateStr != "" {
				msg = fmt.Sprintf("WARNING: You are forcefully setting the state to %s.", *targetStateStr)
			}
			fmt.Printf("%s\nAre you sure you want to proceed? [y/N]: ", msg)
			var response string
			_, err := fmt.Scanln(&response)
			if err != nil || strings.ToLower(strings.TrimSpace(response)) != "y" {
				fmt.Println("Aborting.")
				os.Exit(0)
			}
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store, err := membership.NewEtcdStore(strings.Split(*etcdUrlsStr, ","))
	if err != nil {
		slog.Error("failed to connect to etcd", "component", "CLI", "error", err)
		os.Exit(1)
	}
	pub := kafka.NewKafkaPublisher(strings.Split(*kafkaBrokersStr, ","))

	err = RunResolve(ctx, store, pub, workerID, errorType, *fixCorruptFSM, *force, *targetStateStr, time.Now().UTC())
	if err != nil {
		slog.Error("resolve failed", "component", "CLI", "event", "RESOLVE_ERR", "error", err)
		os.Exit(1)
	}

	slog.Info("Successfully resolved incident", "component", "CLI", "event", "RESOLVE_SUCCESS", "worker_id", workerID, "error_type", errorType, "fix_corrupt_fsm", *fixCorruptFSM, "force", *force, "state", *targetStateStr)
}

func printUsageAndExit() {
	fmt.Fprintf(os.Stderr, "Usage: aegis-cli <subcommand> [flags] [args...]\n")
	fmt.Fprintf(os.Stderr, "Subcommands:\n")
	fmt.Fprintf(os.Stderr, "  resolve <worker_id> <error_type> [--fix-all] [--fix-corrupt-fsm] [--force] [--state <STATE>]\n")
	fmt.Fprintf(os.Stderr, "\nIMPORTANT NOTE: Running this command before an FSM terminates natively can cause FSM corruption.\n")
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

	if existing == nil || existing.CurrentState == "" {
		return errors.New("cannot resolve: no existing FSM state found for this worker")
	}

	marker, err := store.GetDLQMarker(ctx, workerID, errorType)
	if err != nil {
		return fmt.Errorf("failed to check DLQ marker: %w", err)
	}

	if marker != "" && !fixCorruptFSM && !force {
		return fmt.Errorf("cannot resolve natively: FSM is corrupt (evidence: %s). You must use --fix-corrupt-fsm to resolve it", marker)
	}

	if fixCorruptFSM {
		if marker == "" && !force {
			return errors.New("cannot fix corrupt FSM: no corruption evidence (DLQ marker) found in etcd; use --force to override")
		}

		if marker == "" && force {
			slog.Warn("no corruption evidence found, but --force was provided. Proceeding with fix.", "component", "CLI", "event", "FORCE_FIX")
		}
	}

	fromState := state.WorkerHealthState(existing.CurrentState)
	incidentID := existing.IncidentID
	if incidentID == "" {
		incidentID = fmt.Sprintf("manual-resolve-%s-%d", workerID, now.Unix())
	}
	correlationID := existing.CorrelationID
	if correlationID == "" {
		correlationID = fmt.Sprintf("corr-cli-%d", now.UnixNano())
	}

	toState := state.WorkerResolved
	if fixCorruptFSM {
		toState = state.WorkerHealthy
	}
	if targetStateStr != "" && force {
		toState = state.WorkerHealthState(targetStateStr)
	}

	if force && fromState == toState {
		return fmt.Errorf("cannot force transition: FSM is already in state %s", toState)
	}

	if !force && !fixCorruptFSM && fromState == toState {
		return fmt.Errorf("cannot resolve: FSM is already in state %s", toState)
	}

	if !force && !fixCorruptFSM && fromState != toState {
		if !incident.ValidTransition(fromState, toState) {
			return fmt.Errorf("illegal FSM transition from %s to %s; use --force to override", fromState, toState)
		}
	}

	// Clear any DLQ marker in etcd
	if err := store.DeleteDLQMarker(ctx, workerID, errorType); err != nil {
		return fmt.Errorf("delete dlq marker: %w", err)
	}

	// Also clear any deferred DELIVERED events to prevent state leakage to future incidents
	_ = store.DeleteDeferredEvent(ctx, workerID, errorType, incident.TopicDeliveryStatus)

	// Set authoritative etcd state
	st := membership.WorkerState{
		WorkerID:      workerID,
		ErrorType:     errorType,
		IncidentID:    incidentID,
		CurrentState:  string(toState),
		CorrelationID: correlationID,
		UpdatedAt:     now,
	}
	if err := store.SetWorkerState(ctx, st, "", 0, 0); err != nil {
		return fmt.Errorf("set worker state: %w", err)
	}
	slog.Info("successful FSM transition", "component", "CLI", "event", "FSM_TRANSITION",
		"worker_id", workerID,
		"error_type", errorType,
		"incident_id", incidentID,
		"from_state", string(fromState),
		"to_state", string(toState),
	)

	// Publish repair tombstone event to DLQ topic ONLY when fixing a corrupt FSM
	if fixCorruptFSM && pub != nil {
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
