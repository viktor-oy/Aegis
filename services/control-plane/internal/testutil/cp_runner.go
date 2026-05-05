package testutil

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	aegisv1 "github.com/aegis/aegis/gen/go/aegis/v1"
	"github.com/aegis/aegis/services/control-plane/internal/hashring"
	"github.com/aegis/aegis/tests/integration/testutils"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

var nextPort int32 = 30000

// GetWorkerForCP generates a worker ID that deterministically hashes to the targetCP node.
func GetWorkerForCP(t *testing.T, targetCP string, nodeIDs []string) string {
	t.Helper()
	var members []hashring.Member
	for _, id := range nodeIDs {
		members = append(members, hashring.Member{ID: id, Address: fmt.Sprintf("%s:50051", id)})
	}
	ring, err := hashring.New(members, 128)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 1000; i++ {
		// Use a random suffix to avoid collisions between tests
		workerID := fmt.Sprintf("worker-%d-%d", time.Now().UnixNano(), i)
		owner, ok := ring.Owner(workerID)
		if ok && owner.ID == targetCP {
			return workerID
		}
	}
	t.Fatalf("failed to find a worker for %s", targetCP)
	return ""
}

// startCPNode executes the pre-compiled control plane binary as a child process.
func startCPNode(id, address, redisAddr string, kafkaBrokers []string, extraEnv map[string]string, binPath string) (string, *exec.Cmd, error) {
	// Pick a unique high port using atomic counter to avoid TOCTOU collisions
	port := atomic.AddInt32(&nextPort, 1)
	grpcAddr := fmt.Sprintf("127.0.0.1:%d", port)

	cmd := exec.Command(binPath)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Env = append(os.Environ(),
		"AEGIS_CP_ID="+id,
		"AEGIS_CP_ADDRESS="+address,
		"AEGIS_GRPC_ADDRESS="+grpcAddr,
		"AEGIS_REDIS_ADDR="+redisAddr,
		"AEGIS_KAFKA_BROKERS="+strings.Join(kafkaBrokers, ","),
		"AEGIS_QUEUE_SIZE=256",
		"AEGIS_MEMBERSHIP_INTERVAL=50ms",
	)
	for k, v := range extraEnv {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	var stderrBuf strings.Builder
	cmd.Stderr = &stderrBuf
	if verbose := os.Getenv("AEGIS_TEST_INFRA_SETUP_VERBOSE"); verbose == "1" || verbose == "true" {
		cmd.Stdout = os.Stdout
	}

	if err := cmd.Start(); err != nil {
		return "", nil, fmt.Errorf("failed to start main.go subprocess for %s: %v", id, err)
	}

	return grpcAddr, cmd, nil
}

// teardownCPNode gracefully shuts down the CP process.
func teardownCPNode(cmd *exec.Cmd) {
	if cmd == nil || cmd.Process == nil {
		return
	}
	_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM)
	
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}
}

// waitForCPReady polls the provided CP process gRPC address until it responds successfully.
func waitForCPReady(grpcAddr string, cmd *exec.Cmd) (aegisv1.ControlPlaneTelemetryClient, *grpc.ClientConn, error) {
	var conn *grpc.ClientConn
	var client aegisv1.ControlPlaneTelemetryClient
	var err error
	ready := false
	for i := 0; i < 100; i++ { // wait up to 10 seconds
		conn, err = grpc.NewClient(grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err == nil {
			client = aegisv1.NewControlPlaneTelemetryClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			_, err := client.SubmitDiagnostics(ctx, &aegisv1.DiagnosticBundle{})
			cancel()
			if err != nil && status.Code(err) != codes.Unavailable {
				ready = true
				break
			}
			conn.Close()
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !ready {
		_ = cmd.Process.Kill()
		return nil, nil, fmt.Errorf("main.go gRPC server failed to become ready on %s", grpcAddr)
	}
	return client, conn, nil
}

type CPTestClusterFactory struct {
	once         sync.Once
	cmds         []*exec.Cmd
	conns        []*grpc.ClientConn
	clients      []aegisv1.ControlPlaneTelemetryClient
	nodeIDs      []string
	redisAddr    string
	kafkaBrokers []string
	extraEnv     map[string]string
	binPath      string
}

func NewCPTestClusterFactory(nodeIDs []string, redisAddr string, kafkaBrokers []string, extraEnv map[string]string) *CPTestClusterFactory {
	if extraEnv == nil {
		extraEnv = make(map[string]string)
	}
	if _, ok := extraEnv["AEGIS_FSM_GROUP_ID"]; !ok {
		extraEnv["AEGIS_FSM_GROUP_ID"] = "aegis-cp-fsm-group-" + fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return &CPTestClusterFactory{
		nodeIDs:      nodeIDs,
		redisAddr:    redisAddr,
		kafkaBrokers: kafkaBrokers,
		extraEnv:     extraEnv,
	}
}

func (f *CPTestClusterFactory) StartCluster(t *testing.T) []aegisv1.ControlPlaneTelemetryClient {
	f.once.Do(func() {
		// Build binary once for all nodes
		rootDir := testutils.GetProjectRoot(t)
		mainPath := filepath.Join(rootDir, "services", "control-plane", "main.go")
		binFile, err := os.CreateTemp("", "aegis-cp-bin-*")
		if err != nil {
			if t != nil {
				t.Fatalf("failed to create temp file for binary: %v", err)
			}
			panic(fmt.Sprintf("failed to create temp file for binary: %v", err))
		}
		binFile.Close() // close immediately so go build can write it
		f.binPath = binFile.Name()

		buildCmd := exec.Command("go", "build", "-o", f.binPath, mainPath)
		buildCmd.Dir = rootDir
		if output, err := buildCmd.CombinedOutput(); err != nil {
			if t != nil {
				t.Fatalf("failed to build control plane binary: %v, output: %s", err, string(output))
			}
			panic(fmt.Sprintf("failed to build control plane binary: %v, output: %s", err, string(output)))
		}

		for _, id := range f.nodeIDs {
			address := fmt.Sprintf("%s:50051", id)
			
			grpcAddress, cmd, err := startCPNode(id, address, f.redisAddr, f.kafkaBrokers, f.extraEnv, f.binPath)
			if err != nil {
				t.Fatalf("failed to start %s: %v", id, err)
			}
			f.cmds = append(f.cmds, cmd)

			client, conn, err := waitForCPReady(grpcAddress, cmd)
			if err != nil {
				t.Fatalf("failed to wait for %s: %v", id, err)
			}
			f.clients = append(f.clients, client)
			f.conns = append(f.conns, conn)
		}

		// Give the nodes enough time to tick their membership loops and build the hashring
		time.Sleep(200 * time.Millisecond)
	})

	return f.clients
}

func (f *CPTestClusterFactory) Teardown() {
	for _, conn := range f.conns {
		if conn != nil {
			_ = conn.Close()
		}
	}
	for _, cmd := range f.cmds {
		teardownCPNode(cmd)
	}
	if f.binPath != "" {
		_ = os.Remove(f.binPath)
	}
}
