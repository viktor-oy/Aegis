package testutils

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)
// WipeTestState wipes the specified infrastructure ("redis", "kafka", or "" for all).
func WipeTestState(t *testing.T, targets string) {
	t.Helper()
	LogInfo(t, "🧹 Wiping test state (targets: %q)...", targets)

	// Find project root by looking for Makefile
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("Failed to get current directory: %v", err)
	}
	
	rootDir := ""
	for i := 0; i < 10; i++ {
		if _, err := os.Stat(filepath.Join(dir, "Makefile")); err == nil {
			rootDir = dir
			break
		}
		dir = filepath.Dir(dir)
	}
	if rootDir == "" {
		t.Fatalf("Could not find project root (Makefile not found)")
	}

	args := []string{"wipe-infra-state"}
	if targets != "" {
		args = append(args, "TARGETS=" + targets)
	}

	cmd := exec.Command("make", args...)
	cmd.Dir = rootDir
	cmd.Env = append(os.Environ(), "AEGIS_KUBE_CONTEXT=kind-aegis-intg-test")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("Failed to wipe test state: %v", err)
	}
}

// LogInfo prints a nicely formatted, cyan-colored log message with the test name prefixed.
func LogInfo(t *testing.T, format string, args ...any) {
	t.Helper()
	msg := fmt.Sprintf(format, args...)
	fmt.Printf("\033[36m[TEST: %s] %s\033[0m\n", t.Name(), msg)
}

