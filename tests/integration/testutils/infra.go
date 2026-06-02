package testutils

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

var (
	DefaultRedisAddr    = "localhost:36380"
	DefaultKafkaBrokers = []string{"localhost:39093"}
)

// GetProjectRoot finds the root directory of the project by looking for the Makefile.
func GetProjectRoot(t *testing.T) string {
	if t != nil {
		t.Helper()
	}
	dir, err := os.Getwd()
	if err != nil {
		if t != nil {
			t.Fatalf("Failed to get current directory: %v", err)
		}
		panic(fmt.Sprintf("Failed to get current directory: %v", err))
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
		if t != nil {
			t.Fatalf("Could not find project root (Makefile not found)")
		}
		panic("Could not find project root (Makefile not found)")
	}
	return rootDir
}

// WipeTestState wipes the specified infrastructure ("redis", "kafka", or "" for all).
func WipeTestState(t *testing.T, targets string) {
	if t != nil {
		t.Helper()
		LogInfo(t, "🧹 Wiping test state (targets: %q)...", targets)
	} else {
		fmt.Printf("🧹 Wiping test state (targets: %q)...\n", targets)
	}

	rootDir := GetProjectRoot(t)

	args := []string{"wipe-infra-state"}
	if targets != "" {
		args = append(args, "TARGETS="+targets)
	}

	cmd := exec.Command("make", args...)
	cmd.Dir = rootDir
	env := os.Environ()
	cleanEnv := make([]string, 0, len(env))
	for _, e := range env {
		if !strings.HasPrefix(e, "AEGIS_KUBE_CONTEXT=") {
			cleanEnv = append(cleanEnv, e)
		}
	}
	cmd.Env = append(cleanEnv, "AEGIS_KUBE_CONTEXT=kind-aegis-intg-test")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		if t != nil {
			t.Fatalf("Failed to wipe test state: %v", err)
		}
		panic(fmt.Sprintf("Failed to wipe test state: %v", err))
	}
}

// LogInfo prints a nicely formatted, cyan-colored log message with the test name prefixed.
func LogInfo(t *testing.T, format string, args ...any) {
	t.Helper()
	msg := fmt.Sprintf(format, args...)
	fmt.Printf("\033[36m[TEST: %s] %s\033[0m\n", t.Name(), msg)
}

