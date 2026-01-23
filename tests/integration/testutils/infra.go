package testutils

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"syscall"
	"time"
)

// EnsureInfra ensures the requested services (e.g., "kafka", "redis") are running.
// If any service is unavailable, it starts the infrastructure via tilt.
// It returns a cleanup function that should be executed to shut down tilt.
func EnsureInfra(services []string, tiltfilePath string) func() {
	fmt.Println("Bootstrapping testing workflow...")

	needsTilt := false

	// Check if requested infra services are running
	for _, svc := range services {
		var addr string
		if svc == "kafka" {
			addr = "localhost:9094"
		} else if svc == "redis" {
			addr = "localhost:6379"
		} else if svc == "mailpit" {
			addr = "localhost:1025"
		} else {
			continue // Skip unknown services for now
		}

		if svc == "mailpit" {
			resp, err := http.Get("http://localhost:8025/api/v1/messages")
			if err != nil || resp.StatusCode != 200 {
				needsTilt = true
				if resp != nil {
					resp.Body.Close()
				}
			} else {
				resp.Body.Close()
			}
		} else {
			conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
			if err != nil {
				needsTilt = true
			} else {
				conn.Close()
			}
		}
	}

	var tiltCmd *exec.Cmd
	if needsTilt {
		fmt.Printf("Starting infrastructure via tilt up --port 10352 (this may take a minute)...\n")
		tiltCmd = exec.Command("tilt", "up", "--port", "10352", "-f", tiltfilePath)
		if verbose := os.Getenv("AEGIS_TEST_INFRA_SETUP_VERBOSE"); verbose == "1" || verbose == "true" || true { // always verbose for debugging
			tiltCmd.Stdout = os.Stdout
			tiltCmd.Stderr = os.Stderr
		}

		if err := tiltCmd.Start(); err != nil {
			fmt.Printf("failed to start tilt up: %v\n", err)
			os.Exit(1)
		}

		// Poll for readiness
		ready := false
		for i := 0; i < 300; i++ { // wait up to 300 seconds
			time.Sleep(1 * time.Second)

			allReady := true
			for _, svc := range services {
				var addr string
				if svc == "kafka" {
					addr = "localhost:9094"
				} else if svc == "redis" {
					addr = "localhost:6379"
				} else {
					continue
				}

				if svc == "mailpit" {
					resp, err := http.Get("http://127.0.0.1:8025/api/v1/messages")
					if err != nil || resp.StatusCode != 200 {
						if resp != nil {
							resp.Body.Close()
						}
						allReady = false
						break
					}
					resp.Body.Close()

					conn, err := net.DialTimeout("tcp", "127.0.0.1:10250", 1*time.Second)
					if err != nil {
						allReady = false
						break
					}
					conn.SetReadDeadline(time.Now().Add(1 * time.Second))
					buf := make([]byte, 256)
					_, err = conn.Read(buf)
					conn.Close()
					if err != nil {
						allReady = false
						break
					}
				} else {
					conn, err := net.DialTimeout("tcp", addr, 1*time.Second)
					if err != nil {
						allReady = false
						break
					}
					conn.Close()
				}
			}

			if allReady {
				ready = true
				break
			}
		}

		if !ready {
			fmt.Println("infrastructure failed to become ready")
			if tiltCmd.Process != nil {
				_ = tiltCmd.Process.Kill()
			}
			os.Exit(1)
		}
		fmt.Println("Infrastructure is ready!")
	}

	// Return cleanup function
	return func() {
		if tiltCmd != nil && tiltCmd.Process != nil {
			fmt.Println("Shutting down background infrastructure...")
			_ = tiltCmd.Process.Signal(syscall.SIGINT)
			done := make(chan error, 1)
			go func() { done <- tiltCmd.Wait() }()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				_ = tiltCmd.Process.Kill()
			}
		}
	}
}
