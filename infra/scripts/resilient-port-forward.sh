#!/bin/bash
# Purpose: Solves the Kubernetes idle connection timeout issue for port-forwards.
# kubectl port-forward drops connections silently after 5 minutes of inactivity
# but leaves the local socket open (causing 'unexpected EOF' errors for clients).
# This script forcefully restarts the port-forward every 4 minutes (240s) 
# to guarantee a fresh, active connection is always available to Tilt/Kafka.

TARGET=$1
NAMESPACE=$2
PORTS=$3

PID=""

# Cleanup function to kill the background kubectl process when Tilt stops this resource
cleanup() {
  if [ -n "$PID" ]; then
    kill -TERM "$PID" 2>/dev/null
  fi
  exit 0
}

# Trap SIGINT and SIGTERM from Tilt to ensure we don't leave zombie port-forwards
trap cleanup SIGINT SIGTERM

# Kubelet typically drops idle port-forward connections after 5 minutes (300 seconds).
KUBELET_TIMEOUT_SECONDS=300
# We want to restart it safely before that limit (e.g. 1 minute before).
REFRESH_INTERVAL_SECONDS=$((KUBELET_TIMEOUT_SECONDS - 60))

while true; do
  kubectl port-forward "$TARGET" -n "$NAMESPACE" "$PORTS" &
  PID=$!
  
  # Run until the refresh interval then forcefully restart to beat the idle timeout.
  # We use a loop of 1-second sleeps so the trap can interrupt it immediately if Tilt exits.
  for ((i=1; i<=REFRESH_INTERVAL_SECONDS; i++)); do
    sleep 1
  done
  
  kill -TERM "$PID" 2>/dev/null
  wait "$PID" 2>/dev/null
done
