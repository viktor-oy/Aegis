#!/bin/bash
# Starts Ollama and ensures the configured model is available.
# OLLAMA_MODEL env var can be overridden at runtime via:
#   docker run -e OLLAMA_MODEL=llama3.2:1b ...
# If not set, falls back to the model baked in at build time.

set -e

MODEL="${OLLAMA_MODEL:-qwen2.5:7b}"

echo "[aegis-llm] Starting Ollama server (model: ${MODEL})"

# Start the server in the background
ollama serve &
SERVER_PID=$!

# Wait until the API is reachable
echo "[aegis-llm] Waiting for Ollama to be ready..."
until curl -sf http://localhost:11434/api/tags > /dev/null 2>&1; do
    sleep 1
done

# Pull the model if it is not already present (handles runtime overrides)
if ! ollama list | grep -q "^${MODEL}"; then
    echo "[aegis-llm] Pulling model: ${MODEL}"
    ollama pull "${MODEL}"
else
    echo "[aegis-llm] Model '${MODEL}' already present, skipping pull"
fi

echo "[aegis-llm] Ready — serving ${MODEL}"

# Hand off to the server process
wait "${SERVER_PID}"
