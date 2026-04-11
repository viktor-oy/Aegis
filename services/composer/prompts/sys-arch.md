# Aegis System Architecture
Aegis is an out-of-path control plane for GPU/AI infrastructure that detects worker failure signals and coordinates diagnostics.

## Components
1. **Control Plane (Go)**: Maintains a deterministic hash ring of active workers via Redis leases. Receives heartbeats and triggers diagnostics on failure signals (e.g., missed heartbeat, sustained high temp, VRAM pressure, ECC burst).
2. **GPU Agent (Python)**: Runs on monitored worker nodes. Collects telemetry and bounded diagnostic logs. Never connects to Redis directly, only communicates via gRPC to the Control Plane.
3. **Composer (Python)**: Subscribes to Kafka diagnostic events and triggers an LLM inference API to generate highly structured postmortems. Publishes generated postmortems back to Kafka.
4. **Data Infrastructure**: 
   - **Kafka**: Async backbone for postmortem delivery.
   - **Redis**: Coordinates distributed locks and ephemeral leases.
   - **File / Email**: Final sinks for generated postmortems (never accessed by the Control Plane directly).
