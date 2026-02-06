from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Deque

from .telemetry import TelemetrySample


@dataclass
class DiagnosticBuffer:
    worker_id: str
    max_items: int = 128
    telemetry: Deque[dict[str, object]] = field(init=False)
    logs: Deque[str] = field(init=False)
    events: Deque[dict[str, object]] = field(init=False)

    def __post_init__(self) -> None:
        self.telemetry = deque(maxlen=self.max_items)
        self.logs = deque(maxlen=self.max_items)
        self.events = deque(maxlen=self.max_items)

    def add_telemetry(self, sample: TelemetrySample) -> None:
        self.telemetry.append(sample.to_dict())

    def add_log(self, message: str) -> None:
        self.logs.append(message)

    def add_event(self, name: str, payload: dict[str, object] | None = None) -> None:
        self.events.append(
            {
                "name": name,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "payload": payload or {},
            }
        )

    def build_bundle(
        self,
        incident_id: str,
        failure_type: str,
        correlation_id: str,
        status: str = "complete",
    ) -> dict[str, object]:
        latest = self.telemetry[-1] if self.telemetry else {}
        return {
            "worker_id": self.worker_id,
            "incident_id": incident_id,
            "collected_at": datetime.now(timezone.utc).isoformat(),
            "diagnostic_status": status,
            "correlation_id": correlation_id,
            "telemetry_window": list(self.telemetry),
            "logs": list(self.logs),
            "events": list(self.events),
            "failure_indicators": {
                "failure_type": failure_type,
                "synthetic_failure": latest.get("synthetic_failure_flag", "unknown"),
                "model_server_healthy": latest.get("model_server_healthy", "unknown"),
                "temperature_celsius": latest.get("temperature_celsius", "unknown"),
                "vram_used_bytes": latest.get("vram_used_bytes", "unknown"),
                "vram_total_bytes": latest.get("vram_total_bytes", "unknown"),
                "ecc_error_count": latest.get("ecc_error_count", "unknown"),
            },
            "suspected_cause_hints": self._suspected_cause_hints(latest, failure_type),
        }

    def _suspected_cause_hints(self, latest: dict[str, object], failure_type: str) -> list[str]:
        hints = [f"control-plane detected {failure_type}"]
        if latest.get("model_server_healthy") is False:
            hints.append("local model-server health probe failed")
        temp = latest.get("temperature_celsius")
        if isinstance(temp, (float, int)) and temp >= 85:
            hints.append("GPU temperature exceeded sustained threshold")
        return hints

