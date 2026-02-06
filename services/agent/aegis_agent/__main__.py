from __future__ import annotations

import json
import time

from .config import AgentConfig, TelemetryDataSource
from .diagnostics import DiagnosticBuffer
from .telemetry import build_collector


def main() -> int:
    config = AgentConfig.from_env()
    collector = build_collector(config)
    diagnostics = DiagnosticBuffer(worker_id=config.worker_id)
    print(
        "aegis-agent starting "
        f"worker_id={config.worker_id} telemetry_data_source={config.telemetry_data_source}",
        flush=True,
    )
    while True:
        try:
            sample = collector.collect()
        except Exception as exc:
            diagnostics.add_event("telemetry_collection_failed", {"error": str(exc)})
            if config.telemetry_data_source == TelemetryDataSource.GPU:
                print(json.dumps({"error": str(exc), "source": "GPU"}), flush=True)
            sample = None
        if sample is not None:
            diagnostics.add_telemetry(sample)
            print(json.dumps(sample.to_dict(), sort_keys=True), flush=True)
        else:
            diagnostics.add_event("heartbeat_suppressed", {"mode": config.simulation_mode})
        time.sleep(config.heartbeat_interval_seconds)


if __name__ == "__main__":
    raise SystemExit(main())
