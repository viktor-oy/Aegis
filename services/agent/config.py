from __future__ import annotations

import os
from dataclasses import dataclass


class TelemetryDataSource:
    GPU = "GPU"
    MOCK = "MOCK"


@dataclass(frozen=True)
class AgentConfig:
    worker_id: str
    telemetry_data_source: str
    simulation_mode: str
    heartbeat_interval_seconds: float
    bootstrap_urls: list[str]
    api_port: int

    @classmethod
    def from_env(cls) -> AgentConfig:
        source = (os.getenv("AEGIS_TELEMETRY_DATA_SOURCE") or TelemetryDataSource.MOCK).upper()
        if source not in {TelemetryDataSource.GPU, TelemetryDataSource.MOCK}:
            raise ValueError("AEGIS_TELEMETRY_DATA_SOURCE must be GPU or MOCK")
        bootstrap = [
            item.strip()
            for item in (os.getenv("AEGIS_CP_BOOTSTRAP_URLS") or "aegis-control-plane:50051").split(",")
            if item.strip()
        ]
        return cls(
            worker_id=os.getenv("AEGIS_WORKER_ID") or "local-worker",
            telemetry_data_source=source,
            simulation_mode=os.getenv("AEGIS_SIMULATION_MODE") or "normal",
            heartbeat_interval_seconds=float(os.getenv("AEGIS_HEARTBEAT_INTERVAL_SECONDS") or "5"),
            bootstrap_urls=bootstrap,
            api_port=int(os.getenv("AEGIS_AGENT_API_PORT") or "8080"),
        )

