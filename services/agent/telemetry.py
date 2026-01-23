from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from random import Random
from subprocess import run

from .config import AgentConfig, TelemetryDataSource


@dataclass(frozen=True)
class TelemetrySample:
    worker_id: str
    timestamp: datetime
    gpu_utilization: float
    vram_used_bytes: int
    vram_total_bytes: int
    temperature_celsius: float
    power_watts: float
    ecc_error_count: int
    inference_latency_ms: float
    local_queue_depth: int
    model_server_healthy: bool
    synthetic_failure_flag: str
    correlation_id: str

    def to_dict(self) -> dict[str, object]:
        payload = asdict(self)
        payload["timestamp"] = self.timestamp.isoformat()
        return payload


class SimulationMode:
    NORMAL = "normal"
    VRAM_PRESSURE = "vram_pressure"
    OVERHEAT = "overheat"
    LATENCY_SPIKE = "latency_spike"
    MISSED_HEARTBEAT = "missed_heartbeat"
    FREEZE = "freeze"
    ECC_BURST = "ecc_burst"
    MODEL_CRASH = "model_crash"
    NETWORK_PARTITION = "network_partition"
    RESTART = "restart"


class SyntheticCollector:
    def __init__(self, worker_id: str, mode: str = SimulationMode.NORMAL, seed: int = 17) -> None:
        self.worker_id = worker_id
        self.mode = mode
        self._random = Random(seed)
        self._ecc_errors = 0
        self._sequence = 0

    def set_mode(self, mode: str) -> None:
        self.mode = mode

    def collect(self, correlation_id: str | None = None) -> TelemetrySample | None:
        self._sequence += 1
        if self.mode in {SimulationMode.MISSED_HEARTBEAT, SimulationMode.FREEZE, SimulationMode.NETWORK_PARTITION}:
            return None

        temp = 62.0 + self._random.random() * 4
        vram_used = 12 * 1024**3
        vram_total = 24 * 1024**3
        latency = 95.0 + self._random.random() * 25
        model_healthy = True
        queue_depth = 2
        failure_flag = "normal"

        if self.mode == SimulationMode.VRAM_PRESSURE:
            vram_used = int(vram_total * 0.96)
            queue_depth = 18
        elif self.mode == SimulationMode.OVERHEAT:
            temp = 91.0 + self._random.random() * 3
        elif self.mode == SimulationMode.LATENCY_SPIKE:
            latency = 3100.0 + self._random.random() * 200
            queue_depth = 32
        elif self.mode == SimulationMode.ECC_BURST:
            self._ecc_errors += 12
        elif self.mode == SimulationMode.MODEL_CRASH:
            model_healthy = False
            failure_flag = "model_crash"
        elif self.mode == SimulationMode.RESTART:
            failure_flag = "restart"

        return TelemetrySample(
            worker_id=self.worker_id,
            timestamp=datetime.now(UTC),
            gpu_utilization=0.74,
            vram_used_bytes=vram_used,
            vram_total_bytes=vram_total,
            temperature_celsius=temp,
            power_watts=230.0,
            ecc_error_count=self._ecc_errors,
            inference_latency_ms=latency,
            local_queue_depth=queue_depth,
            model_server_healthy=model_healthy,
            synthetic_failure_flag=failure_flag,
            correlation_id=correlation_id or f"{self.worker_id}-{self._sequence}",
        )


class GPUTelemetryCollector:
    def __init__(self, worker_id: str, nvidia_smi_path: str = "nvidia-smi") -> None:
        self.worker_id = worker_id
        self.nvidia_smi_path = nvidia_smi_path
        self._sequence = 0

    def collect(self, correlation_id: str | None = None) -> TelemetrySample:
        self._sequence += 1
        fields = [
            "utilization.gpu",
            "memory.used",
            "memory.total",
            "temperature.gpu",
            "power.draw",
            "ecc.errors.uncorrected.volatile.total",
        ]
        completed = run(
            [
                self.nvidia_smi_path,
                f"--query-gpu={','.join(fields)}",
                "--format=csv,noheader,nounits",
            ],
            capture_output=True,
            check=True,
            text=True,
            timeout=2,
        )
        line = completed.stdout.strip().splitlines()[0]
        gpu_util, mem_used_mib, mem_total_mib, temp_c, power_w, ecc_errors = [
            item.strip() for item in line.split(",")
        ]
        return TelemetrySample(
            worker_id=self.worker_id,
            timestamp=datetime.now(UTC),
            gpu_utilization=_parse_float(gpu_util) / 100.0,
            vram_used_bytes=int(_parse_float(mem_used_mib) * 1024 * 1024),
            vram_total_bytes=int(_parse_float(mem_total_mib) * 1024 * 1024),
            temperature_celsius=_parse_float(temp_c),
            power_watts=_parse_float(power_w),
            ecc_error_count=int(_parse_float(ecc_errors)),
            inference_latency_ms=0.0,
            local_queue_depth=0,
            model_server_healthy=True,
            synthetic_failure_flag="",
            correlation_id=correlation_id or f"{self.worker_id}-gpu-{self._sequence}",
        )


def build_collector(config: AgentConfig) -> SyntheticCollector | GPUTelemetryCollector:
    if config.telemetry_data_source == TelemetryDataSource.GPU:
        return GPUTelemetryCollector(config.worker_id)
    return SyntheticCollector(config.worker_id, mode=config.simulation_mode)


def _parse_float(value: str) -> float:
    if value in {"", "N/A", "[N/A]"}:
        return 0.0
    return float(value)
