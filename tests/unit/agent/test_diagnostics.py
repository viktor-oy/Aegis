from datetime import UTC, datetime

from services.agent.diagnostics import DiagnosticBuffer
from services.agent.telemetry import TelemetrySample


def test_diagnostic_buffer_initialization():
    buf = DiagnosticBuffer(worker_id="worker-1", max_items=5)
    assert buf.worker_id == "worker-1"
    assert len(buf.telemetry) == 0
    assert len(buf.logs) == 0
    assert len(buf.events) == 0

def test_diagnostic_buffer_adds_items():
    buf = DiagnosticBuffer(worker_id="worker-1", max_items=5)
    
    sample = TelemetrySample(
        worker_id="worker-1", timestamp=datetime.now(UTC), gpu_utilization=0.5,
        vram_used_bytes=100, vram_total_bytes=200, temperature_celsius=60.0,
        power_watts=150.0, ecc_error_count=0, inference_latency_ms=10.0,
        local_queue_depth=1, model_server_healthy=True, synthetic_failure_flag="normal",
        correlation_id="corr-1"
    )
    
    buf.add_telemetry(sample)
    buf.add_log("System started")
    buf.add_event("startup", {"version": "0.1.0"})
    
    assert len(buf.telemetry) == 1
    assert len(buf.logs) == 1
    assert len(buf.events) == 1

def test_diagnostic_buffer_rollover():
    buf = DiagnosticBuffer(worker_id="worker-1", max_items=2)
    buf.add_log("log 1")
    buf.add_log("log 2")
    buf.add_log("log 3")
    assert len(buf.logs) == 2
    assert list(buf.logs) == ["log 2", "log 3"]

def test_build_bundle_with_hints():
    buf = DiagnosticBuffer(worker_id="worker-1")
    sample = TelemetrySample(
        worker_id="worker-1", timestamp=datetime.now(UTC), gpu_utilization=0.5,
        vram_used_bytes=100, vram_total_bytes=200, temperature_celsius=90.0,
        power_watts=150.0, ecc_error_count=0, inference_latency_ms=10.0,
        local_queue_depth=1, model_server_healthy=False, synthetic_failure_flag="model_crash",
        correlation_id="corr-1"
    )
    buf.add_telemetry(sample)
    
    bundle = buf.build_bundle(incident_id="inc-1", failure_type="missed_heartbeat", correlation_id="corr-1")
    assert bundle["incident_id"] == "inc-1"
    assert bundle["worker_id"] == "worker-1"
    assert "local model-server health probe failed" in bundle["suspected_cause_hints"]
    assert "GPU temperature exceeded sustained threshold" in bundle["suspected_cause_hints"]
