from __future__ import annotations

import logging
from typing import Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from .diagnostics import DiagnosticBuffer
from .telemetry import SyntheticCollector

logger = logging.getLogger(__name__)

app = FastAPI(
    title="Aegis Agent Diagnostics API",
    description="Local diagnostic API for Aegis GPU/AI Agent",
    version="0.1.0",
)

# We'll attach state to the app object for the API routes to access
app.state.buffer = None
app.state.collector = None


class SimulationRequest(BaseModel):
    mode: str


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/diagnostics/buffer")
def get_buffer() -> dict[str, Any]:
    """Retrieve the current contents of the bounded diagnostic buffer."""
    buffer: DiagnosticBuffer | None = getattr(app.state, "buffer", None)
    if not buffer:
        raise HTTPException(status_code=503, detail="Diagnostic buffer not initialized")
    
    return buffer.build_bundle(
        incident_id="manual-api-inspection",
        failure_type="inspection",
        correlation_id="api-req",
        status="partial",
    )


@app.post("/simulate")
def set_simulation_mode(req: SimulationRequest) -> dict[str, str]:
    """Change the synthetic failure mode for testing purposes."""
    collector = getattr(app.state, "collector", None)
    if not collector or not isinstance(collector, SyntheticCollector):
        raise HTTPException(status_code=400, detail="Agent is not using SyntheticCollector")
    
    valid_modes = {
        "normal", "vram_pressure", "overheat", "latency_spike", 
        "missed_heartbeat", "freeze", "ecc_burst", "model_crash", 
        "network_partition", "restart"
    }
    if req.mode not in valid_modes:
        raise HTTPException(status_code=400, detail=f"Invalid mode. Must be one of {valid_modes}")
        
    collector.set_mode(req.mode)
    logger.info("Simulation mode set to %s via API", req.mode)
    return {"status": "success", "mode": req.mode}
