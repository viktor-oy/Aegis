from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, patch

import httpx
from services.composer.api import app
from services.composer.service import GeneratedPostmortem


async def run_health_test() -> None:
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/health")
        assert response.status_code == 200
        assert response.json() == {"status": "ok"}

def test_health_endpoint() -> None:
    asyncio.run(run_health_test())


async def run_compose_test() -> None:
    mock_postmortem = GeneratedPostmortem(
        event_type="aegis.postmortem.generated",
        incident_id="inc-api",
        worker_id="worker-api",
        markdown="# Postmortem",
        metadata={"validation_status": "valid"},
        correlation_id="corr-api",
    )
    
    with patch("services.composer.api.ComposerService") as MockService:
        mock_instance = MockService.return_value
        mock_instance.compose = AsyncMock(return_value=mock_postmortem)
        
        req = {
            "diagnostic_event": {
                "incident_id": "inc-api",
                "worker_id": "worker-api",
                "payload": {"severity": "low"}
            },
            "guidance": {}
        }
        
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            response = await client.post("/compose", json=req)
        assert response.status_code == 200
        
        data = response.json()
        assert data["incident_id"] == "inc-api"
        assert data["worker_id"] == "worker-api"
        assert data["markdown"] == "# Postmortem"
        
        mock_instance.compose.assert_called_once()

def test_compose_postmortem_endpoint() -> None:
    asyncio.run(run_compose_test())
