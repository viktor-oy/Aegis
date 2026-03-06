import asyncio
import pytest
import pytest_asyncio
import grpc
import httpx
from collections import deque
from services.agent.config import AgentConfig
from services.agent.telemetry import SyntheticCollector
from services.agent.diagnostics import DiagnosticBuffer
from services.agent.main import run_grpc_stream
from services.agent.api import app
from services.agent.client import ControlPlaneDiscovery, OwnerDecision
from gen.python.aegis.v1 import aegis_pb2, aegis_pb2_grpc

class SpyDiscovery(ControlPlaneDiscovery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.redirects_received = []
        self.accepts_received = []
        self.backoffs_triggered = 0

    def redirect(self, owner_hint: str) -> OwnerDecision:
        self.redirects_received.append(owner_hint)
        return super().redirect(owner_hint)

    def accept(self, target: str) -> OwnerDecision:
        self.accepts_received.append(target)
        return super().accept(target)

    def owner_failed(self) -> float:
        self.backoffs_triggered += 1
        return super().owner_failed()

class MockControlPlane(aegis_pb2_grpc.ControlPlaneTelemetryServicer):
    def __init__(self):
        self.received_samples = []
        self.responses = deque()
        self.connection_count = 0

    async def StreamTelemetry(self, request_iterator, context):
        self.connection_count += 1
        try:
            async for request in request_iterator:
                self.received_samples.append(request)
                if self.responses:
                    yield self.responses.popleft()
                else:
                    # Default behavior if no queued responses
                    yield aegis_pb2.ControlPlaneDirective(directive_type="accepted")
        except asyncio.CancelledError:
            pass

@pytest_asyncio.fixture
async def cp_server_factory():
    """Factory to spin up multiple mock CP servers."""
    servers = []
    
    async def _create():
        server = grpc.aio.server()
        servicer = MockControlPlane()
        aegis_pb2_grpc.add_ControlPlaneTelemetryServicer_to_server(servicer, server)
        port = server.add_insecure_port("127.0.0.1:0")
        await server.start()
        servers.append(server)
        return servicer, f"127.0.0.1:{port}"
        
    yield _create
    
    for s in servers:
        await s.stop(grace=0.1)

@pytest_asyncio.fixture
async def agent_setup():
    """DRY setup for initializing the agent state and tracking its task lifecycle."""
    tasks = []
    
    def _create(target: str):
        config = AgentConfig(
            worker_id="aegis-system--test-worker",
            bootstrap_urls=[target],
            heartbeat_interval_seconds=0.1,
            simulation_mode="normal",
            telemetry_data_source="synthetic",
            api_port=8080
        )
        collector = SyntheticCollector(config.worker_id)
        diagnostics = DiagnosticBuffer(worker_id=config.worker_id, max_items=10)
        discovery = SpyDiscovery(config.bootstrap_urls)
        
        agent_task = asyncio.create_task(run_grpc_stream(config, collector, diagnostics, discovery))
        tasks.append(agent_task)
        return agent_task, discovery
        
    yield _create
    
    for t in tasks:
        t.cancel()
        try:
            await t
        except asyncio.CancelledError:
            pass


@pytest.mark.asyncio
@pytest.mark.integration
async def test_grpc_stream_accepted(cp_server_factory, agent_setup):
    servicer, target = await cp_server_factory()
    agent_task, discovery = agent_setup(target)
    
    # Wait to allow stream collection to run
    await asyncio.sleep(0.5)
    
    assert servicer.connection_count >= 1
    assert len(servicer.received_samples) >= 2
    # The default mock CP response is 'accepted'
    assert target in discovery.accepts_received
    assert discovery.backoffs_triggered == 0

@pytest.mark.asyncio
@pytest.mark.integration
async def test_grpc_stream_redirect(cp_server_factory, agent_setup):
    # Spin up two separate valid mock CP servers
    servicer1, target1 = await cp_server_factory()
    servicer2, target2 = await cp_server_factory()
    
    # Instruct the first server to redirect to the second server
    servicer1.responses.append(aegis_pb2.ControlPlaneDirective(
        directive_type="redirect",
        owner_hint=target2
    ))
    
    agent_task, discovery = agent_setup(target1)
    
    # Give time for initial connection, redirect processing, and subsequent reconnection
    await asyncio.sleep(0.8)
    
    # Verify the first target actually issued the redirect
    assert servicer1.connection_count >= 1
    assert target2 in discovery.redirects_received
    
    # Verify the agent successfully established a new connection to the redirected target
    assert servicer2.connection_count >= 1
    assert len(servicer2.received_samples) >= 1
    
    # Verify the agent correctly identified as accepted by the new owner
    assert target2 in discovery.accepts_received
    # Ensure it didn't crash into backoff logic
    assert discovery.backoffs_triggered == 0

@pytest.mark.asyncio
@pytest.mark.integration
async def test_grpc_stream_backoff_directive(cp_server_factory, agent_setup):
    servicer, target = await cp_server_factory()
    
    # Send explicit backoff directive
    servicer.responses.append(aegis_pb2.ControlPlaneDirective(
        directive_type="backoff"
    ))
    
    agent_task, discovery = agent_setup(target)
    
    await asyncio.sleep(0.5)
    
    assert servicer.connection_count >= 1
    
    # Confirm that the agent service processed the backoff directive correctly
    assert discovery.backoffs_triggered >= 1


@pytest.mark.asyncio
@pytest.mark.integration
async def test_agent_api_health():
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.get("/health")
        assert resp.status_code == 200
        assert resp.json() == {"status": "ok"}

@pytest.mark.asyncio
@pytest.mark.integration
async def test_agent_diagnostics_buffer_read():
    app.state.buffer = DiagnosticBuffer(worker_id="aegis-system--test-worker", max_items=10)
    app.state.buffer.add_event("test_event", {"info": "test"})
    
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.get("/diagnostics/buffer")
        assert resp.status_code == 200
        data = resp.json()
        assert data["worker_id"] == "aegis-system--test-worker"
        assert "failure_indicators" in data

@pytest.mark.asyncio
@pytest.mark.integration
async def test_agent_simulate_failure():
    app.state.collector = SyntheticCollector("aegis-system--test-worker")
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.post("/simulate", json={"mode": "model_crash"})
        assert resp.status_code == 200
        assert resp.json()["mode"] == "model_crash"
        assert app.state.collector.mode == "model_crash"
