import asyncio
import contextlib
import os
import sys
import time
from collections import deque

import grpc
import httpx
import pytest
import pytest_asyncio
from gen.python.aegis.v1 import aegis_pb2, aegis_pb2_grpc


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
                    resp = self.responses.popleft()
                    if isinstance(resp, Exception):
                        await context.abort(grpc.StatusCode.RESOURCE_EXHAUSTED, "exhausted")
                    else:
                        yield resp
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
    """DRY setup for starting the agent as a black-box subprocess."""
    processes = []
    
    async def _create(target: str, port: int = 8080):
        env = os.environ.copy()
        env["AEGIS_WORKER_ID"] = "aegis-system--test-worker"
        env["AEGIS_CP_BOOTSTRAP_URLS"] = target
        env["AEGIS_HEARTBEAT_INTERVAL_SECONDS"] = "0.1"
        env["AEGIS_SIMULATION_MODE"] = "normal"
        env["AEGIS_TELEMETRY_DATA_SOURCE"] = "MOCK"
        env["AEGIS_AGENT_API_PORT"] = str(port)

        proc = await asyncio.create_subprocess_exec(
            sys.executable, "-m", "services.agent.main",
            env=env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        processes.append(proc)
        
        # Wait for the HTTP API to be ready
        async with httpx.AsyncClient() as client:
            for _ in range(50):
                try:
                    resp = await client.get(f"http://127.0.0.1:{port}/health")
                    if resp.status_code == 200:
                        break
                except Exception:
                    pass
                await asyncio.sleep(0.1)
            else:
                stdout_data, stderr_data = await proc.communicate()
                print("Agent stdout:", stdout_data.decode())
                print("Agent stderr:", stderr_data.decode())
                raise RuntimeError(f"Agent API failed to start on port {port}")
                
        return proc
        
    yield _create
    
    for p in processes:
        if p.returncode is None:
            p.terminate()
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(p.wait(), timeout=2.0)
            if p.returncode is None:
                p.kill()


@pytest.mark.asyncio
@pytest.mark.integration
async def test_grpc_stream_accepted(cp_server_factory, agent_setup):
    servicer, target = await cp_server_factory()
    agent_proc = await agent_setup(target, port=8081)
    
    # Wait to allow stream collection to run
    await asyncio.sleep(0.5)
    
    assert servicer.connection_count >= 1
    assert len(servicer.received_samples) >= 2


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
    
    agent_proc = await agent_setup(target1, port=8082)
    
    # Give time for initial connection, redirect processing, and subsequent reconnection
    await asyncio.sleep(0.8)
    
    # Verify the first target actually issued the redirect
    assert servicer1.connection_count >= 1
    
    # Verify the agent successfully established a new connection to the redirected target
    assert servicer2.connection_count >= 1
    assert len(servicer2.received_samples) >= 1


@pytest.mark.asyncio
@pytest.mark.integration
async def test_grpc_stream_backoff_directive(cp_server_factory, agent_setup):
    servicer, target = await cp_server_factory()
    
    # Simulate CP ingest queue full
    servicer.responses.append(Exception("resource exhausted"))
    
    agent_proc = await agent_setup(target, port=8083)
    
    await asyncio.sleep(0.5)
    
    assert servicer.connection_count >= 1


@pytest.mark.asyncio
@pytest.mark.integration
async def test_agent_api_health(cp_server_factory, agent_setup):
    servicer, target = await cp_server_factory()
    agent_proc = await agent_setup(target, port=8084)
    
    async with httpx.AsyncClient() as client:
        resp = await client.get("http://127.0.0.1:8084/health")
        assert resp.status_code == 200
        assert resp.json() == {"status": "ok"}


@pytest.mark.asyncio
@pytest.mark.integration
async def test_agent_diagnostics_buffer_read(cp_server_factory, agent_setup):
    servicer, target = await cp_server_factory()
    agent_proc = await agent_setup(target, port=8085)
    
    async with httpx.AsyncClient() as client:
        # Give some time for background telemetry collection to populate buffer
        await asyncio.sleep(0.5)
        resp = await client.get("http://127.0.0.1:8085/diagnostics/buffer")
        assert resp.status_code == 200
        data = resp.json()
        assert data["worker_id"] == "aegis-system--test-worker"
        assert "failure_indicators" in data


@pytest.mark.asyncio
@pytest.mark.integration
async def test_agent_simulate_failure(cp_server_factory, agent_setup):
    servicer, target = await cp_server_factory()
    agent_proc = await agent_setup(target, port=8086)
    
    async with httpx.AsyncClient() as client:
        resp = await client.post("http://127.0.0.1:8086/simulate", json={"mode": "model_crash"})
        assert resp.status_code == 200
        assert resp.json()["mode"] == "model_crash"
