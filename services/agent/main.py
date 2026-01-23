import asyncio
import logging
import sys

import grpc
import uvicorn
from aegis.v1 import aegis_pb2, aegis_pb2_grpc

from .api import app
from .client import ControlPlaneDiscovery
from .config import AgentConfig
from .diagnostics import DiagnosticBuffer
from .telemetry import GPUTelemetryCollector, SyntheticCollector, build_collector

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


async def run_grpc_stream(
    config: AgentConfig, 
    collector: SyntheticCollector | GPUTelemetryCollector, 
    diagnostics: DiagnosticBuffer,
    discovery: ControlPlaneDiscovery | None = None
) -> None:
    discovery = discovery or ControlPlaneDiscovery(config.bootstrap_urls)

    while True:
        target = discovery.next_target()
        logger.info(f"Attempting to connect to Control Plane at {target}")
        
        try:
            async with grpc.aio.insecure_channel(target) as channel:
                # Wait for the network connection to be fully established
                # (timeout to prevent hanging)
                try:
                    await asyncio.wait_for(channel.channel_ready(), timeout=5.0)
                    logger.info(f"Successfully connected to Control Plane network at {target}")
                except TimeoutError as exc:
                    raise Exception("Connection timed out waiting for channel readiness") from exc
                
                stub = aegis_pb2_grpc.ControlPlaneTelemetryStub(channel)
                
                async def request_generator():
                    while True:
                        try:
                            sample = collector.collect()
                        except Exception as exc:
                            diagnostics.add_event(
                                "telemetry_collection_failed", {"error": str(exc)}
                            )
                            logger.error(f"Telemetry collection failed: {exc}")
                            sample = None
                            
                        if sample is not None:
                            diagnostics.add_telemetry(sample)
                            try:
                                yield aegis_pb2.AgentTelemetry(
                                    worker_id=sample.worker_id,
                                    timestamp=sample.timestamp.isoformat(),
                                    gpu_utilization=sample.gpu_utilization,
                                    vram_used_bytes=sample.vram_used_bytes,
                                    vram_total_bytes=sample.vram_total_bytes,
                                    temperature_celsius=sample.temperature_celsius,
                                    power_watts=sample.power_watts,
                                    ecc_error_count=sample.ecc_error_count,
                                    inference_latency_ms=sample.inference_latency_ms,
                                    local_queue_depth=sample.local_queue_depth,
                                    model_server_healthy=sample.model_server_healthy,
                                    synthetic_failure=sample.synthetic_failure_flag,
                                    correlation_id=sample.correlation_id or "",
                                )
                            except Exception as ex:
                                logger.error(f"Failed to create AgentTelemetry: {ex}")
                                raise
                        else:
                            diagnostics.add_event(
                                "heartbeat_suppressed", {"mode": config.simulation_mode}
                            )
                            
                        await asyncio.sleep(config.heartbeat_interval_seconds)

                call = stub.StreamTelemetry(request_generator())
                
                # runs concurrently with the request_generator method using asyncio
                async for response in call:
                    # Handle owner redirect
                    if response.directive_type == "redirect":
                        owner_hint = getattr(response, "owner_hint", None)
                        if owner_hint:
                            logger.info(f"Redirected by {target} to {owner_hint}")
                            discovery.redirect(owner_hint)
                            call.cancel()
                            break
                    elif response.directive_type == "accepted":
                        logger.info(f"Telemetry accepted by Control Plane owner at {target}")
                        discovery.accept(target)
                    elif response.directive_type == "backoff" or response.directive_type == "":
                        delay = discovery.owner_failed()
                        logger.warning(
                            f"Control plane instructed backoff. Backing off for {delay:.2f}s"
                        )
                        await asyncio.sleep(delay)
                        call.cancel()
                        break
                        
        except grpc.aio.AioRpcError as e:
            delay = discovery.owner_failed()
            logger.warning(
                f"gRPC connection to {target} failed: {e.code()}. Backing off for {delay:.2f}s"
            )
            await asyncio.sleep(delay)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            delay = discovery.owner_failed()
            logger.error(
                f"Unexpected error communicating with {target}: {e}. Backing off for {delay:.2f}s"
            )
            await asyncio.sleep(delay)

    print("RUN_GRPC_STREAM EXITED WHILE LOOP")


async def async_main() -> None:
    config = AgentConfig.from_env()
    collector = build_collector(config)
    diagnostics = DiagnosticBuffer(worker_id=config.worker_id)
    
    # Attach state to FastAPI app
    app.state.buffer = diagnostics
    app.state.collector = collector

    logger.info(
        f"aegis-agent starting worker_id={config.worker_id} "
        f"telemetry_data_source={config.telemetry_data_source}"
    )

    grpc_task = asyncio.create_task(run_grpc_stream(config, collector, diagnostics))
    
    uvicorn_config = uvicorn.Config(app=app, host="0.0.0.0", port=config.api_port, log_level="info")
    server = uvicorn.Server(uvicorn_config)
    api_task = asyncio.create_task(server.serve())

    await asyncio.gather(grpc_task, api_task)


def main() -> int:
    try:
        asyncio.run(async_main())
    except KeyboardInterrupt:
        logger.info("aegis-agent shut down requested")
    return 0

if __name__ == "__main__":
    sys.exit(main())
