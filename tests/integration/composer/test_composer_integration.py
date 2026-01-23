from __future__ import annotations

import asyncio
import json
import logging
import socket
import subprocess
import time
from uuid import uuid4

import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
import httpx

from services.composer.kafka_app import KafkaComposerApp, KafkaComposerConfig
from services.composer.service import ComposerService
from services.composer.llm_client import OpenAICompatibleInferenceClient, InferenceConfig


def is_port_open(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(("127.0.0.1", port)) == 0

def is_llm_ready() -> bool:
    try:
        r = httpx.get("http://localhost:11434/", timeout=2.0)
        return r.status_code == 200
    except Exception:
        return False


@pytest.fixture(scope="session", autouse=True)
def tilt_infra() -> None:
    # Use Tilt to spin up infra if Kafka is not already running on 9094
    if is_port_open(9094):
        yield
        return

    print("Starting tilt infra...")
    process = subprocess.Popen(["tilt", "up", "-f", "Tiltfile.infra", "--port", "10351"])
    
    for _ in range(120):  # Allow up to 4 minutes
        if is_port_open(9094) and is_llm_ready():
            break
        time.sleep(2)
    else:
        process.terminate()
        raise RuntimeError("Kafka or LLM server did not start in time")
    
    time.sleep(5)
    
    yield
    
    print("Tearing down tilt infra...")
    subprocess.run(["tilt", "down", "-f", "Tiltfile.infra"])
    process.terminate()
    process.wait()


async def run_test() -> None:
    config = KafkaComposerConfig(
        bootstrap_servers="localhost:9094",
        group_id=f"test-group-{uuid4()}",
        input_topic=f"input-{uuid4()}",
        output_topic=f"output-{uuid4()}",
    )
    llm_client = OpenAICompatibleInferenceClient(
        InferenceConfig(
            base_url="http://localhost:11434",
            model="llama3.2:3b",
            timeout_seconds=240.0
        )
    )
    service = ComposerService(llm_client)
    app = KafkaComposerApp(config, service)
    
    task = asyncio.create_task(app.run())
    await asyncio.sleep(2)
    
    try:
        producer = AIOKafkaProducer(bootstrap_servers=config.bootstrap_servers)
        await producer.start()
        
        diagnostic_event = {
            "event_id": str(uuid4()),
            "incident_id": "test-inc-1",
            "worker_id": "test-worker",
            "payload": {
                "severity": "critical"
            }
        }
        
        await producer.send_and_wait(
            config.input_topic,
            json.dumps(diagnostic_event).encode("utf-8")
        )
        await producer.stop()
        
        consumer = AIOKafkaConsumer(
            config.output_topic,
            bootstrap_servers=config.bootstrap_servers,
            group_id=f"test-verify-{uuid4()}",
            auto_offset_reset="earliest"
        )
        await consumer.start()
        
        try:
            # Inference can take a while on CPU, wait up to 180 seconds
            message = await asyncio.wait_for(consumer.getone(), timeout=180.0)
            output_event = json.loads(message.value.decode("utf-8"))
            
            assert output_event["event_type"] == "aegis.postmortem.generated"
            assert output_event["incident_id"] == "test-inc-1"
            assert isinstance(output_event["payload"]["markdown"], str)
            assert len(output_event["payload"]["markdown"]) > 0
            
            # Assert that the AI successfully integrated critical context from the prompt
            markdown_content = output_event["payload"]["markdown"]
            
            assert diagnostic_event["event_id"] in markdown_content, "Model dropped event_id"
            assert diagnostic_event["incident_id"] in markdown_content, "Model dropped incident_id"
            assert diagnostic_event["worker_id"] in markdown_content, "Model dropped worker_id"
        finally:
            await consumer.stop()
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


@pytest.mark.integration
def test_composer_kafka_workflow() -> None:
    asyncio.run(run_test())
