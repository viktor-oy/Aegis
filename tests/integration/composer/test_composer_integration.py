from __future__ import annotations

import asyncio
import contextlib
import json
import os
import httpx
from uuid import uuid4
import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from services.composer.kafka_app import KafkaComposerApp, KafkaComposerConfig
from services.composer.llm_client import InferenceConfig, OpenAICompatibleInferenceClient
from services.composer.service import ComposerService
from tests.integration.testutils import wipe_infra_state



async def run_test() -> None:
    config = KafkaComposerConfig(
        bootstrap_servers="localhost:9094",
        group_id=f"test-group-{uuid4()}",
        input_topic="aegis.postmortem.requested",
        output_topic="aegis.postmortem.generated",
    )
    llm_client = OpenAICompatibleInferenceClient(
        InferenceConfig(
            base_url="http://localhost:11434",
            model=os.environ.get("OLLAMA_MODEL", "qwen2.5:0.5b"),
            timeout_seconds=240.0
        )
    )
    service = ComposerService(llm_client)
    app = KafkaComposerApp(config, service)
    
    task = asyncio.create_task(app.run())
    # Sleep hack: give the Composer's Kafka consumer group time to negotiate and assign
    # partitions. If we don't sleep, we might publish the event before it's fully listening.
    await asyncio.sleep(2)
    
    try:
        producer = AIOKafkaProducer(bootstrap_servers=config.bootstrap_servers)
        await producer.start()
        
        incident_id = f"test-inc-{uuid4()}"
        diagnostic_event = {
            "event_id": str(uuid4()),
            "incident_id": incident_id,
            "worker_id": "aegis-system--test-worker",
            "payload": {
                "severity": "critical",
                "failure_type": "gpu_overheat",
                "reason": "Temperature reached 90C which exceeds the 85C threshold for 3 consecutive readings."
            }
        }

        print(f"Sending mock diagnostic event {{config.input_topic}} via kafka: ", diagnostic_event)
        
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
            async def wait_for_message():
                async for msg in consumer:
                    output_event = json.loads(msg.value.decode("utf-8"))
                    if output_event.get("incident_id") == incident_id:
                        assert output_event["event_type"] == "aegis.postmortem.generated"
                        # Use lower case aegis-system--test-worker since LLMs might lowercase it
                        assert "aegis-system--test-worker" in output_event["payload"]["markdown"].lower()
                        return output_event
            
            output_event = await asyncio.wait_for(wait_for_message(), timeout=180.0)
            
            assert len(output_event["payload"]["markdown"]) > 0
            
            # Assert that the AI successfully integrated critical context from the prompt
            assert "critical" in output_event["payload"]["markdown"].lower()
            assert "temperature" in output_event["payload"]["markdown"].lower()
            
            markdown_content = output_event["payload"]["markdown"]
            
            # Write markdown artifact to disk
            from pathlib import Path
            artifact_dir = Path("docs/services/composer/local/artifacts")
            artifact_dir.mkdir(parents=True, exist_ok=True)
            artifact_path = artifact_dir / f"{incident_id}.md"
            artifact_path.write_text(markdown_content, encoding="utf-8")
            print(f"\nWritten generated postmortem to: {artifact_path}\n")
            
            assert diagnostic_event["event_id"] in markdown_content, "Model dropped event_id"
            assert diagnostic_event["incident_id"] in markdown_content, "Model dropped incident_id"
            assert diagnostic_event["worker_id"] in markdown_content, "Model dropped worker_id"
        finally:
            await consumer.stop()
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


@pytest.mark.integration
def test_composer_kafka_workflow() -> None:
    wipe_infra_state("kafka:aegis.postmortem.requested,aegis.postmortem.generated")
    asyncio.run(run_test())
