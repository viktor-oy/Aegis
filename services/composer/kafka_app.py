from __future__ import annotations

import asyncio
import json
import logging
import os
from dataclasses import dataclass
from uuid import uuid4

from .llm_client import InferenceConfig, OpenAICompatibleInferenceClient
from .service import ComposerService


@dataclass(frozen=True)
class KafkaComposerConfig:
    bootstrap_servers: str
    group_id: str
    input_topic: str
    output_topic: str

    auto_offset_reset: str = "earliest"

    @classmethod
    def from_env(cls) -> KafkaComposerConfig:
        return cls(
            bootstrap_servers=os.getenv("AEGIS_KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
            group_id=os.getenv("AEGIS_COMPOSER_GROUP_ID", "aegis-postmortem-composer"),
            input_topic=os.getenv("AEGIS_COMPOSER_INPUT_TOPIC", "aegis.diagnostics.collected"),
            output_topic=os.getenv("AEGIS_COMPOSER_OUTPUT_TOPIC", "aegis.postmortem.generated"),
            auto_offset_reset=os.getenv("AEGIS_COMPOSER_OFFSET_RESET", "earliest"),
        )


from services.pkg.pylogger.logger import logger


class KafkaComposerApp:
    def __init__(self, kafka_config: KafkaComposerConfig, service: ComposerService) -> None:
        self.kafka_config = kafka_config
        self.service = service

    async def run(self) -> None:
        from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

        consumer = AIOKafkaConsumer(
            self.kafka_config.input_topic,
            bootstrap_servers=self.kafka_config.bootstrap_servers,
            group_id=self.kafka_config.group_id,
            enable_auto_commit=False,
            auto_offset_reset=self.kafka_config.auto_offset_reset,
        )
        producer = AIOKafkaProducer(bootstrap_servers=self.kafka_config.bootstrap_servers)
        
        logger.info(f"Starting Kafka consumer on {self.kafka_config.bootstrap_servers}, topic: {self.kafka_config.input_topic}", extra={"component": "KAFKA", "event": "CONSUMER_START", "bootstrap_servers": self.kafka_config.bootstrap_servers, "topic": self.kafka_config.input_topic})
        await consumer.start()
        await producer.start()
        try:
            async for message in consumer:
                event = json.loads(message.value.decode("utf-8"))
                incident_id = event.get("incident_id")
                logger.info(f"Received diagnostic event for incident {incident_id}", extra={"component": "KAFKA", "event": "RECEIVE_EVENT", "incident_id": incident_id})
                try:
                    generated = await self.service.compose(event)
                    output = generated.to_event(source_event=event, event_id=str(uuid4()))
                    await producer.send_and_wait(
                        self.kafka_config.output_topic,
                        json.dumps(output, sort_keys=True).encode("utf-8"),
                        key=generated.incident_id.encode("utf-8"),
                    )
                    logger.info(f"Successfully generated and published postmortem for {incident_id}", extra={"component": "KAFKA", "event": "PUBLISH_SUCCESS", "incident_id": incident_id})
                    await consumer.commit()
                except Exception as e:
                    logger.error(f"Failed to process event for incident {incident_id}: {e}", exc_info=True, extra={"component": "KAFKA", "event": "PROCESS_ERR", "incident_id": incident_id, "error": str(e)})
                    # TODO: Implement DLQ for failed events.
                    # By skipping commit, the offset is not advanced until the next successful message.
                    continue
        finally:
            await consumer.stop()
            await producer.stop()


async def run_from_env() -> None:
    inference_client = OpenAICompatibleInferenceClient(InferenceConfig.from_env())
    app = KafkaComposerApp(
        kafka_config=KafkaComposerConfig.from_env(),
        service=ComposerService(inference_client=inference_client),
    )
    await app.run()


def main() -> int:
    asyncio.run(run_from_env())
    return 0

