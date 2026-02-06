from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from .llm_client import InferenceClient
from .prompt import build_prompt
from .validator import validate_postmortem


@dataclass(frozen=True)
class GeneratedPostmortem:
    event_type: str
    incident_id: str
    worker_id: str
    markdown: str
    metadata: dict[str, object]
    correlation_id: str

    def to_event(self, source_event: dict[str, object], event_id: str) -> dict[str, object]:
        return {
            "event_id": event_id,
            "event_type": self.event_type,
            "incident_id": self.incident_id,
            "worker_id": self.worker_id,
            "producer": "aegis-composer",
            "timestamp": self.metadata["generated_at"],
            "schema_version": "aegis.events.v1",
            "correlation_id": self.correlation_id,
            "causation_id": source_event.get("event_id", ""),
            "payload": {
                "markdown": self.markdown,
                "metadata": self.metadata,
            },
        }


class ComposerService:
    def __init__(
        self,
        inference_client: InferenceClient,
        prompt_version: str = "aegis-postmortem-v1",
    ) -> None:
        self.inference_client = inference_client
        self.prompt_version = prompt_version

    async def compose(
        self,
        diagnostic_event: dict[str, object],
        guidance: dict[str, object] | None = None,
    ) -> GeneratedPostmortem:
        incident_id = str(diagnostic_event["incident_id"])
        worker_id = str(diagnostic_event["worker_id"])
        correlation_id = str(diagnostic_event.get("correlation_id", ""))
        prompt = build_prompt(diagnostic_event, guidance)
        markdown = await self.inference_client.complete(
            prompt,
            incident_id=incident_id,
            worker_id=worker_id,
        )
        validation = validate_postmortem(markdown, incident_id=incident_id, worker_id=worker_id)
        return GeneratedPostmortem(
            event_type="aegis.postmortem.generated",
            incident_id=incident_id,
            worker_id=worker_id,
            markdown=markdown,
            correlation_id=correlation_id,
            metadata={
                "model": self.inference_client.model,
                "prompt_version": self.prompt_version,
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "validation_status": "valid" if validation.ok else "invalid",
                "validation_errors": validation.errors,
            },
        )
