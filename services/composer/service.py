from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from .llm_client import InferenceClient
from .prompt import build_prompt
from .validator import validate_postmortem

logger = logging.getLogger(__name__)


def load_prompt_string(env_var: str, default_file_path: str, name: str) -> str:
    env_val = os.getenv(env_var, "").strip()
    if env_val:
        logger.info(f"Skipping default file [{default_file_path}], using ENV for {name}")
        return env_val
    else:
        logger.info(f"Using default prompt file from [{default_file_path}] for {name}")
        try:
            return Path(default_file_path).read_text(encoding="utf-8")
        except FileNotFoundError:
            logger.warning(f"Default prompt file [{default_file_path}] not found for {name}")
            return ""

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
        
        self.label_worker_id = os.getenv("POSTMORTEM_LABEL_WORKER_ID", "Worker ID")
        self.label_incident_id = os.getenv("POSTMORTEM_LABEL_INCIDENT_ID", "Incident ID")
        self.label_event_id = os.getenv("POSTMORTEM_LABEL_EVENT_ID", "Event ID")
        
        prompts_dir = Path(__file__).parent / "prompts"
        self.sys_arch = load_prompt_string(
            "AEGIS_COMPOSER_SYS_ARCH_PROMPT",
            str(prompts_dir / "sys-arch.md"),
            "System Architecture"
        )
        self.format_prompt = load_prompt_string(
            "AEGIS_COMPOSER_FORMAT_PROMPT",
            str(prompts_dir / "postmortem-format.md"),
            "Postmortem Format"
        )

    async def compose(
        self,
        diagnostic_event: dict[str, object],
        guidance: Optional[dict[str, object]] = None,
    ) -> GeneratedPostmortem:
        incident_id = str(diagnostic_event["incident_id"])
        worker_id = str(diagnostic_event["worker_id"])
        correlation_id = str(diagnostic_event.get("correlation_id", ""))
        
        logger.info(f"Building prompt for incident {incident_id}")
        prompt = build_prompt(
            diagnostic_event=diagnostic_event, 
            sys_arch=self.sys_arch, 
            format_prompt=self.format_prompt, 
            guidance=guidance,
            label_worker_id=self.label_worker_id,
            label_incident_id=self.label_incident_id,
            label_event_id=self.label_event_id,
        )
        
        logger.info(f"Sending prompt to LLM for incident {incident_id}")
        markdown = await self.inference_client.complete(
            prompt,
            incident_id=incident_id,
            worker_id=worker_id,
        )
        
        logger.info(f"Validating generated postmortem for incident {incident_id}")
        validation = validate_postmortem(markdown, incident_id=incident_id, worker_id=worker_id)
        if not validation.ok:
            logger.warning(f"Postmortem validation failed for incident {incident_id}: {validation.errors}")
        else:
            logger.info(f"Postmortem validation successful for incident {incident_id}")

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
