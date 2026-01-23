from __future__ import annotations

import os
import logging
from dataclasses import dataclass
from typing import Protocol

logger = logging.getLogger(__name__)

class InferenceClient(Protocol):
    model: str

    async def complete(self, prompt: str, incident_id: str, worker_id: str) -> str:
        raise NotImplementedError


@dataclass(frozen=True)
class InferenceConfig:
    base_url: str
    model: str
    api_key: str | None = None
    timeout_seconds: float = 120.0
    temperature: float = 0.2
    max_tokens: int = 1500

    @classmethod
    def from_env(cls) -> InferenceConfig:
        return cls(
            base_url=os.getenv("AEGIS_LLM_BASE_URL") or "http://vllm.aegis-system.svc.cluster.local:8000",
            model=os.getenv("AEGIS_LLM_MODEL") or "meta-llama/Llama-3.1-8B-Instruct",
            api_key=os.getenv("AEGIS_LLM_API_KEY") or None,
            timeout_seconds=float(os.getenv("AEGIS_LLM_TIMEOUT_SECONDS") or "120"),
            temperature=float(os.getenv("AEGIS_LLM_TEMPERATURE") or "0.2"),
            max_tokens=int(os.getenv("AEGIS_LLM_MAX_TOKENS") or "1500"),
        )


class OpenAICompatibleInferenceClient:
    def __init__(self, config: InferenceConfig) -> None:
        self.config = config
        self.model = config.model

    async def complete(self, prompt: str, incident_id: str, worker_id: str) -> str:
        headers = {"Content-Type": "application/json"}
        if self.config.api_key:
            headers["Authorization"] = f"Bearer {self.config.api_key}"
        payload = {
            "model": self.config.model,
            "temperature": self.config.temperature,
            "max_tokens": self.config.max_tokens,
            "frequency_penalty": 0.3,
            "messages": [
                {
                    "role": "system",
                    "content": (
                        "You write factual infrastructure incident postmortems from diagnostics. "
                        "Preserve event, incident, and worker IDs exactly in the final markdown."
                    ),
                },
                {"role": "user", "content": prompt},
            ],
            "metadata": {"incident_id": incident_id, "worker_id": worker_id},
        }
        import httpx

        url = f"{self.config.base_url.rstrip('/')}/v1/chat/completions"
        logger.info(f"Making LLM request to {url} for incident {incident_id}")
        
        async with httpx.AsyncClient(timeout=self.config.timeout_seconds) as client:
            response = await client.post(
                url,
                headers=headers,
                json=payload,
            )
            response.raise_for_status()
        body = response.json()
        choices = body.get("choices", [])
        if not choices:
            raise ValueError("inference response did not include choices")
        message = choices[0].get("message", {})
        content = message.get("content") or choices[0].get("text")
        if not isinstance(content, str) or not content.strip():
            raise ValueError("inference response did not include non-empty content")
        return content
