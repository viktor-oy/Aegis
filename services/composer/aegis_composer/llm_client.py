from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Protocol


class InferenceClient(Protocol):
    model: str

    async def complete(self, prompt: str, incident_id: str, worker_id: str) -> str:
        raise NotImplementedError


@dataclass(frozen=True)
class InferenceConfig:
    base_url: str
    model: str
    api_key: str | None = None
    timeout_seconds: float = 60.0
    temperature: float = 0.2
    max_tokens: int = 2200

    @classmethod
    def from_env(cls) -> InferenceConfig:
        return cls(
            base_url=os.getenv("AEGIS_LLM_BASE_URL", "http://vllm.aegis-system.svc.cluster.local:8000"),
            model=os.getenv("AEGIS_LLM_MODEL", "meta-llama/Llama-3.1-8B-Instruct"),
            api_key=os.getenv("AEGIS_LLM_API_KEY") or None,
            timeout_seconds=float(os.getenv("AEGIS_LLM_TIMEOUT_SECONDS", "60")),
            temperature=float(os.getenv("AEGIS_LLM_TEMPERATURE", "0.2")),
            max_tokens=int(os.getenv("AEGIS_LLM_MAX_TOKENS", "2200")),
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
            "messages": [
                {
                    "role": "system",
                    "content": (
                        "You write factual infrastructure incident postmortems from diagnostics. "
                        "Preserve incident and worker IDs exactly."
                    ),
                },
                {"role": "user", "content": prompt},
            ],
            "metadata": {"incident_id": incident_id, "worker_id": worker_id},
        }
        import httpx

        async with httpx.AsyncClient(timeout=self.config.timeout_seconds) as client:
            response = await client.post(
                f"{self.config.base_url.rstrip('/')}/v1/chat/completions",
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
