from __future__ import annotations

import logging
from typing import AsyncGenerator, Any, Optional
from contextlib import asynccontextmanager
import asyncio

from fastapi import FastAPI
from pydantic import BaseModel

from .llm_client import InferenceConfig, OpenAICompatibleInferenceClient
from .service import ComposerService

logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    from .kafka_app import run_from_env
    task = asyncio.create_task(run_from_env())
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

app = FastAPI(
    title="Aegis Composer",
    description="Postmortem Composer Service for Aegis GPU/AI infrastructure.",
    version="1.0.0",
    lifespan=lifespan,
)


class ComposeRequest(BaseModel):
    diagnostic_event: dict[str, Any]
    guidance: Optional[dict[str, Any]] = None


class ComposeResponse(BaseModel):
    event_type: str
    incident_id: str
    worker_id: str
    markdown: str
    metadata: dict[str, Any]
    correlation_id: str


def get_composer_service() -> ComposerService:
    inference_client = OpenAICompatibleInferenceClient(InferenceConfig.from_env())
    return ComposerService(inference_client=inference_client)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/compose", response_model=ComposeResponse)
async def compose_postmortem(req: ComposeRequest) -> ComposeResponse:
    logger.info("Received synchronous compose request", extra={"incident_id": req.diagnostic_event.get("incident_id")})
    service = get_composer_service()
    generated = await service.compose(req.diagnostic_event, req.guidance)
    return ComposeResponse(
        event_type=generated.event_type,
        incident_id=generated.incident_id,
        worker_id=generated.worker_id,
        markdown=generated.markdown,
        metadata=generated.metadata,
        correlation_id=generated.correlation_id,
    )
