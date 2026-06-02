from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager, suppress
from typing import Any, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from .llm_client import InferenceConfig, OpenAICompatibleInferenceClient
from .service import ComposerService

from services.pkg.pylogger.logger import logger

@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None]:
    from .kafka_app import run_from_env
    task = asyncio.create_task(run_from_env())
    app.state.kafka_task = task
    yield
    task.cancel()
    with suppress(asyncio.CancelledError):
        await task

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
    task = getattr(app.state, "kafka_task", None)
    if task is not None and task.done():
        if task.cancelled():
            raise HTTPException(status_code=503, detail="Kafka consumer loop cancelled")
        elif task.exception():
            logger.error(f"Kafka loop crashed: {task.exception()}", extra={"component": "API_HEALTH", "event": "KAFKA_LOOP_CRASHED", "error": str(task.exception())})
            raise HTTPException(status_code=503, detail=f"Kafka loop crashed: {task.exception()}")
        else:
            raise HTTPException(status_code=503, detail="Kafka loop finished unexpectedly")
    return {"status": "ok"}


@app.post("/compose", response_model=ComposeResponse)
async def compose_postmortem(req: ComposeRequest) -> ComposeResponse:
    logger.info(
        "Received synchronous compose request",
        extra={"component": "API", "event": "COMPOSE_REQUEST", "incident_id": req.diagnostic_event.get("incident_id")},
    )
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
