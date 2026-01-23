from __future__ import annotations

import pytest
import asyncio

from services.composer.prompt import build_prompt
from services.composer.service import ComposerService
from services.composer.validator import validate_postmortem


class StubInferenceClient:
    model = "vllm-test-model"

    async def complete(self, prompt: str, incident_id: str, worker_id: str) -> str:
        return "\n".join(
            [
                f"# GPU Worker Incident {incident_id}",
                "",
                "## Title",
                f"GPU worker incident {incident_id}",
                "## Summary",
                f"Summary for {worker_id}",
                "## Severity",
                "critical",
                "## Affected Worker",
                worker_id,
                "## Timeline",
                "Telemetry arrived and diagnostics were collected.",
                "## Detection",
                "Detected by deterministic control-plane rules.",
                "## Impact",
                "Inference worker capacity was reduced.",
                "## Suspected Cause",
                "GPU failure signal crossed threshold.",
                "## Contributing Factors",
                "Sustained pressure on the worker.",
                "## Recovery",
                "Worker was isolated for remediation.",
                "## Remediation",
                "Tune alerting and drain affected node.",
                "## Follow-ups",
                "Review thermal and model health alerts.",
                "## Trace Links",
                "correlation id retained in event metadata.",
                "## Raw Appendix",
                prompt,
            ]
        )


def test_prompt_includes_guidance_and_ids() -> None:
    event = {
        "incident_id": "inc-1",
        "worker_id": "worker-a",
        "payload": {"severity": "critical", "failure_type": "gpu_overheat"},
    }
    prompt = build_prompt(event, {"audience": "SRE", "extra_prompt": "focus on remediation"})
    assert "inc-1" in prompt
    assert "worker-a" in prompt
    assert "focus on remediation" in prompt


def test_composer_generates_valid_postmortem() -> None:
    event = {
        "incident_id": "inc-1",
        "worker_id": "worker-a",
        "correlation_id": "corr-1",
        "payload": {"severity": "critical", "failure_type": "gpu_overheat"},
    }
    generated = asyncio.run(ComposerService(StubInferenceClient()).compose(event))
    assert generated.event_type == "aegis.postmortem.generated"
    assert generated.metadata["validation_status"] == "valid"
    assert generated.metadata["model"] == "vllm-test-model"
    assert "inc-1" in generated.markdown


def test_generated_postmortem_event_targets_kafka_payload() -> None:
    event = {
        "event_id": "diag-event-1",
        "incident_id": "inc-1",
        "worker_id": "worker-a",
        "correlation_id": "corr-1",
        "payload": {"severity": "critical"},
    }
    generated = asyncio.run(ComposerService(StubInferenceClient()).compose(event))
    output = generated.to_event(event, event_id="generated-event-1")
    assert output["event_type"] == "aegis.postmortem.generated"
    assert output["causation_id"] == "diag-event-1"
    assert "markdown" in output["payload"]


def test_validator_rejects_missing_sections() -> None:
    result = validate_postmortem("# nope\nshort", "inc-1", "worker-a")
    assert result.ok is False
    assert len(result.errors) > 0
