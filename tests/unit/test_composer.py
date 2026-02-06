from __future__ import annotations

import asyncio
import unittest

from aegis_composer.prompt import build_prompt
from aegis_composer.service import ComposerService
from aegis_composer.validator import validate_postmortem


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


class ComposerTests(unittest.TestCase):
    def test_prompt_includes_guidance_and_ids(self) -> None:
        event = {
            "incident_id": "inc-1",
            "worker_id": "worker-a",
            "payload": {"severity": "critical", "failure_type": "gpu_overheat"},
        }
        prompt = build_prompt(event, {"audience": "SRE", "extra_prompt": "focus on remediation"})
        self.assertIn("inc-1", prompt)
        self.assertIn("worker-a", prompt)
        self.assertIn("focus on remediation", prompt)

    def test_composer_generates_valid_postmortem(self) -> None:
        event = {
            "incident_id": "inc-1",
            "worker_id": "worker-a",
            "correlation_id": "corr-1",
            "payload": {"severity": "critical", "failure_type": "gpu_overheat"},
        }
        generated = asyncio.run(ComposerService(StubInferenceClient()).compose(event))
        self.assertEqual(generated.event_type, "aegis.postmortem.generated")
        self.assertEqual(generated.metadata["validation_status"], "valid")
        self.assertEqual(generated.metadata["model"], "vllm-test-model")
        self.assertIn("inc-1", generated.markdown)

    def test_generated_postmortem_event_targets_kafka_payload(self) -> None:
        event = {
            "event_id": "diag-event-1",
            "incident_id": "inc-1",
            "worker_id": "worker-a",
            "correlation_id": "corr-1",
            "payload": {"severity": "critical"},
        }
        generated = asyncio.run(ComposerService(StubInferenceClient()).compose(event))
        output = generated.to_event(event, event_id="generated-event-1")
        self.assertEqual(output["event_type"], "aegis.postmortem.generated")
        self.assertEqual(output["causation_id"], "diag-event-1")
        self.assertIn("markdown", output["payload"])

    def test_validator_rejects_missing_sections(self) -> None:
        result = validate_postmortem("# nope\nshort", "inc-1", "worker-a")
        self.assertFalse(result.ok)
        self.assertTrue(result.errors)


if __name__ == "__main__":
    unittest.main()
