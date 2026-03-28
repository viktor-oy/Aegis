from __future__ import annotations

import asyncio
import json
import unittest
from pathlib import Path

from aegis_composer.service import ComposerService


class EndpointBackedStub:
    model = "vllm-contract-model"

    async def complete(self, prompt: str, incident_id: str, worker_id: str) -> str:
        sections = [
            "Title",
            "Summary",
            "Severity",
            "Affected Worker",
            "Timeline",
            "Detection",
            "Impact",
            "Suspected Cause",
            "Contributing Factors",
            "Recovery",
            "Remediation",
            "Follow-ups",
            "Trace Links",
            "Raw Appendix",
        ]
        lines = [f"# Incident {incident_id}", ""]
        for section in sections:
            lines.extend(
                [
                    f"## {section}",
                    f"{section} content for `{incident_id}` on `{worker_id}`. {prompt[:60]}",
                    "",
                ]
            )
        return "\n".join(lines)


class ComposerKafkaContractTests(unittest.TestCase):
    def test_diagnostic_event_becomes_generated_postmortem_event(self) -> None:
        diagnostic = {
            "event_id": "diag-evt-1",
            "event_type": "aegis.diagnostics.collected",
            "incident_id": "inc_abc",
            "worker_id": "gpu-node-7",
            "producer": "aegis-control-plane/cp-a",
            "timestamp": "2026-03-18T10:00:00Z",
            "schema_version": "aegis.events.v1",
            "correlation_id": "corr-7",
            "causation_id": "incident-evt-1",
            "payload": {
                "failure_type": "gpu_overheat",
                "severity": "critical",
                "diagnostic_status": "complete",
            },
        }
        generated = asyncio.run(ComposerService(EndpointBackedStub()).compose(diagnostic))
        output = generated.to_event(diagnostic, "postmortem-evt-1")
        report = Path("reports/integration/composer-kafka-contract.json")
        report.parent.mkdir(parents=True, exist_ok=True)
        report.write_text(json.dumps(output, indent=2, sort_keys=True), encoding="utf-8")
        self.assertEqual(output["event_type"], "aegis.postmortem.generated")
        self.assertEqual(output["incident_id"], "inc_abc")
        self.assertEqual(output["payload"]["metadata"]["validation_status"], "valid")

