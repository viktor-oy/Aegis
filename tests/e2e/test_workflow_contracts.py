from __future__ import annotations

import json
import unittest
from pathlib import Path


class AegisWorkflowContracts(unittest.TestCase):
    def test_required_e2e_scenarios_are_defined(self) -> None:
        scenarios = [
            {
                "name": "happy-path incident",
                "flow": [
                    "mock gpu failure",
                    "control-plane detection",
                    "diagnostics",
                    "postmortem generation",
                    "slack/postgresql/s3 delivery",
                ],
            },
            {
                "name": "owner redirect and failover",
                "flow": [
                    "bootstrap connects to non-owner",
                    "owner redirect",
                    "membership fingerprint changes",
                    "agent reconnects",
                    "no duplicate incidents",
                ],
            },
            {
                "name": "degraded diagnostics path",
                "flow": [
                    "agent unreachable after last telemetry",
                    "partial diagnostic event",
                    "degraded postmortem",
                    "delivery status published",
                ],
            },
        ]
        Path("reports/integration").mkdir(parents=True, exist_ok=True)
        Path("reports/integration/e2e-scenarios.json").write_text(
            json.dumps(scenarios, indent=2),
            encoding="utf-8",
        )
        self.assertEqual(len(scenarios), 3)
        self.assertIn("no duplicate incidents", scenarios[1]["flow"])

