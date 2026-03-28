from __future__ import annotations

import json
import unittest
from pathlib import Path

from aegis_agent.diagnostics import DiagnosticBuffer
from aegis_agent.telemetry import SimulationMode, SyntheticCollector


class AgentDiagnosticsContractTests(unittest.TestCase):
    def test_trigger_diagnostics_bundle_shape(self) -> None:
        collector = SyntheticCollector("gpu-node-3", mode=SimulationMode.ECC_BURST)
        buffer = DiagnosticBuffer("gpu-node-3", max_items=8)
        for _ in range(3):
            sample = collector.collect("corr-3")
            assert sample is not None
            buffer.add_telemetry(sample)
        bundle = buffer.build_bundle("inc_ecc", "ecc_burst", "corr-3")
        Path("reports/integration").mkdir(parents=True, exist_ok=True)
        Path("reports/integration/agent-diagnostics-contract.json").write_text(
            json.dumps(bundle, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        self.assertEqual(bundle["worker_id"], "gpu-node-3")
        self.assertEqual(bundle["diagnostic_status"], "complete")
        self.assertIn("telemetry_window", bundle)
        self.assertIn("suspected_cause_hints", bundle)

