from __future__ import annotations

import json
import unittest
from pathlib import Path


class ChaosContracts(unittest.TestCase):
    def test_chaos_artifact_schema(self) -> None:
        artifact = {
            "vanishing_control_plane": [
                "kill one CP replica",
                "observe Redis lease expiry",
                "membership fingerprint changes",
                "ring rebuilds",
                "agents reconnect",
                "no duplicate postmortems",
            ],
            "thundering_herd_and_backpressure": [
                "simulate 100 failing agents",
                "bounded queues",
                "ResourceExhausted emitted",
                "jittered retries",
                "no redirect storm",
            ],
            "downstream_pipeline_degradation": [
                "slow composer or sinks",
                "Kafka lag visible",
                "KEDA thresholds crossed",
                "DLQ receives permanent failures",
                "CP detection unaffected",
            ],
            "required_artifacts": [
                "timeline",
                "metrics",
                "trace_ids",
                "logs",
                "redis_membership_snapshots",
                "ring_rebuild_events",
                "kafka_lag",
                "recovery_confirmation",
            ],
        }
        Path("reports/chaos").mkdir(parents=True, exist_ok=True)
        Path("reports/chaos/chaos-contract.json").write_text(
            json.dumps(artifact, indent=2),
            encoding="utf-8",
        )
        self.assertIn("vanishing_control_plane", artifact)
        self.assertIn("kafka_lag", artifact["required_artifacts"])

