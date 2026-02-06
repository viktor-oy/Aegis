from __future__ import annotations

import unittest

from aegis_agent.backoff import FullJitterBackoff
from aegis_agent.client import ControlPlaneDiscovery
from aegis_agent.config import AgentConfig, TelemetryDataSource
from aegis_agent.diagnostics import DiagnosticBuffer
from aegis_agent.telemetry import GPUTelemetryCollector, SimulationMode, SyntheticCollector, build_collector


class AgentTests(unittest.TestCase):
    def test_synthetic_overheat_metrics(self) -> None:
        collector = SyntheticCollector("worker-a", mode=SimulationMode.OVERHEAT, seed=1)
        sample = collector.collect("corr-1")
        self.assertIsNotNone(sample)
        assert sample is not None
        self.assertGreaterEqual(sample.temperature_celsius, 91)
        self.assertEqual(sample.correlation_id, "corr-1")

    def test_freeze_suppresses_heartbeat(self) -> None:
        collector = SyntheticCollector("worker-a", mode=SimulationMode.FREEZE)
        self.assertIsNone(collector.collect())

    def test_diagnostic_buffer_limits_items(self) -> None:
        collector = SyntheticCollector("worker-a")
        buffer = DiagnosticBuffer("worker-a", max_items=2)
        for _ in range(3):
            sample = collector.collect()
            assert sample is not None
            buffer.add_telemetry(sample)
        bundle = buffer.build_bundle("inc-1", "gpu_overheat", "corr-1")
        self.assertEqual(len(bundle["telemetry_window"]), 2)
        self.assertEqual(bundle["incident_id"], "inc-1")

    def test_full_jitter_backoff_stays_under_cap(self) -> None:
        backoff = FullJitterBackoff(base_seconds=1.0, cap_seconds=2.0, seed=7)
        sleeps = [backoff.next_sleep() for _ in range(5)]
        self.assertTrue(all(0 <= sleep <= 2.0 for sleep in sleeps))
        backoff.reset()
        self.assertLessEqual(backoff.next_sleep(), 2.0)

    def test_owner_redirect_fallback(self) -> None:
        discovery = ControlPlaneDiscovery(["cp-a", "cp-b"], FullJitterBackoff(seed=4))
        self.assertEqual(discovery.next_target(), "cp-a")
        redirect = discovery.redirect("cp-owner")
        self.assertTrue(redirect.redirected)
        self.assertEqual(discovery.next_target(), "cp-owner")
        sleep = discovery.owner_failed()
        self.assertGreaterEqual(sleep, 0)
        self.assertIn(discovery.next_target(), {"cp-b", "cp-a"})

    def test_collector_uses_mock_data_source(self) -> None:
        config = AgentConfig(
            worker_id="worker-a",
            telemetry_data_source=TelemetryDataSource.MOCK,
            simulation_mode=SimulationMode.NORMAL,
            heartbeat_interval_seconds=5,
            bootstrap_urls=["cp-a"],
        )
        self.assertIsInstance(build_collector(config), SyntheticCollector)

    def test_collector_uses_gpu_data_source(self) -> None:
        config = AgentConfig(
            worker_id="worker-a",
            telemetry_data_source=TelemetryDataSource.GPU,
            simulation_mode=SimulationMode.NORMAL,
            heartbeat_interval_seconds=5,
            bootstrap_urls=["cp-a"],
        )
        self.assertIsInstance(build_collector(config), GPUTelemetryCollector)


if __name__ == "__main__":
    unittest.main()
