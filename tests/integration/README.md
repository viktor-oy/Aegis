# Integration Tests

These tests verify one subsystem boundary at a time and write raw artifacts to `reports/integration/`.

They are intentionally narrower than E2E tests. Examples include Agent diagnostics bundle shape, Composer Kafka event contract, CP + Redis leases, CP + Kafka event envelopes, and one sink dependency at a time.

