# Chaos and Performance Tests

Chaos tests live separately from integration and E2E tests. Full execution requires a Kubernetes validation cluster and writes raw artifacts to `reports/chaos/`.

Required scenarios:

- vanishing Control Plane
- thundering herd and backpressure
- downstream pipeline degradation

