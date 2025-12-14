# Local Kubernetes Validation Runbook

1. Open the repository in the Devcontainer.
2. Create a local cluster:

   ```sh
   make kind-up
   ```

3. Start Tilt:

   ```sh
   tilt up
   ```

4. Watch:

   ```sh
   kubectl -n aegis-system get pods
   kubectl -n aegis-system logs deploy/aegis-control-plane
   ```

5. Trigger a synthetic failure by setting the Agent simulator mode to `overheat`, `vram_pressure`, `ecc_burst`, `latency_spike`, or `model_crash`.

6. Inspect artifacts:

   ```sh
   ls reports/integration
   ls reports/chaos
   ```

## Notes

Local Kind validates cloud-style manifests. Do not add Docker Compose as the primary runtime. Use Tilt live updates for service code and keep stateful dependencies running across app edits.

