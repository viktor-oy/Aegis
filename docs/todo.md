
FUTURE TODO
- postmortem gen thunderingherd due to massive node failures?
- troubleshooting cap
- stop CP if any critical goroutine(e.g. membershipLoop) ends?
- if heap not enough to store worker state?
- cant UpdateRing currentRing locking be replaced with channels
- ensure cluster components authorization e.g. the controlplane current does not have a verification process for a worker sending it telemetry
