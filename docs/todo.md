
FUTURE TODO
- use PVC or similar(e.g. s3) means to load AI model instead of docker build or entryscript(to avoid data logistics or always redownloading)
- implement submitdiagnosis
- remove hardcoded configs including fallbacks(.urls ) that makes the program too forgiving instead of failing fast when the user has not provided config
- postmortem gen thunderingherd due to massive node failures?
- troubleshooting cap
- stop CP if any critical goroutine(e.g. membershipLoop) ends?
- if heap not enough to store worker state?
- cant UpdateRing currentRing locking be replaced with channels
- ensure cluster components authorization e.g. the controlplane current does not have a verification process for a worker sending it telemetry
- test coverage tool