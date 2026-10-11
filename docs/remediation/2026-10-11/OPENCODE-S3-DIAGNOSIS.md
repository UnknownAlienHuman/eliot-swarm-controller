# Retained OpenCode S3 diagnosis

Status: blocked; zero dispatch calls; native model consumption unverified.

Run `c5f3428e-9f72-47ab-add8-389948a79b25` used source95
`95b69d1d0e37487648df0161282551e2315ac18a`. Read-only correlation of its
retained Operation and supervisor observations proves `agent.open` was rejected
with `NATIVE_MODEL_UNAVAILABLE` at `2026-10-11T04:00:24.388Z`. The runner then
reported `LAUNCH_READY_READBACK_TIMEOUT` because the required applied open did
not exist. The timeout does not establish an operation.get transport failure.

The model gate polls the scoped `/api/model` catalog for15 seconds at200ms
intervals. It requires exactly one enabled route/model match and exactly one
requested variant. The old diagnostic does not distinguish catalog unavailable,
model absent, model disabled or variant absent. The exact failed predicate
remains unknown; this historical run is not replayed to reconstruct it.

The matching module disconnect was `host_stopping` at
`2026-10-11T04:03:01.698Z`, 157.31 seconds after the rejected open. It records
teardown rather than the initiating failure. The separate
`OWNED_SERVICE_DEPARTURE_NOT_PROVED` observation remains uncertain and proves
neither service liveness nor departure.

The retained qualification receipt SHA-256 is
`84158ec7e12ac7a0811154f3551592eb38dca754ce91dfd304c2d0373481dc9a`.
The read-only diagnosis report SHA-256 is
`628ee336a2aa5820ccf050a95d7f03d933b9efb9be4e6a804b2634fef406bf41`.

A closed categorical diagnostic must survive the actual public Operation
readback. Adapter-only metadata would be persisted internally but discarded by
the Store's closed public receipt projection. The next source change therefore
needs both the bounded adapter producer and an explicit public projection,
without weakening the model gate or permitting a dispatch on open rejection.
