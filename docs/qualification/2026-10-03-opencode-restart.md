# OpenCode restart observations, 2026-10-03

These observations used the frozen R6 controller and pinned native OpenCode
2.0.7 under Bun 1.4.0. Each isolated trial admitted exactly two inputs through
the controller: one running and one queued. The configured model was
`opencode-go/space-bunny-free`, variant `low`, using the operator-authorized
provider key. Existing sessions and credentials were not modified.

## Graceful restart

The owned native service was stopped and restarted against its original private
database. The controller recovered both original Operations without creating
fresh dispatches: each native input was enqueued once and delivered at most
once. The queued input completed; the running input remained recovery-pending.
The binding remained reconciling. Session-only restart events did not identify
continuation of the original running input, so running-input continuation is
unqualified.

## Killed service

The exact owned native process was killed at 11:11:44.832 UTC. Readback retained
the original running and queued Operations, their admitted producers, and the
reconciling binding. No replacement input was submitted.

The first passive observer started at 12:06:14.534 UTC. Its baseline check failed:
the event snapshot taken at 11:11:28.147 UTC contained six events, while four
more events had timestamps between 11:11:29.226 and 11:11:32.213 UTC, before
the kill. That chronology explains the mismatch but does not independently bind
the event set to a pre-observer database snapshot. The original
`identity_verified: false` receipt is retained. No automatic-resume observer
was launched after that failed gate.

The passive observer's API reported the exact new process and native version.
It was then stopped with exit code zero. The original native ownership record
remained unchanged. This establishes the observed process identity and clean
observer stop; it does not turn the failed baseline receipt into success.

## Cleanup and limits

All services, controllers and observers created for these two trials were
stopped. Original desktop Codex processes remained alive. Their private
databases, inputs, failed harness records and readback receipts remain retained.
No global Codex restart occurred.

The graceful case proves recovery without input replay and queued completion
for the recorded trial. Neither case qualifies running-input continuation,
whole-family completeness, or a new controller executable. Earlier harness
failures remain separate evidence and were not repaired in place.
