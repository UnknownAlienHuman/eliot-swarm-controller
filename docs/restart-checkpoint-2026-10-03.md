# Restart checkpoint, 2026-10-03

The operator requested saving the current assignments before restarting the
computer and Codex. This checkpoint preserves product source and records the
remaining work. It is not whole-project completion or qualification of a new
installed executable.

## Current continuation direction, 2026-10-05

The historical receipts below remain tied to their recorded source and binaries.
For current work, the owner requires completing source first, then compilation
and focused testing. Use [current implementation status](implementation-status.md)
and the private runtime continuation checkpoint for the actual published main
SHA and unfinished assignments. Do not resume the older R7/Bunny sequence from
this document: Bunny is disabled and the selected test model is
`inclusionai/ling-3.1-flash`. Local models, Linux/WSL and Zed remain deferred.
Keep the current desktop Codex/OpenCodex processes alive.

## Saved implementation

- CheckRunner resolves and pins source, baseline, executable/version, toolchain,
  configured environment and versioned inputs before admission. Unknown Cargo
  graph inputs widen scope; missing reproducibility evidence disables reuse.
  Cache reuse points directly to an accepted original process CheckRun and
  rechecks outputs, coverage and current acceptance.
- Store status reads use a separate query-only connection. Contiguous
  `message.send` requests use bounded FIFO batching with per-request receipts
  and savepoints; replies follow the durable commit. Performance is unmeasured.
- Missed schedule slots coalesce to the latest relevant configured slot.
- Task reads expose the same normalized source-indexed brief used by Attempt
  snapshots. Historical Attempts and the accepted policy edition are preserved.
- Muse bridge.7 records child freshness and conservative command rejection.
  Codex bridge.3 adds bounded child/lifecycle/history reads and rejects
  conflicting native child identities.
- The optional loopback Remote Gateway reuses RMCP Streamable HTTP, a fixed
  restricted profile and the existing authenticated IPC. Its private bearer
  mapping does not establish Cloudflare Access, OAuth or remote-client identity.

Independent Luna source reviews covered CheckRunner identity/cache/coverage,
Windows process ownership, Codex child identity and Store batching. The Windows
probe passed after waiting on exact process handles during cancellation. Final
gate receipts and file digests are retained in the local restart handoff.

## Existing live evidence

The last installed launcher is the frozen R6 executable, source
`34bd4a42ebd7f15cc5ad6099f406d1c9cf3b8c6b`, SHA-256
`96c46cc6fbab070dc81d4ec5d0b5132e6f2f0b77eb7b2ce720632608a6ffde28`.
The new source has not replaced it.

The [native Forge record](qualification/2026-10-03-forge-native.md) binds a real
captured-source check, independent acceptance and one non-force main
publication. The [OpenCode restart record](qualification/2026-10-03-opencode-restart.md)
preserves graceful recovery without replay, queued completion and the failed
killed-service baseline gate. Running-input continuation remains unqualified.
All services created for those trials were stopped; original desktop Codex
processes remained alive.

## Resume after restart

1. Read this checkpoint, the private handoff and the current owner policy.
   Inspect Git status and fetch main; protect any later user or external edits.
   Rediscover processes and listener ownership. Historical PIDs are not current
   process identities.
2. Read the final compile/test and CI receipts. Resolve an actual failure before
   widening checks. Build one R7 release from the frozen reviewed source and
   record its commit, tree and executable digest before installation.
3. Run one quiet host-only load measurement of the new status/batching path,
   comparing durable observations and loaded status latency with the retained
   baseline. Two hundred controller clients do not imply native agents.
4. Finish the isolated Codex composition harness. Its current preparation is
   incomplete: the service start fails closed and the inference runner is
   absent. Provision a new private OpenCodex profile with the authorized Bunny
   route, verify ChatGPT subscription forwarding and an empty fallback, then
   review the exact harness before one admitted Task. Do not use a paid OpenAI
   API fallback or restart the desktop Codex process.
5. Qualify the new gateway locally before any external deployment. Preserve
   failed native restart evidence; do not replay its original model inputs.
   Claude inference remains deferred until the other requested routes are
   ready; Zed installation remains deferred by the operator.

The Eliot Memory OS compile-packet request is committed, but the service is not
connected. Standalone work proceeds under the documented conditional
[integration contract](eliot-memory-os-integration.md); no substitute memory
store or fabricated packet is required. Legacy Swarm code remains retired
outside skill discovery with its verified evidence retained.

## Post-restart readback

After the actual restart, native Codex reported version 0.160.0. The retained
first final test run had 210 passes and six failures: adding the Task brief had
made old unreadable specs fail Task reads, and the new batching fixture counted
a rejected receipt as an observation. The repair keeps raw legacy specs
readable with an explicit unavailable brief and preserves strict new-Attempt
validation; the fixture now follows the existing successful-admission event
contract. The changed source passed warnings-denied Clippy, all 217 library
tests and the Windows ownership probe.

The operator also requested implementation of PR #22 (communication, launcher
and MCP catalogue) and PR #23 (manager-owned Rust automation). Those programs
remain separate from this checkpoint's verified source. Luna assignments cover
Participant scope, exact-slot review, durable automation routing, MCP catalogue
presentation and bounded launcher projections. New source stays unqualified
until integrated and reviewed; the full objective remains open.
