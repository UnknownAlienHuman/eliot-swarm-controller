# Audit register

Durable resume point for the source audit. It records the inspected commit, exact
paths, confirmed/refuted findings, preserved mechanisms and the next untraced chain.
It is not another runtime registry, implementation queue or approval gate.
[Implementation](implementation.md) remains the work order;
[status](../implementation-status.md) records landed behavior.

## Resume here

- Pass: **2026-10-23 / cross-adapter result source and Store-delivery ownership**.
- Inspected runtime head: **`ac3389c013840b02a78dc310bcee481d24086402`**.
- Overall audit: **incomplete**. No measured completeness percentage is claimed.
- Newly traced: selected Claude, Codex, OpenCode, Command and Muse result sources,
  pre-IPC page retention, ACK replay, restart behavior and source release.
- New findings: **AR-025–AR-031**. AR-006 is broadened beyond Antigravity; AR-024 is
  corrected: an adapter-local ACK-range ledger would duplicate Store coverage and is
  removed from the target.
- **Next exact chain:** current root/session replacement and historical result reads in
  Codex, OpenCode, Muse and Claude; then Store object authorization for a first
  historical `agent.result` and the exact `capture_once` query. After that continue
  with queue/root capacity B rather than another result framework.
- This pass changes documentation only. No runtime code, Clippy, tests, installed
  harness or independent verifier ran. Source tests are not new executed evidence.

On resume compare changed source paths with this head. Recheck changed callers and
consumers, not the whole archive. Update this register and the owning contract together.
An interrupted chat does not clear findings or make inherited claims current.

## Evidence labels

- **source-confirmed**: named path read; consequence follows; not live reproduction.
- **preserve**: useful current behavior, not a deletion target.
- **partial**: exact untraced boundary named.
- **inherited**: earlier source evidence not rechecked in this pass.

Source review, implementation and executed verification remain separate.

## Findings and coverage

| ID | Requirement / path | Disposition | Remaining boundary |
| --- | --- | --- | --- |
| AR-001 | Antigravity report failure → native shutdown | **Open defect.** Report retry runs in the terminal reducer; failure reaches stdin shutdown and drained facts can stay memory-only. | Independent Store delivery and continuous native draining. |
| AR-002 | Complete result retention | **Open defect.** Antigravity's 32-response cache can evict the only uncollected body. | Durable body/source lifetime and admission pressure. |
| AR-003 | Exact result identity | **Preserve.** Operation/receipt/conversation/generation checks and explicit unavailable oversized output. | Keep causal identity while deleting package-proof copies. |
| AR-004 | Adapter restart without native replay | **Open defect.** New Antigravity Controller has empty body/delivery caches. | Original-Operation successor delivery. |
| AR-005 | Previously admitted page completion | **Preserve.** Sealed normalized origin permits narrow late completion and replay ACK. | First historical admission and partial storage. |
| AR-006 | Complete continuation result | **Open defect, now cross-route.** Store generic origin and selected Antigravity, Claude, Codex and OpenCode normalized-result paths remain initial-Dispatch-centric. Muse can address a native item, but Store admission is still narrower. | One source-Operation path for attributable `agent.send`; taskless input stays distinct. |
| AR-007 | Assembly and artifact reads | **Preserve.** Publish/register separation, contiguous coverage, digest checks and Bytes authorization. | Crash/power-loss and remaining grant/cache races. |
| AR-008 | Authentication versus admitted completion | **Preserve boundary.** Disabled credentials fail new handshake; an authenticated carrier has narrow prior-page completion. | Revocation and successor delivery. |
| AR-009 | Continuation Task/Attempt attribution | **Mixed.** Selected Manager, Repair and Goal paths retain scope; binding-only input may be intentionally taskless. | Explicit task-linked versus taskless authority. |
| AR-010 | Repair delivery readback | **Open defect.** Reader requires provider-specific input ID/condition and treats every Rejected/Cancelled as no effect. | Common admission/terminal facts. |
| AR-011 | Correction submission | **Preserve.** Same unreleased `needs_correction` Attempt can resubmit; stale publication remains historical. | Continuation origin through candidate validators. |
| AR-012 | Task revision fences input | **Open defect.** Revision preserves old Attempt; direct Send can become accidentally taskless or dispatch without current-revision recheck. | Supersede unsent old-revision effects and recheck exact Task/Attempt. |
| AR-013 | GM disposes superseded work | **Open defect.** Current GM can revise beneath another owner but then cannot release the old Attempt. | Scoped historical cleanup authority without owner rewrite. |
| AR-014 | Release preserves result capture | **Open defect.** Release cancels queued `agent.result` with every non-check Operation. | Cancellation by effect semantics. |
| AR-015 | Module credential on restart | **Preserve.** Binding-scoped credential is reused; implicit rotation is forbidden. | Live restart/revocation qualification. |
| AR-016 | Old-boot first delivery | **Open defect.** Unseen old-boot terminal fails current-boot validation; exact already-committed outcome may replay-ACK. | Execution origin distinct from delivery actor. |
| AR-017 | Capture while execution is recovery-fenced | **Open defect.** `recovery_required` filters sealed Result before completion-only exception. | Permit exact completion/readback without session readiness. |
| AR-018 | Supervisor recovery meaning | **Preserve/correction.** Supervisor recovery reconciles automation/Hook intake and process demand, not native bytes. | Keep import in adapter/Store owners. |
| AR-019 | Complete inconclusive readback | **Open defect.** Antigravity Reconcile reports the lookup itself Unknown after completed inconclusive inspection. | Applied lookup with `resolved:false`; target remains unknown. |
| AR-020 | Terminal evidence versus release | **Boundary.** Late terminal may update immutable unreleased old Attempt; terminal alone does not prove body capture. | Complete-output obligation without weakening producer fences. |
| AR-021 | First historical result admission | **Open defect.** New normalized Result requires current open unreleased Task/Attempt; only a previously admitted Result can finish later. | Historical source-Operation completion authority. |
| AR-022 | Automatic Result/Reconcile owner | **Open defect.** Current automatic consumers create neither Result nor Reconcile; Doctor only recommends a command. | Bounded `capture_once` / `reconcile_once` current-state loop. |
| AR-023 | Supervisor operation readback | **Scope correction.** It carries unresolved IDs/status only; it does not settle targets. | Store/adapter semantic recovery. |
| AR-024 | Antigravity page ACK/source release | **Open defect, target corrected.** Adapter checks only `recorded:true` and has no complete-source release. A local range union would duplicate Store state and is rejected. | Store-authoritative `coverage_complete` / next missing range in exact replay-safe ACK. |
| AR-025 | Result sources are heterogeneous | **Source-confirmed design boundary.** Local body/file, provider history and provider output-ref sources have different recovery/lifetime semantics. | Share Store delivery only; keep source reader adapter-local. |
| AR-026 | Claude result delivery | **Open defect.** Selected Rust adapter persists result intent, but not exact page bytes before `module.result`; ambiguous ACK is settled Unknown and replay is explicitly refused on same/replacement boot. | Persist ready exact page before IPC; replay only Store delivery, never SDK execution. |
| AR-027 | Codex result delivery | **Preserve donor plus gap.** Exact full `module.result` params are persisted before IPC and replayed after restart. Provider history remains source. Result read requires current root and initial Dispatch. | Keep exact delivery; remove full-snapshot/tombstone machinery and current-root-only historical gate. |
| AR-028 | OpenCode result delivery/retention | **Preserve donor plus defects.** Exact page params are journaled and replayed; exact assistant parent is verified. Input Operations marked `result_source` are excluded from reclamation with no release transition; normalized result targets Dispatch only. | Coverage-driven source release and continuation result; delete retention graph/full journal. |
| AR-029 | Command result release | **Preserve donor plus overengineering.** Exact page is saved, ACKed and removable. Source-file release is wrapped in public/private release-evidence Operation, tombstones and cleanup markers. | Release local source from Store-complete coverage; delete proof protocol. |
| AR-030 | Muse Node result source | **Preserve donor.** On-demand exact item/revision/cursor/output-ref read, bounded range and digest validation; ready page/outcome checkpointed for retry. | Capture promptly before provider retention disappears; no transcript mirror. |
| AR-031 | Page coverage owner | **Source-confirmed correction to target.** Store already owns immutable pages and per-page `next_offset_bytes`; immediate ACK exposes only recorded/artifact ref. Adapter-local ACK-range state would duplicate authority. | Store returns authoritative complete/next-missing coverage on exact ACK/replay; adapter retains source until complete. |

## Current pass: route matrix

| Route | Native/result source | Exact Store-delivery behavior | Keep | Delete / repair |
| --- | --- | --- | --- | --- |
| Antigravity | Boot-local serialized response body; no history API | Outcome memory queue; result page not durable; ACK only boolean | Sequential Operation/body identity | Memory-only body, count eviction, no complete-source ACK, report failure changing native lifecycle |
| Claude Rust | Boot-local SDK result frame/body in `NativeControl` | Result intent journaled; page bytes sent directly; ambiguous ACK becomes Unknown and replay is refused | Exact SDK frame/input/digest validation; Applied `resolved:false` Reconcile pattern | Persist ready page; remove result-intent-without-bytes dead end and dispatch-only target |
| Codex Rust | App-server thread/turn/final assistant history | Exact full `module.result` params persisted before IPC and replayed after restart | This exact Store-delivery pattern; exact user/turn/final assistant read | Full checkpoint/tombstones; current-root-only historical gate; dispatch-only result |
| OpenCode standalone | Provider session/message history with exact assistant parent edge | Exact page params/outbox persisted, ACKed and replayed | Exact delivery, native input ID, parent edge, Applied `resolved:false` Reconcile | Permanent `result_source`, full journal/retention graph, dispatch-only normalized result |
| Muse Node | Provider `view/page` and `item/readOutput` using item revision/cursor/output ref | Checkpointed outcome/page retried exactly after reconnect/restart | Provider locator, bounded range/digest checks, exact ready page delivery | Broad checkpoint copies and proof duplication; do not snapshot transcript |
| Command | Local captured output files | Exact saved page → Store ACK → delete | Local source file and exact ready-page delivery | Release-evidence method, tombstones, cleanup/proof state machine |

The sources cannot share one journal format. A local body needs durable bytes; a
provider-history source needs a stable locator and honest expiry/gap; a captured file
needs path custody. Translating them into one schema would recreate the current tumor.

## Minimal common boundary

Only these mechanics qualify for shared extraction after two routes delete their old
loops:

```text
StoreDelivery
  method = module.outcome | module.result
  immutable delivery key/digest
  exact serialized params (including ready page bytes)
  binding/generation + origin Operation/boot metadata needed by Store
  atomic publish before IPC
  resend identical params until exact Store ACK
  remove after ACK; if removal durability is unknown, safe replay remains
```

No persistent ACK tombstone is required. If Store committed but the process crashed
before local deletion, resending identical params must return replay ACK. Conflicting
params fail.

`ResultSource` remains adapter-local:

- durable local body/file; or
- provider locator + native identity/cursor/digest/coverage; or
- explicit unavailable/expired gap.

Store is the page-coverage authority. `module.result` ACK/replay should return the
registered artifact plus either complete coverage or the next missing range for the
same source identity. The adapter does not maintain a second union of ACKed ranges.
It retains local source bytes/locator until Store reports complete coverage or explicit
policy disposes them.

Observation remains one replaceable latest checkpoint per source. Native intent and
native effect recovery remain adapter-specific.

## Why the existing journals are anti-donors

- **OpenCode:** full operation journals, reverse-reference graph, address memos,
  startup census, retention cursor and permanently pinned result sources.
- **Codex:** large full checkpoint, per-operation pending page/ACK/tombstone state and
  route/session state in one file.
- **Command:** output capture, Store page delivery and release-proof cleanup combined.
- **Claude:** effect journal records result intent but omits the ready page bytes that
  actually need exact retry.
- **Antigravity:** simple but memory-only and coupled to the native event loop.

Do not generalize any one of these. Extract exact Store delivery only after the first
vertical route proves the boundary and a second current route deletes its duplicate.

## Inspection log for this pass

| Source | Inspected portion |
| --- | --- |
| `crates/swarm-adapter-claude/src/lib.rs` | result intent, SDK body lookup, direct page send, ACK ambiguity/replay refusal, Reconcile outcome. |
| `crates/swarm-adapter-codex/src/lib.rs` | provider-history result build, pending page checkpoint, ACK/tombstone and current-root check. |
| `crates/swarm-adapter-opencode/src/lib.rs` | exact result intent/page/outbox, assistant parent validation, restart replay and Reconcile. |
| `crates/swarm-adapter-opencode/src/journal.rs` / `journal/retention.rs` | result-source marking, reclamation eligibility and retention graph. |
| `crates/swarm-adapter-command/src/journal.rs` / adapter result path | saved page, exact ACK and release-evidence cleanup. |
| `modules/muse/results.mjs` / `bridge.mjs` | on-demand provider read, range/digest checks, checkpointed result delivery. |
| `store/results.rs::record` | page Operation settlement, replay ACK, `next_offset_bytes`; immediate ACK lacks aggregate coverage. |

## Next work

1. Trace historical provider-root/session replacement for Codex, OpenCode, Muse and
   Claude and identify which source locators survive without another native effect.
2. Define exact Store query/authorization for `capture_once`, including taskless output
   retention and superseded Task scope.
3. Update implementation with the first Antigravity vertical slice only after the
   caller-to-Store path is fully specified; no shared crate first.
4. Then continue queue/root-capacity audit B.

Earlier detailed traces remain in repository history:

- [`c3e331a`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/c3e331a37ef58b3b98f7859bfb9e40935501e106/docs/agent-operations/audit-register.md)
- [`70b99db3`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/70b99db3d8732d10a69d03d1dbe83afdd39a7229/docs/agent-operations/audit-register.md)
- [`1c61629b`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/1c61629b49e8d6b9763dc7f5be32a0135509d7e3/docs/agent-operations/audit-register.md)
- [`ec019052`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/ec0190526508af1daf0da3a1aae51ee425b6ab04/docs/agent-operations/audit-register.md)
- [`f5ccc418`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/f5ccc418ba401733d1f778619abeec5c0554416b/docs/agent-operations/audit-register.md)
- [`ac3389c`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/ac3389c013840b02a78dc310bcee481d24086402/docs/agent-operations/audit-register.md)

A docs commit is not a repair. A finding closes only with connected runtime behavior
and exact evidence.
