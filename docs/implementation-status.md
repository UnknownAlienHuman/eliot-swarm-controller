# Implementation status

## Current state

**Overall: PARTIAL_PROGRESS.** This page separates studied source, the earlier
CLI implementation and recorded runtime observations. It does not inventory the
currently installed machines or claim a complete product acceptance run.

The owner-supplied baseline is
[`f1b854b6`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/commit/f1b854b6fd058dc2440d3848fc95acc279657d3f),
on `27fcae1e`. Prepared work through
[`f7e1b63`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/commit/f7e1b634b619dc2c1a723fe357307f7daab12541)
is in `main` as of 18 October; [PR #183](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/183)
is closed. Runtime source is unchanged since `25a8e3bc`. Older installed artifacts
and receipts retain their own source and run identities.
[Implementation](agent-operations/implementation.md) owns the current work order;
[source findings](agent-operations/refactoring.md) explain reductions. This page
records implementation and evidence.

### Architecture and refactoring preparation — 2026-10-18

[Architecture](agent-operations/architecture.md) is now a short product manifesto;
[implementation](agent-operations/implementation.md) owns concrete source owners,
replacements and checks. The current pass corrects the previous exclusive-Goal
design: native Harness Goal is the default where supported; the Eliot-side plugin
sends ordinary new/repeated messages and may run alongside it at the Governor's
choice. It also restores required passive observation: a local process/worktree/
GitHub/provider-JSON view, readable without a harness request and independently
of automation or diagnostic logging. The [original script review](remediation/2026-10-10/CLAUDE-SCRIPT-DONOR-REVIEW.md)
now covers the actual ZIP runners and monitors, checked against the old specification,
audits and current vendor sources. [Donor mappings](agent-operations/donor-map.md#9-official-vendor-sdks-and-native-protocol-boundaries)
separate vendor APIs/local readers from missing adapter mappings. Source study also covers
preview/schema/credential coupling, the manual acceptance gate, result-file/receipt
ordering and unbounded Muse stop waits.
Communication has [one compact contract](agent-communication-program.md). These are
documentation changes and deletion targets, not completed runtime removals or new
native qualification. The earlier CLI implementation is scoped below.

### CLI ownership reduction — in main

Commit `25a8e3b` removes the host's duplicate general application CLI and MCP launcher. Public
`swarm` commands remain in `swarm-cli`; the host retains delegated local admin,
daemon and worker entrypoints. The
[refactoring map](agent-operations/refactoring.md#cli-reduction-implemented-in-main)
records the net 1,206-line Rust deletion. This is the earlier source change;
method parity and the integrated native workflow remain broader unresolved work.

## Current source and completion map

The [source study](agent-operations/refactoring.md) identifies the
following implemented responsibilities and remaining structural gaps. These are
source observations, not measured speedups or completed deletions.

| Area | Current source and remaining gap |
| --- | --- |
| Shared harness control | Store's Eliot progression is terminal-event-only, branches by vendor and lacks recurring-message policy. Codex native Goal and Claude SDK live controls/usage exist upstream but are not mapped by the current selected adapters. Native and Eliot Goals must remain independently controlled; their corrected mapping is documented, not implemented. |
| Passive agent observation | Binding native JSON, process identity and workspace leases exist separately. A complete local joined view and provider-local collectors are pending; the public reader still contains OpenCode-specific session-ID validation. Muse Rust quota snapshots also fail Store's Node-only artifact list, rejecting accompanying activity. The documentation now names both ingress and public-reader changes. |
| Manager → agent → result → review | Store owns durable work and complete-result validation. Direct `task.accept` still accepts caller-authored requirement reviews; only its automatic producer requires an assigned pass. Moving that prerequisite into the shared handler and retaining review references is pending. The named historical accepted Task below does not prove this prerequisite is enforced on every call. |
| Store reads | Most reads still use the single writer queue. Status/monitor have a separate FIFO reader. Existing reader primitives can support bounded snapshots, but the broader read-path reduction is not implemented. |
| Capacity and automation state | Capacity v2 separates a scope header and JSON rows in `meta`; the v1 whole-ledger reader remains. Automation still stores sealed configuration/state and owner-dependent entry keys with transfer journals. Relational simplification and stable entry identity remain candidates. |
| Runtime ownership | Host still depends on six worker implementation crates. OpenCode has built-in and standalone paths; Muse has Node and Rust paths. Their convergence remains source work. Codex has already moved its current executor to Rust; old Python evidence is historical. |
| Rights and result identity | Assignment and Store authorization remain necessary. Document editions, install/provenance checks and full artifact-identity comparisons add separate coupling. Source findings identify their callers; implementation owns their finite transitions; this documentation pass changes no admission or result-reader code. |
| MCP and ScriptRun | Exact MCP exposure already has a shared owner in `swarm-contracts`; it is not duplicate policy to rebuild. New ScriptRun work already requires a selected standalone executor and rejects queued `executor=None`; its old fallback wording does not establish an active default backend. |
| Shutdown and diagnostics | Muse transport/custodian waits are unbounded in the inspected Rust path; no live hang was reproduced. Current diagnostics use Producer/HostRecorder, not the target tracing path or a fleet-wide policy-ACK interface. Bounded stop/readback and diagnostic simplification remain implementation work. |
| Product breadth | Concilium, generic negotiation/ratification, integration ACKs, watches and publication automation exist in source. The compact target excludes separate council/agreement engines and automatic publication from required completion; ordinary messages, independent review, Goals and explicit publication of accepted work remain. |

The concrete source anchors and deletion boundaries live in the single
[code-reduction list](agent-operations/refactoring.md#code-reduction-candidates).
The [communication contract](agent-communication-program.md) replaces the former
parallel communication programs. Earlier O/C/I delivery labels are historical
planning references, not a cumulative list of features to implement.

## Runtime status

The [example configuration](../config/controller.example.toml) describes routes;
a selected example or published descriptor is not an installed-machine inventory.
The runtime guides own setup and method details. Evidence below applies only to
the named artifact and observation.

| Runtime | Source/configuration state | Retained evidence and gap |
| --- | --- | --- |
| [Codex](../modules/codex/README.md) | Current executor `codex-rust-controller.1`, version `11.4`, attaches to the existing native app-server. The Python bridge and vendored SDK are retired for new work. | Bounded Rust result-page/turn-status cases are recorded in the earlier delivery evidence. Python fixture and native history do not establish Rust native or mixed-provider acceptance. |
| [OpenCode](../crates/swarm-adapter-opencode/README.md) | Example route still names built-in `eliot-opencode-v2.http.2`; prospective standalone `eliot-opencode-v2.rust-http.1@0.7.3` uses the existing authorized native service. | 0.7.1 has recorded native root readback; 0.7.2 exposed an exact input-status body defect; 0.7.3 prepares its source repair. Assistant-result correlation and complete native restart/family behavior remain unresolved. |
| [Muse](../modules/muse/README.md) | Example route remains Node `muse-sdk-1.3.0-bridge.13`. Rust `muse-rust-controller.1@0.1.2` is a separate candidate with a disabled descriptor template. | The 17 October receipt proves one exact normalized result on the retained Core. Complete method, family and recovery coverage remains open. |
| [Claude](../modules/claude/README.md) | Rust descriptor `claude-agent-sdk-rust-controller.8` exists; the disabled example still names Node `claude-agent-sdk-0.3.287-bridge.6`. | Connected Store/result-ACK and EOF/write-retirement fixtures exist. Native callbacks, uncertain ACK handling, family and installed acceptance residuals remain recorded in Issue #159. Unavailable durable defer/resume is not a new core requirement. |
| [Command](../crates/swarm-adapter-command/README.md) | Rust Batch/ACP descriptor templates exist alongside example `command-mod-0.1.0-glue.6`. ACP source retains the active prompt/client across host-link recovery. | Scoped reconnect/cancellation consumers passed in the recorded runs. Installed Command 1.81 setter/readback used no prompt. Native server restart, model result and family acceptance remain in Issue #159. |
| [Antigravity](../modules/antigravity/README.md) | Rust descriptor version 6 exists alongside example `antigravity-cli-warm-bridge.4`; the older `.3` entrypoint remains. | Normalized result/EOF and serialized-turn fixtures exist. Earlier `.2` native observations do not qualify current `.3`/`.4` live execution; the native gap remains recorded under Issue #140. |
| [Zed](zed-batch.md) | Native batch/process path exists. | Selected real-process and wait-error fixtures passed. Installed Zed and native model qualification are not recorded. |

### Muse exact normalized result — 2026-10-17

The [verification receipt](remediation/2026-10-17/MUSE-NORMALIZED-RESULT-NATIVE-VERIFICATION.json)
records Core `7edacae6`, a hot-installed Rust module built from `ba2e0d8d`, and
unchanged Muse source at integrated `b835c30c`. Native Muse Code
`1.4.0-R4161.1` used its existing provider/model configuration with requested
`xhigh` reasoning. Provider authentication stayed in the native harness.

The dispatch and exact durable revision-1 result settled `applied`. Public
`artifact.get` and `artifact.read` returned 1,023 bytes with a verified SHA-256.
The receipt still records Task completion as `unknown`; result availability is
not a claim that this Task was accepted. Native family refresh was applied with
`partial` family completeness.

The earlier revision-2 request remains
`outcome_unknown / RESULT_REVISION_NOT_AVAILABLE`: native live and reconstructed
history revisions differed. No native input was replayed. This establishes the
named module result path on that Core, without resolving the earlier unknown
Operation or qualifying all Muse methods and recovery.

### OpenCode input status — 2026-10-16

[PR #167](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/167)
prepares the standalone `0.7.3` adapter's exact input-status body. Repeated native
User projections can establish input delivery without retained inbox events;
this does not correlate assistant output or change the original input Operation.

The development `0.7.2` package observed the native User row but could not obtain
Store acknowledgement. Its saved status body was 582 bytes against the expected
438: two extra correlation fields caused the mismatch, and removing them gave
exact equality in the recorded read-only comparison. The repair keeps correlation
limitations in source metadata. The old outbox, artifacts, binding and unknown
read Operation remain unchanged; no native input was replayed. This record is a
source diagnosis, with no successful native `0.7.3` settlement claimed.

The earlier [PR #162](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/162)
observation recorded an applied `agent.open` and ready standalone `0.7.1` root on
the existing OpenCode `2.0.31` service, with zero controller model inputs. That
root-resource evidence does not establish inference or a completed Task.

The supported connection uses the already authorized native service. The
15 October correction removed controller-side provider-key bootstrap and retired
fresh isolated SDK service starts. Controller IPC/MCP authentication remains
separate. The [adapter guide](../crates/swarm-adapter-opencode/README.md) records
native-options schema and input/result boundaries.

## Issue #153 and retained delivery evidence

The [dated closeout](remediation/2026-10-15/ISSUE-153-CLOSEOUT.md) retains the
original findings, source links, public CI scopes and residual owners. Its
16 October overlay does not close Issues
[#153](https://github.com/UnknownAlienHuman/eliot-swarm-controller/issues/153) or
[#159](https://github.com/UnknownAlienHuman/eliot-swarm-controller/issues/159).
Their live state was not re-queried by this documentation pass.

Source repairs cover the qualification-fault compile gate, cached CheckRun's
source-Operation lookup, fair launch cursor, assembly read cache and retained
OpenCode uncertainty. PR #161's connected Store/worker/IPC case covers malformed
queued CheckRun, launch and automation records. The later
[`b248a8a`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/commit/b248a8a322fdf74b3eb0bafbfc1c3b8de10bc057)
repair also activates isolation for sealed v1 automation entries with missing or
non-object `scope`. These concrete repairs do not establish universal schema,
semantic or outbox isolation.

The closeout records a useful end-to-end result from
[PR #157](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/157): a
native documentation Task reached typed result, immutable submission, independent
review and acceptance by a separate native GM. Acceptance belongs to that named
Task; cross-harness live/family and release acceptance remain unestablished.

The original 17 remainder PRs were recorded as **13 done / 4 superseded / 0 open**.
Claude #42 and Command #44 were superseded with their residuals retained in
Issue #159. Closing those handoffs did not complete the remaining engineering.
The closeout also retains the evidence-access and development-process findings;
this status distillation supplies no new independent runtime verification.

### Recorded test scope

The latest manual full run checked by the closeout,
[38923761449](https://github.com/UnknownAlienHuman/eliot-swarm-controller/actions/runs/38923761449)
at `87411365`, passed Linux and failed Windows. The retained whole-Host run at
`10941ea0` recorded 1,014 passed, 14 failed and 7 ignored. Later scoped PR checks
passed, but do not replace those broader results. The subsequent full run
[38988720266](https://github.com/UnknownAlienHuman/eliot-swarm-controller/actions/runs/38988720266)
was recorded as dispatched at `a5e70bd3`; its result is not recorded here.
These are historical receipts, not fresh test results for `25a8e3bc`.

## Optional scope and evidence history

Automatic Forge publication/Issue-label projection and separate council/ACK
engines are no longer requirements for compact-core refactoring completion.
Their existing source and unresolved historical operations remain real; choosing
the smaller target does not mark those operations successful. Explicit publication
of an accepted candidate remains part of the existing Forge/GitHub surface.

The optional [Remote Gateway](gateway.md) has source implementation; live HTTP,
Cloudflare/OAuth and external-client qualification remain unrecorded. The optional
local-model/Kilo/WSL2 program has no recorded installation or native/model
qualification. Neither is a prerequisite for the current compact core.

Exact historical source checkpoints, C7/C8 and other CI runs, earlier adapter
attempts and the former delivery matrices remain in the
[pre-distillation status at `25a8e3bc`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/25a8e3bceaadeefb700defa164a4e401885f7e1c/docs/implementation-status.md).
Existing receipts retain exact identities and measurements, including
[Muse native result](remediation/2026-10-17/MUSE-NORMALIZED-RESULT-NATIVE-VERIFICATION.json),
[connected Store cases](remediation/2026-10-13/CONNECTED-STORE-BATCH-VERIFICATION.json),
and [Antigravity EOF settlement](remediation/2026-10-12/ANTIGRAVITY-EOF-SETTLEMENT-VERIFICATION.json).
They remain evidence for their recorded revisions; the current contract and
reduction list are the linked owning documents above.
