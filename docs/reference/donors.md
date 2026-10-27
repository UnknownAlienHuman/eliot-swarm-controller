# Donors

Borrow one proven mechanism at a time. Do not import an application, its workflow model
or its document hierarchy. Before implementing a boundary listed below, read the exact
source path and copy the shape unless a concrete Eliot constraint makes it incompatible;
record that incompatibility in the PR.

Vendor protocol facts live on the [harness pages](../harnesses/README.md).

## Adopted runtime patterns

| Eliot boundary | Primary donor and exact source | Pattern to copy | Explicitly reject |
| --- | --- | --- | --- |
| Stable Agent + incarnation fencing | [`agent-primitives/sessions`](https://github.com/agent-primitives/sessions): `api/session.proto`, `api/harness.proto`, `eventlog/store.go`; [`sandbox-driver`](https://github.com/lithoscomputer/sandbox-driver): `crates/sandbox-driver-protocol/src/supervisor.rs` | Stable session/resource id; separate execution/turn id; one monotonic fence/generation; every append/call checks current fence; replacement serves only future work; old handles/results are stale. | Eliot's former `agent_bindings`, binding revisions/current pointer, grants or proof packets. |
| Durable state reduction | [`Petri`](https://github.com/lithoscomputer/petri): `crates/core/execution/src/state.rs`, `crates/core/engine/src/apply.rs`, `replay.rs`; Agent Sessions `eventlog/store.go` | Validate against current state, durably append, then apply one pure reducer; replay reconstructs projection. Stable logical invocation id is separate from execution id. | Workflow DSL and Petri's product graph. |
| Input/turn settlement | [`ACPX`](https://github.com/openclaw/acpx): `docs/shared-sessions.md`, `docs/session-watch.md`, `docs/2026-02-27-acpx-session-model.md` | Queue acceptance, actual prompt start and final result are separate; request id survives queueing; owner death without a result is unknown and not safely retryable; one authoritative journal and one derived checkpoint. | ACPX flow graph or queue format as Eliot's public model. |
| Live/history reconciliation | [`Paseo`](https://github.com/getpaseo/paseo): `docs/timeline-sync.md`, `packages/app/src/timeline/viewed-timeline-sync.ts`; ACPX watch docs | Live stream is immediate overlay; authoritative paged history/cursor proves coverage; gaps are explicit; epoch/range replacement is atomic; never keep two discontiguous canonical ranges. | Paseo UI, client cache and JSON stores. |
| Shared native service | [`Waku`](https://github.com/egoist/waku): `crates/waku-core/src/opencode_service.rs`, `driver/opencode.rs` | Adopted service is never killed; exact demultiplex by session id; subscribe before create; one settle point; stream disconnect is not process/session death; reconnect generation orders readback/reconciliation; HTTP acceptance is not delivery. | GPL code copying; app/UI architecture. Pattern only. |
| Workspace materialization and recovery | [`Kandev`](https://github.com/kdlbs/kandev): `apps/backend/internal/worktree/worktree.go`; `docs/decisions/2026-09-29-missing-worktree-checkout-recovery.md` | Persist canonical worktree/session/repository/path/branch identity; attach-only reuse is read-only; preserve exact recorded slot/head; uncertain path/owner/branch/liveness fails closed; replacement is an explicit recovery action; external materialization is serialized and revalidated. | Kandev's Task/environment product model, broad recovery workflow and remote-executor architecture. |
| Restart reconciliation | [`controller-runtime`](https://github.com/kubernetes-sigs/controller-runtime): `pkg/reconcile/reconcile.go`, `pkg/internal/controller/controller.go` | Queue a stable object key; deduplicate keys; read durable desired/current state every pass; never reconcile one key concurrently with itself. | Kubernetes stack and retrying uncertain external effects. |
| Plugin/provider conformance | sandbox-driver: `crates/sandbox-driver-conformance/`, `crates/sandbox-driver-protocol/src/supervisor.rs`; Agent Sessions: `conformance/` | One semantic interface in-process and over transport; same black-box suite; real persistent Store plus fake/echo backend; replacement preserves provider namespace. | A second adapter interface or one-off fixture as proof. |
| Mail identity and acknowledgement | [`CCCC`](https://github.com/ChesterRa/cccc): `crates/cccc-contracts/src/connect_message.rs`, `crates/cccc-core/src/connect_delivery.rs`; [`MCP Agent Mail`](https://github.com/Dicklesworthstone/mcp_agent_mail_rust): recipient/read/ack schema and conformance | Stable delivery id; sender id + generation; concrete recipients; content digest; persist before acknowledge; same id/different body rejects; reply/cancel bound to original delivery; read and ack are recipient-scoped and idempotent. | CCCC scheduler/ledger/PTY model; MCP Agent Mail liveness chatter, implicit identities and restrictive code. |

## Resulting Eliot model

The donor patterns above deliberately produce a small middle ground:

1. one stable Agent/native subject row;
2. one integer `generation` fencing the current controlled incarnation;
3. exact turn/request ids beneath it;
4. one authoritative source per lifecycle family;
5. an opaque cursor for every source that may replay; no cursor means live-edge + gap,
   never historical replay;
6. commit acknowledgement for persisted plugin events;
7. one pure reducer and level-based restart reconciliation;
8. unknown effects remain unknown and block their narrow lane;
9. automation actions are ordinary commands;
10. one durable workspace phase with exact identity/readback and no blind setup replay;
11. mail uses explicit delivery/recipient identities.

This does **not** require binding revisions, source epochs/sequences, universal receipts,
activation hierarchies, a generic run/action engine or content-addressed artifact GC.

## v1 code donors

Frozen source is commit `dc6f70aa`; after L0.1 the same tree is tag `v1-final`.
Copy the narrow helper into v2 and cut every old dependency.

| Mechanism | v1 path | Keep / reject |
| --- | --- | --- |
| Windows Job objects, process birth identity, Unix groups | `crates/swarm-process/src/process_group.rs` | Keep assign-before-resume Job, exact `(pid,start_time)` and kill/verify. Unix process group is degraded. Reject operation/scheduler coupling. |
| Bounded child pipe capture | `crates/swarm-process/src/capture.rs` | Keep nonblocking bounded drain; v2 caps script streams at 1 MiB. |
| Private file permissions and durable replace | `crates/swarm-process/src/permissions.rs` | Keep only helpers used by daemon/token/config files. |
| Current-user Windows named-pipe ACL | `crates/swarm-kernel-host/src/platform/windows.rs` | Small direct helper; no internal auth platform. |
| Multiplexed line IPC with frame limits | `crates/swarm-client/src/multiplex.rs`, `client.rs` | Keep framing/correlation; drop v1 grants/provenance. |
| Process CPU/RSS sampling | `crates/swarm-observer/src/process_metrics.rs` | Use only if smaller/more exact than `sysinfo`. |
| Codex app-server edge cases | `crates/swarm-adapter-codex/src/lib.rs` | Read method/correlation failures; do not port journals, checkpoints or receipts. |
| Antigravity examples | `modules/antigravity/fixtures/*.stream.json` | Syntax hints only, not live evidence. |
| Atlas redaction (Apache-2.0) | `vendor/atlas/crates/atlas-redact` | Optional late pattern matcher after known-value masking exists. |

Never donate v1 Task/Attempt/review/acceptance/publication state, proof/receipt types,
policy editions, adapter journals/tombstones, module descriptors, compatibility
executors, CI patchers or audit/remediation trees.

## Rust dependencies

Check current release, license, maintenance and MSRV in the PR that first uses a crate.
Do not add it merely because it appears here.

| Crate | Concrete use |
| --- | --- |
| `tokio`, `serde`, `serde_json`, `thiserror`, `anyhow`, `tracing` | runtime/data/error/logging boundaries |
| `rusqlite` with bundled SQLite and no bindgen hooks | Store |
| `clap` derive | CLI |
| `interprocess` or Tokio platform sockets | local IPC; choose one after a Windows spike |
| `ulid` | external write ids |
| `sysinfo` | bounded process metrics |
| `notify` + one debouncer | filesystem invalidation |
| `globset` | rule/path matching and claims |
| one maintained cron parser | timers; verify timezone/DST semantics before choosing |
| `windows-sys` | exact Job, process, handle and ACL calls |
| `agent-client-protocol` | ACP plugin |
| `rmcp` | MCP surface only if a required harness cannot use the CLI |

## Other systems

Overstory, Agency Swarm and Gas Town remain product references for worktrees,
directional communication and durable work outside model context, but they do not own a
v2 core mechanism. Systems reviewed in v1 remain available under `v1-final`; do not
re-import them without a concrete missing boundary.
