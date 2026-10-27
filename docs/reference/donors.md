# Donors

Use existing code before designing another mechanism. This is an extraction map for the
current layer, not a required tour of every agent framework. The pinned early-layer source entries were
inspected on 2026-10-27. Later entries are deferred reading pointers, not claims of fresh
verification or plug-compatible code. A reference commit is not a runtime version gate.

**Dependency:** use the maintained library directly. **Extract:** copy the named small
helper into Eliot and remove donor coupling. **Pattern:** implement the narrow contract
with Eliot types; do not import the donor application. Pattern-only is not a claim that
copy-ready code exists for Eliot's wire protocol.

## L1–L2 implementation map

Target paths are v2 destinations, not claims that these files already exist.

| Need | Mode and source | Put it in Eliot | Exact boundary |
| --- | --- | --- | --- |
| JSON/NDJSON and async child pipes | **Dependency:** `serde`, `serde_json`, Tokio and `tokio-util::codec::LinesCodec` with a maximum frame length | `eliot-plugin` framing; `eliot-daemon/src/plugin_host/` | Use existing codecs/process I/O. One continuous reader and bounded channels. Do not write a new JSON-RPC transport framework. |
| SQLite and CLI | **Dependency:** bundled `rusqlite`; `clap` | `eliot-store`; `eliot` CLI | One writer connection and normal numbered migrations. Use transactions, not event-sourcing/database frameworks. |
| Process birth and Windows Job ownership | **Extract:** [v1 process_group.rs](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/dc6f70aa08ea5fad3da04624ce50c46d45edd57c/crates/swarm-process/src/process_group.rs), especially `process_creation_filetime`, `process_birth_identity` and the Job OS calls | `eliot-daemon/src/platform/windows.rs` and small custody module | Keep OS handle lifetime, birth checks, Job assignment/termination. Replace v1 JSON/error wrappers with local typed data. Do **not** copy `hash_process_image`, named proof identities, scheduler purposes or `swarm_contracts` dependencies. |
| Bounded output on platforms where Tokio is insufficient | **Extract only if needed:** [v1 capture.rs](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/dc6f70aa08ea5fad3da04624ce50c46d45edd57c/crates/swarm-process/src/capture.rs), `PipeRead`, `PipeWrite`, nonblocking pipe helpers | same process-I/O module | Prefer Tokio first. Do not copy `CaptureReceipt`, SHA archives, 64-MiB spool defaults or the capture-file subsystem. |
| One generation for replacement plugins | **Pattern:** [sandbox-driver supervisor.rs at 5043331e](https://github.com/lithoscomputer/sandbox-driver/blob/5043331e9294bfbc98903d0bf3af675f9768198a/crates/sandbox-driver-protocol/src/supervisor.rs), `PluginGeneration`, `PluginSupervisor::current` | `eliot-daemon/src/plugin_host/` | Single-flight replacement, increasing generation, old handles remain stale, failed call is not replayed. Keep one frozen route. Do not import Sandbox/Volume APIs, executable-checksum policy or the whole supervisor. |
| Pure transition function | **Pattern:** [Petri apply.rs at cfd57a52](https://github.com/lithoscomputer/petri/blob/cfd57a52245e54b702d60321d94144800fdd92c2/crates/core/engine/src/apply.rs), `apply` | `eliot-core/src/reducer.rs` | No I/O/clock/randomness in reduction; return new state and effects. Eliot commits before publication. Do not copy graph/IR/firing/join/middleware code. |
| Codex protocol types and native calls | **Generate/use upstream:** [official app-server](https://learn.chatgpt.com/docs/app-server), installed `codex app-server generate-json-schema --out <dir>`; [upstream protocol source](https://github.com/openai/codex/tree/main/codex-rs/app-server-protocol/src) | `plugins/codex/src/native/` | Generate the installed version's schemas; deserialize the required messages into typed structs. Native types stay in the plugin. Do not hand-invent method/field names or import the Codex model loop. |

For the first path, these entries are sufficient. No agent-session service, Kubernetes
controller, mailbox framework or second harness is a prerequisite.

## Later layers: one donor per concrete need

These are source-reading entry points. Pin the selected file revision in the first PR
that actually extracts code; do not install all these projects as dependencies.

| Layer / need | Source | Use in Eliot; exclude |
| --- | --- | --- |
| L2 input vs result | [ACPX shared sessions](https://github.com/openclaw/acpx/blob/main/docs/shared-sessions.md), [watch](https://github.com/openclaw/acpx/blob/main/docs/session-watch.md) | Pattern for queued/admitted/started/terminal outcomes. Target: router/Codex result mapping. No queue/flow graph import. |
| L3 live/history recovery | [Paseo timeline-sync](https://github.com/getpaseo/paseo/blob/main/docs/timeline-sync.md), `packages/app/src/timeline/viewed-timeline-sync.ts` | Pattern for bounded history coverage and explicit gaps. Target: monitoring/plugin recovery. No UI/cache/timeline product. |
| L3 worktrees | [Kandev worktree.go](https://github.com/kdlbs/kandev/blob/main/apps/backend/internal/worktree/worktree.go) and neighboring implementation | Preserve exact repository/path/branch identity; read back before reuse. Target: workspaces. Do not port Task/Environment/cleanup workflow types. |
| L3/L6 service ownership | [Waku OpenCode service](https://github.com/egoist/waku/blob/main/crates/waku-core/src/opencode_service.rs), `driver/opencode.rs` | Pattern only: subscribe before create, exact session routing, adopted service never killed. No GPL code copying or application/UI import. |
| L4 restart reconciliation | [controller-runtime reconcile.go](https://github.com/kubernetes-sigs/controller-runtime/blob/main/pkg/reconcile/reconcile.go), `pkg/internal/controller/controller.go` | Read current state by deduplicated key; implement a small Tokio worker. No Kubernetes dependency and no automatic retry of uncertain writes. |
| L5 mail | [CCCC connect_message.rs](https://github.com/ChesterRa/cccc/blob/main/crates/cccc-contracts/src/connect_message.rs), `crates/cccc-core/src/connect_delivery.rs`; [MCP Agent Mail](https://github.com/Dicklesworthstone/mcp_agent_mail_rust) recipient/read/ack behavior | Pattern only: exact delivery/recipient, content binding, independent acknowledgement. Target: coordination/mail. Do not copy restrictive code or import ledger, PTY, scheduler, identity discovery or liveness chatter. |
| L7 conformance | [sandbox-driver conformance](https://github.com/lithoscomputer/sandbox-driver/tree/5043331e9294bfbc98903d0bf3af675f9768198a/crates/sandbox-driver-conformance) | One black-box capability suite over fake/recorded adapters. Do not build it before the real Codex path. |

## Reuse procedure

For a dependency, add the narrow needed features and use its API. For extraction, copy
only the helper and required support functions, retain license notices, replace old
project types, and connect its caller in the same change. Record repository/commit/path
in a source comment or PR; no donor dossier, receipt, generated inventory or approval
packet is required.

If extraction drags in a scheduler, proof system or several application crates, stop
extracting that route. Use the already-selected library/OS API and the donor's small
behavioral contract instead. A donor is evidence, not an architectural authority.

v1 is available at `dc6f70aa08ea5fad3da04624ce50c46d45edd57c` before the archive tag
exists. Never import its Task/Attempt/review/acceptance schema, compatibility executors,
per-adapter journals, policy hashes, executable provenance or audit tooling.

## Source and license boundary

No external source code is imported by this documentation change. The external
supervisor/reducer/mail/service references above are **patterns**, not blanket license
clearance. Check the exact revision's license and notices once when importing code;
unknown/incompatible terms mean no copying, not a request to copy first and document
later. Package versions/MSRV are selected when adding the actual dependency, not
continually re-approved in every task.
