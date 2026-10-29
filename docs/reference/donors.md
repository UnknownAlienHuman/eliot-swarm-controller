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
| L3 Git/fs observation | **Dependency:** installed [Git status](https://git-scm.com/docs/git-status) and [log](https://git-scm.com/docs/git-log); **pattern:** v1 `crates/swarm-kernel-host/src/workspace.rs` at `dc6f70aa`, `git_path_argument` | Invoke the existing CLI under scoped custody; normalize supported Windows verbatim paths and parse bounded porcelain-v2/NUL output. Git 2.55.0.windows.5 and its GPL-v2 license were checked on 2026-10-27. No Git source is copied or linked, and no v1 Forge/workspace executor is imported. |
| L3 filesystem invalidation | **Dependency:** [`notify` 8.2.1](https://github.com/notify-rs/notify/tree/notify-8.2.1), default features disabled | Coalesce Windows/Linux filesystem wake hints with atomics; Git readback and periodic polling remain authoritative. CC0-1.0 and declared Rust 1.85 minimum checked on 2026-10-27 against the pinned Rust 1.99.1 toolchain. No notification paths or file contents are retained. |
| L3 opened rollout identity | **Dependency:** existing `windows-sys` 0.61.2; [FileIdInfo](https://learn.microsoft.com/en-us/windows/win32/api/winbase/ns-winbase-file_id_info), `GetFinalPathNameByHandleW`; stable Unix `MetadataExt` and Linux `/proc/self/fd` | Shared safe helpers borrow the same open file for full Windows volume/128-bit file ID and final path, or Unix device/inode. Unsupported calls fail without path/mtime/64-bit fallback. MIT OR Apache-2.0, Rust 1.71 minimum inspected 2026-10-28 against pinned Rust 1.99.1. No donor source copied; Codex retains its unsafe-code prohibition. |
| L2 input vs result | [ACPX shared sessions](https://github.com/openclaw/acpx/blob/main/docs/shared-sessions.md), [watch](https://github.com/openclaw/acpx/blob/main/docs/session-watch.md) | Pattern for queued/admitted/started/terminal outcomes. Target: router/Codex result mapping. No queue/flow graph import. |
| L3 live/history recovery | **Pattern:** [Paseo timeline-sync](https://github.com/getpaseo/paseo/blob/abc23d77214f4bd690f67a45fe41a71e42aa97bc/docs/timeline-sync.md), `packages/app/src/timeline/viewed-timeline-sync.ts` at `abc23d77`, inspected 2026-10-27 | Continue bounded pages until the native end, require an advancing cursor and reject stale scope/profile responses. Preserve coverage and explicit gaps independently of visible data. Target: monitoring/plugin recovery. No source copied and no UI/cache/timeline product imported. |
| L3 native store scope and Codex route override | **Pattern:** Codex 0.161.0 [config manager](https://github.com/openai/codex/blob/979011409de0a60b52f179721948e65531d26144/codex-rs/app-server/src/config_manager_service.rs), config loader, `core/src/config/mod.rs` and named `model_providers` override/readback at `97901140`, inspected 2026-10-28 | Native effective config precedes environment selection; config paths use their layer base, while trimmed environment paths use runtime cwd. Use the installed process-scoped override and effective `config/read` to verify the selected provider; do not rewrite owner config. Bind discovery to the verified canonical home/store and reject unproven selection. No upstream source or config/model executor copied. |
| L3 adopted route readiness and URL validation | **Dependencies:** [reqwest 0.13.5](https://crates.io/crates/reqwest/0.13.5), default features disabled with rustls only; [url 2.5.8](https://crates.io/crates/url/2.5.8). Package manifests inspected 2026-10-29: both MIT OR Apache-2.0; Rust MSRVs 1.85.0 and 1.63 respectively, below pinned workspace Rust 1.99.1. | Use reqwest only for bounded readiness reads of the explicit adopted endpoint/path and url for pure binding validation. No model/provider API call and no source copied. |
| L3 worktrees | **Pattern:** [Kandev worktree.go](https://github.com/kdlbs/kandev/blob/04c4d5d022911e4416f0dfa6a6c67d894268ec16/apps/backend/internal/worktree/worktree.go) and `checkout_identity.go` at `04c4d5d0`, inspected 2026-10-27; **dependency:** installed Git 2.55.0.windows.5 | Preserve canonical repository/common-directory/path/branch identity and reciprocal registration; read back before reuse. Use standard Git worktree mutations in scoped custody. No donor code copied, automatic recreation, remote refresh, branch rollback or Task/Environment/cleanup workflow imported. |
| L3/L6 adopted service ownership | **Pattern:** [Waku OpenCode service](https://github.com/egoist/waku/blob/main/crates/waku-core/src/opencode_service.rs), `driver/opencode.rs` | Owner-run adopted service is never started, stopped or repaired by Agent lifecycle. Keep endpoint readiness separate from process custody. Pattern only; no GPL source, service framework or application/UI code copied. |
| L4 restart reconciliation | **Pattern:** [controller-runtime reconcile.go at `1c75cb0185ee6eb2e5571b14fd5724acee5c34d6`](https://github.com/kubernetes-sigs/controller-runtime/blob/1c75cb0185ee6eb2e5571b14fd5724acee5c34d6/pkg/reconcile/reconcile.go), `pkg/internal/controller/controller.go`, inspected 2026-10-29 | Read current state by deduplicated key; implement a small Tokio worker. Pattern only: no source or dependency copied, no Kubernetes dependency and no automatic retry of uncertain writes. |
| L4 rule matching | **Normal dependencies:** [`globset` 0.4.21](https://crates.io/crates/globset/0.4.21), edition 2024, Unlicense OR MIT, Rust 1.88; [`regex` 1.13.1](https://crates.io/crates/regex/1.13.1), edition 2021, MIT OR Apache-2.0, Rust 1.65. Registry manifests inspected 2026-10-29; both compiled with pinned Rust 1.99.1. | Use globset for `glob:` and regex for `re:` in `eliot-daemon/src/automation/definition.rs`. No dependency source copied; exact, `one_of` and `exists` checks stay local. |
| L4 timer schedules | **Dependencies:** [Croner 4.0.2](https://github.com/Hexagon/croner-rust/tree/24c9bbb02dbcb498a8bbf31a33f1e6416e75ac83), MIT (no `rust-version` declared); [`chrono-tz` 0.10.4](https://crates.io/crates/chrono-tz/0.10.4), MIT OR Apache-2.0, Rust 1.65; [`humantime` 2.4.0](https://crates.io/crates/humantime/2.4.0), MIT OR Apache-2.0, Rust 1.60. Package metadata inspected 2026-10-29. Existing workspace `chrono` supplies epoch/RFC3339 conversion. | Use Croner for five-field parsing and naive civil occurrence iteration, resolve the specified DST behavior with `chrono-tz`, and parse interval durations with `humantime` in `eliot-daemon/src/automation/schedule.rs`. No donor source copied; do not adopt Croner's own DST or catch-up policy. |
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

v1 is preserved on branch `archive/v1-final` at
`dc6f70aa08ea5fad3da04624ce50c46d45edd57c`. Its original notices remain in that tree.
Never import its Task/Attempt/review/acceptance schema, compatibility executors,
per-adapter journals, policy hashes, executable provenance or audit tooling.
