# Donors

A donor supplies one mechanism or protocol fact; it is never a reason to import an
application or architecture. Recheck source, version, license and MSRV before copying.
Vendor facts live on the [harness pages](../harnesses/README.md).

## v1 code

Frozen source is commit `dc6f70aa`; after L0.1 the same tree is tag `v1-final`.

```text
git show dc6f70aa:<path>      # before L0.1
git show v1-final:<path>      # after L0.1
```

Copy one narrow mechanism into v2 and cut every v1 dependency.

| Mechanism | v1 path | Use in | Keep / reject |
| --- | --- | --- | --- |
| Windows Job objects and process birth/image identity | `crates/swarm-process/src/process_group.rs` | L1.6 custody, L3.1 monitor | Keep assign-before-resume, no-breakaway, pinned handles, exact Job membership/empty verification and birth/image helpers. Add v2 `host_boot_id` and one custody owner. Reject scheduler/operation coupling. |
| Unix proc/process-group observation | same path | L3.1 degraded fallback only | Keep `/proc` start ticks/boot-id parsing and bounded PGID observation. **Do not** copy the claim that process groups provide exact containment or daemon-death cleanup; v2 exact Linux needs cgroup/equivalent + guardian. |
| Bounded child pipe capture | `crates/swarm-process/src/capture.rs` | L1.6, L6 scripts/checks/setup | Keep nonblocking bounded drain. Apply v2 per-stream/frame limits and secret masking; reject generic large archives. |
| Private file permissions and durable replace | `crates/swarm-process/src/permissions.rs` | L1.4/configuration | Keep current-user files and atomic replacement for owner token, routing key, client ULID state and artifacts. Add symlink/reparse/ownership checks. |
| Current-user Windows named pipe ACL | `crates/swarm-kernel-host/src/platform/windows.rs` | L1.4 | Small transport helper only. API authority still requires explicit owner/Agent/automation/plugin credential; pipe ACL is not principal selection. |
| Line-framed multiplexed IPC with frame limits | `crates/swarm-client/src/multiplex.rs`, `client.rs` | L1.4 | Keep bounded framing/multiplexing. Drop v1 grants/provenance/proof layers; add exact principal handshake and cancellation-safe readers. |
| Windows process CPU/RSS sampling | `crates/swarm-observer/src/process_metrics.rs` | L3.1 | Use only if smaller/more exact than current maintained crate/platform APIs. Process identity is `(host_boot_id,pid,start_time)`. |
| Codex app-server protocol edge cases | `crates/swarm-adapter-codex/src/lib.rs` | L2 | Read method names, paging and correlation failures. Do not port adapter journals/checkpoints/tombstones/receipts or assume current schema. |
| Antigravity stream examples from vendor docs | `modules/antigravity/fixtures/*.stream.json` | L4.5 | Syntax hints only; not live evidence, replay identity or qualification. |
| Pattern secret redaction (vendored Atlas, Apache-2.0) | `vendor/atlas/crates/atlas-redact` | L1.10/L7 | Copy only the narrow matcher after known-value/prefixed-token masking exists and current license/code is rechecked. Never copy the whole Atlas stack. |

**Never donate from v1:** Task/Attempt/review/acceptance/publication Store, proof-receipt
or policy-edition types, per-adapter universal journals/tombstones, result assembly,
module descriptors/provenance packaging, compatibility executors, one-shot CI patchers,
audit/remediation machinery, process-based API authority or claims that Unix PGID is
exact custody.

## Rust dependencies

Check the current release, license, maintenance and MSRV in the implementing PR. Add only
when a linked task has a real caller.

| Crate/category | Concrete use |
| --- | --- |
| `tokio`, `serde`, `serde_json`, `thiserror`, `anyhow`, `tracing` | runtime/data/error/logging boundaries |
| `rusqlite` with bundled SQLite/no bindgen hooks | L1.2 Store |
| `clap` derive | L1.9 CLI |
| one local IPC option (`interprocess` or direct Tokio/platform sockets) | L1.4; choose one after a Windows spike |
| `ulid` | canonical external WriteId parsing/generation |
| maintained BLAKE3 implementation + one fixed base32 encoder | derived ids, payload/definition/artifact digests and keyed routing-token derivation; do not invent crypto |
| `sysinfo` or narrow platform APIs | bounded CPU/RSS only; not custody/identity authority |
| `notify` + one debouncer | L3.3 filesystem invalidation |
| `globset` | rule/path matching and claims |
| one maintained cron/timezone parser | L6 timers after DST qualification |
| `windows-sys` | Job/process/handle/ACL calls in the platform module |
| Linux cgroup v2 via narrow direct filesystem/platform code or one maintained crate | exact Linux custody only after delegation/guardian spike; no Kubernetes/container stack |
| `agent-client-protocol` | L4.6 ACP selected schema/client |
| `rmcp` | L5 only if a required harness cannot use CLI |

Do not add a dependency merely because it appears here. Prefer standard library or an
already-selected crate when the implementation remains direct.

## External patterns

| Project | License at review | Borrow | Explicitly reject |
| --- | --- | --- | --- |
| `sandbox-driver` | MIT | one provider boundary and conformance shape | its isolation claims or full runtime |
| `agentsessions` | Apache-2.0 | Session/Harness/Runtime separation; model-free replay | pre-1.0 wire as Eliot protocol |
| `Paseo` | Apache-2.0 | live vs canonical timeline, explicit gaps, declaration-first children | app/UI/JSON stores |
| `ACPX` | MIT | one native session owner, opaque cursor, replay/live boundary | flow/work graph and queue identity |
| `Waku` | GPL-3.0, pattern only | stream break is not session death | code copying/app architecture |
| `Overstory` | recheck | typed SQLite mail, worktrees, discovery | scheduler/work graph |
| `MCP Agent Mail` | restrictive rider | exact recipients, mail not chat, advisory paths | code copying, liveness chatter, implicit identities |
| `CCCC` | Apache-2.0 | delivery id, recipient scope, reply/cancel binding, limits | scheduler, JSONL ledger, PTY heuristics |
| `Petri` | MIT | pure reducer/table/replay test style | workflow DSL |
| `controller-runtime` | Apache-2.0 | level reconcile and stable dedupe keys for current-state reconciliation | Kubernetes stack and retries of uncertain effects |
| `OpenCodex` | MIT | read-only management health/usage facts | provider translation/config authority inside Eliot |
| old Claude swarm scripts | recheck | compact assignments, explicit route, batch findings | marker files, lockdirs, newest-log ownership, hidden fallback |

Other v1 research is not a current implementation input. Re-import only for one concrete
missing mechanism with an owner, license check and deletion of incompatible architecture.
