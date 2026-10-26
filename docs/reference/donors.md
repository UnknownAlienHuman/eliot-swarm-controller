# Donors

A donor supplies one mechanism or protocol fact; it is never a reason to import an
application or architecture. Recheck source, version, license and MSRV before copying.
Vendor facts live on the [harness pages](../harnesses/README.md).

## v1 code

Frozen source is commit `dc6f70aa`; after roadmap L0.1 creates the archive tag, the
same tree is `v1-final`.

```text
git show dc6f70aa:<path>      # before L0.1
git show v1-final:<path>      # after L0.1
```

Copy the narrow part into v2 and cut every old dependency.

| Mechanism | v1 path | Use in | Keep / reject |
| --- | --- | --- | --- |
| Windows Job objects, process birth/image identity, Unix groups | `crates/swarm-process/src/process_group.rs` | L1.6 custody, L3.1 monitor | Keep exact identity, assign-at-create and kill/verify group. Reject scheduler/operation coupling. |
| Bounded child pipe capture | `crates/swarm-process/src/capture.rs` | L1.6, L6.5 | Keep nonblocking bounded drain. v2 script output cap is 1 MiB, not v1's large generic archive. |
| Private file permissions and durable replace | `crates/swarm-process/src/permissions.rs` | L1.4 | Keep only daemon/config/token file helpers actually used. |
| Current-user Windows named pipe ACL | `crates/swarm-kernel-host/src/platform/windows.rs` | L1.4 | Small direct helper; no internal authentication platform. |
| Line-framed multiplexed IPC with frame limits | `crates/swarm-client/src/multiplex.rs`, `client.rs` | L1.4 | Keep framing/multiplexing; drop v1 auth/grant/provenance layers. |
| Windows process CPU/RSS sampling | `crates/swarm-observer/src/process_metrics.rs` | L3.1 | Use only if smaller/more exact than current `sysinfo`; identity remains `(pid,start_time)`. |
| Codex app-server protocol edge cases | `crates/swarm-adapter-codex/src/lib.rs` | L2 | Read method names/correlation failures; do not port journals, checkpoints, receipts or broad adapter state. |
| Antigravity stream examples derived from vendor docs | `modules/antigravity/fixtures/*.stream.json` | L4.5 | Syntax hints only; not live evidence or qualification. |
| Pattern secret redaction (vendored Atlas, Apache-2.0) | `vendor/atlas/crates/atlas-redact` | L7.6 | Copy the narrow matcher only after known-value masking exists and qualification proves need. |

**Never donate from v1:** Task/Attempt/review/acceptance/publication Store, proof/
receipt types, policy editions/hashes, per-adapter journals/tombstones, result paging
and assembly, module descriptors/provenance packaging, compatibility executors,
one-shot CI patchers, audit/remediation machinery.

## Rust dependencies

Check current release, license, maintenance and MSRV at PR date. Add only when the
linked task has a caller.

| Crate | Concrete use |
| --- | --- |
| `tokio`, `serde`, `serde_json`, `thiserror`, `anyhow`, `tracing` | runtime/data/error/logging boundaries |
| `rusqlite` with bundled SQLite and no bindgen hooks | L1.2 Store |
| `clap` derive | L1.9 CLI |
| `interprocess` or Tokio platform sockets | L1.4 local IPC; choose one after a Windows spike, not both |
| `ulid` | generated command/write ids |
| `sysinfo` | L3.1 bounded metrics if platform helpers are insufficient |
| `notify` + one debouncer | L3.3 filesystem invalidation |
| `globset` | rule/path matching and claims |
| one maintained cron parser | L6.3; verify timezone/DST semantics before selecting |
| `windows-sys` | exact Job, process, handle and ACL calls in the platform module |
| `agent-client-protocol` | L4.6 ACP native schema/client |
| `rmcp` | L5.5 only if a required harness cannot use CLI |

Do not add a dependency merely because this table lists it. Prefer standard library
or already-selected crates when the path stays direct.

## External patterns

| Project | License at review | Borrow | Explicitly reject |
| --- | --- | --- | --- |
| `sandbox-driver` | MIT | one provider contract usable in-process/JSON-RPC; provider conformance shape | its isolation claims or entire runtime |
| `agentsessions` | Apache-2.0 | Session/Harness/Runtime separation; model-free replay concept | pre-1.0 wire as our stable protocol |
| `Paseo` | Apache-2.0 | live vs canonical timeline, explicit ranges/gaps, declaration-first children | app/UI/JSON stores |
| `ACPX` | MIT | one native session owner, opaque cursor, replay/live boundary | its flow/work graph and queue identity |
| `Waku` | GPL-3.0 (pattern only) | ordered owner; stream break is not session death | code copying or its app architecture |
| `Overstory` | recheck | typed SQLite mail, isolated worktrees, discovery | scheduler/work graph |
| `MCP Agent Mail` | restrictive rider | exact recipients, mail not chat, advisory path announcements | code copying, liveness chatter, implicit identities |
| `CCCC` | Apache-2.0 | delivery id, recipient scope, reply/cancel binding, size limits | scheduler, JSONL ledger, PTY heuristics |
| `Petri` | MIT | pure `apply(state,event)` reducer and table/replay cases | workflow DSL |
| `controller-runtime` | Apache-2.0 | level-based reconcile and stable dedupe keys for current-state reconciliation | Kubernetes stack and retrying non-idempotent effects |
| `OpenCodex` | MIT | read-only management health/usage facts | provider translation/config authority inside Eliot |
| old Claude swarm scripts | recheck | corrections before fresh work, explicit route, compact assignments, batch review findings | marker files, lockdirs, newest-log ownership, hidden fallback |

Other systems reviewed in v1 are not current implementation inputs. Their notes remain
at the frozen commit/tag; do not re-import them without a concrete missing mechanism
and owner of the resulting code.
