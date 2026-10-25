# Donors

What to borrow, from where, for which layer. A donor is a source of mechanisms or
code; it is never a reason to import an application. Check the license before copying
code. Vendor harness documentation is on the [harness pages](../harnesses/README.md).

## Code from v1 (`git show v1-final:<path>`)

Copy and cut down; do not depend on v1 crates. Paths are as of `dc6f70aa`.

| Mechanism | v1 path | Use in | Note |
| --- | --- | --- | --- |
| Windows Job objects, process birth/image identity, Unix groups | `crates/swarm-process/src/process_group.rs` | L3.1 | 1,555 lines; take identity + job + kill-tree only. |
| Bounded child pipe capture | `crates/swarm-process/src/capture.rs` | L1.5, L5.4 | 64 MiB cap per stream. |
| Private files (0600 / Windows ACL), durable replace | `crates/swarm-process/src/permissions.rs` | L1.4 (`daemon.json`, lock) | Keep only what L1.4 needs. |
| Named pipe with user-only ACL | `crates/swarm-kernel-host/src/platform/windows.rs` | L1.4 | 145 lines, small and direct. |
| Line-framed multiplexed IPC with frame limits | `crates/swarm-client/src/multiplex.rs`, `client.rs` | L1.4 | Drop the authentication layer. |
| Process CPU/RSS sampling on Windows | `crates/swarm-observer/src/process_metrics.rs` | L3.1 | Or replace with `sysinfo`. |
| Codex app-server protocol knowledge | `crates/swarm-adapter-codex/src/lib.rs` | L2 | Read for method names and edge cases; do not port its journals/checkpoints. |
| Antigravity stream examples (from official docs) | `modules/antigravity/fixtures/*.stream.json` | L4.5 | Doc-derived, not live captures. |
| Secret redaction (Apache-2.0, vendored Atlas) | `vendor/atlas/crates/atlas-redact` | L7.9 | Pattern-based redaction of persisted event text. |

Anti-donors in v1: result paging/assembly, per-adapter journals and tombstones,
receipt/proof types, the Task/Attempt/review/acceptance Store, policy editions,
module descriptors and provenance packaging.

## Rust crates (check current version, license and MSRV when adding)

| Crate | For |
| --- | --- |
| `tokio`, `serde`, `serde_json`, `thiserror`, `anyhow`, `tracing` | everywhere |
| `rusqlite` (`bundled`, no bindgen features) | L1.2 |
| `clap` (derive) | L1.8 |
| `interprocess` or `tokio::net` named pipes + `UnixListener` | L1.4 |
| `ulid` | command ids |
| `sysinfo` | L3.1 |
| `notify` (+ debouncer) | L3.3 |
| `globset` | matchers, claims |
| `croner` | L5.2 cron |
| `windows-sys` | Job objects, pipe ACLs |
| `agent-client-protocol` (Apache-2.0) | L4.6 ACP plugin |
| `rmcp` (Apache-2.0) | L6.5 MCP server |

## External design patterns

| Project | License | Borrow | Do not borrow |
| --- | --- | --- | --- |
| [sandbox-driver](https://github.com/lithoscomputer/sandbox-driver) | MIT | One provider trait usable in-process and over JSON-RPC; a conformance suite every provider must pass. | Its isolation claims. |
| [agentsessions](https://github.com/aramase/agentsessions) | Apache-2.0 | Session/Harness/Runtime split; zero-model replay conformance tests. | Pre-1.0 wire as a stable API. |
| [Paseo](https://github.com/getpaseo/paseo) | Apache-2.0 | Live vs canonical timeline, ranges/gaps, declaration-first child discovery, profiles with "when to use". | App, UI, JSON stores. |
| [ACPX](https://github.com/openclaw/acpx) | MIT | One session owner, opaque cursors, replay/live boundary for ACP. | Its flows, queue as replay key, storage. |
| [Waku](https://github.com/egoist/waku) | GPL-3.0 (pattern only, no code) | One ordered state owner; stream break ≠ session death; reconnect ≠ exit. | Any source. |
| [Overstory](https://github.com/Nokodoko/overstory) | — | Typed SQLite mail, isolated worktrees per agent, agent discovery. | — |
| [MCP Agent Mail](https://github.com/Dicklesworthstone/mcp_agent_mail_rust) | restrictive rider | Mail not chat; exact recipients; advisory path reservations. | Liveness chatter (92% of its mail), implicit identities. |
| [Agency Swarm](https://github.com/VRSEN/agency-swarm) | — | Directional communication flows; handoff separate from send. | — |
| [Gas Town](https://github.com/gastownhall/gastown) | — | Durable work state outside the model context; tiered escalation. | Separate work graph. |
| [CCCC](https://github.com/ChesterRa/cccc) | Apache-2.0 | Delivery id, recipient scope, reply/cancel binding, size limits. | Scheduler, JSONL ledger, PTY heuristics. |
| [Petri](https://github.com/lithoscomputer/petri) | MIT | Pure `apply(state, event)` reducer; replay tests. | Workflow DSL. |
| [controller-runtime](https://github.com/kubernetes-sigs/controller-runtime) | Apache-2.0 | Level-based reconcile, dedup keys, error backoff. | Kubernetes stack. |
| [OpenCodex](https://github.com/lidge-jun/opencodex) | MIT | Documented Management API (health, usage, settings). | Provider translation inside Eliot. |
| Claude Swarm scripts (v1 predecessor) | — | Corrections before fresh work; explicit model/effort per agent; compact task cards; all review findings at once. | Marker files, lockdirs, newest-log ownership, hidden fallbacks. |

Reviewed in v1 and not needed for v2 (details at `v1-final:docs/remediation/2026-10-08/36-donor-registry.md`):
Restate, DBOS, ractor, Kingfisher, AgentGateway, OpenTelemetry, Langfuse, MCPProxy,
portable-pty, alacritty_terminal, landlock, seccompiler, Snyk Agent Scan, Kandev
(AGPL), OpenHands, Fabro, Cayu, systemd, REAPI, Pebble, Emdash, Goose, Helicon,
Claw, Agent of Empires, Poracode, Sinew, CodeGraff, Pi-Go, OpenChamber, Delta,
Agent Teams AI, Vicoa, JetBrains Air.
