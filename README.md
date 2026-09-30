# Eliot Swarm Controller

Headless modular Rust controller for native coding-agent harnesses; a prototype for the Agent Execution Fabric in Eliot Memory OS.

**Status:** documentation and reference specifications only. Rust service, SDK bridges and Windows runtime are not implemented or qualified. Reference SQL is not a completed Store.

## Implementation entry points

| Document | Read for |
| --- | --- |
| [Architecture v18](docs/agent_swarm.md) | Responsibilities, execution and current design |
| [Implementation plan v6](docs/agent_swarm.implementation-v6.md) | C01–C11, files and transactional boundaries |
| [Module contract v2](docs/agent_swarm.module-contract-v2.md) | Adapter capabilities, delivery and lifecycle |
| [Reference specification](docs/agent_swarm.spec-v18/README.md) | Current SQL, configuration and protocol examples |
| [Donor inventory](docs/agent_swarm.donors-20260929.toml) | Source candidates and pins; not an installation lockfile |

Read the relevant section, not every document on each task. Work on `main`, without worktrees. Implement complete paths first, then focused formatting/Clippy. Broad runtime and load qualification follows a working slice.

**Next:** C01 — `model/config/store`; C02 — `host/CLI/IPC`; then Muse Code Max and direct OpenCode V2. C01 proves local reservation and durable queued operations, not native execution before an adapter exists. One Rust host, one SQLite, local IPC; native harnesses retain their model loops and subscription routes. General Manager uses MCP; managers use CLI and native subagents. No UI or external broker.

## Reference, not additional worker instructions

| Document | Content |
| --- | --- |
| [Lessons learned](docs/lessons-learned.md) | Operational failures, corrected design mistakes and retained evidence |
| [Runtime notes](docs/runtime-notes.md) | Essential differences between the seven selected harnesses |
| [Candidate notes](docs/candidate-notes.md) | Reusable ideas and limitations of optional alternatives |
| [Runtime matrix](docs/agent_swarm.runtime-matrix-v16.json) / [sources](docs/agent_swarm.runtime-sources-v16.json) | Dated, machine-readable research; not live qualification |

## Documentation history

On 2026-09-30, useful findings were distilled from old briefs, research and reviews. Superseded snapshots, version patches, package metadata and duplicate reports were removed from the current tree. The subsequent contract review amended the existing files: explicit initial-start ownership, run-scoped producers, cache provenance without a fictitious process, and exact negative-review/acceptance decisions. Filenames stay stable; no archive copies were added. Nine reference tables remain.

Originals remain at commit `b5a437f57488f8ddcdcc3f4aaea24746a3ea1f62` and can be read with `git show <commit>:docs/<old-path>`. [The extraction map](docs/lessons-learned.md#4-что-удалено-и-где-осталось-существенное) points to retained conclusions. Git history was not rewritten; deleting current files does not erase previously published content from history.

No donor code has been vendored, credentials installed, native agents launched or runtime tests completed by these documentation reviews. Local paths and permissions from historical briefs are not install defaults.
