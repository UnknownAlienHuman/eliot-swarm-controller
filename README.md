# Eliot Swarm Controller

Headless, modular Rust controller for native coding-agent harnesses, developed as a prototype for the Agent Execution Fabric in Eliot Memory OS.

**Status:** design and reference specifications only. The Rust service, SDK bridges and Windows runtime have not been implemented or qualified. The reference SQL is not a completed Store implementation.

## Start here

| Document | Purpose |
| --- | --- |
| [Architecture v18](docs/agent_swarm.md) | Responsibilities, execution model and current design decisions |
| [Implementation plan v6](docs/agent_swarm.implementation-v6.md) | C01–C11 implementation sequence and transactional boundaries |
| [Module contract v2](docs/agent_swarm.module-contract-v2.md) | Native adapters, capabilities, delivery and lifecycle semantics |
| [Reference specification](docs/agent_swarm.spec-v18/README.md) | Initial SQL, configuration examples and protocol examples |
| [Donor inventory](docs/agent_swarm.donors-20260929.toml) | Pinned candidate SDKs and reusable components; not an installation lockfile |
| [Design review v18](docs/agent_swarm.design-review-v18-20260929.md) | Recorded counterexamples and design corrections |

The documents above are the current implementation entry points. Other files under `docs/` preserve research and comparison evidence; historical briefs and older snapshots are not additional instructions for workers.

## Scope

One Rust host, one SQLite database, local IPC and replaceable native runtime modules. General Manager controls work through MCP; managers use the CLI and their harness's native subagents. No user interface, external message broker or replacement model loop.

Initial runtime targets are native Muse Code Max and direct OpenCode V2 HTTP. Codex, Claude Code, Command Code, Antigravity and Zed remain separate integrations with explicitly qualified capabilities.

## Development

Work on `main`, without worktrees. Implement complete paths first and run focused formatting/Clippy checks once Rust code exists. Broad runtime and load qualification follows the working slice.

**Next implementation package:** C01 — `model/config/store`, followed by C02 — `host/CLI/IPC`. Do not mark these complete based on the presence of SQL or example payloads.

No donor code has been vendored, no CLI credentials or local runtime configuration are installed, and no agents are launched by this repository bootstrap.

## Provenance

Documentation imported from the supplied `agent_swarm.docs-v18-20260929.zip` on 2026-09-29. The supplied documentation and reference files are retained under `docs/`; Git normalizes text line endings according to `.gitattributes`. Local Windows paths appearing in historical briefs are source observations, not install defaults.
