# Eliot

A small local controller for vendor coding-agent harnesses — Codex, Claude Code,
Muse, OpenCode, Antigravity, ACP agents. Eliot launches them (optionally through a provider
proxy such as OpenCodex), watches them in real time (events, processes, Git), controls
them with one harness-neutral command language (`send`, `steer`, `inject`,
`interrupt`, `approve`, `goal`, ...), automates reactions with rules and cron timers,
and lets agents coordinate directly.

```text
eliot launch --profile codex-opencodex --worktree feat/routes "Add the /v2 routes"
eliot watch a12
eliot steer a12 "keep the v1 handlers, only add new ones"
eliot who src/api/
eliot goal set a12 "all tests pass" --check "cargo test" --max-turns 15
eliot send a13 "review the diff of a12" --wait
```

## Status

**v2 is being rebuilt from scratch, layer by layer** ([roadmap](docs/roadmap.md)).
The v1 controller grew to 533k lines of Rust and 61k lines of process documents
without qualifying its core paths; it is archived at tag `v1-final` and removed from
`main` by roadmap task L0.1. Until then the code in this tree is v1 and is not
maintained.

## Documentation

- [Vision](docs/vision.md) — what and why, with the owner's original statement.
- [Architecture](docs/architecture.md) — how it fits together.
- [Roadmap](docs/roadmap.md) — what to build next and how it is tested.
- [All docs](docs/README.md).

Contributors and agents: read [AGENTS.md](AGENTS.md).

Third-party material in the v1 tree: [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).
