# Eliot

A small local controller for vendor coding-agent harnesses — Codex, Claude Code, Muse,
OpenCode, Antigravity and ACP agents. Eliot launches them (optionally through an
explicit provider proxy), observes native/process/Git facts, controls them through one
semantic command language, automates bounded reactions and lets agents coordinate
directly.

```text
eliot launch --profile codex-opencodex --worktree feat/routes "Add the /v2 routes"
eliot watch a12
eliot steer a12 "keep the v1 handlers, only add new ones"
eliot who src/api/
eliot goal set a12 "all tests pass" --check "cargo test" --max-turns 15
eliot send a13 "review the diff of a12" --wait
```

## Status

**v2 is being rebuilt from scratch, layer by layer**
([roadmap](docs/roadmap.md)). The v1 controller grew to about 533k lines of Rust and
61k lines of process/audit documentation without qualifying its core live paths.

v1's exact frozen pre-reset commit is
`dc6f70aa08ea5fad3da04624ce50c46d45edd57c`. Roadmap task L0.1 creates tag
`v1-final` at that commit **before** deleting v1 code from `main`. Until L0.1 lands,
use the commit directly:

```text
git show dc6f70aa:<path>
```

The code currently in the tree is v1 legacy. Do not extend or repair it; read only the
narrow donor mechanisms named by v2 documentation.

## Documentation

- [Vision](docs/vision.md) — product intent and owner's original statement.
- [Architecture](docs/architecture.md) — components, authority and exact runtime boundaries.
- [Roadmap](docs/roadmap.md) — implementation order, scoped Clippy gate and deferred L7 qualification.
- [All docs](docs/README.md).
- [AGENTS.md](AGENTS.md) — worker instructions.

During L0–L6, write the connected implementation first and run formatting plus the
smallest changed-package Clippy gate. Full deterministic, crash/restart, plugin,
live-harness, platform and load qualification starts after feature completion in L7.

Third-party material still present in the v1 tree is listed in
[THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md); L0.1 removes it with v1 code after
the archive tag exists.
