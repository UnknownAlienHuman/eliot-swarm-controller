# Eliot

A small local controller for vendor coding-agent harnesses—Codex, Claude Code, Muse,
OpenCode, Antigravity and ACP agents. Eliot launches them through explicit provider
routes, watches native/process/Git facts, controls them with one harness-neutral command
language, automates bounded reactions and lets Agents coordinate directly.

```text
eliot launch --profile codex-opencodex --worktree feat/routes "Add the /v2 routes"
eliot watch a12
eliot steer a12 "keep the v1 handlers, only add new ones"
eliot who src/api/
eliot goal set a12 "all tests pass" --check "cargo test" --max-turns 15
eliot send a13 "review the diff of a12" --wait
```

## Status

**v2 is being rebuilt from scratch in connected layers.** v1 grew to roughly 533k Rust
lines and 61k lines of process/audit documents without qualifying its core live paths.
Its frozen pre-reset commit is `dc6f70aa`; roadmap L0.1 creates `v1-final` there before
removing v1 from `main`. Until then, current implementation code is v1 legacy and must
not be extended.

v2 copies small proven mechanisms from active donor projects—stable session + one
incarnation fence, single authoritative event history, opaque cursors, level-based
reconciliation and explicit delivery identity—rather than inventing another framework.
See [donors](docs/reference/donors.md).

## Start here

1. [Vision](docs/vision.md)
2. [Architecture](docs/architecture.md)
3. [Roadmap](docs/roadmap.md)
4. Only the linked specification/harness page and exact donor source
5. [AGENTS.md](AGENTS.md)

During L0–L6: write the connected code first, run formatting plus smallest changed-package
Clippy and avoid broad test bureaucracy. After the connected feature set exists, L7
builds/runs the complete crash/restart, conformance, live, platform and load campaign.

Third-party material still present in the v1 tree is listed in
[THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md); L0.1 removes it with v1 code after the
archive tag exists.
