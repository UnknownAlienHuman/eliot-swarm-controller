# Eliot

A small local controller for vendor coding-agent harnesses — Codex, Claude Code, Muse,
OpenCode, Antigravity and ACP agents. Eliot launches them through explicit profiles and
provider routes, observes native/process/Git facts, controls them through one semantic
language, automates bounded reactions and lets agents coordinate directly.

Owner CLI calls use explicit owner mode; agents receive their scoped identity:

```text
eliot --owner launch --profile codex-opencodex --worktree feat/routes "Add the /v2 routes"
eliot --owner watch a12
eliot --owner steer a12 "keep the v1 handlers, only add new ones"
eliot --owner who src/api/
eliot --owner goal set a12 "all checks pass" --check "cargo clippy" --max-turns 15
```

## Status

**v2 is being rebuilt from scratch, layer by layer**
([roadmap](docs/roadmap.md)). v1 grew to about 533k Rust lines and 61k lines of
process/audit documentation without qualifying its core live paths.

v1's frozen pre-reset commit is
`dc6f70aa08ea5fad3da04624ce50c46d45edd57c`. L0.1 creates tag `v1-final` at that
commit before deleting v1 implementation code from `main`:

```text
git show dc6f70aa:<path>
```

The current Rust/Node/Python implementation, migrations, vendor tree and old workflows
are v1 legacy. Do not extend or repair them. **The v2 documentation validators are not
legacy and must survive L0.1:**

- `.github/workflows/v2-docs.yml`;
- `tools/ci/validate-v2-docs.py`;
- `tools/ci/validate-v2-schema.py`.

Read only the narrow v1 donor mechanisms named by v2 documentation.

## Documentation

- [Vision](docs/vision.md) — product intent and owner's original statement.
- [Architecture](docs/architecture.md) — components, authority and runtime boundaries.
- [Roadmap](docs/roadmap.md) — implementation order and qualification boundary.
- [All docs](docs/README.md).
- [AGENTS.md](AGENTS.md) — worker instructions.

During L0–L6, write the connected implementation first and run formatting plus the
smallest changed-package Clippy gate. Full deterministic, crash/restart, plugin,
live-harness, platform and load qualification starts after feature completion in L7.

Third-party material still present in the v1 tree is listed in
[THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md); L0.1 removes it with v1 code after
the archive tag exists.
