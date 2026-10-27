# Eliot

A local Rust controller for native coding-agent harnesses. One CLI/API launches,
observes and controls Agents; later layers add automation and direct coordination.
The vendor harness keeps its own model loop, tools and native history.

## Status and build order

**v2 is a specification and implementation roadmap, not a working release yet.**
The current implementation is v1 until reset task L0.1 lands. Its frozen reference is
`dc6f70aa08ea5fad3da04624ce50c46d45edd57c`; verify `v1-final` there before deleting v1.
Do not extend v1 while implementing the new architecture.

**Core/Store/bus → direct Codex/Luna → monitoring/workspaces → automation/scripts →
agent communication → other selected harnesses → final qualification.**

The first useful path is deliberately small: one installed Codex, an existing login
and workspace, one native model, one real task and its result. It does not require a
proxy, workflow engine, additional adapter catalogue or testing platform.

## Start here

Read [vision](docs/vision.md) and [architecture](docs/architecture.md), then the next
[roadmap](docs/roadmap.md) task and only its owning spec. [AGENTS.md](AGENTS.md) is the
short implementation instruction. [Donors](docs/reference/donors.md) names exact sources,
what to reuse, where to put it and what not to import.

During implementation use fmt and changed-package Clippy; briefly exercise the new path
when available. Full crash/restart/conformance/live/platform/load qualification is L7.
No runtime guarantee follows from documentation review or compilation alone.

[Third-party notices](THIRD_PARTY_NOTICES.md) describe material in the current tree.
When removing v1, preserve attribution for any code actually retained or extracted.
