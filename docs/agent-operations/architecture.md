# Eliot Swarm Controller — Architecture

This manifesto defines the product and constrains its code. Keep it in the lead
developer's context. [Implementation](implementation.md) owns source owners,
algorithms and donor choices; [status](../implementation-status.md) records what
exists. This is the target, not a claim that the rewrite is complete.

## Purpose

Eliot carries work from assignment to a reviewed, accepted result using native
coding agents. It makes execution, output, ownership and remaining work visible,
reducing repeated effort and manager attention. Activity, code volume and elaborate
evidence machinery are not measures of completion.

## Product boundary

The core covers assignments, agent execution, attributable results, independent
review, correction, acceptance and observation. Addressed messages support this
work. A Goal continues an existing assignment toward an explicit completion
criterion; it does not introduce another task system.

Management is manual by default. A manager can enable selected helpers within
their existing authority. Helpers use the same actions as direct commands.
GitHub publication, hooks, scripts and remote access are optional integrations.
Council rounds, generic agreement/ACK engines and automatic Issue-label
publication are outside the compact core.

## One command language, one owner

The manager gives every harness the same kinds of tasks and commands. The adapter
translates their meaning into native calls and translates observations back.
Vendor method names, payload quirks and protocol versions stay at that boundary.

One durable controller owns work state and decisions. Each adapter owns its native
connection; each process has one lifecycle owner. Frontends expose shared actions.
These owners do not duplicate execution, scheduling, authorization or recovery.
A Goal has one continuation owner: the native harness where suitable, otherwise
Eliot's common continuation mechanism. Native progress never accepts a Task.

Rust owns the control plane. Use maintained native APIs and SDKs for capabilities
that already exist. Keep a thin SDK driver when it reduces total maintained code.
Adapters can change without rebuilding the controller. Small internal helpers
remain ordinary code rather than separate services.

## Work must progress

An authorized request can start assigned work directly; preview is optional.
A ready workspace and session permit the first input. Optional telemetry or tool
discovery cannot hold it up. Return complete output associated with that input,
including protocols that omit repeated parent IDs. Unsupported actions and missing
output have explicit reasons.

Activity, results and usage are readable without starting a model. Execution state,
connection health, task completion and account limits have different meanings.
Missing quota data is unknown, never zero. Observation and manual control remain
available when automation is disabled.

Stopping future continuation preserves running work and its results. Interrupting
a turn, removing queued input and closing a process each have an explicit subject
and observable outcome. A failed connection, optional worker or slow viewer affects
its own scope. Bound queues, buffers and concurrency.

## State and trust

Keep one authoritative record of an admitted action, its inputs and its outcome.
Commit related state atomically. Reconnect and retry cannot duplicate an uncertain
external effect; use native readback where available. Late results belong to their
original assignment and candidate.

Authenticate participants and protect credentials at access boundaries. Use stable
IDs and revisions for local state. Hashes serve necessary byte verification,
not another permission, approval or compatibility system. Documents, compiler
versions and checkout paths do not authorize execution.

## Simplicity is a design constraint

Support one current implementation and format per retained runtime. Preserve
concrete required work through a finite transition, then delete the obsolete path.
Old experiments create no permanent compatibility obligation.

Delete unnecessary responsibilities before extracting packages. Prefer an existing
handler, table or library to another framework, workflow language or registry.
Reuse a donor's needed mechanism without importing its whole product. Complexity
must serve current work and replace or simplify a named mechanism.

## What completion means

A manager can assign work, obtain output, request independent review, return
corrections and accept the resulting candidate. Selected continuation works.
Restart preserves known outcomes and exposes unresolved work. Replaced paths are
removed, and verification demonstrates behavior at the changed boundary.
