# Eliot Swarm Controller — Architecture

This manifesto defines the product and the principles that constrain its code.
Keep it in the lead developer's context. [Implementation](implementation.md) owns
files, algorithms, donor choices and work order; [status](../implementation-status.md)
records what exists. This is the target, not a claim that the rewrite is complete.

## Purpose

Eliot helps a manager carry work from assignment to a reviewed, accepted result
using native coding agents. It makes execution, output, ownership and remaining
work visible. Its value is completed work with less repeated effort and manager
attention. Source volume, agent activity and elaborate evidence machinery are
not measures of completion.

## Product boundary

The core covers assignments, agent execution, attributable results, independent
review, correction, acceptance and observation. Addressed messages support this
work. A Goal continues existing assignments against an explicit completion
criterion; it does not introduce another task system.

Management is manual by default. A manager can enable selected helpers within
their existing authority. Each helper uses the same actions as a direct command.
GitHub publication, hooks, user scripts and remote access are optional integrations.
Council rounds, generic agreement/ACK engines and automatic Issue-label publication
are outside the compact core. Their old specifications do not make them prerequisites.

## One owner for each responsibility

One durable controller owns work state and decisions. An adapter translates a
native protocol and owns its connection. A process has one lifecycle owner.
Frontends expose the controller's actions. These responsibilities share contracts
without duplicating execution, scheduling, authorization or recovery.

Rust owns the control plane. Native APIs and maintained SDKs supply capabilities
that already exist. A thin SDK driver is useful when it reduces the total code
we maintain. Independent adapters can change without rebuilding the controller;
small internal helpers remain ordinary code rather than separate services.

## Work must progress

An authorized request can start assigned work directly; preview is optional.
An assigned agent starts when its workspace and session are ready. Optional
telemetry or tool discovery cannot hold up its first input. The controller returns
complete output associated with that input, including protocols that omit repeated
parent IDs. Unsupported capabilities and missing output have explicit reasons.

Observation and manual control remain available with automation disabled. Stopping
future automation preserves running work and its results. One failed connection,
optional worker or viewer affects its own work; it does not stop unrelated agents.
Limits apply to queues, buffers and concurrency so growth does not become polling,
full-history scans or a model call for every status update.

## State and trust

Keep one authoritative record of an admitted action, its inputs and its outcome.
State changes are atomic. Reconnection and retries do not duplicate uncertain
external effects; readback resolves them where the native system supports it.
Late results belong to their original assignment and candidate.

Authenticate participants and protect credentials at access boundaries. Use stable
IDs and revisions for local state. Hashes are for necessary byte verification,
not a second system of permissions, approvals or compatibility. A document,
compiler, checkout path or diagnostic record does not authorize execution.

## Simplicity is a design constraint

Support one current implementation and format for each retained runtime. Old
experiments create no permanent compatibility obligation. Preserve concrete data
that is needed through a finite transition, then remove the obsolete path.

Delete unnecessary responsibilities before extracting packages. Prefer an existing
handler, table or library to another framework, workflow language, migration engine
or registry. Reuse the needed donor mechanism without importing the donor's whole
product. A change earns its complexity by serving current work and replacing or
simplifying a named mechanism.

## What completion means

A manager can assign work, obtain its output, request independent review, return
corrections and accept the resulting candidate. Selected continuation works;
restart preserves known outcomes and makes unresolved work visible. The retained
implementations have one owner each, and their replaced paths are removed.
Verification demonstrates this behavior at the changed boundary. It does not
require every historical experiment or optional integration to become a product.
