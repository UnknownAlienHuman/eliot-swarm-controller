# Architecture

Eliot is a local controller around existing coding-agent harnesses, not another harness
or workflow platform. [Vision](vision.md) owns requirements; [roadmap](roadmap.md) owns
build order. Specs describe the final contract, but a layer implements only what it uses.

## One command path, one event path

```text
CLI / Agent / later rules
          |
          v
     daemon API -> router -> plugin host -> harness plugin -> native harness
                      |                         ^
                      v                         | native facts/readback
                SQLite writer <----------------+
                      |
              commit event + projection
                      |
              bus -> CLI / later consumers
```

The bus is an in-process persisted log plus bounded live subscriptions, not Kafka,
Redis or another service. Plugins are separate processes; internal daemon features are
ordinary Rust modules. There is no internal plugin framework or dependency-injection
platform to build first.

## Modules and dependency direction

| Location | Responsibility | Allowed dependencies |
| --- | --- | --- |
| `eliot-core` | ids, verbs, events, pure reducer | data/error crates; no I/O, database, runtime or vendor SDK |
| `eliot-store` | migrations, one writer, bounded queries | core + SQLite |
| `eliot-plugin` | small wire types/framing shared by host and plugins | core + transport/serialization; no Store |
| `eliot-daemon` | API, router, bus, process/plugin ownership | core, store, plugin protocol |
| `plugins/codex` | app-server translation and native readback | plugin protocol; no daemon/Store dependency |
| `eliot` | CLI and composition/entry points | application crates and built-in plugin entry points |

Inside `eliot-daemon`, add `monitoring/`, `workspaces/`, `automation/` and
`coordination/` when their roadmap layer starts. Each owns its feature logic; its SQL queries and
migrations live in Store. Every module calls the same router for effects. No automation-to-vendor or coordination-to-vendor
shortcut. Only Store code issues SQL. Plugins never receive a database connection.

The fake uses the same protocol as Codex. It is initially an echo/turn process, not a
large simulator. Create crates as their callers arrive; this table is the eventual map,
not six empty crates required for `--version`.

## First useful slice

L1 delivers local CLI → Store/router → fixed-source fake → bus/result. L2 replaces
only the fake with a per-Agent Codex app-server and adds native controls/readback. It
uses an existing workspace and account, with Luna selected explicitly when available.

L3 adds monitoring/worktrees and the optional OpenCodex route. L4 adds automation on
Codex; L5 adds communication between two Codex Agents; L6 adds other harnesses. Neither
mail nor timers require a second vendor. Unimplemented features are not advertised.

## State and effects

Compute the next projection with the pure reducer without mutating published state.
In one Store transaction write the event, projection and any accepted commands; only
after commit publish the result and acknowledge it. Native I/O runs outside that
transaction and only after command dispatch is recorded.

Store failure stops new writes and external dispatch. It does not rerun an external
effect to reconstruct a missing result. Restart reconciles the durable current objects
and exact native state; it is not an event-by-event replay of external side effects.

The detailed invariants have one owner:

| Invariant | Owning specification |
| --- | --- |
| Stable Agent + monotonic generation; exact turn/request; command states | [core](spec/core.md) |
| Caller identity, write idempotency, wait and shutdown | [API](spec/api.md) |
| One lifecycle source, commit acknowledgement, cursor/gap and native translation | [plugin protocol](spec/plugin-protocol.md) |
| Process ownership, source drain, Git/workspace observation | [monitoring](spec/monitoring.md) |
| Profile/route snapshot, environment and secrets | [configuration](spec/configuration.md) |
| Timers, rules, checks and actions using ordinary commands | [automation](spec/automation.md) |
| Mail, variables, hierarchy and advisory attribution | [coordination](spec/coordination.md) |

Do not duplicate these contracts in worker instructions or add another decision for each
edge case. Repair the owning spec and its actual caller together.

## Process and recovery boundary

Use one process-owning plugin/app-server per Agent generation. A replacement never
inherits an uncertain call. Resume is explicit and does not resend the old prompt.
Shared services are later capabilities and require exact per-session isolation; stopping
one Agent never kills another session or an owner-run service.

Windows Jobs provide the specified exact custody. A Unix process group is degraded and
must report that limitation. Process exit races final output: drain the source before
concluding loss. Silence, transport loss and missing history are not completion.

A source's `open|closed` lifecycle, health and historical gap are independent.
Handoff transfers only the named event families; it does not imply that the previous
source process closed. The protocol owns the exact transition rules.

## Scale without a cluster

Historical/idle Agents are indexed records. Live harnesses consume their native process
resources; logical fleet capacity is not a promise that one host runs a million model
processes. Queries, event buffers, histories and fanouts are bounded/paginated. Add a
pool, scheduler or new persistence table only for an implemented caller.

Use [donors](reference/donors.md) as an extraction map, not a list of frameworks to
combine. Ready-made dependencies/helpers come first; incompatible workflow/UI/security
platforms stay out.
