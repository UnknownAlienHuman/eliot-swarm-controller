# Architecture

Eliot is a local controller around existing coding-agent harnesses, not another harness
or workflow platform. [Vision](vision.md) owns requirements; [roadmap](roadmap.md) owns
build order. Specs describe the final contract, but a layer implements only what it uses.

## One command path, one event path

```text
CLI / Agent / rules and timers
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
| `eliot-plugin` | shared wire types, framing and native lifecycle mechanisms | core + transport/serialization; no Store |
| `eliot-daemon` | API, router, bus, process/plugin ownership | core, store, plugin protocol |
| `plugins/*` (codex, claude, opencode, Muse, antigravity) | harness translation and native readback, one process per Agent generation | plugin protocol; no daemon/Store dependency |
| `apps/swarm-desktop` | optional Windows companion UI | daemon API and `settings.*` only; keeps no other preference store |
| `eliot` | CLI and composition/entry points | application crates and built-in plugin entry points |

Inside `eliot-daemon`, keep monitoring, workspaces, automation and coordination as clear
logical modules when their roadmap layer starts. A module need not have a matching
directory: the current implementation may keep cohesive modules in flat files such as
`monitor.rs` and `workspaces.rs`. Split files when a real ownership boundary needs it,
not to satisfy a path convention. Each feature owns its logic; SQL queries and migrations
live in Store. Effects use the same router. No automation-to-vendor or
coordination-to-vendor shortcut. Only Store code issues SQL; plugins never receive a
database connection.

### Internal module boundaries

Use these boundaries when decomposing the current large Store/Router owners. They are
ordinary modules inside the existing crates, not new services or dispatch abstractions.

| Existing owner | Extraction boundary |
| --- | --- |
| Store `submit_tx` | command admission and its private validation helpers, sharing the caller's transaction and event accumulator |
| Store `dispatch_inner` | dispatch state/generation changes, retaining the same writer, transaction scope and ordering |
| Store migrations, workspace actions and source batches | separate cohesive schema, workspace and source-persistence helpers; keep publication after commit |
| Router `process_command_inner` | command execution and its private preparation/settlement helpers as an internal `impl Router`; native I/O remains outside the Store lock |
| Store `rules.rs` | timer scheduling apart from event-rule traversal; reuse existing `rule_history.rs` for finite catch-up rather than building a second history engine |

Keep entry points small by moving their existing work, not by duplicating gates or
adding forwarding traits. Retain authority, generation, idempotency and source checks at
their owning boundaries. Canonical payload/ACK digests and credential hashes serve those
contracts; file size alone is not a reason to remove them or add another integrity layer.

All five native adapters use the same protocol through their `plugins/*` process;
the desktop is an optional client on the daemon API and `settings.*`.
Adapters declare the capabilities that govern shared control paths when the binary is
composed. Shared code resolves the declaration from the configured identity, then uses
its capabilities to prepare and validate operations. Keep vendor request and response
translation with the adapter, reusable native lifecycle mechanics in `eliot-plugin`,
and routing, admission and durable settlement in the daemon and Store.

## Memory inference boundary

M1 enters the same private authenticated Router and Store writer. One transaction
binds its existing write receipt, client sequence and physical call. The daemon
reuses scoped process custody for a fresh native invocation; the Claude plugin
owns print-mode argv/result translation separately from the Agent adapter. Call-owned
private content is accessible only through the authenticated client binding. No
Agent, Task, service-account framework or second mutation ledger is created.

## First useful slice

Completed history: L1 delivered local CLI, Store/router, a temporary fake, bus and
result. L2 replaced that fake with a per-Agent Codex app-server and added native
controls and readback over an existing workspace and account. Current code has no fake
runtime: the five native adapters launch from explicit profiles, or by `--role` through
the ordered `[routing]` candidates when `--profile` is omitted. A profile may omit
`cmd`; the harness-specific executable resolver is shared with ad-hoc launches and
freezes the selected native path in the launched generation. Routing freezes before
admission. The opted-in provider policy may
admit a fresh switch after an exact native pre-work refusal. Uncertain effects
keep their original bindings and outcome.

L3 through L6 are implemented (monitoring, worktrees and the proxy route; automation and
scripts; agent communication; the five harnesses with worker MCP and the `eliot-swarm`
skill; the cloud MCP caller; diagnosis and notification), and queue assignment runs as an
automation timer plus a completion rule, not a task engine. L7 qualification is next;
see [roadmap](roadmap.md) for the only project status list. Neither mail nor timers
require a second vendor. Unimplemented features are not advertised.

## Cloud MCP caller and agent-health diagnosis

A cloud Dot/MuseAgent reaches Swarm only as a remote MCP caller through the local
StreamableHTTP MCP server exposed via `cloudflared` at
`swarm.iuriilisenkov.cloud`. It is authenticated with Cloudflare Access Managed OAuth
and native JWT validation per the existing account documentation; no new OAuth code is
required here. The cloud caller carries a remote identity, connection and call binding
through the existing router/bus. It is not a controlled local native harness and has no
PID, process custody or local lifecycle of its own. The local One OS user trust boundary
remains for local control.

Stalled or unhealthy agents are detected and diagnosed from read-only native, process,
turn and event facts. Native process statistics alone do not prove model progress, and
silence is not completion. The daemon runs per-kind monitoring reactions through the
existing router and uses exact generation and turn bindings for controls. The
[monitoring contract](spec/monitoring.md#automatic-reactions) defines the allowed
actions and notices routed to the responsible manager or Owner.

## State and effects

Core owns the pure Agent reducer and pure command-transition/error-classification
predicates; Store proves command-specific context and persists their results. Compute
the next projection without mutating published state. In one Store transaction write the
event, projection and any derived command facts; only after commit publish and
acknowledge. Native I/O runs outside that transaction and only after command dispatch
is recorded.

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

## Work truth lives in Git and the file system

The [owner principle](vision.md#owner-statement-source-of-truth) of 2026-11-22: the
truth about work is stored in Git and in the file system, outside every harness.

- **Worktree.** A task's state is its managed worktree: the Git branch with its commits,
  the files on disk including uncommitted changes, and the task card naming the branch
  and goal.
- **Disposable session.** A harness session (process, model, context, native transcript)
  is a disposable worker on that worktree. Losing it loses none of the work on disk.
- **Checkpoints.** Agents commit progress at meaningful checkpoints on their own branch.
  Commits carry the work beyond the worktree: review, merge, or a successor in another
  worktree.
- **Handoff brief.** Work moves to another model from the worktree and branch, not from a
  vendor transcript. A successor in the same worktree continues from the files as they
  are. This is the ordinary path for every reason, not a special failover: a provider
  limit, a model or cost choice, a failed or lost session, or an Owner request.
- **Store's role.** Store records who works in which worktree and what happened. Git and
  the file system hold the work itself.

[Coordination](spec/coordination.md#agent-switch-implemented) owns the switch and handoff
contract, including when a handoff is admitted, what transfers and how uncertain effects
are reported (never replayed). [Configuration](spec/configuration.md#routing-policy-s3)
owns which models may take over. #918 brings both in line with this principle.

## Process and recovery boundary

Use one process-owning plugin/app-server per Agent generation. A replacement never
inherits an uncertain call. Restart recovery can reopen a retained session in a new
generation once custody and work-state checks pass. Resume never resends the old prompt.
Shared services are later capabilities and require exact per-session isolation; stopping
one Agent never kills another session or an owner-run service.

Windows Jobs provide the specified exact custody. A Unix process group is degraded and
must report that limitation. Process exit races final output: drain the source before
concluding loss. Silence, transport loss and missing history are not completion.

The supported Linux target is the [Docker image](../docker/README.md), currently
linux/amd64 on Debian bookworm, rather than separate distro packages. Its nonroot
daemon uses a private Unix socket and volume. Linux custody remains degraded:
process groups can be escaped and do not die automatically with the daemon.
The container's cgroup v2 is not per-Agent delegated custody. Whole-container
teardown is a separate boundary and never evidence of an exact Agent stop.
The Windows desktop's named-pipe client cannot attach to this container daemon.

A source's `open|closed` lifecycle, health and historical gap are independent.
Handoff transfers only the named event families; it does not imply that the previous
source process closed. The protocol owns the exact transition rules.

Crash capture and diagnosis are read paths, not stores. Panic hooks write bounded
redacted markers beside the data directory; `diagnose` joins command outcomes,
turn `preceded_by` links, process exits, stderr tails and the previous marker at
read time. Debug telemetry is numeric metadata at the same choke points the
release path already crosses: RPC timing, plugin calls, store commit counters
and plugin channel/process records, all gated behind the debug log level.

## Scale without a cluster

Historical/idle Agents are indexed records. Live harnesses consume their native process
resources; logical fleet capacity is not a promise that one host runs a million model
processes. Queries, event buffers, histories and fanouts are bounded/paginated. Add a
pool, scheduler or new persistence table only for an implemented caller.

A page limit bounds returned data, not the rows SQLite visits or sorts. Review actual
filter/cursor/order plans while preserving visibility and ordering; confirm that a new
index is selected, including continuation pages. Batch shared OS observation within one
sweep where possible. Keep per-owner identity and completeness checks, and measure work
before adding parallel workers or claiming a latency gain.

Use [donors](reference/donors.md) as an extraction map, not a list of frameworks to
combine. Ready-made dependencies/helpers come first; incompatible workflow/UI/security
platforms stay out.
