# Codex app-server

Current harness; see [L2](../roadmap.md#l2--first-useful-harness-codex-with-luna).
Use `plugins/codex`, Rust/built-in, with one plugin/app-server process per controlled
Agent generation. The source exists and the CLI exposes the control path below.
Live qualification is still pending; [roadmap](../roadmap.md) owns milestone status.

## Sources

Checked against official documentation on 2026-10-27:

- [app-server transport, initialization, methods and schema generation](https://learn.chatgpt.com/docs/app-server);
- [available model names and rollout caveats](https://learn.chatgpt.com/docs/models);
- [upstream protocol definitions](https://github.com/openai/codex/tree/main/codex-rs/app-server-protocol/src).

The installed binary's generated schema controls wire names. Generate it once for the
version being integrated; update when that version changes, not before every command:

```text
codex app-server generate-json-schema --out <temporary-schema-directory>
```

Use typed messages for the implemented methods. Do not import the Codex harness/model
loop or make its unstable types part of `eliot-core`.

## First useful path

Use an existing authenticated `CODEX_HOME` and a disposable existing workspace.
No proxy, native Goal, subagent, worktree creation or transcript handoff is needed yet.

1. Spawn `codex app-server --listen stdio://` inside Agent custody and start its reader.
2. Send `initialize`, await success, then send `initialized`.
3. Read paginated `model/list`. Select the explicitly configured Luna model and an
   effort supported by that response. Do not silently substitute another model/account.
4. `thread/start` without a prompt establishes the thread; commit `session.ready`.
5. Submit the linked input with `turn/start`; record the exact returned/notified turn
   id. Input application and terminal turn result are separate facts.
6. Stream text/tool/request events, answer exact native requests, and commit final text
   before the terminal turn. Expose the result through the existing CLI.

Official docs currently name Luna `gpt-6-luna`; availability depends on account/client.
Use that id only if the selected account's catalogue accepts it. This is configuration,
not a hardcoded alias in the plugin. No paid probe or alternate billing route is invented
when the requested model is unavailable.

Example Eliot config:

```toml
[profiles.codex-luna]
harness = "codex"
model = "gpt-6-luna"
permissions = "workspace-write"
pass_env = ["CODEX_HOME"]
```

```powershell
& $eliot --data-dir $dataDir launch --profile codex-luna --workspace $workspace "Create hello.txt containing hello." --wait
$agent = 'AGENT_ID_FROM_OUTPUT'
& $eliot --data-dir $dataDir result $agent
& $eliot --data-dir $dataDir send $agent "Read hello.txt and report its contents." --wait
& $eliot --data-dir $dataDir steer $agent "Adjust course." --generation 1 --turn TURN_ID
& $eliot --data-dir $dataDir interrupt $agent --generation 1 --turn TURN_ID
& $eliot --data-dir $dataDir approve $agent --request REQUEST_ID
& $eliot --data-dir $dataDir deny $agent --request REQUEST_ID
& $eliot --data-dir $dataDir answer $agent --answers-file answers.json --request REQUEST_ID
& $eliot --data-dir $dataDir resume $agent "Follow up." --wait
& $eliot --data-dir $dataDir stop $agent
```

Flags map to `command.submit`: `--generation` to `expected_generation`,
`--turn` to `expected_turn_id`, `--request` to `request_id`.
A missing request is captured only for a single pending native request.
`answer --answers-file` submits the parsed bounded body, not a path, and failures
never echo file contents. `resume --wait` with a prompt follows the new linked
`<id>/send` in generation N+1, without it follows resume readiness. No old input
is replayed.

The plugin uses `model/list` internally; a separate model-catalogue product/UI is not
required. Keep the model choice in the profile, outside core types and tests.

## Identity, transport and ownership

Native subject is `(canonical Codex store/account scope, exact thread id)`. The native
wire is line-delimited JSON; it does not require a `jsonrpc:"2.0"` field. The Eliot
plugin envelope is separate and includes Agent/generation.

One app-server can host many threads, but per-Agent processes keep environment, Eliot
token, Git identity and custody separate. Native child threads share the parent
process environment and have no independent Eliot CLI principal unless isolation is
actually supported. They are added after the basic path, not emulated in L2.

Start owns one live stream. It publishes the implemented lifecycle families only; do not
install hooks or tail rollouts in parallel to duplicate those facts. The reader exists
before create/resume/input. A lost native write reply is readback/unknown, never a second
write. A lost Eliot event acknowledgement follows the common last-batch rule.

## Control mapping

| Eliot operation | Native mechanism and result |
| --- | --- |
| launch | `thread/start`; readiness proves a session, not prompt execution |
| send | `turn/start`; bind command to exact turn, then track completion separately |
| steer | `turn/steer` with `expectedTurnId`; stale turn rejects |
| inject | supported exact steer or next-send prepend; report which and when |
| interrupt | `turn/interrupt` for the captured turn; no blind retry |
| stop | end this Agent's session/process; clear a native Goal if one was enabled; never stop another Agent |
| resume | new Eliot generation/app-server plus `thread/resume` of retained thread; no old input replay |
| approve/deny/answer | reply to the exact offered native request and choice schema |
| configure | supported turn/session fields; report application point and effective value |
| fork / native Goal / child control | later optional methods verified against installed schema, not advertised until implemented |

Read effective model/provider/cwd/permissions from documented native fields. When an
endpoint/billing fact is not exposed, report it as unverified rather than inventing
readback. An unexpected effective route does not authorize further input.

## Recovery and later capabilities

The stdio stream has no assumed replay cursor. If it breaks, mark a gap and use exact
thread/turn/request readback; do not republish old history as live completions. Owned
process loss ends that controlled generation after buffered output is drained.

At L3, observe-only rollout reading may use file identity plus byte offset as an opaque
exclusive cursor. Source handoff is explicit. Observation never calls resume, fork,
Goal or send merely to obtain history. Paged history must be read through its cursor;
parent idle does not prove children completed.

OpenCodex is added as a named provider route in L3, initially attached to an owner-run
service. Do not edit owner global Codex config. Keep client/admin credentials separate
and report only routing facts that native/readback or a real request demonstrates.

Add native Goals with L4 automation, exact native children with L5 hierarchy, and
other native controls only when selected. Hook lifecycle
must not duplicate the already-authoritative stream. Unknown request types receive a
typed error, never an invented default reply.

L7 exercises lost-start/input replies, stale generation/turn/request, final-output vs
exit ordering, requested/effective route, paginated recovery and child limitations.
Do not make that complete campaign a prerequisite to the first useful file edit.
