---
name: eliot-swarm
description: Use Eliot Swarm to coordinate Agents, claim shared work, delegate to children, exchange mail, collect results, or run an authorized private live proof.
---

# Eliot Swarm

Use the inherited `swarm` MCP tool with `{method, params}`. Use `api.methods`
to find canonical names and `api.help` with `{method: "mail.send"}` (or the
method you need) for fields and limits. If MCP is unavailable, set `$swarm`
to the full path of the approved release binary and use the PowerShell
examples below. Append `--help` to check the installed CLI. Keep your
inherited Agent identity and never pass tokens in arguments. If you received
this skill as first-turn text, find its companion at
`$env:ELIOT_HOME/skills/skills/eliot-swarm/reference.md`.

## Quick start

1. **Identity.** Call `api.identity {}` or run `& $swarm identity`. Record your
   Agent ID and generation and check your scope.
2. **Who.** Call `who {}` or run `& $swarm who`. Find your marked row and check
   who is working in the same area.
3. **Claim.** Before shared edits, call `claim.set` with a project-relative
   glob or run `& $swarm claim set 'src/area/**'`. Claims are advisory.
   Coordinate overlaps by mail and give workers disjoint paths.
4. **Work.** Read and acknowledge addressed mail, delegate via Swarm children
   where authorized, and collect exact outcomes while this turn is active.
5. **Finish and report.** Register your artifacts, settle and clean up owned
   work within scope, and release your glob with `claim.release` or
   `& $swarm claim release 'src/area/**'`. Report changes, verified outcomes,
   and open or `unknown` work. See [artifact and cleanup rules](reference.md#artifacts-and-cleanup).

## Common tasks

The examples use IDs, generations, paths, versions and turn IDs obtained
from your own reads. Assign those values to the PowerShell variables first.

### Launch a child and wait

- Read your own `agent.get` snapshot with your Agent ID, or run
  `& $swarm show $myAgent --json`. Use the retained workspace and
  `profile_snapshot.child_profiles`. Same-harness profiles may differ
  only in model and effort. See [child profile and wait details](reference.md#children-and-results).
- Launch a child with `command.submit`, verb `launch`, or run
  `& $swarm launch --parent $myAgent --profile $childProfile --workspace $parentWorkspace --wait --timeout-seconds 30 $task`.
  Give the child an explicit task and file scope. With a prompt, CLI
  `--wait` waits for the first turn. Keep the write ID for readback.
- Use `swarm.wait` for a captured child turn or set of turns.
  `& $swarm wait --all --timeout-seconds 30 "$child/$childGeneration/$turnId"`
  waits on one exact turn. A timeout only ends the wait.
  `& $swarm digest --all --to $myAgent "$child/$childGeneration/$turnId"`
  registers a durable mailbox summary via `swarm.digest`.

### Send, steer and interrupt

Use `command.submit` with the verb in the first column. For live turn
controls, read the generation and active turn with `agent.get` first,
then supply `expected_generation` and `expected_turn_id` to MCP.

| Verb | PowerShell |
| --- | --- |
| `send` | `& $swarm send $child $text --generation $childGeneration` |
| `steer` | `& $swarm steer $child $text --generation $childGeneration --turn $turnId` |
| `interrupt` | `& $swarm interrupt $child --generation $childGeneration --turn $turnId` |

### Mail and acknowledgment

| MCP method | PowerShell |
| --- | --- |
| `mail.send` | `& $swarm msg send --to $recipient -- $text` |
| `mail.inbox` | `& $swarm inbox` |
| `mail.read` | `& $swarm msg show $deliveryId` |
| `mail.ack` | `& $swarm msg ack --delivery $deliveryId` |

Use queue mail for coordination. Read the message, then acknowledge its exact
delivery ID. After a finished task, a mail-only wake needs a brief read and ack.

### Shared variables with compare-and-swap

Read with `var.get` or `& $swarm var get $key`. Write with `var.set` using
`expect_version` or `& $swarm var set $key $value --expect-version $version`.
Use version zero for an absent key. On a confirmed version conflict, re-read
and recompute the change.

### Register artifacts and read results

- Register a directory you own under `ELIOT_ARTIFACT_ROOT` with
  `artifact.register` or `& $swarm artifact register $artifactDir --kind temp --purpose $purpose`.
- Read another Agent's retained result within your scope with `agent.result`
  or `& $swarm result $child --generation $childGeneration --turn-id $turnId`.
  Check the retained outcome and final text before reporting completion.

### Run a private live proof

Follow the [private daemon procedure](reference.md#private-live-proof-on-windows)
for ACLs, registration and cleanup. Use an authorized private data directory.
Agent launches require a fresh one. Pass its exact path to every private command.

| Step | PowerShell | MCP method |
| --- | --- | --- |
| Start | `& $swarm daemon --data-dir $proofDir` | CLI startup only |
| Read status | `& $swarm status --data-dir $proofDir` | `daemon.status` (private Owner) |
| Stop | `& $swarm daemon stop --data-dir $proofDir --drain` | `daemon.shutdown` with `mode: "drain"` (private Owner) |

## Do not

- Do not end your turn waiting on a shell job or native Monitor. Run long commands in the foreground or poll them to completion.
- Do not retry a refused Owner operation as Owner on prod, change inherited identity, or read the Owner token.
- Do not guess child profiles or bypass a same-harness permission mismatch. Read your frozen `child_profiles`.
- Do not start a private daemon on an unprotected data directory, leave it undrained, or omit its artifact registration.
- Do not launch Agents from a copy of production data. Copied stores can share production process custody names.
- Do not leave a test desktop open after its screenshots. Use plain test repo names under one `qa-…` folder.
- Do not accept a black screenshot as UI evidence. Check whether the console display is off.
- Do not redo finished work on a mail-only wake. Read, acknowledge the exact delivery, and finish briefly.
- Do not replay uncertain effects. Keep `unknown`, read back, or reuse only the same mutation ID with identical input.

## Owner-only surfaces

Harness catalogues, models and quota, projects, settings and profiles, rules,
timers, goals, native discovery, diagnosis, daemon administration, the production
desktop as Owner, handoff, reparenting and switching, manual outcome
resolution, and broader or remote cleanup require Owner authority.
If a change needs one of these surfaces, propose it in your report.
See the [method map](reference.md#method-to-cli-map) for details.
