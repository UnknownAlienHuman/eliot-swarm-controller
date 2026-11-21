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

## Brief, finish and stop

Delegate with a short brief, not "improve everything":

```text
Result: the observable behavior that must change.
Read: the exact required files or sections, and the callers that matter.
Change: the write area and the linked interfaces it needs.
Limits: explicit prohibitions, compatibility, allowed external actions.
Check: the concrete scenario and the project's required commands.
Return: result, change locations, checks actually run, what is left.
```

- Pass the required reading as `sources` on launch or send (CLI `--source
  PATH`). Swarm delivers each file with its path, digest and completeness, or
  says why it could not. Don't reopen a file that arrived complete.
- "Start from these files" names entry points, not a closed write area.
- A child gets the same or a narrower area. To cross an explicit limit, it
  reports the reason and the minimal change instead of crossing it.
- **Done** means three things: the effect is reached, the limits held, and a
  proportionate check passed.
- "Already correct, verified against current behavior" is a valid outcome.
  "Could not reproduce" alone is not a fix.
- Once the criterion is met and the required review is done, stop. A
  further edit needs a concrete defect.
- After two failed approaches to one problem, stop continuing. Send the
  manager both attempts, the failure, and the observation that would tell them
  apart.
- Report what was obtained, where the change is, what was actually checked,
  and what is left. Keep errors, `unknown`, numbers and negations, and mark
  any truncation.

## Common tasks

The examples use IDs, generations, paths, versions and turn IDs obtained
from your own reads. Assign those values to the PowerShell variables first.

### Launch a child and collect its result

The daemon fills in parent, workspace and profile and follows the child's turn:

1. **Identity** (once): `api.identity {}`.
2. **Launch:** `command.submit` with verb `launch` and args `{"text": $task}`.
   Give the child an explicit task and file scope. Keep the response's
   `linked_command`, the child's first input.
3. **Wait:** `swarm.wait` with `commands: [linked_command]`, `mode: "all"` and
   `wait_ms: 30000`. A timeout only ends the wait; wait again.
4. **Result:** the entry's `status` and `final_text`. Call `agent.result`
   only when `truncated` is true.

PowerShell: `& $swarm launch --wait --timeout-seconds 600 $task` does steps
2–4 and prints the child's turn. `mailbox: true` (`--mail`) also ends a wait
when your own mail arrives. For a durable mailbox summary, use `swarm.digest`
or `& $swarm digest --all --to $myAgent --command $linkedCommand`. Name a
profile only to override the inherited one; see
[child profile and wait details](reference.md#children-and-results).

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
To get an answer in the same turn, send the question, then long-poll
`mail.inbox` with `{"reply_to": $deliveryId, "wait_ms": 30000}` (or
`& $swarm inbox --reply-to $deliveryId --wait 30`). Answer with
`reply_to_delivery` set to the question's delivery ID: that wakes an idle
asker at once.

### Shared variables with compare-and-swap

Read with `var.get` or `& $swarm var get $key`. Write with `var.set` using
`expect_version` or `& $swarm var set $key $value --expect-version $version`.
Use version zero for an absent key. On a confirmed version conflict, re-read
and recompute the change. To wait for a change, pass `after_version` with
`wait_ms` (or `& $swarm var get $key --after-version $version --wait 30`).

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
