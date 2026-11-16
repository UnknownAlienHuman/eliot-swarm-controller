---
name: eliot-swarm
description: Split work and delegate through Swarm children, with identity, mail and ack, who and advisory claims, shared variables with CAS, launch/wait/digest, and honest unknown results. Use whenever splitting work among agents, editing shared areas, contacting another agent, or finishing delegated work.
---

# Eliot Swarm coordination

You run inside Eliot Swarm alongside other agents. Coordinate through the
controller's live surface, never through files other agents own. Names below
are canonical methods (`api.methods` / `api.help` on the MCP `swarm` tool)
or CLI commands (`swarm <cmd> --help`). Field details always come from those
surfaces, never from memory.

## Who you are

Your agent id is in `ELIOT_AGENT_ID`. Confirm it with `api.identity`
(`swarm identity`): it returns your id, generation and allowed scopes.
`swarm who` lists every worker with the caller's own row marked.

## Mail: read it, then ack it

An attention notice means addressed mail arrived. Read it with `mail.inbox`
(`swarm inbox`), open one delivery with `mail.read` (`swarm msg show`),
and acknowledge exactly what you consumed with `mail.ack`
(`swarm msg ack`). Reading never acknowledges: unacked mail stays pending.

## Before editing a shared area

1. Run `who` (`swarm who`) and look for workers on the same files.
2. Set an advisory claim with `claim.set`
   (`swarm claim set --glob '<area>/**'`). Claims warn, they never lock.
3. Tell the other agent what you are doing with `mail.send`
   (`swarm msg send --to <agent> -- <text>`). Only `queue` mode is live.
4. Edit, then release with `claim.release` (`swarm claim release`).

## Shared variables

`var.get` / `var.set` (`swarm var get`, `swarm var set`) share small
state. Guard a read-modify-write with compare-and-swap: pass the
`expect_version` you read. On a version conflict, re-read and retry with
the new version; never blindly overwrite.

## Children, waiting and handoff

When splitting work among agents, **launch Swarm children**, using
`command.submit` with verb `launch` (`swarm launch`) and `role: "worker"`
or an allowed profile. Use different installed harnesses when available;
the parent's frozen `child_profiles` grants access to their own policies.
Use `api.help` for launch fields; get allowed profile names from your own
`agent.get` frozen snapshot, never guess them.
Do not use native subagents for delegated implementation or shared-file work:
they bypass Swarm claims, mail and completion tracking. Native subagents are
fine for private, read-only exploration touching no shared files.
Give each child disjoint files and require `who`, a claim before editing,
and mail or an exact ack. Wait with `swarm.wait` or register `swarm.digest`,
then collect the durable results before integrating.
Inspect one child with `command.children` (`swarm cmd children`).
A manager moves task inputs, claims and children with `swarm.handoff`;
only the owner moves agents (`swarm.reparent`, `swarm.switch`).

## Finishing

Report your result as your final message. The parent reads it with
`agent.result` (`swarm result`); a digest may also deliver it to the
parent mailbox. State plainly what you changed and what remains open.

## Honest unknown

If an effect is uncertain (timeout, lost reply, interrupted call), report
`unknown` and never replay it: retry with the same mutation `id`, or read
the retained result back. Never invent a success, a delivery or a
readback you did not observe.

## Off limits

Settings, rules, timers, goals, projects, discovery, reparenting and
switching are owner-only (`not_allowed` for agents). Do not attempt them:
propose changes in your final report instead.

## No MCP tool? Use the CLI

Without the MCP `swarm` tool the same surface is the `swarm` binary on
`PATH`: `swarm identity`, `swarm who`, `swarm inbox`, `swarm msg`,
`swarm var`, `swarm claim`, `swarm launch`, `swarm wait`,
`swarm digest`, `swarm result`. Append `--help` to any command.
