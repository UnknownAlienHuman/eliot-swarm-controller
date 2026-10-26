# ACP agents

Layer L4.6. Generic Rust plugin using a selected ACP v1 crate/schema; runtime mode is
`per_agent`: one configured ACP subprocess/custody unit per Eliot generation.

Facts checked 2026-10-25 against official ACP v1 transport/schema/registry and crate
releases. Recheck installed Agent capabilities; ACP v2 remains outside this slice.

## Start and source setup

ACP separates session creation and prompt:

1. initialize/capability negotiation without creating a session;
2. persist/open authoritative source epochs and install update/response routing;
3. call `session/new` with exact cwd/frozen binding;
4. commit-acknowledge `session.ready` with native session/effective capabilities;
5. dispatch linked initial send through `session/prompt`.

`start_requires_input=false`. Lost `session/new` and prompt replies are separate
unknowns. Resume opens new-generation source epochs/routing first, then uses advertised
load/resume; optional text is a linked prompt, never old-input replay. Routing must be
installed before load/prompt so early updates enter the bounded source spool.

The live connection declares stable native update/event ids where available. If the
installed Agent has only current-state readback and no replay cursor/id, recovery uses
`dedupe_mode=none` and atomic gapped source replacement; it never re-emits old terminal
events as new. Discovery/readback pages obey plugin item/byte limits.

## Verb mapping

| Eliot | ACP v1 |
| --- | --- |
| send | one `session/prompt`; applied only when exact prompt/turn association is known; terminal response ends turn, not product task |
| steer | unsupported |
| inject | explicit prepend-next-prompt only |
| interrupt | exact active `session/cancel`; queued cancel/session close are distinct |
| stop | exact `session/close` if advertised, then per-Agent custody |
| approve/deny | exact permission request + one offered option id |
| answer | only advertised exact user-input capability |
| configure | typed config/mode setters + application point/readback |
| Goal | Eliot Goal only |

Capability absence rejects before native I/O. A non-ACP headless CLI is never a hidden
fallback; it is a separate harness/profile.

## Events and readback

The live ACP connection owns controlled lifecycle. Map exact selected-schema variants
and preserve session/turn/tool/request/update identities. A successful transport write
is not send applied until exact command→prompt/turn association. A separate native input
id is optional. Prompt response defines terminal turn; process exit/silence does not.

Reconnect/load returns running, exact terminal, not-active-unknown or subject absent.
Logs/transcripts cannot publish duplicate lifecycle. Old generation/request updates are
diagnostic only. Final text and event data obey persisted bounds; larger native detail
stays in the Agent/native store.

Per-Agent subprocess/descendants may register only after custody verification. OS
monitor owns process exit/stats.

## Children and routing

ACP child support is capability-dependent. A native child registers only with exact
child scope/ref and stable registration id. Parent-local ids are scoped by parent
subject; parent/root/project conflict rejects.

Declare `child_environment=true` only when the installed ACP Agent exposes a distinct
child process/session environment and plugin can inject child Eliot token/Git identity
before tools run. Otherwise child row may be observable or natively controllable through
parent, but `routing_enabled=false`; CLI/Git activity is attributed to parent and token
is omitted. Guard-exceeding vendor children remain visible with reduced authority/wake.

## Profiles and routing

Profile `cmd` names the ACP Agent (for example `gemini --acp`). Minimal environment
passes only named auth/config and parent Eliot identity. One plugin may support many
binaries; each process has an independent frozen binding/custody/effective manifest.
Prefer native Codex/Claude plugins for extra semantics, but never silently switch a
requested ACP route.

## L7 cases

- source open/routing installed before `session/new`, load and prompt;
- lost source-open/session-new/ready/linked-prompt acknowledgements;
- prompt association with and without a separate native input id;
- terminal turn versus product success;
- advertised capability narrowing/unsupported verbs;
- exact permission choices and stale generation/request;
- cursor/event-id/no-replay recovery, atomic gap handoff and page/frame bounds;
- child capability/guard/parent attribution/token retry where supported;
- descendant registration vs OS enumeration and no cross-Agent force;
- ACPX backend replacement deletes old route, never two owners.
