# ACP agents

Layer L4.6. Generic Rust plugin using selected ACP v1 crate/schema; runtime mode
`per_agent`: one configured ACP subprocess/custody unit per Eliot generation.

Facts checked 2026-10-25 against official ACP v1 transport/schema/registry and crate
releases. Recheck installed agent capabilities; ACP v2 remains outside this slice.

## Start and source setup

ACP separates session creation and prompt:

1. initialize/capability negotiation;
2. `session/new` with exact cwd/frozen config;
3. open authoritative source families and install update/response routing;
4. commit ready with native session/effective capabilities;
5. dispatch linked initial send through `session/prompt`.

`start_requires_input=false`. Lost `session/new` and prompt are separate unknowns.
Resume creates new generation/process and uses advertised load/resume; optional text is
linked prompt, never old input replay. Routing must be installed before load/prompt so
early updates enter bounded source spool rather than disappear.

ACP source uses stable open id/key, contiguous commit-ack sequence and exact native
session/request/update identity where schema provides it. If installed agent has no
replay cursor, reconnect combines exact current-state readback with explicit source gap;
it never re-emits old terminal events as new.

## Verb mapping

| Eliot | ACP v1 |
| --- | --- |
| send | one `session/prompt`; applied only when exact prompt/turn association is known; terminal response ends turn, not product task |
| steer | unsupported |
| inject | explicit prepend-next-prompt only |
| interrupt | exact active `session/cancel`; queued cancel/session close distinct |
| stop | exact `session/close` if advertised, then per-agent custody |
| approve/deny | exact permission request + one offered option id |
| answer | only advertised exact user-input capability |
| configure | typed config/mode setters + application point/readback |
| goal | Eliot goal only |

Capability absence rejects before native I/O. Non-ACP headless CLI is never hidden
fallback; it is separate harness/profile.

## Events and readback

Live ACP connection owns controlled lifecycle. Map exact selected-schema variants and
preserve session/turn/tool/request/update IDs. A successful transport write is not send
applied until exact prompt/turn association. Prompt response defines terminal turn;
process exit/silence does not.

Reconnect/load returns running, exact terminal, not-active-unknown or subject absent.
Logs/transcripts cannot publish duplicate lifecycle. Old generation/request updates are
diagnostic only.

Per-agent subprocess/descendants may register only after custody verification. OS
monitor owns process exit/stats.

## Children and environment

ACP child support is capability-dependent. A native child is registered only with exact
child scope/ref and stable registration id. Parent-local ids are scoped by parent
subject; parent/root/project conflict rejects.

Declare `child_environment=true` only when the installed ACP agent exposes a distinct
child process/session environment and the plugin can inject child Eliot token/Git
identity before child tools run. Otherwise child row may be observable but child CLI/Git
activity is attributed to parent and token is omitted. L7 must cover lost child reply
and retry-safe token injection for any true claim.

## Profiles and routing

Profile `cmd` names ACP agent (for example `gemini --acp`). Minimal environment passes
only named agent auth/config and parent Eliot identity. One plugin may support many
binaries; each process has independent custody/effective manifest. Prefer native
Codex/Claude plugins for extra semantics, but never silently switch requested ACP route.

## L7 cases

- lost source-open/session-new/ready/linked-prompt acknowledgements;
- response route before load/prompt and bounded early-update spool;
- prompt association vs terminal turn vs product success;
- advertised capability narrowing/unsupported verbs;
- exact permission choices and stale generation/request;
- reconnect current-state/gap/vendor duplicate;
- child capability/parent attribution/token retry where supported;
- descendant registration vs OS enumeration and no cross-agent force;
- ACPX backend replacement deletes old route, never two owners.
