# ACP agents

Layer L4.6. Generic Rust plugin using the selected ACP v1 crate/schema, but runtime mode
is `per_agent`: one configured ACP subprocess per Eliot generation.

Facts checked 2026-10-25 against official ACP v1 transport/schema/registry and crate
releases. Recheck installed agent capabilities; ACP v2 remains outside this slice.

## Start

ACP cleanly separates session creation and prompt:

1. initialize/capability negotiation;
2. `session/new` with exact cwd/config;
3. emit ready with native session id/effective capabilities;
4. dispatch optional linked initial send through `session/prompt`.

`start_requires_input=false`. Lost `session/new` and lost initial prompt are separate
unknowns. Resume creates a new Eliot generation/process and uses advertised
`session/load`/`session/resume`; optional text is a linked prompt, never old input
replay. Install update/response routing before load/prompt so early notifications are
not lost.

## Verb mapping

| Eliot | ACP v1 |
| --- | --- |
| send | one `session/prompt`; request terminal response settles that turn, not product success |
| steer | unsupported |
| inject | explicit prepend-next-prompt only |
| interrupt | exact active `session/cancel`; queued cancel/session close remain distinct |
| stop | exact `session/close` if advertised, then this process custody unit |
| approve/deny | exact `session/request_permission` and one offered option id |
| answer | only an advertised exact user-input request/reply capability |
| configure | typed config/mode setters plus readback/application point |
| goal | Eliot goal only |

Capability absence rejects before native I/O. Non-ACP headless CLI is never a hidden
fallback; it would be a separate harness/profile.

## Events/source ownership

Live ACP connection owns controlled lifecycle. Map exact update variants from the
selected schema and preserve native session/turn/tool/request ids. `session/prompt`
response defines turn end; process exit or silence does not. Logs/transcripts cannot
publish duplicate lifecycle beside it.

Reconnect/load must either bridge an exact cursor/current-state readback or report a
gap. Old generation updates and permission ids are diagnostic only.

## Profiles/environment

Profile `cmd` names the ACP agent (for example `gemini --acp`). Minimal environment
passes only named agent auth/config and Eliot token/Git identity. One plugin
implementation may support many binaries; each live process still has independent
custody and effective capability manifest.

Prefer native Codex/Claude plugins when their extra semantics are required, but never
silently switch a requested ACP profile.

## L7 cases

- session/new vs linked first prompt at every lost-reply point;
- response route installed before load/prompt;
- advertised capability narrowing and unsupported verbs;
- exact permission option ids and stale generation/request rejection;
- prompt completion vs task success;
- source reconnect/gap and no duplicate events;
- per-agent environment/custody and no cross-agent force;
- ACPX backend replacement only with old path deleted, never two owners.
