# Claude module — official Agent SDK boundary and current implementation status

This module integrates the installed Claude Code/Agent SDK harness; it does not define Claude’s permission model, session semantics or provider authentication. The authoritative sources are the [ELIOT module contract](../../docs/agent_swarm.module-contract-v2.md) and Claude’s current official Agent SDK documentation.

External release numbers in this repository describe the source that was implemented and tested at a point in time. They are not an allowlist for the user’s installation. A compatible update is admitted by checking the native interfaces actually used; a missing guarantee disables that capability rather than forcing a downgrade.

## Authentication boundary

Use the owner-selected native authorization path. For a subscription-backed route:

- do not inject `ANTHROPIC_API_KEY` or `ANTHROPIC_AUTH_TOKEN`;
- do not inherit a stray provider key from the controller environment;
- do not substitute a different endpoint or separately billed API;
- observe provider/auth mode without exposing credentials;
- fail before the first model input if the selected route does not match the declared route.

Claude’s official environment reference states that `ANTHROPIC_API_KEY` overrides Pro, Max, Team or Enterprise subscription use, and in non-interactive mode is always used when present. `CLAUDE_CODE_SIMPLE` also does not read OAuth/keychain credentials. These are connection facts the adapter must handle, not configuration suggestions for the owner.

An ELIOT module credential authenticates local IPC only. It is never a Claude provider credential.

Official sources:

- [Environment variables](https://code.claude.com/docs/en/env-vars)
- [Agent SDK permissions](https://code.claude.com/docs/en/agent-sdk/permissions)
- [Approvals and user input](https://code.claude.com/docs/en/agent-sdk/user-input)

## What exists in this repository

There are two distinct implementations:

1. this directory’s historical JavaScript bridge and stream mapper;
2. `crates/swarm-adapter-claude`, whose Rust adapter owns a Node SDK driver under `sdk-harness/`.

Capabilities cannot be transferred between them by name. The current Rust driver still contains a hard equality check for SDK package version `0.3.287`; documentation does not remove it. [R16](../../docs/remediation/2026-10-07/16-claude-interactions.md) specifies the connected repair.

Do not develop a third executor. Move required behavior into one selected implementation, qualify it, stop creating new bindings on the superseded executor, then retain only the minimum historical receipt reader needed for old evidence.

## Ownership and session lifecycle

One adapter boot owns one live SDK query/session control boundary. Host credentials are restricted to its binding/generation. IPC reconnect to the same live Node owner must not:

- close the native query;
- repeat admitted model input;
- resolve a permission request twice;
- adopt another session under the old binding;
- turn missing evidence into no-effect.

A lost Node process cannot recreate an in-memory callback Promise. Live and durable interaction modes are therefore separate:

- **live callback:** the Node owner stays alive until `canUseTool` returns;
- **durable defer:** an official `PreToolUse` hook returns `defer`, the native session persists, and later work resumes through the documented session mechanism.

JSON checkpoint data may retain request identity and evidence; it does not resurrect JavaScript closures.

## Current capability status

The table describes current ELIOT source behavior, not limits of Claude Code.

| Operation | Current implementation | Required boundary |
|---|---|---|
| describe | Present | Report actual executor/package versions, requested configuration and observed capabilities separately. |
| open | Present | Rootless preparation; first admitted input claims the one-shot prepared handle. |
| task dispatch / next input | Present | Echoed user identity and native session evidence; lost reply is not replay permission. |
| state / refresh | Present | Compact local projection; no model call. |
| reconcile | Present, local evidence only | Never repeats native input. |
| attach/resume/recover | Not exposed as a qualified controller capability | Native session features do not implement ELIOT recovery without exact ownership/readback. |
| configure/goal/exact-turn steer | Not exposed | Do not invent setters or target guards. |
| permission/question reply | Not connected | Current driver denies requests that reach its callback. R16 adds official live/deferred paths. |
| result paging | Not exposed | Do not fabricate a native page contract. |

## Permission model

Claude’s official evaluation order is:

1. `PreToolUse` hooks;
2. deny rules;
3. ask rules;
4. permission mode;
5. allow rules and native auto-approved actions;
6. `canUseTool` for the unresolved remainder.

Therefore:

- `canUseTool` is not an audit stream of every tool call;
- `PreToolUse` is the place for a guard that must run on every call;
- bare allow rules and auto-approval may shadow the callback;
- `dontAsk` denies instead of calling the callback;
- inherited and explicit permission mode are different facts;
- omitting `permissionMode` is not equivalent to explicitly passing `default` on current SDK behavior.

The adapter should project requested mode, observed/effective mode and callback/hook coverage separately. It must not enable `bypassPermissions`, `default` or another mode merely to make an integration test pass.

## Approvals and questions

A live callback receives `toolName`, input and context containing cancellation signal and optional permission suggestions. The Node owner keeps the original input and resolver. ELIOT stores a bounded, redacted request reference and exact fingerprint.

`agent.reply` must resolve the same current request exactly once. A decision acknowledgement means permission/question handling completed; it does not mean the tool, child or Task finished.

`AskUserQuestion` uses the official `questions` and `answers` shape. Current documentation limits one call to 1–4 questions with 2–4 options each and states that the tool is not available in subagents spawned via the Agent tool. The controller must not advertise child-question support without another documented native mechanism.

Persistent “always allow” rules are not an incidental reply field. Applying a permission suggestion changes settings and requires its own ELIOT authority/Operation. The first R16 slice supports allow-once, deny and question answers only.

If a human response may outlive the Node process, use the documented hook `defer` path. Keeping a Promise indefinitely is valid only while the owner process is deliberately retained. A lost live callback becomes `callback_lost`; it is not silently converted into deferred state.

## Stream evidence

The existing mapper must preserve native distinctions:

- multiple blocks with one assistant message ID are not duplicate messages;
- deltas are not independent completed messages;
- child identity comes from native parent/tool references;
- parent idle does not prove child completion;
- initialization failure, turn failure and successful result are different terminals;
- cumulative SDK usage estimates are not remaining subscription quota or invoices;
- bounded summaries report overflow and never copy the whole transcript into Store.

Requested model and permission values are not evidence of effective application. Use initialization/settings/session observations supplied by the actual runtime.

## Update behavior

For each new launch:

1. resolve the current owner-selected installation;
2. preserve its selected subscription/provider route;
3. import the required SDK boundary;
4. validate required exports, options, callback/hook shapes and stream/session forms;
5. report actual versions and unavailable capabilities;
6. start model input only after compatibility and route checks succeed.

A version mismatch alone is not an error. A missing required interface is a capability gap. No status or Doctor read installs packages, updates Claude, logs in, changes permission settings or rewrites live private configuration.

Do not overwrite a live bridge or swap the SDK underneath a running query. New ELIOT bytes receive their own artifact identity; external runtime compatibility remains observed separately.

See [UPDATE.md](UPDATE.md) for the file/activation boundary.

## Verification

After R16 implementation, the manager performs scoped formatting, warnings-denied Clippy for changed Rust packages and JavaScript syntax checks. Final qualification covers:

- subscription route preserved with no provider-key override;
- compatible update without release-number rejection;
- explicit/inherited/effective permission modes;
- auto-approved, ask, deny and `dontAsk` paths;
- live callback reply/abort races;
- durable defer/resume when supported;
- duplicate/conflicting reply and old-boot identity;
- `AskUserQuestion` multi-select/free-text shapes;
- reconnect and Node loss without replay;
- no false Task/tool completion from decision ACK.

Existing fixtures are authored protocol examples, not live qualification of every later runtime. This document does not claim the production gate or reply path is already fixed.