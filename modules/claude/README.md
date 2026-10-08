# Claude — subscription integration and implementation status

**Owner correction, 2026-10-08:** use the existing authorized subscription harness. Do not require an API key, a separate inference-billing route, a frozen SDK/CLI release, disabled vendor updates or a downgrade. Record the runtime actually used and validate the required native interfaces. A version found in this repository is evidence about its implementation, not a required version for the user's installation.

## What this directory currently contains

This is the legacy JavaScript bridge. The reviewed source uses `@anthropic-ai/claude-agent-sdk` 0.3.287 and identifies itself as `claude-agent-sdk-0.3.287-bridge.3`. The standalone Rust adapter is separate: `crates/swarm-adapter-claude` with its existing Node SDK driver. Neither implementation's capabilities can be inferred from the other's name.

These historical package/artifact values describe code that is still present. The dependency manifests and hardcoded version checks have **not** been repaired by this documentation change. In particular, the Rust driver's `prepare` currently rejects an SDK package whose version is not 0.3.287. That is an adapter defect to remove, not a reason to change the user's working runtime.

The external SDK is used under the terms recorded in [third-party notices](../../THIRD_PARTY_NOTICES.md); it is not a permissive source-code donor. [R16](../../docs/remediation/2026-10-07/16-claude-interactions.md) defines the next connected implementation: remove the release gate, preserve the subscription route, and implement the pending callback → attention → reply path. The PR remains Draft; those runtime changes are not yet implemented.

## Ownership and connection

The existing bridge owns one live SDK query. Host credentials are scoped to its binding/generation; they do not make the module a GM or permit Task acceptance. Native authorization, tools and the model loop remain with Claude Code. Host IPC reconnect must not close the query or repeat native input.

Use the owner's current native installation and account. Resolve the executable through the owner's normal installation path for a new launch; do not retain an obsolete versioned installation path as a future launch requirement. Do not silently substitute the SDK's historical bundled executable, a new account or a separately billed API for the working subscription route.

A launch still needs the actual workspace, host endpoint and binding-scoped credential. The selected model and explicit permission settings come from the current route. No example below prescribes an old model/release or enables a route. A local module credential is an ELIOT IPC credential, **not** a provider API key.

The existing `swarm module-run` ownership boundary is preferable to launching an unguarded bridge directly. It records the process owner; it does not prove that every native capability works. Updating documentation neither launches nor changes any running process.

## Capability matrix — legacy bridge only

These are implementation facts for this directory at source 40591a295af94b1541ec2ba30afe8e3247701a71, not limits of current Claude Code.

| Operation | Current implementation | Boundary |
|---|---|---|
| describe | Present | Requested model, observed init model, executor and permission metadata remain separate. |
| open | Present | `startup()` prepares a rootless executor; first input claims the one-shot WarmQuery. |
| next-turn send / task.dispatch | Present | Exact Task input and echoed user UUID bind admission to the observed session. UUID is not a native turn ID. |
| state / refresh | Present | Local compact stream projection; no new model call. |
| reconcile | Present, bridge-local | Reads retained evidence; never repeats input. |
| attach / resume / recover | Not exposed here | The SDK's possible resume capability does not implement controller recovery by itself. |
| configure / goal / exact steer | Not exposed here | No invented setter or expected-turn guarantee. |
| permission/question reply | Not exposed here | Current callback immediately denies requests that reach it. R16 implements a live round trip in the Rust+Node path. |
| result paging | Not exposed here | No fabricated native paging contract. |

Do not delete a real working executor before the replacement covers its required scenarios. Reading old receipts does not require keeping a second executor indefinitely.

## Input and stream evidence

The first Task input claims the prepared handle once. Only the native session initialization and echoed user-message identity bind the input to that session. A lost startup/query response remains unknown; the bridge must not turn uncertainty into another prompt.

The legacy `stream.mjs` mapper is shared with its fixtures:

- Assistant frames may share one message ID while carrying different blocks. Preserve block arrival and native tool IDs; do not deduplicate an entire message by message ID.
- Partial stream events are token deltas, not independent messages or child inventory.
- Child links use native tool IDs and parent_tool_use_id. Completion needs an actual tool result/task notification, not parent idle. Family coverage remains partial.
- Native result subtypes distinguish completion, failure and initialization failure. A correlated terminal-input record requires actual native result/session/user-message evidence. Several merged input UUIDs are ambiguous, not several independently completed Tasks.
- The legacy mapper replaces its cumulative SDK usage estimate rather than summing every result. This estimate is not remaining subscription quota or a billing invoice. Verify the current native usage contract before extending it.
- Existing bounded summaries retain recent message, child and turn metadata and report overflow. They do not copy the whole transcript into Store.

Requested model/permission choices are not proof of their effective application. Read the actual native initialization/settings evidence; no model label in a prompt or an old package manifest establishes the current runtime settings.

## Permission behavior

When no `permissionMode` is supplied, the bridge omits that option. Report this as inherited/requested-unknown until the native effective mode is observed; do not fabricate `default`. Pass an explicit owner-selected mode unchanged. Do not enable bypass or force a different mode to compensate for a missing callback.

`canUseTool` is not a universal interception point for every tool. Native permission evaluation can resolve a call before that callback. In this legacy bridge, calls that do reach the callback are recorded and immediately denied because reply is unavailable. Such recorded denials are **history**, not still-live requests that can be answered later.

The R16 path keeps the real pending callback in the existing Node owner, publishes a scoped request reference, and resolves it once through the current `agent.reply` authority path. Raw input and Promise resolvers remain local. A user answer is not a new prompt; a delivered allow decision is not proof that its tool or Task finished.

## Recovery and updates

A host reconnect to the same live bridge must retain existing ownership and input uncertainty. This legacy bridge has no durable resume/recover implementation; after process loss it cannot recreate a native Promise or prove that an input never ran. Preserve history and the unknown outcome rather than replaying work.

For new native releases, validate the interfaces this adapter actually uses. A missing required function limits that capability; a different release number alone is not a reason to reject a working harness. Correct package loading, descriptors and consumers together, without rewriting old receipts or changing a live session's executable.

[UPDATE.md](UPDATE.md) describes the corrected update boundary. Source review, syntax checks, fixture results and live subscription qualification are separate evidence. No new runtime qualification is claimed here.

## Verification and remaining work

After implementation, the manager performs scoped formatting and the minimal Clippy gate for changed Rust packages, plus JavaScript syntax checks. Broad tests and native execution follow in the project's final qualification phase; writers do not run Cargo.

The existing `selftest.mjs` and authored SDK-shaped fixtures remain available for that phase. They are not live captures and do not qualify a newer runtime merely because an older fixture passed. R16 must verify subscription-route preservation, compatible updates, pending/reply/abort races, reconnect, driver loss and exact input/decision correlation. Further configure/resume/child parity must be based on actual native interfaces, not assumptions about a frozen SDK.
