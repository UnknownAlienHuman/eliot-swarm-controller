# Owner-machine qualification, 2026-10-03

This record separates native execution, fixture checks and Task acceptance.
The measured R2 launcher executable has SHA-256
`5893e3184f499b1b29841562b086f7519f43c40f35719802ba27e706708125bc`.
It was an integration build from `3ecdf527`, before the later prepared-Claude,
Antigravity local receipt and OpenCode persistence guards. A later build must
name its own source and executable rather than inherit this checksum.

## Codex and OpenCodex

The installed native Codex app-server **0.159.0** used a fresh private home,
SQLite directory, workspace and authenticated loopback WebSocket endpoint.
Its private external-managed ChatGPT token profile had no API key and no
usable refresh token. Account and complete model-catalog reads confirmed
ChatGPT **Pro** and the exact visible model `gpt-6-luna` before the request.
The `openai` provider pointed to a separate, explicitly owned OpenCodex
**2.64.0** instance. All native Codex injection, integration, autostart and
automatic restoration options were disabled in that proxy's private profile.
User-level routing uses the documented built-in provider base-URL override.
([OpenAI configuration reference](https://learn.chatgpt.com/docs/config-file/config-reference))

One Task input was sent. An early history read produced an Unknown admission;
after native completion, an explicit reconciliation read the same input and
turn and settled the original Operation without resending it. Exact native
history contained one user message and one assistant message, the requested
marker, and no tools or unknown item types. The user item's client ID matched
the dispatch Operation; prompt SHA-256 and all 1,316 UTF-8 bytes matched the
frozen Task prompt. A recorded terminal observation completed its producer.
The Task remained open and was not accepted by the native result.

Idempotent open/dispatch requests returned their retained receipts; changed
payloads under the same request IDs were rejected. Read-only Management API
observation saw the owned OpenCodex PID/version, one OpenAI attempt, adapter
`openai-responses` and auth mode `forward`. It reported billing provenance
`chatgpt-plan-via-codex-login`. The proxy did not report a served-model ID or
wire-model ID; those remain unknown. Its version differs from the adapter's
2.75.0 contract baseline, and several newer Management API sections were
unavailable. Those observations do not imply version alignment or full
protocol/configuration qualification. Adapter shutdown detached and did not
touch the service.

Before and after this test, the original Codex auth/config and original
OpenCodex config SHA-256 values matched. The original Codex app processes
remained running. Only the canary's verified module process family and its
empty fixture host were stopped after exact completion; neither the shared
Codex processes nor their service ownership were targeted.

## OpenCode

The direct CLI marker test with **OpenCode 2.0.7** and
`opencode/space-bunny-free` succeeded earlier. That is CLI execution evidence,
not a pass for the controller's HTTP execution contract.

The isolated HTTP-service canary admitted its input but returned a native
assistant error: `provider.auth`, status **403**, empty assistant content.
The marker was present only in its input. The native session became idle;
this is a failed native invocation, not successful Bunny execution.

Its exact execution-log GET returned only `log.synced` at watermark 9 and no
durable event bodies. Version 2.0.7 defaults event persistence to false; its
Bus advances sequence numbers even when event-row persistence is disabled.
The standard `serve` command does not expose an event-persistence switch.
([Bus persistence default](https://github.com/anomalyco/opencode/blob/v2.0.7/packages/core/src/bus.ts#L174-L203),
[event-row writes](https://github.com/anomalyco/opencode/blob/v2.0.7/packages/core/src/bus.ts#L395-L429),
[CLI serve options](https://github.com/anomalyco/opencode/blob/v2.0.7/packages/cli/src/commands/commands.ts#L515-L527))
Neither a synced watermark nor an empty inbox proves execution. The native
inbox removes delivered inputs when they are projected into history.
([Native delivery](https://github.com/anomalyco/opencode/blob/v2.0.7/packages/core/src/session/inbox.ts#L324-L335))
Controller qualification for this standard-service configuration is therefore
unavailable under the durable execution-log contract. No input was retried,
and no API-key/provider fallback was used.

## Local MCP and load

Real authenticated stdio-to-host IPC checks verified tool discovery and
dispatch for observer, local-full and GM profiles: 20, 53 and 48 tools,
respectively. An observer's manual `task.create` request was rejected and
created no Task. This does not qualify the proposed remote gateway.

The 200-client Windows host contour retained exactly all 5,661 admitted
messages with zero admission errors. Loaded `host.status` p95 was about
1.284 seconds and throughput 262.6 durable events/s; the performance targets
were not met. See [the measured contour](host-load-qualification.md).

## Command Code

The R3 launcher SHA-256 was
`a36328c3373f883153bf3ee6471b69314ccecc8b04df90a40c8c5b92ceae9627`.
Its `command-mod-0.1.0-glue.2` route ran installed Command Code **1.74.1**
with the explicit catalog ID `stealth/space-bunny-alpha`. One native input
returned a successful result and the exact 57-byte marker. The native result
text matched the launcher's retained output SHA-256. The canary initially
looked for a snake-case field; read-only recovery verified the original
`result.finalText` field without repeating dispatch, refresh or model input.

Dispatch and refresh settled, the exact Attempt producer completed, and the
Task remained open. The binding stayed sessionless; a per-run native session
ID was retained separately from its controller binding. Requested model and
the native catalog are known; Command's terminal result did not report an
effective served model, so that field remains unknown.

## R4 Codex Bunny and Antigravity

The R4 launcher SHA-256 was
`50e2c8cc113a1368aeba66072dbe41163c26464ef3ac5b7414913388a235090b`.
Warnings-denied Clippy and all **178** Rust library tests passed on its source.
The module fixture gate also passed; the later Antigravity receipt regression
passed separately after its JavaScript repair.

Native Codex **0.159.0** listed exact `opencode-go/space-bunny-free` before
dispatch. The operator-authorized key-backed provider was configured only in
the owned OpenCodex profile, with native injection and automatic integration
disabled. One input returned the exact marker; the original Unknown admission
was later resolved by exact native history, without a resend. Its input UUID,
client ID and 1,316-byte prompt digest matched the Operation. The recorded
producer completed and the Task stayed open. Read-only OpenCodex usage showed
one `opencode-go`/`space-bunny-free` attempt; the earlier subscription OpenAI
attempt count remained one. Proxy usage labels are known, while a separately
reported upstream served-model ID and actual billing amount remain unknown.
The native Codex login remains ChatGPT Pro; that login is distinct from the
key-backed Bunny provider route.

Antigravity **1.2.15**, bridge.2 and `gemini-3.8-flash-high` returned the exact
marker through the R4 launcher. Open, dispatch and refresh settled; repeated
request IDs retained their original receipts and changed payloads were
rejected. The Task remained open, and the owned module family exited with
code 0. The bridge now records detached observation snapshots: a later native
result cannot mutate an earlier acknowledged snapshot or be cited against its
stale observation ID. The earlier failed run and its captured native success
remain separate evidence; they were not relabelled as settled controller work.
