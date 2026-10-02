# Direct OpenCode V2 — `eliot-opencode-v2.http.1`

Built-in Rust adapter for an **already running, externally owned** OpenCode V2 HTTP service. No Node bridge, CLI invocation, process launch, service restart, hidden inference fallback or extra task store. Disabling the route does not terminate native work.

## Contract and exact scope

Implements the HTTP/inbox/readback part of C04: native session creation, immutable Task delivery, next-turn input, family snapshots, addressed form/permission replies, refresh, read-only reconciliation, scoped projected-result export and native per-turn patch export. One connection pool and volatile SSE reader serve bindings with the same configured `service_id`. Aliases must share that namespace, connection file and exact version. Do not assign different service IDs/files to the same service: physical endpoint aliases are not automatically discovered.

Canonical requirements: [architecture §8](../../docs/agent_swarm.md), [implementation C04](../../docs/agent_swarm.implementation-v6.md), [module contract §§4–7](../../docs/agent_swarm.module-contract-v2.md). Wire contract reviewed against the [official V2 API](https://opencode.ai/v2/docs/api) and [OpenAPI document](https://opencode.ai/v2/openapi.json), captured 2026-10-01. These documents move; they are not proof that a particular installed server has been qualified.

## Explicit configuration

Keep the shipped route disabled until these values are filled for the intended installation. Use absolute native paths for `connection_file` and `directory`, an exact `/api/info` version, and the exact provider/model/**variant** from the installed model catalog. The adapter refuses missing/disabled variants and verifies resolved location and native settings; it never silently downgrades reasoning or changes provider. A fallback to another workspace is rejected. Native path normalization is accepted only for the same local directory; a controller-created root may not become a child or fork.

```toml
[[routes]]
alias = 'opencode-manager'
runtime = 'opencode_v2'
module_artifact_id = 'eliot-opencode-v2.http.1'
enabled = false
[routes.native_options]
service_id = 'my-local-opencode-store'
connection_file = 'C:\SwarmPrivate\opencode-connection.json'
expected_version = 'REPLACE_WITH_EXACT_INSTALLED_VERSION'
directory = 'C:\Projects\YourRepository'
[routes.native_options.model]
id = 'REPLACE_WITH_NATIVE_MODEL_ID'
providerID = 'REPLACE_WITH_NATIVE_PROVIDER_ID'
variant = 'REPLACE_WITH_EXACT_VARIANT'
```

The connection file is **ELIOT's own explicit record**, not an invented interpretation of OpenCode's `service.json`. Populate it from the service owner's known configuration. It is a bounded regular JSON file; links and pipes are rejected. Store it outside source control and restrict its OS permissions to the intended user.

```json
{
  "schema_version": 1,
  "endpoint": "http://127.0.0.1:12345",
  "pid": 1234,
  "username": "REPLACE_WITH_CONFIGURED_BASIC_USER",
  "password": "REPLACE_WITH_CONFIGURED_BASIC_PASSWORD"
}
```

Port, PID and credentials above are placeholders, not defaults. Credentials are read into the HTTP client, not copied into bindings, operations, Task text or diagnostic errors. Only loopback HTTP origins are accepted; redirects, proxies, URL credentials and automatic HTTP retries are disabled. `/api/info` must match the recorded PID and selected version. This is a same-user trusted-service boundary, **not** an OS sandbox or cryptographic proof that a process with a recycled PID has the same identity. The stable `service_id` must name the same native store across an operator-managed restart.

No native connection occurs merely to list routes or read host status. After explicit `agent.open` admission, the host attaches its built-in worker. No `client.register` module token or `module-run` process is needed for this adapter; those are Muse bridge setup steps. Existing controller credentials and request IDs still apply. Read an operation's result before using its returned binding/generation.

## Delivery, observation and recovery

A native session has a deterministic controller-owned ID plus binding/generation/operation metadata. Initial creation verifies the model and location. Task dispatch includes the frozen Task snapshot. Each admitted input has a deterministic native message ID. **A prompt response confirms inbox admission, not an executed turn or finished Task.** The retained producer has `native_input_id`, `admission_kind=native_inbox`, `disposition=admitted`; it does not acquire a fabricated turn/run ID.

Lost creation and prompt replies become `outcome_unknown`. Readback checks the exact owned session, queued inbox or delivered user-message ID and original content/metadata, rejecting additional native attachments. It never repeats the POST, including after host restart or while new-work admission is disabled. Unproven outcomes remain unknown. A changed service, model, binding owner or foreign child cannot silently receive the command.

`agent.send` currently supports `delivery=next_turn` only. Exact-turn steer is rejected: a preflight read followed by a steer without an atomic expected-turn guard would still race. Forms and permissions require the exact native request ID, the current pending-body fingerprint and verified ancestry under the binding's owned root. A native answer ACK is not Task acceptance.

The shared SSE reader uses the complete `eventsource-stream` crate. Events invalidate a compact view; they do not become replayable history or terminal evidence. Disconnects, malformed events, native stream-failure envelopes and bounded stream recycling retain a gap. Reconnect issues GET only. There is **no Last-Event-ID replay guarantee**. Notifications are coalesced; periodic bounded GET readback supplements the volatile stream.

Family enumeration uses parent-filtered pagination and bounded ancestry checks. Read failures retain previously observed children as stale rather than deleting them. Root inactivity is not family idleness; unknown active-map or pagination schemas are explicit gaps. Even a completed enumeration is non-atomic and retains `family_completeness=partial`. Native question bodies are scrubbed with the complete pinned Atlas redaction donor before persistence; identities and content fingerprints remain outside the scrubbed payload. Pattern detection is not a guarantee that every possible secret format is recognized.

Per-response body cap: 4 MiB; connection file: 64 KiB; family: 256 retained sessions; pending requests: 128; one family readback: 20 seconds. SSE connections recycle after at most 16 MiB of input, including an unterminated frame. These limits bound observation, never terminate a native agent or imply success. Reaching a bound records incomplete coverage.

## Read-only results

`agent.result` uses the existing `swarm result` → `operation` → `artifact assemble/export` path. The selector file supports an exact completed assistant message (root or a child with a verified parent chain):

```json
{"kind":"message","session_id":"ses_EXACT_NATIVE_ID","message_id":"msg_EXACT_NATIVE_ID"}
```

or a projected interval anchored to an already-sent input Operation on this exact root/binding/generation:

```json
{"kind":"input_interval","session_id":"ses_EXACT_NATIVE_ROOT","input_operation_id":"EXACT_DISPATCH_OR_SEND_OPERATION_ID"}
```

or the native per-file patches for the same input's isolated, closed projected turn:

```json
{"kind":"turn_diff","session_id":"ses_EXACT_NATIVE_ROOT","input_operation_id":"EXACT_DISPATCH_OR_SEND_OPERATION_ID"}
```

```powershell
swarm --data-dir C:\SwarmState --request-id result-page-0 result BINDING_ID --generation 1 --file selector.json --offset 0 --length 65536
swarm --data-dir C:\SwarmState operation RESULT_OPERATION_ID
```

The message body is canonical native JSON, preserving text, reasoning and tool/result references as **data**; references never trigger URL or filesystem reads. Completed tool states are required. Intervals use unfiltered ordered pagination to find the exact user ID and its first subsequent idle message, not the latest assistant or matching prompt text. Original text/metadata must match; an intervening additional input, compaction or settings switch is rejected. The root's selected model must match every included assistant. Forks, staged reverts, duplicate IDs/cursors, changed bodies and incomplete reads are not silently accepted. The body is read twice and scope checked before/after; this detects observed changes, not an atomic native snapshot.

**None of these selectors proves native run completion.** V2's projected idle message has an outcome but no input/run reference. Source metadata retains `correlation=projected_order_only` for intervals/diffs and `execution_complete=false`, `family_complete=false` for every kind. No producer disposition, native turn ID, Task acceptance or release is synthesized.

`turn_diff` calls `GET /api/session/{sessionID}/diff` with **both** `from` and `to` set to the original input's exact native ID. The native range starts at the first user after the preceding idle marker, so the reader also scans backward to that boundary (or the beginning of history) and refuses another user in the same turn. Missing first/last assistant snapshot endpoints report `RESULT_SNAPSHOT_UNAVAILABLE`, not an empty successful diff. Completed assistants and the closed interval exclude the documented active-step working-copy fallback. The diff is read twice, then the input interval and binding scope are rechecked. A changed source, duplicate file path, truncated response or unknown diff schema remains unresolved.

The exported JSON contains `session_id`, `native_input_id`, both snapshot IDs, the interval digest and native `files` entries (`file`, `patch`, `additions`, `deletions`, `status`). It preserves the full-file patch context returned by the native default. Source metadata records both snapshot IDs, the interval digest and file count. Patches and paths remain **data**: no patch application, arbitrary URI/file download or live-checkout verification occurs. This is not a captured CheckRunner source; binary contents are not included. The native turn-range and working-copy behavior were reviewed against [OpenCode source `4c0d0ff4`, `session/diff.ts`](https://github.com/anomalyco/opencode/blob/4c0d0ff478ca9150c163fb8b04a76395e4dccafe/packages/core/src/session/diff.ts) and the 2026-10-01 OpenAPI capture, not a live installed service.

Successful Operations return `result.details.artifact_ref`, `source.content_digest` and `next_offset_bytes`. Optional `expected_digest: "sha256:<64 hex digits>"` pins the complete canonical source body. Use the identical selector for all pages; after an unpinned discovery read, re-read page zero with the pinned selector before assembly. Every page and the assembled body are hashed. Native JSON stays in private artifact files, not in SQLite telemetry.

Unfinished reads recover with GET only, at most one background retry per binding per five seconds, rotating past unresolved reads; queued commands have priority. Explicit `agent.reconcile` also retries the exact saved read. Publication uses the existing no-overwrite file/SQLite boundary. An orphan file can be registered after the same source is read again; changed or missing native data stays unresolved rather than overwriting the orphan or replaying a prompt. A settled request ID returns its saved receipt.

Each timeline pass is bounded to 32 pages of at most 50 messages and 8 MiB of scanned JSON; one result read has a 20-second deadline and publishes at most 64 KiB. Large/old/unavailable results report their limit rather than partial success. Native turn-diff responses also obey the 4 MiB per-response cap. Detached binary/tool-output reference retrieval remains unsupported.

## Remaining C04 work — do not declare end-to-end completion

Exact input-to-native-run/terminal correlation, detached binary/tool-output retrieval, configuration/goal controls and complete family reconstruction are not implemented by this artifact. An admitted inbox producer remains unresolved and blocks release until genuine native disposition can be established; parent idle or the latest assistant outcome cannot discharge it. Unsupported methods report an explicit capability error.

Live installed OpenCode, actual inference/subscription behavior and native Windows service interoperability remain unqualified. This adapter must not be presented as a fully qualified automatic Task-completion route yet. It does not require or install the optional OpenCodex provider proxy (Issue #1).

## Focused implementation evidence

The projected-result baseline `e88a0fb7` passed [Windows/Linux CI 36954512403](https://github.com/UnknownAlienHuman/eliot-swarm-controller/actions/runs/36954512403). The subsequent turn-diff reader passed local Rust 1.98.1 owned-crate formatting and locked offline lib/bin Clippy with warnings denied. Its exact Windows/Linux CI must be checked separately. No tests or live native calls were run for this addition; dependencies, migrations and the Atlas donor are unchanged.

The interrupted 2026-10-01 implementation retained 16 protocol/Store fixtures. They were preserved, not rerun during source recovery. Fresh Rust 1.98.1 package formatting and minimal warnings-denied Clippy passed; the Windows/Linux workflow checks the exact commit's formatting, Clippy, donor hashes, release build and existing Muse SDK import. Tests remain deferred while the product code is being completed. Fixture HTTP servers are not OpenCode, and compilation is not live service, model, billing or subscription qualification.
