# R20 current consumer inventory

Source review on 2026-10-10, `main` commit
`3d9dd2ffb283bd45d5d322f57a46accf8a7da917`. This inventories production
callers; it does not establish native model consumption or close R20 / PR #46.
The duplicate `source_refs` admission and historical first-occurrence projection
repairs are already included in this commit.

The subsequent source migration selects exact Store-produced TaskPrompt bytes
for the built-in OpenCode `.2` artifact and retires new `.1` and trusted Rust
Command BatchV3 bindings. Their historical decoders remain for retained
Operations. The current boundaries are recorded below; native qualification
and fixture execution remain separate acceptance items.

## Store producer and projection

`store/task_prompt.rs::build` renders the frozen Attempt brief once and retains
its Task/Attempt/revision identity, snapshot digest, exact UTF-8 prompt digest
and byte count. `load` validates retained bytes against that frozen input.
`store/operations.rs::dispatch` owns the first effect Operation's envelope.

`store/runtime.rs` loads this envelope for an opted-in descriptor and removes
`task_snapshot` and `task_snapshot_canonical` from the native command. Its other
branch still enriches a command with the raw snapshot. This branch is required
by the consumers below and cannot yet be deleted. Trusted descriptor selection
for a new module binding, established by #116, does not by itself mean that
the selected descriptor declares `swarm.task_prompt@1`.

## Consumers of the retained prompt bytes

| Consumer | Current artifact / contract | Production boundary |
| --- | --- | --- |
| Rust Codex | `codex-rust-controller.1`, version `5` | `swarm-adapter-codex::prompt_for` requires selected TaskPrompt and rejects raw snapshot fields. Native send uses the returned prompt. |
| Rust OpenCode | `eliot-opencode-v2.rust-http.1`, version `0.5.0` | `task_prompt_for` requires the exact current descriptor and checks envelope/context identity and digest. Native preflight receives an `agent.send` projection of those bytes and must return identical text. |
| Rust Claude | `claude-agent-sdk-rust-controller.5`, version `5` | Module bootstrap requires the TaskPrompt command schema. `selected_task_prompt` validates the first Task input; dispatch uses `envelope.prompt`. |
| Rust Antigravity | `eliot-antigravity.rust-headless.1`, version `5` | `modules/antigravity-rust/src/wire.rs` validates envelope/context and rejects raw snapshot fields. |
| Muse | `muse-sdk-1.3.0-bridge.9`, version `9` | `modules/muse/bridge.mjs` requires the selected schema and validates exact bytes/context before native input. |
| Command glue | `command-mod-0.1.0-glue.5`, version `5` | `modules/command/glue.mjs` rejects raw snapshot fields and checks the envelope before native prompt and admission. |
| Rust Command | `eliot-command.rust-headless.1`, version `4`; ACP profile | `adapter.rs` uses `acp_prompt::prepare` for BatchV4. The ACP path also consumes that validated envelope. BatchV3 remains a separate raw-snapshot path. |
| Built-in Zed | `eliot-zed.eval-cli.2` | Store explicitly selects TaskPrompt for this exact built-in route. Zed dispatch validates it and publishes the corresponding receipt. |
| Built-in OpenCode | `opencode_v2` / `eliot-opencode-v2.http.2` | Store selects the exact envelope and dispatch context. The effect and readback validate and preserve its UTF-8 bytes and typed admission receipt; invalid or missing envelopes have no raw-snapshot fallback. |

These are source facts. Their native receipt/model qualification remains a
separate acceptance item.

## Raw-snapshot renderers and deletion conditions

| Renderer / late enrichment | Live caller or retained contract | Disposition and removal condition |
| --- | --- | --- |
| `runtime/opencode_v2/effects.rs::legacy_prompt` | Built-in `opencode_v2` / `eliot-opencode-v2.http.1` | Historical decoder. New `.1` bindings are rejected; `.2` uses TaskPrompt. Remove only after retained `.1` bindings and unresolved Operations no longer need exact old prompt/readback identity. |
| `swarm-adapter-command::native::prompt_for` | `adapter.rs` BatchV3 branch, artifact `eliot-command.rust-headless.1`, version `3` | Historical decoder. New bindings with this exact trusted version are rejected. V4 and ACP retain their TaskPrompt paths. Remove the renderer only after retained V3 bindings and Operations are drained. |
| `runtime/batch.rs::instruction` fallback | Command receipt/reconcile paths when input has no TaskPrompt | Keep exact historical prompt identity for old Operations. Delete only after every live caller uses a retained envelope and no retained raw operation requires reconciliation. |
| `runtime/codex.rs::dispatch_instruction`; `modules/codex/controller.py` | `codex-sdk-18194bf-bridge.3` | Named retained SDK bridge. `require_new_binding` rejects this artifact. Delete together after its retained bindings, input receipts and unresolved Operations no longer require readback. The frozen vendor donor is not a migrated Rust consumer. |
| `runtime/prepared.rs` legacy receipt renderers; `modules/claude/bridge.mjs` | `claude-agent-sdk-0.3.287-bridge.3` | Named retained prepared-executor contract. New binding is rejected. Keep until exact first-input identity and retained reconciliation no longer need its old receipt format. |
| `swarm-adapter-claude::input_text` first-dispatch branch | Internal unselected-claim path; the current executable bootstrap requires TaskPrompt | No supported current `.5` descriptor reaches it. Remove together with its legacy/internal fixtures after confirming no retained caller depends on it; absence from the shipped descriptor is not native qualification. |
| `modules/antigravity/bridge.mjs` | `antigravity-cli-warm-bridge.2` | Named retained JavaScript bridge. New binding is rejected. Delete after retained conversation/result readback drains, preserving immutable old receipts. |
| `swarm-adapter-opencode::native::prompt` Task branch | Public native helper; current adapter passes a validated TaskPrompt as an `agent.send` preflight command | No current standalone Task dispatch calls this branch directly. Audit library/test callers before removing it; built-in OpenCode uses its own renderer above. |
| `store/runtime.rs` raw enrichment | Unselected descriptor / legacy built-in branches above | Delete only after their current admission and retained readback boundaries are resolved. Selected descriptors already receive exact Store bytes. |

OpenCodex provider routing uses the current Rust Codex consumer; no independent
Task snapshot renderer was found in `modules/opencodex`.

## Remaining acceptance

1. Execute the migration fixtures for current built-in OpenCode and retained
   Rust Command BatchV3 boundaries.
2. Prove the migration boundary for earlier versions under stable artifact IDs;
   exact descriptor identity alone is not an artifact-version retirement rule.
3. Remove unreachable local renderers without rewriting retained prompt or
   receipt identity. Keep the named historical decoders while needed.
4. Compile the affected targets, run scoped Clippy, then execute the meaningful
   prompt/admission/reconciliation fixtures in the test phase.
5. Qualify exact native prompt consumption separately from source review.
