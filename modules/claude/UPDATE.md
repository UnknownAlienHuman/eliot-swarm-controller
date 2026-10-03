# Claude module — update contract

## Contract sources

- Controller side: `docs/agent_swarm.module-contract-v2.md` (module package, eight operations, readiness words) and `docs/agent_swarm.implementation-v6.md` §11 (C08 row: native stream, complete child messages/content blocks, repeated `message.id`, init failures, no system-prompt update claim on resume).
- Native side: the pinned package's own types, `@anthropic-ai/claude-agent-sdk` **0.3.287** `sdk.d.ts` (`query`, `Options.model`, `SDKMessage` family), and the runtime matrix sources CL-HEADLESS / CL-STREAM / CL-INPUT / CL-HOOKS / CL-CHILDREN in `docs/agent_swarm.runtime-sources-v16.json`. The SDK is under Anthropic Commercial Terms; it is a used external package, not vendored source.
- Stream fixtures under `fixtures/` are authored from those types. They are protocol-shape evidence for the mapper, not live captures and not a substitute for qualifying the installed runtime.

## What may change here

| File | Change rule |
| --- | --- |
| `bridge.mjs` | Facade only: host commands, explicit route model passed to SDK startup, prepared-session ownership, exact Task input prompt/UUID receipts and outcome classification. No mapper rules. |
| `stream.mjs` / `model-selection.mjs` | SDK-message → observation mapping, compact uniquely correlated terminal input receipts, and pure route-model selection/projection. Every mapping change needs a fixture that proves the boundary it alters. |
| `control.mjs` | Host IPC link; changes must stay wire-compatible with the host's module protocol and the Muse module's link behavior. |
| `fixtures/`, `selftest.mjs` | Add fixtures before relying on new native message shapes; never edit a fixture to match a mapper regression. |
| `package.json` / `package-lock.json` | SDK version changes only as one deliberate pin update (below). No `@latest`, no global installs. |
| `module.example.json`, `README.md` | Artifact id and capability matrix must match the shipped code exactly. |

## Updating the SDK pin

1. Choose the exact new SDK version; the matching native binary packages (`@anthropic-ai/claude-agent-sdk-<platform>`) carry the same version and update with it — never mix versions.
2. Update `package.json`, regenerate `package-lock.json` with `npm install --ignore-scripts`, and diff the new `sdk.d.ts` for the operations this adapter uses (`query`, streaming input, init/result shapes, `parent_tool_use_id`, `user_message_uuid` stamping, `canUseTool`, `forwardSubagentText`). A changed mandatory field makes only the affected capability unconfirmed, not the whole module.
3. Bump the artifact id (`claude-agent-sdk-<version>-bridge.N`) whenever protocol-visible behavior changes; the current slice is `claude-agent-sdk-0.3.287-bridge.3`. It prepares a rootless SDK executor at open, claims the one-shot `WarmQuery` for the first exact Task input, adopts only the later observed `system/init.session_id` plus echoed native user UUID, and records a compact terminal input projection. Update it in `module.example.json`, this file, the module README and the disabled route in `config/controller.example.toml` together.
4. Run the verification below. A new artifact never edits a running bridge in place.

## Verification through the real adapter

- `for file in modules/claude/*.mjs; do node --check "$file"; done`
- `npm ci --ignore-scripts --no-audit --no-fund` in this directory, then `node selftest.mjs` (pinned import surface + all fixture assertions).
- Host smoke: start the host, reserve `agent.open` on a disabled-by-default private route, register the module credential, launch through `swarm module-run`, and confirm `module.hello` acceptance and a `describe` observation in `agent.state`. A model call is live qualification, not part of this check, and is recorded separately when performed.

## Activation and rollback

- Activation is per binding: new bindings reserve the new artifact id through their route; live bindings stay on the artifact they opened with. There is no hot swap of bridge code or SDK heap under a running session.
- Rollback is the inverse: point new routes back to the previous artifact id and let existing bindings finish or be released by the operator. Rolling back the binary never rolls back native effects already performed by a session.
