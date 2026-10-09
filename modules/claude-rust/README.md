# Claude Rust module adapter

This source binds the Rust controller to the generic selected-module
path. The descriptor is disabled. The route example is inert and contains only
operator-supplied local paths and model selection; it does not change the active
Claude configuration or install, register, or launch the artifact.

Install the built `swarm-adapter-claude` executable with the repository's
standard `Install-ModuleArtifact.ps1` and this descriptor template. This is
artifact version 5; versions 2, 3, and 4 remain separate retained artifacts.
The Rust binary embeds the pinned `bridge.mjs` and `prepared-query.mjs` bytes and writes
them to the binding owner's private state directory on demand. Its typed
`module_host_config_path` launch argument is preserved by the generic installer
and resolved by the host at launch to a private, binding-scoped connection file
containing only the configured host data directory and IPC bounds. No path
placeholder or operator-supplied IPC root is required.
The configured `sdkRuntimeRoot` must already contain the pinned
`@anthropic-ai/claude-agent-sdk` 0.3.287 installation; `nodeExecutable` and the
Claude config directory remain explicit operator choices.

The adapter reads native options from the authenticated `module.hello` route,
validates them once, then requires every command and reconnect to carry the same
route. The Store authorizes rootless open only through the descriptor's typed
`pre_input_open` contract. It adopts a native identity only after the first
`task.dispatch` returns the exact SDK frame echo and prompt/snapshot digests.
No descriptor capability grants Store or provider authority.

`claude-native-options.schema.json` documents the route option shape. The
descriptor intentionally keeps `config_schema` null; Store admission does not
interpret this file, and the adapter validates the options returned by the
authenticated route.

The Rust adapter owns operation matching, durable pre-effect intent, receipts,
identity adoption, and bounded readback. The pinned JavaScript shim remains the
SDK transport and `WarmQuery` driver. This is therefore a staged controller
migration, not a full Rust implementation of the vendor SDK.

Version 4 added `agent.refresh` as an exact-session read of the Rust adapter's
bounded SDK metadata cache. This private continuation also retains the pinned
SDK's `task_started`, `task_progress`, `task_notification`, and
`task_updated` frames, `SubagentStart` and `SubagentStop` hook observations,
and assistant/user frames carrying a parent tool link. It preserves only
bounded identity and link metadata: the raw task and agent IDs, tool and
parent-tool IDs, frame and prompt IDs, user-message UUID links, and the
notification `resource_links` URI/name metadata. The cache keeps at most 128
family events; refresh returns the newest 16 family events and 16 input/result
records with truncation and projection-incomplete flags.

The outcome reports `family_completeness: partial` and
`enumeration_complete: false`.
The SDK metadata stream does not enumerate every native process or durable
family member. Refresh records this snapshot; it does not establish Task or
native turn completion. An absent cache is reported as unavailable.

Version 5 declares `swarm.task_prompt@1` and `agent.reply`. An initial
`task.dispatch` requires the selected TaskPrompt envelope and matching
Store-supplied task identity; the adapter validates its prompt digest and byte
length, then sends the prompt unchanged as native input. Permission and
`AskUserQuestion` callbacks remain pending until an exact `agent.reply` is
persisted and acknowledged by the SDK bridge. A callback acknowledgement records
that reply delivery; it does not claim native turn or task completion. Retention
compacts only exact acknowledged terminal outcomes and preserves unknown,
deferred, referenced, and damaged evidence.

Installation, registration, route selection, native SDK startup, and model
requests remain separate operator actions for this source candidate.
