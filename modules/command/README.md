# Command Code module — sessionless batch bridge

The current Command bridge artifact is **`command-mod-0.1.0-glue.5`**. Its trusted descriptor template is [`module-descriptor-glue.5.template.json`](module-descriptor-glue.5.template.json). The descriptor is disabled by default; an installer or supervisor must register and select this exact descriptor for new bindings. Existing `.4` bindings remain historical and retain their schema-2 readback path. `.2` and `.3` evidence remains read-only.

The owned module launcher supplies the bounded descriptor-derived `ELIOT_SWARM_MODULE_CONTRACT` claim. The bridge requires the managed-owner record, accepts only the exact `.5` claim and sends it in `module.hello` as `module_contract`. Before accepting work, it checks that Store negotiated the same registered descriptor, protocol, capabilities, schemas, and route artifact. The bridge never registers or selects a descriptor.

Configure the installed Node executable and absolute native CLI entrypoint in a private copy of [`module.example.json`](module.example.json), along with the host endpoint, credential, and isolated `controlRoot`. The route uses `runtime = "command"`, artifact `.glue.5`, `native_options.modelId = 'stealth/space-bunny-alpha'`, and an absolute `workspaceRoot`. The fixed `commandArgs` prefix cannot contain glue-owned flags or a shell wrapper.

## Operation boundary

| Host operation | Behavior |
|---|---|
| `agent.open` | Read-only executor preflight: probes `--version`, hashes the pinned mod, and validates the workspace. It creates no Command session and proves neither account access nor model execution. |
| `task.dispatch` | One Store-supplied TaskPrompt, one `cmd -p` child, one terminal result. Before any native effect, the bridge checks the exact envelope and context fields, task/revision/attempt/snapshot identity, source-text digest and byte count, prompt digest and UTF-8 byte count, worker/binding/Operation identity, and the Store-supplied `command_core_binding`. It passes `task_prompt.prompt` unchanged. Snapshot fields and local prompt rendering are rejected; there is no fallback. |
| `agent.reconcile` | Reads saved evidence only and never resends input. Current `.5` admission/run evidence uses schema 3 and retains the TaskPrompt contract revision, task identity, prompt digest/bytes, and dispatch context. Valid `.4` schema-2 records remain readable as historical evidence; they cannot authorize a new `.5` dispatch. |
| `agent.refresh` | Reads the sessionless module snapshot. |
| `agent.send`, configuration, goal, attach, resume, steer, reply, recovery, and result pages | Unavailable in this one-shot adapter. The `.5` descriptor lists only implemented operations and the schemas needed for TaskPrompt dispatch; descriptor metadata does not grant method authority. |

The TaskPrompt admission record is persisted before spawn. Its `task_prompt` metadata echoes the exact prompt digest, UTF-8 byte count, task revision, attempt, task and snapshot digest, alongside `task_prompt_contract_revision = "task-prompt-v1"`. The run record and Operation outcome echo the same identity. On a validated successful terminal, the outcome also carries the shared `dispatch_admission` receipt; every versioned outcome carries its exact `module_receipt`. Prompt text itself is not copied into the admission metadata. Admission without a valid terminal record remains `Unknown`; reconcile never retries the native input.

The bridge retains exact native NDJSON lines with their frame format, validates saved projections and terminal process facts, and reports only `native_request_model` when Command emits matching request events. `effective_model` remains unknown. Requested model, process exit, or model-authored text never proves the effective provider/model. The native child environment removes `ELIOT_*`, `SWARM_*`, and capture-named variables while preserving ordinary OS and vendor authentication variables.

## Local inspection

```powershell
node glue.mjs describe --config C:\SwarmConfig\command.json
node glue.mjs snapshot --control-dir C:\SwarmState\command-runs\op-<digest>
```

The standalone `glue.mjs open` path is disabled in `.5`: native dispatch is admitted only from the authenticated Store bridge. Saved-file checks establish local consistency and are not tamper-proof against arbitrary writes by the same user who owns `controlRoot`.

No Cargo commands or test suites were run for this cutover. Native `.5` qualification remains pending. The existing `vendor.lock` is outside this slice’s write scope and still identifies `.4`; refresh its artifact and unit digests before packaging or activating the descriptor.
