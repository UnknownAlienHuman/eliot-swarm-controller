# Updating modules/command

A change to the bridge/glue contract is a new module artifact. The current pair is `command-mod-0.1.0-glue.4`; `.2` and `.3` run files remain immutable historical records and the native mod remains `0.1.0`. `.3` snapshots may expose a read-only semantic projection of the saved parsed frames, but they cannot authorize `.4` replay. Do not change a unit under a running binding.

## Change protocol

1. Re-check the official headless and Mods pages named in `vendor.lock`. Update only behavior supported by those surfaces; the ModApi is experimental.
2. Keep `task.dispatch` one-shot and keyed by the caller’s Operation ID. Persist admission before spawn. Bind the exact shared canonical instruction bytes and Operation digest at the controller store, then validate each saved raw NDJSON event/result frame against its semantic projection, `run.json`, event sequence/summary, native request-model evidence, and process exit facts. Reconcile reads saved evidence and uses the controller-injected identity from the original target Operation and frozen Attempt to frame an `Unknown` receipt when needed; it never repeats native input. Missing or conflicting proof stays `Unknown`.
3. Keep the model ID explicit in the route and pass it through as `--model`. `model_request_start.model` and `model_request_end.model` can support only `native_request_model`; never infer an effective model or provider from a requested route, alias, process exit, or model-authored text. Unsupported settings remain unavailable.
4. The module config's `command` is the absolute Node executable and `commandArgs` is the fixed absolute native CLI entrypoint prefix. Never replace that argv boundary with a shell shim or user-controlled command line.
5. Bump the artifact ID in `glue.mjs`, `bridge.mjs` (via the imported constant), `module.example.json`, `vendor.lock`, and the route config. Keep parent admission/run/NDJSON records outside the mod-only `mod/` directory. For new event records, retain the exact native line and its frame format alongside the unwrapped AgentEvent. Scrub `ELIOT_*`, `SWARM_*`, and capture-named environment variables before the native process while preserving ordinary OS home, `PATH`, and vendor auth. Recompute exact bridge/glue and shared IPC transport digests in `vendor.lock`; do not change the mod digest unless the mod changed.
6. Run the module checks once: `npm run check` and `npm test`. Fixture success is not live or controller qualification. The local-file consistency checks do not establish authenticity against arbitrary same-user filesystem writes.

## Rollback

Retain previous artifacts and their control records without migration. `.2` and `.3` records remain read-only history and cannot authorize `.4` work. A run with an admission marker but no validated terminal record remains `Unknown`; rollback or reconcile must not replay its prompt. Restore configuration to a compatible artifact only after confirming the module artifact ID and pinned mod path.
