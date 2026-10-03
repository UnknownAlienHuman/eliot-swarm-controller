# Updating modules/command

A change to the bridge/glue contract is a new module artifact. The current pair is `command-mod-0.1.0-glue.3`; `.2` run files remain immutable historical records and the native mod remains `0.1.0`. Do not change a unit under a running binding.

## Change protocol

1. Re-check the official headless and Mods pages named in `vendor.lock`. Update only behavior supported by those surfaces; the ModApi is experimental.
2. Keep `task.dispatch` one-shot and keyed by the caller’s Operation ID. Persist admission before spawn. Bind the exact shared canonical instruction bytes and Operation digest at the controller store, then validate the saved NDJSON result frame against `run.json`, event sequence/summary, and process exit facts. Reconcile reads saved evidence and uses the controller-injected identity from the original target Operation and frozen Attempt to frame an `Unknown` receipt when needed; it never repeats native input. Missing or conflicting proof stays `Unknown`.
3. Keep the model ID explicit in the route and pass it through as `--model`. Do not infer effective identity from a request, alias, process exit, or model-authored text. Unsupported settings remain unavailable.
4. The module config's `command` is the absolute Node executable and `commandArgs` is the fixed absolute native CLI entrypoint prefix. Never replace that argv boundary with a shell shim or user-controlled command line.
5. Bump the artifact ID in `glue.mjs`, `bridge.mjs` (via the imported constant), `module.example.json`, `vendor.lock`, and the route config. Keep parent admission/run/NDJSON records outside the mod-only `mod/` directory. Scrub `ELIOT_*`, `SWARM_*`, and capture-named environment variables before the native process while preserving ordinary OS home, `PATH`, and vendor auth. Recompute exact bridge/glue and shared IPC transport digests in `vendor.lock`; do not change the mod digest unless the mod changed.
6. Run the module checks once: `npm run check` and `npm test`. Fixture success is not live or controller qualification. The local-file consistency checks do not establish authenticity against arbitrary same-user filesystem writes.

## Rollback

Retain the previous artifact and its control records without migration. `.2` records remain readable as history but cannot authorize `.3` work. A run with an admission marker but no validated terminal record remains `Unknown`; rollback or reconcile must not replay its prompt. Restore configuration to a compatible artifact only after confirming the module artifact ID and pinned mod path.
