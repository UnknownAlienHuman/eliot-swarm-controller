# Updating modules/command

A change to the bridge/glue contract is a new module artifact. The current pair is `command-mod-0.1.0-glue.2`; the native mod remains `0.1.0`. Do not change a unit under a running binding.

## Change protocol

1. Re-check the official headless and Mods pages named in `vendor.lock`. Update only behavior supported by those surfaces; the ModApi is experimental.
2. Keep `task.dispatch` one-shot and keyed by the caller’s Operation ID. Persist admission before spawn. Reconcile reads the saved run/admission evidence; it never repeats native input. A missing or conflicting terminal fact stays `Unknown`.
3. Keep the model ID explicit in the route and pass it through as `--model`. Do not infer effective identity from a request, alias, process exit, or model-authored text. Unsupported settings remain unavailable.
4. The module config's `command` is the absolute Node executable and `commandArgs` is the fixed absolute native CLI entrypoint prefix. Never replace that argv boundary with a shell shim or user-controlled command line.
5. Bump the artifact ID in `glue.mjs`, `bridge.mjs` (via the imported constant), `module.example.json`, `vendor.lock`, and the route config. Recompute the exact bridge, glue and shared IPC transport digests in `vendor.lock`; do not change the mod digest unless the mod changed.
6. Run the module checks once: `npm run check` and `npm test`. Fixture success is not live or controller qualification.

## Rollback

Retain the previous artifact and its control records. A run with an admission marker but no terminal record remains `Unknown`; rollback or reconcile must not replay its prompt. Restore configuration to a compatible artifact only after confirming the module artifact ID and pinned mod path.
