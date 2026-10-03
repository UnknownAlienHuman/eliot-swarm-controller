# Antigravity bridge — update and rollback

## Pins

| Fact | Value |
|---|---|
| Entrypoint | `native_warm_stream` (runtime matrix v16, runtime `antigravity`) |
| Protocol basis | Official vendor docs: `https://antigravity.google/docs/cli/headless/` (AG-HEADLESS) and `https://antigravity.google/docs/subagents/` (AG-CHILDREN), accessed 2026-09-29 by the repo audit and re-read 2026-10-02 for this artifact |
| Vendor SDK | None exists for this entrypoint; the bridge depends on Node builtins only (`package.json` has no dependencies) |
| Native binary | The owner's installed `agy` executable, named explicitly in the local module config; the bridge installs and pins no binary |
| Installed-version evidence | None: runtime matrix records `installed_runtime_verified: false`; the docs' "installed 1.1.27" note from the old research map is explicitly unverified |
| Bridge artifact | `antigravity-cli-warm-bridge.2` |
| Executor version reporting | `null` — the native stream reports no version and no version readback is documented for this entrypoint |

## Local changes to the vendor surface

None possible and none made: there is no donor unit to vendor. The whole
protocol mapping lives in ELIOT-owned `codec.mjs`; `bridge.mjs` owns the
process and the host link. If a future docs revision moves event names or
payload fields, the adaptation is re-derived in `codec.mjs` and the
fixtures are re-authored from the new documentation examples — never by
guessing from a live stream the qualification has not recorded.

## bridge.2 changes

Terminal warm results now have an adapter-local receipt bound to the exact
operation, native conversation, bridge boot, monotonically increasing
per-boot result ordinal, and SHA-256 of the exact UTF-8 response. The bridge
records the matching observation before sending the Operation outcome and
includes the acknowledged observation ID. It does not invent a native turn
or inbox ID. Only `SUCCESS`, `ERROR`, `CANCELED`, and `INTERRUPTED` settle a
result; `WAITING`, `RUNNING`, unknown statuses, and missing response bytes
remain unresolved. Existing bridge.1 bindings retain their original artifact.

## Update procedure

1. Re-read the two basis pages deliberately; diff the documented event
   envelope (`init` / `step_update` / `result` fields, status values,
   input-message validation table) against `codec.mjs` mappings.
2. Re-author or extend the affected fixtures from the new documentation
   examples, keeping each file's provenance comment accurate, and re-run
   `node selftest.mjs`.
3. Bump the artifact id (`bridge.2`, …) and the route's
   `module_artifact_id`; existing bindings stay on the previous artifact.
4. Activate on a new binding and qualify live (below) before any manager
   role is assigned.

## Rollback

Disable the new route and start the previous artifact on a new binding;
conversations belong to the native CLI store, not to the bridge, so no
bridge-side data migration exists. Never overwrite bridge files in place
under a live binding.

## Live qualification still owed (not attestable from fixtures)

- The installed `agy` version on the owner's machine and its exact stream
  behavior, including whether `init` precedes the first prompt on the
  installed build (the docs state the stream opens with `init`).
- Exact conversation resume (`--conversation`) against a real recorded
  conversation, including which prior state is actually restored.
- Soft-deny evidence on a real run: the `tool_info.error` shape and the
  stderr notice for a tool denied in headless mode.
- Real subagent `step_update` coverage: which child facts the installed
  build emits, and peer-message/idle-wake behavior.
- Native question delivery to a waiting conversation and the completeness
  of the observed family tree (audit R16 open points).
