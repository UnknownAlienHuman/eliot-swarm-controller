# Updating modules/command

The mod and glue are one pinned unit. A changed unit is a **new module
artifact**; never edit the mod or glue in place for a binding that may still
run, and never let a running process pick up a half-updated pair.

## When the vendor surface moves

1. Re-read the official pages against the source registry ids in
   `vendor.lock` (CC-MODS, CC-HEADLESS, CC-AGENTS). The ModApi is documented
   as experimental: diff the event catalog, `queueMessage`, the tool
   controls, and the headless result-line/exit-code tables before touching
   code.
2. If a documented field the module relies on changes meaning, the affected
   capability drops back to `unknown` in `glue.mjs` `describe` until it is
   re-verified — a schema fingerprint is a reason to compare, not a global
   outage, and unaffected paths stay as they are.
3. Current tool argument names are checked against the live contract, never
   carried over from an older brief (the `background:true` vs
   `run_in_background` discrepancy in the notes is the standing example).

## Making a change

1. Edit `mod/eliot-command.ts` and/or `glue.mjs`; bump `MOD_VERSION` in the
   mod and the artifact id (`command-mod-<mod>-glue.<n>`) in `glue.mjs`,
   `module.example.json`, and any route that names it.
2. Recompute the pins and update `vendor.lock`:
   `sha256sum mod/eliot-command.ts glue.mjs`.
3. Run `npm run check` and `npm test`. The fixture host implements only
   documented loader behavior; a mod change that needs more surface than the
   fixture offers is a sign to re-check the docs, not to enrich the fixture.
4. Only after an installed binary is available: qualify live per README
   ("Remaining work") before calling the new artifact verified.

## Rollback

Keep the previous artifact directory intact. Rollback = point the module
config (and route, once wired) back at the previous artifact id and mod
path. Rollback never replays inbox lines or run records into a new run; a
run whose outcome is unknown stays unknown and is reconciled by reading its
control directory, not by resending its prompt.
