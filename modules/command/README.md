# Command Code module — native mod + headless glue

Connects the installed Command Code CLI (`cmd`) through its selected
entrypoint from the runtime matrix v16: **native mod** over the documented
headless NDJSON transport. New bindings use **`command-mod-0.1.0-glue.1`**.

Status: the mod and glue are implemented and verified against fixtures that
emulate only documented behavior. **No installed Command Code binary has been
qualified with this module** (`installed_runtime_verified: false`): the CLI is
not present on the development machine, there is no account probe, and syntax
or fixture success does not attest model execution.

## Ownership and pieces

| Piece | Role |
|---|---|
| `mod/eliot-command.ts` | The pinned mod. Loaded by the CLI via `--mod`; observes native events, journals them, consumes the glue inbox, and exposes tool readback. |
| `glue.mjs` | Host-side driver. Spawns one headless run (`-p --output-format json --mod <mod>`), streams and classifies its NDJSON, and persists a run record. |
| `fixtures/fake-cmd.mjs` | Fixture executable emulating the documented headless stream, exit codes, and mod loader. Not the vendor binary. |
| `vendor.lock` | Exact source pins and the documented vendor basis. |

The native CLI owns the session, tools, and model loop. The mod is inert
unless the glue sets `ELIOT_COMMAND_CONTROL_DIR` to a per-run control
directory; that directory is the only mod↔glue channel:

- `mod-journal.ndjson` — mod records: `mod_loaded`, `mod_ready` (session
  bound), `native_event`, `queue_admitted`, `tools_readback`,
  `model_requested` / `effort_requested`, `inbox_rejected`,
  `mod_local_error`, `mod_session_end`.
- `inbox.ndjson` — glue→mod commands, one JSON object per line:
  `{id, kind: queue_message | set_active_tools | set_model | set_effort, ...}`.
- `events.ndjson`, `run.json` — written by the glue: the classified native
  stream and the run record.

The mod journal is an observation channel, not a second authoritative queue:
the native loop remains the only owner of queued work, so the journal is
never replayed into a later run.

## Operations (module contract §4)

| Operation | State | Boundary |
|---|---|---|
| `describe` | Implemented | Reports the configured executable, a `--version` probe, the pinned mod digest, and capability readiness. A version string is not a working model session. |
| `open` | Implemented, fixture-verified | One headless run with the mod loaded. Terminal fact = the final result line, corroborated by the documented exit code. |
| `snapshot` | Implemented | Reads one control directory back: scope `single_headless_run`, completeness always `partial`. Never a family or Task statement. |
| `queue_message` (mod inbox) | Admission only | `queueMessage` returns void. The journal records `queue_admitted`; nothing in this module promotes it to applied. Steer lands after the current tool batch, follow-up only when the run would stop — both are native semantics, not glue promises. |
| `set_active_tools` (mod inbox) | Implemented with readback | The mod journals requested vs `getActiveTools()` before/after. An omitted tool list means the empty set in the documented contract; the readback shows what actually took effect. |
| `set_model` / `set_effort` (mod inbox) | Requested only | Buffered setters with no documented readback. Journaled with `applied: "unknown"`; never reported as applied settings. |
| `goal` | **Unavailable** | The documented native goal persists idle after resume, but no goal setter is established for the chosen headless/mod entrypoint. TUI `/goal` proves nothing here. Nothing is emulated silently. |
| `resume` / `attach` | **Unavailable in this slice** | The CLI documents exact-ID resume, but this slice has no readback proof against an installed binary, so the capability is not granted. `--continue` (latest-in-directory) is never substituted for exact identity. |
| `configure` as a host operation | Not yet | Settings changes ride the mod inbox above; a host-level configure Operation arrives with the host IPC wiring below. |

## Evidence boundaries (do not blur these)

- **Mod readiness ≠ process liveness.** `mod_loaded` (factory ran) and
  `mod_ready` (session_start observed) are separate facts in the journal,
  tracked independently of whether the CLI process exists.
- **`mod_error` ≠ process death.** A mod exception is isolated by the host
  and surfaces as a `mod_error` event; the run continues to its own result
  line. The glue records stream `mod_error` events as facts and still waits
  for the run's terminal line.
- **Process exit ≠ success.** Exit without a final result line leaves the
  disposition `unknown`. A result line whose subtype contradicts the exit
  code is recorded with an `exit_result_mismatch` anomaly, keeping both facts.
- **`turn_end` may precede commit.** It is an intermediate event in the
  journal, never the run's terminal fact.
- **Definition refresh ≠ mod reload.** Agent definitions are re-read before
  the next turn; reloading a mod restarts the process. This module performs
  neither silently: a changed mod is a new artifact (see UPDATE.md), and no
  definition surface is claimed.
- **Usage is raw.** The result line's totals are reported as emitted;
  run/round figures are never added together.
- **Actual tool argument names are not assumed.** The glue never constructs
  native tool calls; the only tool surface is the documented name list of
  `getActiveTools`/`setActiveTools`.

## Setup and use

1. Install Command Code natively (operator's own authorized account). This
   module installs nothing globally and downloads nothing.
2. Copy `module.example.json` outside Git and set the real installed
   executable path, a control root, and the workspace `cwd`.
3. Run the glue directly (fixture/development path):

```powershell
node glue.mjs describe --config C:\SwarmConfig\command.json
node glue.mjs open --config C:\SwarmConfig\command.json --prompt "read the readme" --control-dir C:\SwarmState\command-runs\run-1
node glue.mjs snapshot --control-dir C:\SwarmState\command-runs\run-1
```

4. To steer a live run, append inbox lines to its control directory while it
   runs; correlate outcomes by `id` in `mod-journal.ndjson`.

## Fixture verification

```powershell
npm test        # node test-glue.mjs — 9 checks
npm run check   # node --check on glue, fixture, and tests
```

The fixture executable loads the **real** pinned mod through a stub host that
implements only the documented ModApi behavior, so the tests cover the mod's
journal, queue admission, tools readback, and the glue's disposition rules
(success / auth error / max turns / mod error / death without a result line /
missing session id). Recorded streams are synthetic constructions from the
documented shapes, labeled as such — not captured live traffic.

## Remaining work

- Live qualification on an installed Command Code binary: mod load with no
  warnings (`cmd mods list`), a real headless run, exact-ID resume probe, and
  the mod lifecycle (reload = process restart). External dependency: the
  vendor CLI and an authorized account on the owner's machine.
- Host wiring: register the `command` route's native options in the Rust
  host and put this facade behind the controller's RuntimePort / module IPC
  (the disabled example route in `config/controller.example.toml` is inert
  until then).
- The earlier migration bundle (`eliot-swarm-control` lanes, donor inventory
  `existing-control-bundle`) remains a separate source unit; this module does
  not absorb it, and its file-CLAIM authority and polling/kill loops stay
  excluded per the donor record.
- CI: no workflow step checks this module yet (the Rust workflow syntax-checks
  only `modules/muse/*.mjs`); adding one is a separate workflow change.
