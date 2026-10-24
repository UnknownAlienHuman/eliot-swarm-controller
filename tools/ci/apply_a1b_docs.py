from pathlib import Path
import re

SOURCE_COMMIT = "7408e77e958aa4e8c7244c7c058eec0935bfa72c"
BASE_COMMIT = "0a228cd3a2fde9f38be136485b53b88c7e0e5c84"
RUN_ID = "40126583064"


def read(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")


def write(path: str, text: str) -> None:
    Path(path).write_text(text, encoding="utf-8")


def replace_once(path: str, old: str, new: str) -> None:
    text = read(path)
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{path}: expected one literal match, found {count}")
    write(path, text.replace(old, new, 1))


def regex_once(path: str, pattern: str, replacement: str) -> None:
    text = read(path)
    updated, count = re.subn(pattern, replacement, text, count=1, flags=re.S)
    if count != 1:
        raise SystemExit(f"{path}: expected one structural match, found {count}: {pattern}")
    write(path, updated)


audit = "docs/agent-operations/audit-current.md"
resume = f"""## Resume here

- Checkpoint: **2026-10-24 / Antigravity A1b runtime cut**.
- Runtime implementation commit: **`{SOURCE_COMMIT}`**, based on main
  **`{BASE_COMMIT}`**. The source commit removes the mandatory Antigravity
  refresh/session-proof prerequisite before dispatch.
- Connected chain:
  `swarm-contracts::runtime::AntigravityDispatchSessionProof` →
  `launcher_dispatch/antigravity_capability` latest-refresh lookup and packet proof →
  adapter `controller::dispatch_session_proof` → qualification's mandatory
  `agent.refresh` readiness call and focused readiness fixture.
- Overall audit remains **incomplete, 170 entries**. A1b closes an implementation
  item; it does not create a new finding ID or imply whole-project completion.
- Landed in A1b: dispatch capability now derives from current Store-owned
  binding/generation, opening evidence, adapter boot/root/scope, native observation,
  model readback and explicit `turn_pending=false`; no `agent.refresh` Operation or
  copied assignment/lease/session proof is required. The adapter still exposes the
  real semantic Refresh action for explicit readback.
- Deleted with the old path: proof constants/type/validator, latest-refresh SQL,
  refresh-generated proof payload, assignment/lease/session proof digests and the
  mandatory qualification refresh. Net change: **−271 production Rust lines**,
  **−37 qualification PowerShell lines** and **+6 Rust test lines**.
- Verification: run
  [`{RUN_ID}`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/actions/runs/{RUN_ID})
  passed exact retired-symbol scanning, `cargo fmt --all` and warnings-denied
  production Clippy for `swarm-contracts`, `swarm-antigravity-adapter` and
  `swarm-kernel-host`. No test suite was run.
- Preserved fences: exact Task/Attempt/binding/route/actor scope, binding generation,
  ready/unreleased/connected state, recovery exclusion, current bridge/open/native boot,
  root/scope equality, explicit-model verification or honest native-default unknown,
  native stream health and no pending Open/turn.
- **Next exact chain — implementation A2:** adapter
  `Controller::{{record_outcome,pending_receipts,completed_turns}}` and retained response
  bodies → `main::flush_reports/request_saved` → `result_page` → process restart/hello
  custody and startup recovery → Store `module.outcome/module.result` old-origin
  validation and exact ACK/replay. Persist already-formed outcome/page bytes atomically,
  recover them under the original Operation/binding/origin boot and never persist or
  replay native prompt intent.
- Runtime implementation order is now **A2, A3, then B–V**.
- Still unqualified: focused PowerShell readiness execution, installed Antigravity,
  Windows custody, crash/power loss, same-boot disconnect, successor delivery, result
  source pressure and autonomous end-to-end work.
- This checkpoint includes runtime source plus documentation. It does not claim tests,
  installed-system qualification or A2 behavior.

On resume, compare live `main` with `{SOURCE_COMMIT}`, reopen changed A2 owners/consumers
only, and continue the named chain. A passing production Clippy is not crash or
installed qualification.
"""
regex_once(
    audit,
    r"## Resume here\n.*?On resume,.*?\n(?=\n<a id=\"findings-ar-160170\">)",
    resume,
)
checkpoint = f"""## Runtime checkpoint A1b

A1b landed at
[`7408e77`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/commit/{SOURCE_COMMIT}).
It replaces the refresh-selected proof packet with one compact capability identity
recomputed from current Store binding/opening/observation facts. The immutable launch
packet still carries a digest for ancestry, but no longer copies Task assignment,
workspace lease or session proof structures.

The adapter observation now exposes `turn_pending` directly, so dispatch fails closed
when Open or a turn remains unresolved. Explicit model selection still requires exact
requested/observed/verified equality; native-default mode preserves an unknown model
unless the native init observed one. The semantic `agent.refresh` command remains
available for explicit current-state readback and is no longer a dispatch prerequisite.

Verification run `{RUN_ID}` passed formatting and warnings-denied production Clippy.
The focused readiness script was updated but not executed, and no installed provider,
restart, crash or result-delivery behavior was qualified. A2 remains open.

"""
text = read(audit)
marker = '<a id="findings-ar-160170"></a>'
if text.count(marker) != 1:
    raise SystemExit(f"{audit}: findings marker changed")
write(audit, text.replace(marker, checkpoint + marker, 1))

index = "docs/agent-operations/audit-index.md"
index_checkpoint = f"""Current checkpoint: Antigravity A1b implemented at
`{SOURCE_COMMIT}` on 2026-10-24. Run
[`{RUN_ID}`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/actions/runs/{RUN_ID})
passed exact retired-symbol scanning, formatting and warnings-denied production Clippy
for the affected packages. The audit still has 170 entries; A1b is an implementation
checkpoint, not a new finding or installed qualification. The exact continuation is A2
outcome/body/page durability and authorized successor delivery.
"""
regex_once(
    index,
    r"Current checkpoint:.*?(?=\n<a id=\"evidence-checkpoints\">)",
    index_checkpoint + "\n",
)
landed = f"""## Landed runtime checkpoints

| Slice | Source evidence | Verification | Remaining boundary |
| --- | --- | --- | --- |
| Antigravity A1b | [`7408e77`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/commit/{SOURCE_COMMIT}): removed refresh-selected proof type/SQL/payload/digests and mandatory qualification refresh; current Store binding/open/native observation owns dispatch capability. | Run [`{RUN_ID}`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/actions/runs/{RUN_ID}): retired-symbol scan, rustfmt and warnings-denied production Clippy for contracts/adapter/kernel host. | No tests or installed/crash qualification; A2 durable StoreDelivery and successor completion are next. |

"""
text = read(index)
marker = "## Source and target map"
if text.count(marker) != 1:
    raise SystemExit(f"{index}: source map marker changed")
write(index, text.replace(marker, landed + marker, 1))
replace_once(
    index,
    "| A, M–Q adapters | Durable StoreDelivery, first historical capture, continuation sources and installed restart/version qualification. A1b is the next exact implementation slice. |",
    "| A, M–Q adapters | A1b landed. A2 durable StoreDelivery, first historical capture and authorized successor completion are next; continuation sources and installed restart/version qualification remain open. |",
)

status = "docs/implementation-status.md"
status_intro = f"""Source was implemented on **2026-10-24** at
`{SOURCE_COMMIT}`, based on main `{BASE_COMMIT}`. The active audit contains
**170 entries**. Antigravity A1b is now landed: dispatch no longer requires a separate
`agent.refresh` Operation or copied session-proof packet. Run `{RUN_ID}` passed the
retired-symbol scan, rustfmt and minimal warnings-denied production Clippy. The next
runtime slice is A2; no tests or installed/crash qualification are claimed.
"""
regex_once(
    status,
    r"Source was inspected on \*\*2026-10-24\*\* at\n.*?(?=\nMerged work immediately preceding this checkpoint:)",
    status_intro + "\n",
)
replace_once(
    status,
    "| One dispatch producer across SUCCESS/ERROR/CANCELED | `c92120e`; historical run `39579583956`, job `119153571930` | Scoped regression/Clippy only; no crash/installed/load proof. |",
    "| One dispatch producer across SUCCESS/ERROR/CANCELED | `c92120e`; historical run `39579583956`, job `119153571930` | Scoped regression/Clippy only; no crash/installed/load proof. |\n| Antigravity dispatch proof removed | `7408e77`; run `40126583064`; net −271 production Rust and −37 qualification PowerShell lines | Production Clippy passed; no tests, installed provider, disconnect/crash or A2 delivery proof. |",
)
replace_once(
    status,
    "A1b  NEXT / NOT IMPLEMENTED   remove dispatch proof/refresh prerequisite\nA2   NOT IMPLEMENTED          durable outcome/body/page delivery and successor completion",
    "A1b  LANDED / SCOPED PASS     current Store binding capability; no refresh proof\nA2   NEXT / NOT IMPLEMENTED   durable outcome/body/page delivery and successor completion",
)
replace_once(
    status,
    "The next exact implementation slice is **A1b**. J0 is a bounded independent frontend\nfix; R0–R6 and V0–V3 remain later connected programs and do not reorder A.",
    "The next exact implementation slice is **A2**. J0 is a bounded independent frontend\nfix; R0–R6 and V0–V3 remain later connected programs and do not reorder A.",
)
next_work = """## Next work

**Runtime A2:** persist complete already-formed Antigravity outcome/result delivery
parameters and retained response bytes atomically before acknowledging local retention.
Same-boot IPC reconnect must resend identical parameters; authorized successor delivery
must preserve original Operation/binding/origin boot after custody proves the previous
owner departed. Store deduplicates exact receipt/page identity. No native prompt intent
enters the spool and no uncertain prompt is replayed.

Exact source chain: `Controller::record_outcome/pending_receipts/completed_turns` →
`main::flush_reports/request_saved` → `result_page` → adapter restart/hello recovery →
Store `module.outcome/module.result` origin and old-boot completion checks. Record file
lifetime, pressure, partial-write, ACK-loss, supersession and missing-byte behavior.

**Then A3:** retain only real Open, Dispatch, Send(next_turn), Refresh, Result,
Reconcile and owned-resource release; remove placeholders and retired Node routes after
finite work disposition.

Selected independent future slices remain J0 from
[IPC/MCP](agent-operations/ipc-mcp.md), R0–R6 from
[Observability](agent-operations/observability.md) and V0–V3 from
[Gateway](gateway.md). None is landed by A1b.

During construction run rustfmt plus minimal affected warnings-denied production
Clippy. Focused connected cases close each vertical path when the product slice is
ready; broad installed/live/migration/load qualification remains product-stage work.
"""
regex_once(status, r"## Next work\n.*\Z", next_work)

implementation = "docs/agent-operations/implementation.md"
a1b = f"""**A1b — landed, scoped production Clippy passed:** commit
[`7408e77`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/commit/{SOURCE_COMMIT})
deletes `AntigravityDispatchSessionProof`, latest-refresh SQL, the adapter proof
producer, copied assignment/lease/session proof digests and mandatory
refresh-before-dispatch qualification. Dispatch capability is recomputed from current
Store Task/Attempt/binding scope plus binding generation, connected/recovery state,
opening/current boot, root/scope, native stream health, model readback and
`turn_pending=false`. Semantic Refresh remains available for explicit readback.

Run
[`{RUN_ID}`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/actions/runs/{RUN_ID})
passed retired-symbol scanning, `cargo fmt --all` and warnings-denied production Clippy
for `swarm-contracts`, `swarm-antigravity-adapter` and `swarm-kernel-host`. Net change:
−271 production Rust, −37 qualification PowerShell and +6 Rust test lines. No test suite,
installed harness, disconnect/crash or delivery qualification is claimed.
"""
regex_once(
    implementation,
    r"\*\*A1b:\*\* delete `AntigravityDispatchSessionProof`.*?Store already owns assignment, Attempt,\nbinding/boot, workspace lease and Operation\.\n",
    a1b,
)

installation = "docs/agent-operations/module-installation.md"
replace_once(
    installation,
    "| Antigravity | Rust adapter, active coordinate v6 | Finish A1b–A3: remove dispatch proof, add durable Store delivery, keep real actions only; then retire remaining Node profiles. |",
    "| Antigravity | Rust adapter, active coordinate v6 | A1b landed at `7408e77`: dispatch uses current Store binding/open/native observation without a refresh proof. Finish A2 durable Store delivery and A3 real-action/one-route cut; then retire remaining Node profiles. |",
)

for path in [audit, index, status, implementation, installation]:
    data = read(path)
    stale = (
        "A1b  NEXT / NOT IMPLEMENTED",
        "**A1b:** delete `AntigravityDispatchSessionProof`",
        "Finish A1b–A3: remove dispatch proof",
    )
    for value in stale:
        if value in data:
            raise SystemExit(f"stale A1b status remains in {path}: {value}")
