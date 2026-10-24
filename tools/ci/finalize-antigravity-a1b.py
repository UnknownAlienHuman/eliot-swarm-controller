from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def write(path: str, content: str) -> None:
    (ROOT / path).write_text(content, encoding="utf-8", newline="\n")


def replace_once(path: str, old: str, new: str) -> None:
    content = read(path)
    count = content.count(old)
    if count != 1:
        raise RuntimeError(f"{path}: expected one exact match, found {count}")
    write(path, content.replace(old, new, 1))


def regex_once(path: str, pattern: str, replacement: str) -> None:
    content = read(path)
    updated, count = re.subn(pattern, replacement, content, count=1, flags=re.DOTALL)
    if count != 1:
        raise RuntimeError(f"{path}: expected one regex match, found {count}")
    write(path, updated)


replace_once(
    "crates/swarm-kernel-host/src/store/launcher_dispatch.rs",
    '"launch plan, Task assignment, or C8 capability changed before native input",',
    '"launch plan, Task assignment, or native dispatch capability changed before native input",',
)
replace_once(
    "crates/swarm-kernel-host/src/store/launcher_dispatch.rs",
    '''/// Revalidate the complete current Store tuple before accepting an
/// Antigravity session proof, including when this capability helper is called
/// directly for a readback. `None` means the initial unstarted stage; `Some`
''',
    '''/// Revalidate the complete current Store tuple before accepting an
/// Antigravity dispatch, including when this capability helper is called
/// directly for readback. `None` means the initial unstarted stage; `Some`
''',
)
replace_once(
    "crates/swarm-kernel-host/src/store/launcher_dispatch.rs",
    '''/// Select the current native capability from the trusted route retained by the
/// launch binding. OpenCode keeps its existing C8 verifier; Antigravity uses
/// only its same-process session/turn proof and cannot claim MCP readback.
''',
    '''/// Select the current native capability from the trusted route retained by the
/// launch binding. OpenCode keeps its existing C8 verifier; Antigravity uses
/// current Store binding and successful Open evidence without another
/// refresh/proof Operation and cannot claim MCP readback.
''',
)
replace_once(
    "modules/antigravity-rust/src/controller.rs",
    '''                // native stream observed it. A v2 dispatch proof can attest
                // session identity without claiming verified model selection.
''',
    '''                // native stream observed it. Store retains this Open evidence;
                // Dispatch does not require a second refresh or proof packet.
''',
)

replace_once(
    "modules/antigravity-rust/README.md",
    '''The binary is `swarm-antigravity`; the current source descriptor is `eliot-antigravity.rust-headless.1`, version `6`. Versions 1 through 5 remain retained identities. Version 6 permits omitted model selection and uses a v2 current-session proof that preserves an unknown model without claiming verified selection. Explicit selections retain their exact v1 readback proof. The normalized result contract introduced by version 4 returns bounded captured assistant output when this process retains the exact Operation-to-turn mapping. Source support does not establish installed-native qualification. The adapter has no Store, database, provider-auth or MCP-server ownership. Installed/native qualification remains pending.
''',
    '''The binary is `swarm-antigravity`; the current source descriptor is `eliot-antigravity.rust-headless.1`, version `6`. Versions 1 through 5 remain retained identities. Version 6 permits omitted model selection. A1b removed the mandatory dispatch `agent.refresh`, `AntigravityDispatchSessionProof`, latest-refresh lookup and assignment/lease/session digest packet. Store now revalidates the current Task/Attempt, binding generation, route, current boot/root, successful Open model evidence and workspace authority directly; the adapter still rechecks command binding/root before native I/O. `agent.refresh` remains an optional current-process readback action, not a dispatch prerequisite. The normalized result contract introduced by version 4 returns bounded captured assistant output when this process retains the exact Operation-to-turn mapping. Source support does not establish installed-native qualification. The adapter has no Store, database, provider-auth or MCP-server ownership. Installed/native qualification remains pending.
''',
)
replace_once(
    "modules/antigravity-rust/README.md",
    '''Native `init` and terminal-result events are consumed from the current child stream; the reducer reports terminal facts only when the parsed result has a fingerprint and current observation ID.
''',
    '''Native `init` and terminal-result events are consumed from the current child stream; the reducer reports terminal facts only when the parsed result has a fingerprint and belongs to the exact pending Operation and current stream generation.
''',
)
replace_once(
    "modules/antigravity-rust/README.md",
    '''This guide was reconciled with source `f1b854b6` on 2026-10-17. The documentation correction runs no native/provider call or database operation and adds no compiler, process or live-model qualification. Existing evidence remains bound to its recorded source/artifact; installed Antigravity behavior, served-model identity, productive native result-body readback and whole-family departure need their own evidence.
''',
    '''This guide was reconciled with A1b source `3c7a84ffc6d1e349fe989e4beaa62b03ed4e21bb` on 2026-10-24. Run `40131522836`, job `120984044559`, passed affected rustfmt and warnings-denied Clippy; no native/provider call, database migration, integration test or installed/live-model qualification was run. Existing evidence remains bound to its recorded source/artifact; installed Antigravity behavior, served-model identity, durable result delivery and whole-family departure need their own evidence.
''',
)

replace_once(
    "docs/agent-operations/refactoring.md",
    '''- `c92120e`: one dispatch producer is updated across SUCCESS/ERROR/CANCELED. Run
  `39579583956`, job `119153571930`, passed the connected terminal regression and
  minimal Store Clippy before publishing it.

Still open: Antigravity dispatch proof/refresh gate, crash-unsafe in-memory delivery
and extra command/profile surface. Installed harness/crash/scale behavior is
unqualified.
''',
    '''- `c92120e`: one dispatch producer is updated across SUCCESS/ERROR/CANCELED. Run
  `39579583956`, job `119153571930`, passed the connected terminal regression and
  minimal Store Clippy before publishing it.
- `3c7a84f`: A1b removes the Antigravity refresh/session-proof admission gate and its
  repeated assignment/lease/session digests; run `40131522836`, job `120984044559`,
  passed affected rustfmt and warnings-denied Clippy. Recorded net change is −305
  production Rust lines, −20 qualification-glue lines and +3 test lines.

Still open: crash-unsafe in-memory delivery and extra command/profile surface.
Installed harness/crash/scale behavior is unqualified.
''',
)

replace_once(
    "docs/agent-operations/module-installation.md",
    '''| Antigravity | Rust adapter, active coordinate v6 | Finish A1b–A3: remove dispatch proof, add durable Store delivery, keep real actions only; then retire remaining Node profiles. |
''',
    '''| Antigravity | Rust adapter, active coordinate v6 | A1b removed the dispatch refresh/proof gate. Finish A2–A3: add durable Store delivery, keep real actions only, then retire remaining Node profiles. |
''',
)
replace_once(
    "docs/agent-operations/module-installation.md",
    '''- Antigravity refresh/proof prerequisite;
''',
    '''- Antigravity refresh/proof prerequisite — removed in A1b;
''',
)

replace_once(
    "docs/agent-operations/implementation.md",
    '''**A1b:** delete `AntigravityDispatchSessionProof`, refresh producer, latest-refresh
lookup, binding/snapshot proof validator, assignment/lease/session digests and
mandatory refresh-before-dispatch. Store already owns assignment, Attempt,
binding/boot, workspace lease and Operation.
''',
    '''**A1b — landed, scoped Clippy passed:** `3c7a84f` deletes
`AntigravityDispatchSessionProof`, refresh proof production, latest-refresh lookup,
binding/snapshot proof validation, assignment/lease/session digests and the mandatory
refresh-before-dispatch qualification step. Store now revalidates its existing
Task/Attempt, binding/generation, route, current boot/root, successful Open model
evidence and workspace authority directly; the adapter retains command-time
binding/root checks and `agent.refresh` remains a standalone read action. Run
`40131522836`, job `120984044559`, passed affected rustfmt and warnings-denied Clippy.
No integration, installed harness, crash or load qualification is claimed. Net change:
−305 production Rust lines, −20 qualification-glue lines and +3 test lines.
''',
)

replace_once(
    "docs/agent-operations/audit-current.md",
    '''- AR-142–159 at [`b61c0bd`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/b61c0bd76fc3c16f8e28c262f7607ace2426469b/docs/agent-operations/audit-current.md).
''',
    '''- AR-142–159 at [`b61c0bd`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/b61c0bd76fc3c16f8e28c262f7607ace2426469b/docs/agent-operations/audit-current.md);
- AR-160–170 at [`0a228cd`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/0a228cd3a2fde9f38be136485b53b88c7e0e5c84/docs/agent-operations/audit-current.md).
''',
)
regex_once(
    "docs/agent-operations/audit-current.md",
    r'''## Resume here\n\n.*?\n<a id="findings-ar-160170"></a>''',
    '''## Resume here

- Checkpoint: **2026-10-24 / Antigravity A1b runtime reduction**.
- Runtime source changed from gateway-audit head
  `0a228cd3a2fde9f38be136485b53b88c7e0e5c84` to
  **`3c7a84ffc6d1e349fe989e4beaa62b03ed4e21bb`**.
- Connected chain:
  `launcher_dispatch::require_current_antigravity_scope` → retained descriptor and
  current binding/Open validation → immutable launch-dispatch packet → adapter
  `Controller::complete_open/read_command` → qualification readiness. The mandatory
  `agent.refresh` Operation, `AntigravityDispatchSessionProof`, latest-refresh query,
  snapshot validator and assignment/lease/session digest packet are removed.
- Overall source audit remains **incomplete, 170 entries**. A1b is an implementation
  checkpoint, not another finding range or a whole-project completion claim.
- Preserved: exact Task/Attempt/current-revision and launch-parent checks; binding
  generation and current route; current boot/root/connection/recovery fences;
  explicit-model readback or honest native-default unknown; workspace authority;
  adapter command-time binding/root checks; no replay after uncertain input.
- `agent.refresh` remains a real optional current-process readback action. It no longer
  authorizes Dispatch or emits a duplicate proof packet.
- Verification: run
  [40131522836](https://github.com/UnknownAlienHuman/eliot-swarm-controller/actions/runs/40131522836),
  job `120984044559`, passed affected rustfmt and warnings-denied Clippy. No tests,
  native/provider calls, installed harness, crash, restart or load qualification ran.
- Recorded change: **−305 production Rust lines, −20 qualification-glue lines, +3 test
  lines**.
- **Next exact chain — implementation A2:** adapter
  `Controller::{record_outcome,pending_receipts,completed_turns}` and normalized result
  pages → `HostSession::request_saved/flush_reports` → exact `module.outcome` /
  `module.result` ACK and replay → private atomic retained outcome/body/page files →
  verified prior-owner departure and successor delivery for the original Operation.
  Preserve origin boot and source identity; never put native prompt intent in the
  delivery spool or replay input.
- Runtime implementation order is now **A2–A3, then B–V**.
- Still unqualified: crash before/after durable capture, power loss, old-boot delivery,
  source-pressure behavior, installed Antigravity, live restart and scale.

On resume, compare live `main` with `3c7a84f`, reopen only changed A2 owners/consumers,
and continue the named delivery chain. Do not recreate the deleted refresh/proof gate.

<a id="findings-ar-160170"></a>''',
)

regex_once(
    "docs/agent-operations/audit-index.md",
    r'''Current checkpoint: remote gateway source inspected at\n.*?qualification\.\n''',
    '''Current checkpoint: Antigravity A1b runtime slice at
`3c7a84ffc6d1e349fe989e4beaa62b03ed4e21bb` on 2026-10-24. The refresh/session-proof
admission gate and repeated assignment/lease/session digests are removed while current
Store binding/Open and adapter command fences remain. Run `40131522836`, job
`120984044559`, passed affected rustfmt and warnings-denied Clippy. No test,
installed-provider, crash/restart or load qualification is implied. Continue with A2.
''',
)
replace_once(
    "docs/agent-operations/audit-index.md",
    '''| [AR-160–170](audit-current.md#findings-ar-160170) | [Current gateway trace](audit-current.md#trace-2026-10-24-gateway) | Fixed principal, secret loading, HTTP/Origin boundary, connection/session capacity, unknown outcome and launcher cutover. |
''',
    '''| AR-160–170 | [`0a228cd`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/0a228cd3a2fde9f38be136485b53b88c7e0e5c84/docs/agent-operations/audit-current.md#trace-2026-10-24-gateway) | Fixed principal, secret loading, HTTP/Origin boundary, connection/session capacity, unknown outcome and launcher cutover. |
''',
)
replace_once(
    "docs/agent-operations/audit-index.md",
    '''| A, M–Q adapters | Durable StoreDelivery, first historical capture, continuation sources and installed restart/version qualification. A1b is the next exact implementation slice. |
''',
    '''| A, M–Q adapters | A1b refresh/proof removal is landed with scoped Clippy. Next: A2 durable StoreDelivery, first historical capture, continuation sources and installed restart/version qualification. |
''',
)

regex_once(
    "docs/implementation-status.md",
    r'''Source was inspected on \*\*2026-10-24\*\* at\n.*?\n\nMerged work immediately preceding this checkpoint:''',
    '''Runtime slice was inspected and reduced on **2026-10-24** at
`3c7a84ffc6d1e349fe989e4beaa62b03ed4e21bb`. The active source audit remains
**incomplete at 170 entries**; AR-160–170 are frozen at the preceding gateway
checkpoint `0a228cd`. A1b is the first new runtime slice after that audit: it removes
the mandatory Antigravity dispatch refresh/session-proof chain while preserving
current Store binding/Open and adapter command fences.

Merged work immediately preceding this checkpoint:''',
)
replace_once(
    "docs/implementation-status.md",
    '''- PR #191 at `b61c0bd`: AR-142–159 plus the Governor-observation contract;
  documentation only. No R0–R6 collector/runtime implementation is claimed.
''',
    '''- PR #191 at `b61c0bd`: AR-142–159 plus the Governor-observation contract;
  documentation only. No R0–R6 collector/runtime implementation is claimed.
- PR #192 at `0a228cd`: AR-160–170 plus gateway target/dependency documentation;
  documentation only. No V0–V3 gateway runtime implementation is claimed.
''',
)
replace_once(
    "docs/implementation-status.md",
    '''| Disconnected GitHub PR/projection modules removed | `98b350e`, net −250 Rust lines; run `40087522775` passed scoped Ubuntu/Windows gates | No PR-source feature or remote observation was added. |
''',
    '''| Disconnected GitHub PR/projection modules removed | `98b350e`, net −250 Rust lines; run `40087522775` passed scoped Ubuntu/Windows gates | No PR-source feature or remote observation was added. |
| Antigravity dispatch refresh/proof gate removed | `3c7a84f`, net −305 production Rust and −20 qualification-glue lines; run `40131522836`, job `120984044559` | A2 durable delivery, old-boot completion and installed/crash qualification remain. |
''',
)
replace_once(
    "docs/implementation-status.md",
    '''A1b  NEXT / NOT IMPLEMENTED   remove dispatch proof/refresh prerequisite
A2   NOT IMPLEMENTED          durable outcome/body/page delivery and successor completion
''',
    '''A1b  LANDED / SCOPED PASS      refresh/proof gate removed; current fences retained
A2   NEXT / NOT IMPLEMENTED   durable outcome/body/page delivery and successor completion
''',
)
replace_once(
    "docs/implementation-status.md",
    '''The next exact implementation slice is **A1b**. J0 is a bounded independent frontend
fix; R0–R6 and V0–V3 remain later connected programs and do not reorder A.
''',
    '''The next exact implementation slice is **A2**. J0 is a bounded independent frontend
fix; R0–R6 and V0–V3 remain later connected programs and do not reorder A.
''',
)
replace_once(
    "docs/implementation-status.md",
    '''| Results/recovery, AR-001–035 | Exact selected result identity, assembly/grants and several useful delivery mechanisms. | Durable Antigravity delivery, first historical capture, continuation and live crash/power-loss. |
''',
    '''| Results/recovery, AR-001–035 | Exact selected result identity, assembly/grants and several useful delivery mechanisms; A1b removed the duplicate Antigravity refresh/proof admission gate. | A2 durable Antigravity delivery, first historical capture, continuation and live crash/power-loss. |
''',
)
replace_once(
    "docs/implementation-status.md",
    '''## Gateway status
''',
    '''## Antigravity A1b status

A1b removes `AntigravityDispatchSessionProof`, its runtime DTO/constants, adapter
producer, latest-refresh Store query, binding/snapshot proof validator, mandatory
qualification refresh and assignment/lease/session digest packet. Dispatch now reads
the existing successful Open evidence and revalidates current Task/Attempt, route,
binding generation, boot/root, model and workspace authority. The adapter still rejects
foreign binding/root commands before native I/O. `agent.refresh` remains available as
readback and grants no dispatch right.

Run `40131522836`, job `120984044559`, passed affected rustfmt and warnings-denied
Clippy. No integration tests or installed/native qualification were run. A2 remains
open: all outcome/body/page delivery is still process-memory-backed and cannot yet be
completed safely by a successor boot.

## Gateway status
''',
)
regex_once(
    "docs/implementation-status.md",
    r'''## Next work\n\n.*\Z''',
    '''## Next work

**Runtime A2:** retain complete Antigravity outcome/body/page Store-delivery parameters
in private atomic files before treating them as locally retained. Same-boot reconnect
may resend identical bytes; after verified prior-owner departure, an authorized
successor may deliver the original Operation with immutable origin boot/source identity.
Never persist or replay native prompt intent. Missing/partial capture remains visible
unknown and must not complete a newer Attempt.

**Subsequent A3:** keep only real typed actions and one active Antigravity route; remove
placeholder controls and concrete retired Node work after finite disposition.

Selected independent future slices remain J0 from
[IPC/MCP](agent-operations/ipc-mcp.md), R0–R6 from
[Observability](agent-operations/observability.md) and V0–V3 from
[Gateway](gateway.md). None is landed here.

During construction run rustfmt plus minimal affected warnings-denied compile/Clippy.
Focused connected cases close each slice; broad installed/live/migration/load
qualification remains product-stage work.
''',
)

for path in [
    "crates/swarm-kernel-host/src/store/launcher_dispatch.rs",
    "modules/antigravity-rust/src/controller.rs",
    "modules/antigravity-rust/README.md",
    "docs/agent-operations/refactoring.md",
    "docs/agent-operations/module-installation.md",
    "docs/agent-operations/implementation.md",
    "docs/agent-operations/audit-current.md",
    "docs/agent-operations/audit-index.md",
    "docs/implementation-status.md",
]:
    content = read(path)
    if "A1b  NEXT / NOT IMPLEMENTED" in content:
        raise RuntimeError(f"{path}: stale A1b status remains")

print("Antigravity A1b registry finalized")
