from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def replace_once(path: str, old: str, new: str) -> None:
    target = ROOT / path
    text = target.read_text(encoding="utf-8")
    if new in text:
        return
    if text.count(old) != 1:
        raise RuntimeError(f"{path}: expected one match for {old[:80]!r}")
    target.write_text(text.replace(old, new, 1), encoding="utf-8", newline="\n")


replace_once(
    "docs/agent-operations/audit-current.md",
    """- **Whole-project source audit:** incomplete. The package inventory in
  [source-audit coverage](source-audit-coverage.md) records broad partial coverage for
  `swarm-kernel-host`, partial coverage for several adapters/effect owners, and
  untraced package boundaries including `swarm-cli`, `swarm-kernel`, `swarm-bus` and
  `swarm-telemetry`.
""",
    """- **Whole-project source audit:** incomplete. The first previously untraced package
  cluster now has a reproducible [surface inventory](source-surface-inventory.md) and
  reviewed [CP-001–006 checkpoint](audit-entrypoints-public-surfaces-20261024.md).
  Root compatibility, CLI, kernel, bus and telemetry are still `partial source trace`:
  their exact cross-boundary chains and dispositions listed in
  [source-audit coverage](source-audit-coverage.md) remain open.
""",
)

replace_once(
    "docs/agent-operations/audit-current.md",
    """- **Next exact audit sequence:** (1) root entrypoints plus `swarm-cli`, `swarm-kernel`,
  `swarm-bus`, `swarm-telemetry`; (2) shared contracts/client/process and every current
  consumer; (3) file-level Store/migrations/read models; (4) one adapter at a time;
  (5) effects/observation; (6) tooling, installers, descriptors, tests and evidence.
  Update this register, the [audit index](audit-index.md),
  [source-audit coverage](source-audit-coverage.md) and
  [failure prevention](failure-prevention-register.md) after each cluster.
""",
    """- **Next exact audit sequence:** finish CP-001–006 by comparing CLI methods with
  host policy/catalog, tracing the root compatibility cut, classifying every public
  `swarm-kernel` function, tracing bus cursor/action recovery and telemetry persistence.
  Then continue (2) shared contracts/client/process, (3) Store/migrations/read models,
  (4) adapters, (5) effects/observation and (6) tooling/installers/qualification.
  Update this register, the [audit index](audit-index.md),
  [source-audit coverage](source-audit-coverage.md) and
  [failure prevention](failure-prevention-register.md) after each cluster.
""",
)

replace_once(
    "docs/agent-operations/audit-current.md",
    """<a id="findings-ar-160170"></a>
## Findings AR-160–170
""",
    """<a id="checkpoint-entrypoints-public-surfaces"></a>
## Package coverage checkpoint CP-001–006

At merged source `797ea93`, all Rust files in root entrypoints, `swarm-cli`,
`swarm-kernel`, `swarm-bus` and `swarm-telemetry` were inventoried and their immediate
Cargo/token consumers inspected. The reviewed checkpoint is
[Entrypoints and public surfaces](audit-entrypoints-public-surfaces-20261024.md).

Confirmed: the root facade/wrapper is an active compatibility cut; the CLI is a large
IPC/delegation surface rather than a Store owner; `swarm-kernel` has one production
package dependent; Bus routing, ScriptRun reconciliation and standalone execution are
distinct stages; telemetry is diagnostic-only at the inspected boundary. No duplicate
Script interpreter, Store authority in the CLI/kernel package, or business authority in
telemetry was established. Exact method-policy, retirement, per-function, bus recovery
and telemetry persistence chains remain open, so the cluster is not closed.

<a id="findings-ar-160170"></a>
## Findings AR-160–170
""",
)

replace_once(
    "docs/agent-operations/audit-index.md",
    """[failure prevention](failure-prevention-register.md).

<a id="evidence-checkpoints"></a>
## Evidence checkpoints
""",
    """[failure prevention](failure-prevention-register.md).

## Package coverage checkpoints

| Checkpoint | Inspected source | Status / subject |
| --- | --- | --- |
| [CP-001–006](audit-entrypoints-public-surfaces-20261024.md) | `main` `797ea93`; cluster SHA-256 `c55118a3580219f5fd1f4a90082eb51da3bc718e9d8516fac648007859d3d9f7` | Root compatibility, 33-command CLI, one-dependent kernel, Bus roles and telemetry boundary. Partial; exact cross-boundary chains remain open. |

The generated [surface inventory](source-surface-inventory.md) is syntax/dependency
evidence, not semantic or runtime qualification. CP IDs are package-coverage findings,
not additions to the stable AR-001–170 failure register.

<a id="evidence-checkpoints"></a>
## Evidence checkpoints
""",
)

print("entrypoint audit registers finalized")
