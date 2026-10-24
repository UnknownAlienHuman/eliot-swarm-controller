#!/usr/bin/env python3
"""Apply the finite documentation-authority corrections found by the readiness audit."""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def write(path: str, text: str) -> None:
    (ROOT / path).write_text(text, encoding="utf-8", newline="\n")


def replace_once(path: str, old: str, new: str) -> None:
    text = read(path)
    if new in text:
        return
    count = text.count(old)
    if count != 1:
        raise RuntimeError(f"{path}: expected one old block, found {count}")
    write(path, text.replace(old, new, 1))


def regex_once(path: str, pattern: str, replacement: str) -> None:
    text = read(path)
    if replacement in text:
        return
    updated, count = re.subn(pattern, replacement, text, count=1, flags=re.DOTALL)
    if count != 1:
        raise RuntimeError(f"{path}: expected one regex block, found {count}")
    write(path, updated)


replace_once(
    "docs/agent-operations/implementation.md",
    """# Eliot Swarm Controller — implementation

This is the single implementation program for the
[architecture](architecture.md). The 2026-10-21 preservation pass compared
`c92120ed70ca7e625440a8a19517c481e73b13c9` with the pre-refactor specification,
October 6 audit, master audit v26 and retained Claude-script review. This is a
focused requirements/contract review, not an exhaustive reread of every source file.
[Findings](refactoring.md) explain the current excess;
[status](../implementation-status.md) separates landed code from plans.

Do not start another horizontal framework migration. Complete one vertical
caller-to-result path, delete its replaced path, then use a second current caller to
prove any shared implementation.
""",
    """# Eliot Swarm Controller — implementation

This is the strategic dependency order and source-owner map for the
[architecture](architecture.md). It is **not a self-contained worker prompt** and does
not prove that every current source file was audited. The 2026-10-21 preservation pass
compared `c92120ed70ca7e625440a8a19517c481e73b13c9` with selected pre-refactor
specification, audit and Claude-script evidence; it was a focused requirements review,
not an exhaustive source reread. The repository-wide audit remains incomplete; see
[source-audit coverage](source-audit-coverage.md), [current audit](audit-current.md),
[documentation readiness](documentation-readiness-report.md) and
[status](../implementation-status.md).

A coding agent receives one approved [task packet](task-packet-template.md) for one
exact revision. The packet cites the applicable
[failure-prevention entries](failure-prevention-register.md), current symbols,
positive path, deletions/data disposition and focused acceptance evidence. Do not send
this entire A–V roadmap as an assignment.

Do not start another horizontal framework migration. Complete one vertical
caller-to-result path, delete its replaced path, then use a second current caller to
prove any shared implementation.
""",
)

replace_once(
    "docs/implementation-status.md",
    """**SOURCE_AUDIT_INCOMPLETE / TARGET DOCUMENTS WORKING DRAFT /
RUNTIME REFACTOR IN PROGRESS.**

""",
    """**SOURCE_AUDIT_INCOMPLETE / TARGET DOCUMENTS WORKING DRAFT /
RUNTIME REFACTOR IN PROGRESS.**

**Documentation readiness: NOT READY for whole-project autonomous implementation.**
The current entry path, task-packet format and AR/PF prevention map are being repaired,
but package-level [source-audit coverage](agent-operations/source-audit-coverage.md)
still contains untraced surfaces. The generated
[readiness report](agent-operations/documentation-readiness-report.md) checks
classification and links; it does not prove source completeness. Agents may implement
only an owner-approved narrow packet at an exact revision. A branch or PR is not
landed behavior, and an implementation slice must not replace the outstanding audit.

""",
)

replace_once(
    "docs/implementation-status.md",
    """The next exact implementation slice is **A2**. J0 is a bounded independent frontend
fix; R0–R6 and V0–V3 remain later connected programs and do not reorder A.
""",
    """When the owner resumes implementation, the next selected runtime slice remains
**A2**. It is not the next source-audit action and must not be used as a substitute for
finishing the package/domain coverage recorded in
[source-audit coverage](agent-operations/source-audit-coverage.md). J0 is a bounded
independent frontend fix; R0–R6 and V0–V3 remain later connected programs.
""",
)

resume = """## Resume here

- **Live merged source basis:** `main` at
  `797ea93f1cfcaad8c8dde2629d666e64efb131c2` (PR #194, Antigravity A1b).
- **Last completed source-audit range:** AR-160–170, frozen at
  `0a228cd3a2fde9f38be136485b53b88c7e0e5c84`. A1b is a later implementation
  checkpoint, not a new audit range and not evidence that unchanged packages were read.
- **Whole-project source audit:** incomplete. The package inventory in
  [source-audit coverage](source-audit-coverage.md) records broad partial coverage for
  `swarm-kernel-host`, partial coverage for several adapters/effect owners, and
  untraced package boundaries including `swarm-cli`, `swarm-kernel`, `swarm-bus` and
  `swarm-telemetry`.
- **Documentation readiness:** the 2026-10-24 structural audit found stale authority,
  broken links and unclassified evidence. The current repair adds one
  [authority map](documentation-map.md), one [task-packet format](task-packet-template.md)
  and one [failure-prevention register](failure-prevention-register.md); this improves
  assignment safety but does not complete source review.
- **Preserve from A1b:** exact Task/Attempt/current-revision and launch-parent checks;
  binding generation and current route; boot/root/connection/recovery fences; model
  readback honesty; workspace authority; adapter command-time binding/root checks; no
  replay after uncertain input. `agent.refresh` remains optional readback and is not a
  dispatch permission.
- **Unmerged implementation branch:** `impl/antigravity-a2-delivery-20261024` is an
  experiment/PR candidate only. Do not describe A2a as landed on `main` or use its
  documentation as the whole-project audit checkpoint.
- **Next exact audit sequence:** (1) root entrypoints plus `swarm-cli`, `swarm-kernel`,
  `swarm-bus`, `swarm-telemetry`; (2) shared contracts/client/process and every current
  consumer; (3) file-level Store/migrations/read models; (4) one adapter at a time;
  (5) effects/observation; (6) tooling, installers, descriptors, tests and evidence.
  Update this register, the [audit index](audit-index.md),
  [source-audit coverage](source-audit-coverage.md) and
  [failure prevention](failure-prevention-register.md) after each cluster.
- **Qualification remains separate:** source reading and compiler gates do not prove
  provider, installed harness, crash/restart, migration, security, Windows/Linux,
  concurrency or load behavior.

Do not continue implementation A2 as an audit substitute. Compare each selected audit
cluster with live `main`, trace caller → authority → state/effect → readback → consumer,
and record every uninspected boundary rather than treating it as clean.

<a id="findings-ar-160170"></a>"""
regex_once(
    "docs/agent-operations/audit-current.md",
    r"## Resume here\n.*?\n<a id=\"findings-ar-160170\"></a>",
    resume,
)

replace_once(
    "docs/agent-operations/audit-index.md",
    """Current checkpoint: Antigravity A1b runtime slice at
`3c7a84ffc6d1e349fe989e4beaa62b03ed4e21bb` on 2026-10-24. The refresh/session-proof
admission gate and repeated assignment/lease/session digests are removed while current
Store binding/Open and adapter command fences remain. Run `40131522836`, job
`120984044559`, passed affected rustfmt and warnings-denied Clippy. No test,
installed-provider, crash/restart or load qualification is implied. Continue with A2.
""",
    """Current merged source basis: `main` at
`797ea93f1cfcaad8c8dde2629d666e64efb131c2` (PR #194, Antigravity A1b). The last
completed **source-audit** range remains AR-160–170 at `0a228cd`; A1b is a later
implementation checkpoint, not proof that other packages were inspected. The
repository-wide audit remains incomplete. Continue the package/domain sequence in
[source-audit coverage](source-audit-coverage.md), not implementation A2 as an audit
substitute. Assign implementation only through an exact
[task packet](task-packet-template.md) constrained by
[failure prevention](failure-prevention-register.md).
""",
)

replace_once(
    "README.md",
    """Start documentation work at the [current document index](docs/agent-operations/README.md),
resume the [source audit](docs/agent-operations/audit-register.md#resume-here), and use
the [donor register](docs/agent-operations/donor-map.md) for upstream links and the
mechanism to reuse. Old programs/specifications are dated evidence, not additional
implementation queues.
""",
    """Start at the [documentation authority map](docs/agent-operations/documentation-map.md)
and [current document index](docs/agent-operations/README.md). Resume source research
at [current audit / Resume here](docs/agent-operations/audit-current.md#resume-here),
not the frozen historical register. Coding agents receive one approved
[task packet](docs/agent-operations/task-packet-template.md) constrained by the
[failure-prevention register](docs/agent-operations/failure-prevention-register.md).
Old programs/specifications are dated evidence, never additional implementation
queues.
""",
)

replace_once(
    "README.md",
    """Keep the short [architecture](docs/agent-operations/architecture.md) in the lead
developer's context. Use [implementation](docs/agent-operations/implementation.md)
for the single work order, source owners and donor choices, then read the assigned
source and contract. [AGENTS.md](AGENTS.md) owns collaboration practice;
[source findings](docs/agent-operations/refactoring.md) explain excess and
[implementation status](docs/implementation-status.md) records evidence.
""",
    """Keep the short [architecture](docs/agent-operations/architecture.md) in the lead
developer's context. [Implementation](docs/agent-operations/implementation.md) is the
strategic order/source map, not a worker prompt. Use one exact
[task packet](docs/agent-operations/task-packet-template.md), then read only its named
contract and current source. [AGENTS.md](AGENTS.md) owns collaboration practice;
[source-audit coverage](docs/agent-operations/source-audit-coverage.md) records
untraced packages, [source findings](docs/agent-operations/refactoring.md) explain
excess and [implementation status](docs/implementation-status.md) records evidence.
""",
)

replace_once(
    "docs/lessons-learned.md",
    """# Выводы из прежних запусков и проверок

**Сведено 30.09.2026 из материалов 26–29.09.2026; дополнено проверкой контрактов 30.09.** Это справочник причин принятых решений, не ещё один brief и не новые требования к каждому worker. Действующий порядок — [архитектура](agent_swarm.md), [план реализации](agent_swarm.implementation-v6.md), [контракт модулей](agent_swarm.module-contract-v2.md).
""",
    """# Выводы из прежних запусков и проверок

**Статус: audit-evidence / historical operational lessons. Не является инструкцией агенту.** Сведено 30.09.2026 из материалов 26–29.09.2026 и дополнено проверкой контрактов 30.09. Причины отказов сохраняются как доказательная база; действующие требования задают [архитектура](agent-operations/architecture.md), [карта полномочий документации](agent-operations/documentation-map.md), конкретный [task packet](agent-operations/task-packet-template.md) и [реестр предотвращения ошибок](agent-operations/failure-prevention-register.md). Старые `agent_swarm*` документы — исторические снимки, а не текущий порядок реализации.
""",
)

replace_once(
    "docs/legacy-swarm-transition.md",
    """# Legacy Swarm transition

""",
    """# Legacy Swarm transition

**Status: historical-snapshot / migration evidence. Current authority:**
[documentation map](agent-operations/documentation-map.md),
[architecture](agent-operations/architecture.md) and an approved
[task packet](agent-operations/task-packet-template.md). **Not worker instructions.**

""",
)
legacy_replacements = {
    "../src/model.rs": "../crates/swarm-kernel-host/src/model.rs",
    "../src/runtime/mod.rs": "../crates/swarm-kernel-host/src/runtime/mod.rs",
    "../src/mcp.rs": "../crates/swarm-mcp/src/mcp/mod.rs",
    "../src/store/mod.rs": "../crates/swarm-kernel-host/src/store/mod.rs",
    "../src/artifacts.rs": "../crates/swarm-kernel-host/src/artifacts.rs",
}
legacy = read("docs/legacy-swarm-transition.md")
for old, new in legacy_replacements.items():
    legacy = legacy.replace(old, new)
write("docs/legacy-swarm-transition.md", legacy)

replace_once(
    "docs/mcp-tool-catalog-sources.md",
    """# ELIOT MCP Catalog and Launcher — Source Map
## Official protocol guidance, donor source patterns and user-reported failures

""",
    """# ELIOT MCP Catalog and Launcher — Source Map
## Official protocol guidance, donor source patterns and user-reported failures

**Status: supporting-reference / audit evidence tied to its recorded revision.**
Current authority is the [documentation map](agent-operations/documentation-map.md),
[IPC/MCP contract](agent-operations/ipc-mcp.md) and an approved
[task packet](agent-operations/task-packet-template.md). **Not worker instructions.**

""",
)
replace_once(
    "docs/mcp-tool-catalog-sources.md",
    """- SOURCE: [`src/mcp.rs`](../src/mcp.rs)
- SOURCE: [`src/mcp/profiles.rs`](../src/mcp/profiles.rs)
- SOURCE: [`src/mcp/profiles_tests.rs`](../src/mcp/profiles_tests.rs)
- SOURCE: [`src/config.rs`](../src/config.rs)
""",
    """- SOURCE: [`crates/swarm-mcp/src/mcp/mod.rs`](../crates/swarm-mcp/src/mcp/mod.rs)
- SOURCE: [`crates/swarm-mcp/src/mcp/profiles.rs`](../crates/swarm-mcp/src/mcp/profiles.rs)
- SOURCE: [`crates/swarm-mcp/src/mcp/frontend_contract_tests.rs`](../crates/swarm-mcp/src/mcp/frontend_contract_tests.rs)
- SOURCE: [`crates/swarm-mcp/src/config.rs`](../crates/swarm-mcp/src/config.rs)
- SOURCE: [`crates/swarm-contracts/src/mcp_frontend.rs`](../crates/swarm-contracts/src/mcp_frontend.rs)
""",
)

execution = read("docs/remediation/2026-10-09/EXECUTION-MAP.md")
execution = execution.replace(
    "[old readiness index](../2026-10-07/README.md) (historical only)",
    "[older remediation evidence](../2026-10-07/) (historical only)",
)
write("docs/remediation/2026-10-09/EXECUTION-MAP.md", execution)

replace_once(
    "docs/agent-operations/README.md",
    """| [Failure prevention](failure-prevention-register.md) | AR/PF invariants, implementation owner and closure evidence. |
| [Task packet](task-packet-template.md) | Exact assignment format for one implementation slice. |
""",
    """| [Failure prevention](failure-prevention-register.md) | AR/PF invariants, implementation owner and closure evidence. |
| [Source-audit coverage](source-audit-coverage.md) | Package/non-workspace coverage, untraced boundaries and next audit sequence. |
| [Task packet](task-packet-template.md) | Exact assignment format for one implementation slice. |
""",
)
replace_once(
    "docs/agent-operations/README.md",
    """| Where did source research stop? | [Current audit](audit-current.md#resume-here) |
| What evidence supports a finding? | [Audit index](audit-index.md#evidence-checkpoints) |
""",
    """| Where did source research stop? | [Current audit](audit-current.md#resume-here) |
| Which packages remain untraced? | [Source-audit coverage](source-audit-coverage.md) |
| What evidence supports a finding? | [Audit index](audit-index.md#evidence-checkpoints) |
""",
)

print("documentation authority corrections applied")
