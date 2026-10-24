# Documentation readiness audit

Inspected repository head: `1d7fc248057a1975b1d2bc0d23d7eb4fd1015758`.

## Verdict: NOT READY

- current source-of-truth documents explicitly declare the source audit incomplete or non-exhaustive.
- repository-local Markdown links are broken.
- most Markdown files are not mechanically classified as active or historical.

This verdict concerns whether an implementation agent can safely treat the documentation as complete. It does not convert source reading, design text or a compiler pass into runtime qualification.

## Inventory

| Measure | Count |
| --- | ---: |
| Markdown files | 151 |
| Files directly under `docs/` | 41 |
| Explicit active entry points/contracts | 20 |
| Recognizable historical/snapshot files | 8 |
| Supporting or unclassified files | 123 |
| Broken repository-local Markdown links | 11 |
| Documents at least 25 KiB | 15 |

## Explicit incompleteness evidence

- `docs/agent-operations/audit-current.md`
- `docs/agent-operations/implementation.md`
- `docs/agent-operations/refactoring.md`
- `docs/implementation-status.md`

## Broken local links

- `docs/agent-operations/documentation-map.md` → `task-packet-template.md` (resolved as `docs/agent-operations/task-packet-template.md`)
- `docs/legacy-swarm-transition.md` → `../src/model.rs` (resolved as `src/model.rs`)
- `docs/legacy-swarm-transition.md` → `../src/runtime/mod.rs` (resolved as `src/runtime/mod.rs`)
- `docs/legacy-swarm-transition.md` → `../src/mcp.rs` (resolved as `src/mcp.rs`)
- `docs/legacy-swarm-transition.md` → `../src/store/mod.rs` (resolved as `src/store/mod.rs`)
- `docs/legacy-swarm-transition.md` → `../src/artifacts.rs` (resolved as `src/artifacts.rs`)
- `docs/mcp-tool-catalog-sources.md` → `../src/mcp.rs` (resolved as `src/mcp.rs`)
- `docs/mcp-tool-catalog-sources.md` → `../src/mcp/profiles.rs` (resolved as `src/mcp/profiles.rs`)
- `docs/mcp-tool-catalog-sources.md` → `../src/mcp/profiles_tests.rs` (resolved as `src/mcp/profiles_tests.rs`)
- `docs/mcp-tool-catalog-sources.md` → `../src/config.rs` (resolved as `src/config.rs`)
- `docs/remediation/2026-10-09/EXECUTION-MAP.md` → `../2026-10-07/README.md` (resolved as `docs/remediation/2026-10-07/README.md`)

## Largest documentation surfaces

| File | Bytes | Lines | Classification |
| --- | ---: | ---: | --- |
| `docs/agent_swarm.implementation-v6.md` | 91087 | 464 | historical-or-snapshot |
| `docs/remediation/2026-10-09/CURRENT-AUDIT.md` | 80179 | 562 | supporting-or-unclassified |
| `docs/agent_swarm.md` | 79824 | 419 | historical-or-snapshot |
| `docs/documentation-program.md` | 72125 | 1141 | historical-or-snapshot |
| `docs/agent_swarm.module-contract-v2.md` | 64819 | 384 | historical-or-snapshot |
| `modules/opencode/README.md` | 45912 | 291 | supporting-or-unclassified |
| `docs/agent-operations/audit-register.md` | 44128 | 401 | supporting-or-unclassified |
| `docs/agent-operations/implementation.md` | 43477 | 796 | active |
| `docs/lessons-learned.md` | 40589 | 137 | supporting-or-unclassified |
| `docs/remediation/2026-10-08/36-donor-registry.md` | 38969 | 477 | supporting-or-unclassified |
| `docs/documentation-program-implementation-review.md` | 38029 | 769 | historical-or-snapshot |
| `docs/gemini-spark-integration.md` | 32964 | 838 | supporting-or-unclassified |
| `docs/runtime-notes.md` | 29248 | 157 | supporting-or-unclassified |
| `docs/mcp-tool-catalog-and-loading.md` | 28601 | 904 | supporting-or-unclassified |
| `docs/agent-operations/configuration.md` | 25621 | 463 | active |

## Required closure before a whole-project implementation claim

1. Finish the source audit at a named revision and record every inspected domain, not only the latest runtime slice.
2. Map every confirmed historical failure to one current invariant, source owner, implementation slice and executable acceptance case; unresolved items stay explicitly open.
3. Freeze one compact agent entry path and classify every other document as active contract, evidence, historical snapshot or archive.
4. Replace repository-wide A–V prose as worker input with generated narrow task packets naming exact current symbols, deletions, positive behavior, uncertainty and minimum checks.
5. Reconcile current status, audit resume point, implementation order and branch/PR state in one commit; no document may call unmerged code landed.
6. Run the documentation audit and link check after every source-of-truth change.

## What this audit does not prove

- It does not prove that all code was read.
- It does not prove that historical requirements are complete.
- It does not prove runtime, crash, restart, provider, migration, security or load behavior.
- It does not authorize implementation from an unclassified or historical document.

Machine-readable inventory: [`documentation-inventory.json`](documentation-inventory.json).
