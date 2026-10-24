# Documentation authority map

This map prevents old audits, specifications and remediation plans from becoming accidental worker instructions. A document's existence does not make it current.

## Authority order

1. The owner's latest explicit task.
2. [`AGENTS.md`](../../AGENTS.md): repository-wide working rules.
3. This directory's [`README`](README.md): navigation and current document roles.
4. [`architecture.md`](architecture.md): product purpose and non-negotiable boundaries.
5. An approved [task packet](task-packet-template.md) for one exact source revision.
6. The domain contract named by that task packet.
7. Current source and executed evidence.

[`implementation.md`](implementation.md) is the strategic dependency/order map. It is not a self-contained worker prompt. No agent receives the whole A–V roadmap as its assignment.

## Classes

| Class | Meaning | May direct implementation? |
| --- | --- | --- |
| `entrypoint` | Repository navigation and worker rules. | Yes, together with a task packet. |
| `current-contract` | Normative target for one current responsibility. | Only for the assigned slice. |
| `current-status` | Landed behavior, open limits and qualification state. | No; it constrains claims. |
| `task-packet` | Exact base, symbols, positive path, deletions and gates for one slice. | Yes. |
| `module-guide` | Current package/artifact-specific operation notes. | Only when the packet names that module. |
| `audit-evidence` | Source findings, traces and immutable checkpoints. | No; evidence must be translated into the current contract/packet. |
| `qualification-evidence` | Executed check receipts tied to exact source/binaries. | No; it proves only its recorded scope. |
| `historical-snapshot` | Superseded specifications, programs, queues and dated plans. | Never. |
| `supporting-reference` | Donor research, source notes and explanatory material. | Never by itself. |
| `generated-audit` | Reproducible inventory/link/readiness output. | No. |

## Path classification

### Entry points

- `AGENTS.md`
- `README.md`
- `docs/agent-operations/README.md`

### Current contracts and status

- `docs/agent-operations/{architecture,implementation,delivery,configuration,module-api,module-installation,store-evolution,ipc-mcp,external-effects,github-sources,observability,modularity,shared-packages,donor-map,refactoring}.md`
- `docs/implementation-status.md`
- Current domain documents explicitly linked by the Agent Operations README, including launcher, CheckRun, ScriptRun, communication, gateway and MCP contracts.

### Audit and qualification evidence

- `docs/agent-operations/{audit-current,audit-index,audit-register,modularity-review-*.md}`
- `docs/remediation/**`
- `docs/restart-checkpoint-*.md`
- `docs/qualification-*.md`
- `docs/*-sources.md`, donor/source registries and exact workflow/PR receipts
- `docs/lessons-learned.md`

### Historical snapshots

- `docs/agent_swarm*.md` and versioned `agent_swarm.*` data unless a current contract explicitly imports one field
- `docs/documentation-program*.md`
- `docs/legacy-*.md`
- dated execution maps, old work queues and superseded implementation programs

### Module guides

- `modules/**/README.md`
- `crates/**/README.md`
- module-local installation/qualification notes tied to a current artifact coordinate

Everything else is `supporting-reference` until this map or an owning current contract classifies it more narrowly. Supporting material cannot override a current contract.

## Mandatory header for new dated or historical documents

Every new snapshot/evidence document must begin with:

```text
Status: audit-evidence | qualification-evidence | historical-snapshot
Inspected source/binary: <exact commit/artifact or unknown>
Current authority: <link to owning current contract/status>
Not worker instructions.
```

## Change rule

A change to product meaning updates, in the same commit:

1. the owning current contract;
2. the exact task packet or implementation slice;
3. implementation status;
4. the audit/failure-prevention entry when a finding is affected.

Do not fix contradictions by adding another note. Remove or reclassify the superseded instruction.
