# Implementation task packets

Status: current handoff index.
Current authority: [documentation map](../documentation-map.md), [slice register](../implementation-slice-register.md), and the owner’s latest explicit task.

A worker receives exactly one packet from this directory. The repository-wide A–V program, audit archive, historical specifications and experimental branches are not appended to the assignment.

## Packet states

- `APPROVED`: exact source basis and pre-existing blob SHAs are recorded; the packet may be assigned only on that basis.
- `BLOCKED`: the target is documented, but a named dependency or source refresh is missing.
- `SUPERSEDED`: retained only as evidence; never assign.

Any source-owner blob mismatch changes `APPROVED` to `BLOCKED` until the manager refreshes callers, consumers, deletion/data disposition and focused evidence. Do not “update the SHA” without rereading the changed boundary.

## Current packets

| Packet | State | Purpose |
| --- | --- | --- |
| [A2.1 — durable same-owner Store delivery](a2-1-antigravity-durable-delivery.md) | `APPROVED` for `main@797ea93` source blobs | Persist exact outcome/result delivery before IPC; identical resend; exact ACK deletion; quarantine prior-boot files. |
| [A2.2 — successor delivery and aggregate coverage](a2-2-antigravity-restart-coverage.md) | `BLOCKED` on A2.1 | Authorize cross-boot import without prompt replay; make Store coverage control body release and pressure. |

## Assignment procedure

1. Select only a slice marked `APPROVED_PACKET` in the [slice register](../implementation-slice-register.md).
2. Verify the packet’s commit and every listed blob SHA against the manager worktree.
3. Record the manager worktree and assigned non-overlapping write scope.
4. Give the worker the packet, [`AGENTS.md`](../../../AGENTS.md), its named current contract and the exact source files—nothing else by default.
5. Run formatting and minimal affected warnings-denied Clippy during construction.
6. Integrate under the manager, verify the combined caller-to-result path, delete the old path, then update status/audit/prevention records.

Experimental branches may be inspected as anti-donors. They are never treated as landed, cherry-picked wholesale or allowed to overwrite the packet’s invariants.
