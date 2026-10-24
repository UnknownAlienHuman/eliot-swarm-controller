# Implementation task packet

A coding agent receives one completed copy of this packet, not the repository-wide audit archive or A–V roadmap. Delete bracketed guidance before assignment.

## Identity

- **Slice ID:**
- **Owner decision / Task revision:**
- **Base branch and exact commit:**
- **Manager worktree:**
- **Assigned agent and disjoint write scope:**
- **Owning current contract:**
- **Related audit/failure-prevention IDs:**

## Observable result

[State the positive user/system behavior in one paragraph. Describe what will work, not only what must be removed.]

### Completion condition

[One externally observable condition that closes the slice.]

### Required path

```text
trigger/current fact
-> authorized command
-> state/effect owner
-> exact readback/result
-> next automatic action or one addressed Governor decision
```

## Exact source owners

| Responsibility | Current path and symbol | Required change |
| --- | --- | --- |
| Caller | | |
| Authority/admission | | |
| State/effect owner | | |
| Readback/result | | |
| Consumer/next action | | |

No stale line numbers. Confirm every symbol at the packet's exact base before editing.

## Retained invariants

[List exact invariants from `failure-prevention-register.md` and the owning contract. Include identity, authority, unknown-effect, restart, concurrency and data-disposition rules relevant to this slice.]

## Unsupported and uncertain cases

| Case | Required behavior |
| --- | --- |
| Unsupported native capability | Reject before native I/O with exact scope. |
| Transport response lost after possible effect | Preserve `unknown`; do not replay. |
| Stale binding/Task/revision/authority | Reject without effect. |
| Missing or damaged retained evidence | Expose a bounded gap; never infer success. |

Add slice-specific cases. Do not silently weaken guarantees or substitute a provider/model/permission/workspace policy.

## Deletions and data disposition

| Replaced mechanism/data | Current consumers | Positive replacement | Migration/disposition | Deletion proof |
| --- | --- | --- | --- | --- |
| | | | | |

A new path is incomplete while the replaced producer, caller, config, schema, fixture or compatibility branch still accepts new work.

## Donor/native mechanism

- **Selected mechanism:**
- **Exact version/source:**
- **What is reused:**
- **Known limits requiring local enforcement:**
- **Rejected alternatives / anti-donors:**

## Verification during construction

- **Formatting:**
- **Minimal warnings-denied Clippy/build/syntax command:**
- **Focused connected case when the slice closes:**
- **Checks deliberately deferred until the product path is complete:**

A source read, grep hit, mocked fixture or successful compile is not runtime qualification.

## Acceptance evidence

| Claim | Required evidence | Result/reference |
| --- | --- | --- |
| Positive path connected | caller-to-result trace or focused connected case | |
| Old path absent | exact symbol/config/schema/fixture scan | |
| Uncertain effect not replayed | focused ambiguity/readback case | |
| Routine automation retained | next automatic action observed or source-connected | |
| Documentation synchronized | contract/status/audit/prevention entries | |

## Output

The agent reports only:

1. behavior connected;
2. exact files/symbols changed;
3. mechanisms deleted and data disposition;
4. checks actually run and their exact scope;
5. net production Rust/JS/glue change separately from tests;
6. unresolved facts and the next exact chain.

Do not return a new roadmap, compatibility framework, speculative abstraction or issue series.
