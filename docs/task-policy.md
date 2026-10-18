# Task owner policy and source-indexed brief

## Refactoring target

One current policy checks the authenticated caller, Task/Attempt ownership,
assignment, candidate and requested action. A manager uses its ordinary scoped
rights; a document path, section, SHA or edition is not an additional capability.
Reviewers report their assigned findings without acquiring manager authority.

Remove document-edition permission gates and historical acceptance branches under
the [reduction program](agent-operations/refactoring.md#code-reduction-candidates).
For actual old work, use the [finite transition](agent-operations/refactoring.md#retire-executors-without-losing-history):
convert/revise unfinished work explicitly and preserve required old evidence as
history. Do not silently give a recorded Attempt new meaning or keep an old
executable workflow forever to read that history.

## Current source constraints

[`policy.rs`](../crates/swarm-kernel-host/src/policy.rs) still requires an explicit
`owner_policy_id` for a new Attempt and recognizes `owner-policy-v1` and
`owner-policy-v2`. Its complete edition record includes document identity; v2
allows scoped feedback through the existing Store checks. This is an implementation
gap against the target above. The exact historical definitions remain in
[owner decisions](owner-decisions.md) and [v2](owner-policy-v2.md); copying their
hashes into another guide does not add authority. Acceptance-policy V1/V2 is a
separate old workflow branch, also covered by the reduction program.

## Task brief and source evidence

The existing Attempt snapshot records its Task revision, admitted inputs and
source-indexed brief. The brief projects objective, requirements, dependencies,
scope and acceptance inputs; it does not create a second specification. Task reads
keep the stored spec and expose an unavailable brief when it cannot be decoded.

Selected source entries retain the actual revision/text and its digest. Unknown
or malformed source references remain explicit gaps rather than invented
instructions. Current legacy projections may lack a recognized policy or source
selection; they describe the stored record without assigning it new rights.
