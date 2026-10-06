# Owner policy v2 — scoped manager review disposition

**Edition:** 2 — 2026-10-03
**Status:** accepted owner policy for explicitly selected new Attempts.

## Owner policy v2 — scoped manager review disposition

- A TaskSpec must explicitly select `owner-policy-v2` before a new Attempt can
  use scoped manager review disposition. Existing Attempt snapshots are never
  upgraded or rewritten.
- An authenticated Manager may apply feedback only to an Attempt it currently
  owns and only when the supplied Task revision, Attempt ID, applied
  `submission_ref`, and immutable `candidate_ref` identify the exact current
  open Task submission. The Attempt must be unreleased and in `submitted` or
  `needs_correction` state. Any stale or mismatched anchor is rejected before
  the Attempt changes.
- Scoped manager feedback names an actionable finding from the exact current
  assigned review result. The Store retains the assignment, reviewer, result
  Operation, finding, and candidate identity with the feedback observation.
  The manager's feedback changes the Attempt to `needs_correction` and enters
  the existing durable mailbox path; it does not send native input, accept the
  Task, start repair, or publish a candidate.
- The local Operator and current GM keep their existing guarded feedback
  authority. Their v1 behavior, including retaining stale feedback as
  historical evidence, is unchanged. A Manager who does not own the exact
  current Attempt has no new authority under this edition.
- `review.submit` remains assigned-auditor evidence only. The assigned auditor
  cannot apply feedback or change Task/Attempt state. A late result may be
  retained against its original assignment and candidate, but cannot move the
  current review slot or affect a replacement candidate.
- Correction starts only through the existing owner-controlled path after the
  manager feedback is retained. Acceptance and publication remain separate
  explicit decisions over an exact candidate.

The complete section above is the frozen v2 policy text. Its UTF-8 bytes with
LF line endings, excluding the following paragraph, are identified by the
SHA-256 digest recorded in `src/policy.rs`. Editing the document does not
change the meaning of existing Attempt snapshots; a policy change requires a
new explicit edition and digest.
