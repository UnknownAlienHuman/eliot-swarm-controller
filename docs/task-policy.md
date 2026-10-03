# Task owner policy and source-indexed brief

New Attempts must be claimed under an explicit accepted owner-policy edition. A
TaskSpec selects it with `owner_policy_id`; the currently accepted value is
`owner-policy-v1`. The selection is an identifier, not a date or a file-mtime
lookup. Creating and revising Tasks without a policy remains readable for
legacy compatibility, but such a Task cannot start a new Attempt.

The accepted edition is Owner policy v1 in
[`owner-decisions.md`](owner-decisions.md). Its frozen identity is
`policy_id=owner-policy-v1`, `edition=1`, document path
`docs/owner-decisions.md`, section `1. Owner policy v1 — resolves issue #14`,
and SHA-256
`786df3cddc329ceac270442e1e825e2af37122b4902ecf07410d3a85a4d470dc`. The digest
covers the UTF-8 bytes of that complete section, with LF line endings and
without the following section heading. Updating the accepted policy requires
a new explicit edition identity and digest in code; editing the document does
not silently change what an existing Attempt means.

Each new Attempt freezes the selected policy identity, the Task revision, the
dependency acceptance receipts, and a deterministic brief in its existing
`task_snapshot`. The brief is a projection of the revision's objective, phase,
requirements, dependencies, scope, acceptance fields and ordered source index.
It does not create workflow rules or replace the source specification. The
attempt read projection exposes the frozen `owner_policy` and `task_brief`;
Doctor reports counts for accepted, legacy-unknown and unrecognized policy
records, plus the known accepted edition identity.

`source_index` entries preserve source order and have a `source_ref`, status,
and optional revision, exact text and SHA-256 digest. A `selected` entry must
carry a nonempty revision, exact text and a matching lowercase SHA-256 digest.
A `gap` entry retains any known text and records why selection or parsing is
incomplete. Unknown or malformed source comments remain gaps; the projection
does not interpret them or invent instructions. Legacy `source_refs` remain
accepted and are projected as explicit gaps because they have no pinned
revision or content. Digests are computed over the exact UTF-8 text bytes.

Historical Attempt snapshots are not rewritten. A snapshot with no recorded
policy projects as `legacy_unknown`; an unrecognized recorded policy remains
unrecognized. Neither case is assigned the current edition retroactively.
