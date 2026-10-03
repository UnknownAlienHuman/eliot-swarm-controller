# Task owner policy and source-indexed brief

New Attempts must be claimed under an explicit accepted owner-policy edition. A
TaskSpec selects it with `owner_policy_id`; the accepted values are
`owner-policy-v1` and `owner-policy-v2`. The selection is an identifier, not a
date or a file-mtime lookup. Creating and revising Tasks without a policy
remains readable for legacy compatibility, but such a Task cannot start a new
Attempt.

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

Owner policy v2 is frozen in
[`owner-policy-v2.md`](owner-policy-v2.md). Its identity is
`policy_id=owner-policy-v2`, `edition=2`, document path
`docs/owner-policy-v2.md`, section
`Owner policy v2 — scoped manager review disposition`, and SHA-256
`a3490caa5ee7afa435dd0a4a317917a88b99c746f477a304de9458d45553b5db`. Its
scoped manager right applies only to an Attempt whose complete frozen policy
record matches this accepted edition. An authenticated Manager may apply an
actionable finding from the exact current assigned review result only to the
current open Task revision and the unreleased Attempt they own, with exact
submission and candidate anchors. The existing Operator/current-GM path stays
available under both editions. Owner policy v1 and Attempts with legacy or
unrecognized snapshots do not gain manager feedback rights. No existing
Attempt is migrated when v2 is added.

Each new Attempt freezes the selected policy identity, the Task revision, the
dependency acceptance receipts, and a deterministic brief in its existing
`task_snapshot`. The brief is a projection of the revision's objective, phase,
requirements, dependencies, scope, acceptance fields and ordered source index.
It does not create workflow rules or replace the source specification. The
attempt read projection exposes the frozen `owner_policy` and `task_brief`;
policy projection recognizes both frozen editions and continues to report
legacy-unknown and unrecognized records without assigning them a new edition.

`task.get` and `task.list` expose the same normalized `task_brief` while keeping
the raw stored `spec`. A historical spec that cannot be decoded remains
readable with `task_brief.status=unavailable` and
`reason=stored_task_spec_unreadable`. This does not relax validation for a new
Attempt or invent missing source or objective fields.

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
