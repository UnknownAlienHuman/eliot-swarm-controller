Describe the violated invariant and the resulting behavior. Replace every placeholder; declare exact files or literal prefixes, never globs. An unreviewed draft has `reviewed_head_sha: "not_reviewed"` and `reviews: []`; its gate stays closed until the exact head is reviewed.

<!-- eliot-contract -->
```json
{
  "issue": 110,
  "slice": "G110",
  "manager": "replace-manager-id",
  "track": "governance",
  "base_sha": "replace-with-40-hex-base",
  "head_sha": "replace-with-40-hex-head",
  "reviewed_head_sha": "not_reviewed",
  "depends_on_prs": [],
  "owner_decision_issue": 110,
  "scope_exception": null,
  "owned_files": [],
  "owned_prefixes": [],
  "forbidden_files": [],
  "forbidden_prefixes": [],
  "connected_edge": "replace with producer -> retained fact/intent -> effect/readback -> consumer",
  "replaced_or_deleted_responsibility": "replace with exact old path/symbol or none with reason",
  "gates": {"rustfmt": "not_run", "clippy": "not_run", "syntax": "not_run"},
  "tests": "not_run",
  "native": "not_run",
  "load": "not_run",
  "residual_uncertainty": "replace with exact remaining uncertainty or none with evidence",
  "rollback": "replace with one squash revert and migration caveat",
  "reviews": []
}
```

Append independent records to `reviews` after freezing the head:

```json
{"reviewer_role":"independent-step5","reviewed_head_sha":"40-hex-head","reviewed_paths":["exact/path"],"findings":[],"disposition":"approved"}
```

Review records are process evidence; GitHub owner identity does not prove agent independence. Update the contract and obtain fresh review after any source/base update. Link the exact gate evidence; never promote a source check or zero-dispatch run to native qualification.
