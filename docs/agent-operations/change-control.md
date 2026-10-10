# Repository change control

Issue [#108](https://github.com/UnknownAlienHuman/eliot-swarm-controller/issues/108) defines the operating order; [#110](https://github.com/UnknownAlienHuman/eliot-swarm-controller/issues/110) owns this governance slice. The diagnostic baseline is `fdc8736428df981670a93023890bcc5acbde2df0`. This document governs repository changes; #112 owns reconciliation of the existing product-policy documents and artifact ledger.

## Ownership and candidate lifecycle

Research belongs in an Issue or merged documentation. An implementation PR represents a current candidate for one connected edge and one serialized track. Qualification is a later phase over frozen integrated source. Source review never establishes native/load acceptance.

One manager owns one clean branch/worktree and at most one active implementation PR. The manager posts the exact base-owned path claim on the owning Issue before editing. Keep one active write lease in the worktree; other agents read source or return patch proposals. Agents do not create branches, run Cargo, publish, merge, close Issues or change policy. Preserve unexpected dirty files and stop.

```text
claim scope -> trace caller/authority/fact/consumer -> patch intent
-> implement connected edge and remove replaced responsibility
-> manager review -> scoped rustfmt/minimal Clippy or tooling syntax
-> freeze head SHA -> independent review at that SHA -> required PR checks
-> owner squash merge -> retire merged branch
```

Any source or base update invalidates the frozen SHA and its review. Managers integrate serially and inspect every changed hunk. After two failed approaches at one boundary, release the write lease and publish a causal audit. Stop on unowned files or an unexpected cross-track dependency. An unknown native effect requires readback, never replay from chat intent.

Progress records contain `owner / worktree ID / base SHA / head SHA / files / exact completed edge / last gate / blocker / next bounded action`. Avoid credentials and secret local paths.

## Base authority and required checks

The contract follows `.github/pull_request_template.md`: one marker and one strict fenced JSON object, exact repository-relative files and literal prefixes, explicit test/native/load dispositions, rollback boundary and exact-SHA independent review records. Prefixes use literal `starts_with`, including deliberate family prefixes such as `crates/swarm-adapter-`; there are no glob rules. Absolute paths, traversal, backslashes, control characters, duplicate JSON keys and contradictory declarations fail.

`.github/eliot-change-policy.json` at the event's **base SHA** is the path authority. A contract selects a subset of an enabled track. Candidate policy changes cannot authorize the same candidate. Sensitive workflows, validators and policy require the dedicated governance Issue. Shared Store/contract/migration files require the integration track and an owner decision. Decision Issues must be open, authored by the repository owner and explicitly allowlisted in that base track; citing an unrelated owner Issue gives no exception. No generated-path exclusions are currently authorized.

Only governance #110 is enabled in the bootstrap policy. Other entries reserve the scheduling order from #108; they are locked and do not claim that their path inventory is complete. After bootstrap installation, the owner authorizes a separate governance candidate with source-reviewed scope/Issue mappings to enable the next track. Missing paths require that explicit base-policy change; no broad `crates/` grant or compatibility bridge. Host-contract waits for B1 shared files and seams. #113 remains read-only, #111 consumes its provenance inventory, and #112 gets its own docs candidate.

`Governance Guard` runs on `pull_request_target` with read-only contents/PR/Issue permissions. It checks out and executes **only trusted base code**. PR bodies, head identities and filenames are API data, never scripts or actions. The candidate `PR Gate` runs on ordinary `pull_request`; the contract job also uses the base validator/policy. Both workflows include edited-body events and bind validation to the current head, base and body, checking for drift again before returning.

`PR Gate` always aggregates the current docs/tooling/JS/rustfmt/scoped-Rust/donor jobs and contract validation. A failed or cancelled dependency fails the gate; an applicable skipped job fails too. Full qualification remains manual. Neither contract metadata nor later documentation-only CI upgrades earlier product evidence.

Open implementation PRs are scanned in bounded API pages. Same manager, same track or overlapping requested files/prefixes conflict; the oldest valid PR retains ownership using creation time and PR number. **Every colliding candidate fails approval**, including the oldest, until competing claims are released. An older candidate must not become green while a newer sibling keeps an earlier cached success. Dependencies require merge before implementation approval and never permit concurrent edits. An unreadable/malformed implementation claim or incomplete pagination fails closed. Existing draft handoffs without a contract are ignored as research inventory awaiting #111; a draft with a contract is an implementation claim.

Collision results describe the inventory observed by that run. A read-only check cannot revoke a sibling's cached result; it therefore never grants fresh approval in the presence of any conflict. After the losing claim is released, rerun the owner candidate. Continuous inventory changes and the exact GitHub association of the target-workflow context with head/review evidence still require later qualification. The guard does not create a second task database or send updates to other PRs.

Review records require `reviewer_role / reviewed_head_sha / reviewed_paths / findings / disposition`. Approved records must cover every changed path, including the previous name of a rename, and contain no unresolved findings. Role naming records process independence; actions made under one GitHub owner identity cannot prove it cryptographically.

## Bootstrap and owner publication

The baseline contains neither the trusted validator nor the policy. The first governance PR therefore has an expected failed base-contract check and no installed `Governance Guard` context. It must remain a bootstrap candidate until the owner reviews its exact SHA and explicitly decides to install it by squash merge. There is no fallback to candidate-owned policy and no automatic merge. A green bootstrap authorization would manufacture trust that does not exist.

The owner's explicit instruction on 2026-10-10 supersedes the protection step in #108/#110: `main` remains unprotected. Do not install branch protection or repository rulesets, and do not make their activation a prerequisite for source work. The manager and owner follow the single-slice, path-ownership and exact-SHA review protocol when publishing and merging.

After installing the reviewed bootstrap, verify that both contexts actually appear on a subsequent ordinary PR. Use `Governance Guard` and `PR Gate` as evidence for the owner publication decision. Keep one squash merge per rollback boundary; auto-merge remains off. Product writers resume after the governance bootstrap and the relevant source dependencies are accepted and their exact scopes are enabled. Installed/native qualification remains a later phase opened explicitly by the owner.

## Deferred qualification cases

Current-phase checks are syntax/parsing and ordinary actual PR CI. The following named cases are reserved for later qualification, without a standalone campaign now:

- canonical exact-SHA contract; undeclared/forbidden path; self-granted prefix;
- duplicate keys, malformed shapes, traversal/absolute/backslash paths;
- candidate-modified policy/validator cannot authorize itself;
- rename old-path coverage and stale head/base/body/review;
- same-manager/track/path collisions, draft implementation versus research, incomplete pagination;
- scope exception without a base-authorized owner decision, unmerged dependencies;
- applicable skipped/failed/cancelled jobs.

Acceptance for this owner-adjusted #108/#110 slice remains partial until exact-SHA review, installed trusted contexts and deferred collision qualification exist. Repository protection and direct-push rejection are waived by the owner instruction. No Issue is closed by an agent.

GitHub semantics were checked on 2026-10-10 against the official [workflow event reference](https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows#pull_request_target), [job dependencies](https://docs.github.com/en/actions/reference/workflows-and-actions/workflow-syntax#jobsjob_idneeds) and [paginated PR files API](https://docs.github.com/en/rest/pulls/pulls#list-pull-requests-files). The validator uses REST API version `2022-11-28`, 100 items/page, at most 30 file pages, six open-PR pages per inventory scan and 80 requests/run; API failures or bounds stop approval.
