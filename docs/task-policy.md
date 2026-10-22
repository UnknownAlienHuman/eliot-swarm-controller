# Task policy, specification and model-visible brief

This document defines the target boundary. Current source still retains historical
owner-policy editions and acceptance branches; [implementation](agent-operations/implementation.md)
owns their finite removal. [Status](implementation-status.md) records what actually
works.

## 1. Three different objects

### Task specification

The versioned product request:

```text
goal/objective
observable requirements
selected source references/revisions
dependencies
write/forbidden paths
deliverable
per-requirement verification policy
```

Changing product meaning creates a new Task revision. Existing Attempts keep their
frozen snapshot.

### Workflow/manager policy

How an authorized Manager/Governor agent may execute the Task:

```text
route/fallback/profile
permission/sandbox policy
session reuse/rotation
reviewer/check requirements
correction/no-progress budget
selected publication/integration behavior
```

Policy affects future admissions. It does not rewrite historical authorship,
candidates or results and is not embedded as a document hash capability.

### TaskPacket

A compact model-visible rendering of the frozen specification and current work:

```text
Goal
Requirements + meaningful verification criteria
Initial | continuation | correction state
Selected sources/files/symbols
Write/forbidden paths
Worktree/branch/baseline
Selected route/model/effort
Deliverable
Verification commands/evidence expected
```

TaskPacket is a projection, not authority. Store keeps Task/Attempt/Operation/binding
identity and the frozen requirement evidence plan. The model does not need controller
proof JSON, package receipts or digest chains.

## 2. Current authority target

One current policy checks:

- authenticated role/client;
- exact Task/Attempt/assignment/candidate/review/resource scope;
- expected revision/generation/boot;
- action permitted by current Manager/GM/project policy.

A document path, heading, SHA or historical policy edition is not another permission.
Manager/Governor is an agent role and uses ordinary scoped rights; operator approval is
not required for routine owned planning/execution.

Reviewers retain exact assigned candidate/source/check access without Manager
authority. Participants/children use scoped grants and never receive Manager/operator
credentials.

Current `policy.rs` still recognizes owner-policy v1/v2 and pins document editions;
current acceptance retains legacy/V2 branches. These are implementation gaps, not new
target requirements. Preserve concrete unsettled records/history, then remove policy
hash gates and old executable branches together.

## 3. Requirements and evidence

The current V2 `requirement_evidence_plan` is retained behavior, not bureaucracy to
delete. Every requirement must name at least one accepted evidence source, such as:

- exact CheckProfile/CheckRun;
- assigned independent review coverage;
- retained artifact/result/property readback;
- explicit selected manual/external evidence only where no executable source exists.

Freeze the plan in the Attempt snapshot. Review assignment and acceptance validate the
same plan for the exact candidate. A new submission ID, prose confidence or absence of
new failures does not prove a requirement. Known unrelated baseline failures may be
reported separately under project policy; they do not waive requirement evidence.

TaskPacket renders concise meaningful criteria for the agent. It does not expose the
full internal JSON plan or remove the criteria from Store.

## 4. Sources and revisions

Selected source entries retain actual reference, revision/time where available,
content or bounded excerpt/artifact reference and source gap. A generated brief cannot
hide an unknown required comment as noise or invent missing instructions.

Manager/Governor reads canonical source once for the current revision and assigns the
necessary subset. Agents fetch detail on demand. Do not inject the full audit/Issue
history into every prompt.

External-source collection does not mutate a working Task automatically. Policy may
select new source facts and admit an explicit Task revision. A webhook timestamp or
label alone is not a revision token.

## 5. Assignment and initial input

`task.claim` reserves one Attempt and freezes specification/policy/evidence plan.
`Attempt.start_owner` selects one initial-input owner:

- `native_manager`: native delegation/spawn owns the initial prompt; controller later
  binds the observed producer;
- `controller`: Eliot opens/attaches and admits one `task.dispatch`.

`START_OWNED_BY_MANAGER` prevents a second controller start before producer binding.
One Attempt has one `start_operation_id`; follow-up/correction is `agent.send`, not a
new initial dispatch. A changed request ID cannot repeat the initial prompt.

## 6. Corrections and policy changes

All actionable findings for one candidate return in one `task.request_changes`
transition. Current owner receives one correction message when live; otherwise the
Attempt remains `needs_correction` and the next resume/dispatch renders it.

A requirement/policy change that alters the requested product creates the appropriate
Task revision/new Attempt. A route retry or worker replacement under already selected
policy does not rewrite Task meaning.

Bound unchanged correction rounds. Relevant new evidence can progress the same source;
new IDs or unrelated edits cannot. After the bound, create one addressed Governor
decision with concrete options, not a permanent human approval step.

## 7. Finite transition

Before removing old policy/prompt/acceptance formats:

1. enumerate concrete unsettled Attempts/Operations and required results;
2. finish/read back under the current owner, export retained evidence or create an
   explicit new revision/Attempt;
3. switch new admissions to the current format;
4. delete old producer, reader, schema/policy branch, config, fixture and package path
   together.

Completed historical records may remain readable as opaque history through existing
projections. They do not keep an executable policy engine or universal converter.

## 8. Verification

- Manager agent can create/revise/claim owned Tasks without operator bearer;
- Task revision freezes specification, policy and evidence plan for its Attempt;
- native-manager and controller initial-start paths cannot both send;
- TaskPacket contains required goal/sources/criteria but no controller proof dump;
- missing required source remains an explicit gap;
- each requirement is covered by retained exact-candidate evidence;
- correction/resume renders current findings without replaying initial prompt;
- policy change affects future admissions and does not rewrite history;
- removing document-edition/legacy acceptance branches leaves current scoped rights
  and evidence semantics connected.
