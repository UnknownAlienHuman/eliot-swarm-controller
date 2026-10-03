# ELIOT Agent Communication — Field Evidence and Donor Map

**Revision:** 1 — 2026-10-02  
**Repository baseline:** `3ecdf52707731e3f85e85827a88fbdb28d784f3e`  
**Normative companion:** [Agent Communication and Concilium](agent-communication-concilium.md)  
**Evidence status:** source/issue/paper review. No donor was installed or load-tested by this review.

## 0. Purpose

This document records why the communication contract has its current shape, what concrete donor units are useful, what must not be copied, and which field failures must become acceptance tests.

It is deliberately separate from the normative contract:

- the normative document says what ELIOT must do;
- this document says what evidence led to that decision;
- an issue or paper does not become a shipped capability merely because it is cited;
- source review does not substitute for live qualification.

Evidence labels:

```text
CODE        exact source path and behavior
DOC         official documentation
RELEASE     exact stable tag/release metadata
ISSUE       concrete bug/report/proposal
USER        user experience without full independent reproduction
PAPER       research result under its stated setup
INFERENCE   ELIOT design conclusion from several sources
UNVERIFIED  plausible path without the required end-to-end evidence
```

## 1. Corrected high-level conclusion

The safest communication system is not a better free-form group chat.

The strongest pattern across donors and reports is:

```text
addressed mail
  + exact identity/generation
  + immutable proposals/evidence
  + sparse communication graph
  + deterministic progress/termination
  + manager-owned authority
```

The most dangerous recurring pattern is:

```text
shared full transcript
  + implicit speaker selection
  + prose as workflow trigger
  + automatic model wake
  + consensus/majority as authority
```

ELIOT therefore adopts **coordination mail and structured dissent**, not autonomous social chat.

## 2. How agents actually behave when allowed to talk freely

### 2.1 Politeness and agreement loops

**ISSUE — AutoGen #108, #907, #7409.** Users report two-agent and group-chat runs continuing through blank messages, polite acknowledgements and repeated “how else can I help” turns until the hard reply/round limit. The proposed diagnosis in #7409 is an “infinite agreement loop”: no semantic progress is required, so agents can consume the budget while merely agreeing.

Relevant reports:

- <https://github.com/microsoft/autogen/issues/108>
- <https://github.com/microsoft/autogen/issues/907>
- <https://github.com/microsoft/autogen/issues/7409>
- <https://github.com/microsoft/autogen/issues/538>

**ELIOT consequence:** a communication round advances only when at least one machine-visible progress fact changes:

```text
new proposal revision
new evidence reference
new counterexample/falsifier
formally narrowed disagreement
manager decision
```

`thanks`, `agree`, repeated status, an empty message or a paraphrase of the same proposal does not advance a round. It may be retained as text, but it cannot trigger another model call.

### 2.2 Full-history amplification

**ISSUE — AutoGen #1006, #1070, #4623.** Users report that group-chat participants receive the full growing history and cannot reliably restrict a participant to the final relevant input. The graph-flow proposal explicitly identifies that “every agent sees every message” violates context independence.

- <https://github.com/microsoft/autogen/issues/1006>
- <https://github.com/microsoft/autogen/issues/1070>
- <https://github.com/microsoft/autogen/issues/4623>

**FIELD — ELIOT legacy swarm.** The supplied manager brief records managers sending 260–360 thousand input tokens per step, writers reaching 110–180 thousand, and one Codex thread processing roughly 1.28 billion input tokens during a long session. Repeated reminders and Issue history were part of that growth. The operational fix was fresh context per assignment, compact Task projections, narrow reading and short writer reports.

**ELIOT consequence:** no participant receives an accumulated conversation transcript by default. A participant receives:

```text
thread header
current decision question
latest proposal(s)
selected evidence references
unresolved objections
response schema
```

Historical messages stay pullable by cursor. Large bodies remain immutable artifacts.

### 2.3 Broadcast creates accidental work and queue pressure

**ISSUE — AutoGen #489.** A user questions why group-chat messages are broadcast to all agents instead of only the selected speaker and notes that model servers may queue every incoming message.

- <https://github.com/microsoft/autogen/issues/489>

**ISSUE — MCP Agent Mail #264.** A liveness subsystem produced 24,132 of 26,092 mailbox rows in one deployment. Activity/status chatter became about 92% of all mail and consumed roughly one CPU core while targeting idle/dead identities.

- <https://github.com/Dicklesworthstone/mcp_agent_mail_rust/issues/264>

**ELIOT consequence:**

- no default broadcast;
- no durable liveness/status mail;
- health is a compact replaceable Observation/projection;
- a message has exact recipients;
- a small freshness notification causes a pull, not body broadcast;
- no registered agent owns a dedicated idle polling task.

### 2.4 Comment-driven routing causes token ping-pong

**ISSUE — Multica #8753.** A real Developer → Reviewer → Developer → Reviewer → Product Owner flow processed about 18 million tokens. Coordination comments caused full coding-agent invocations, long sessions were repeatedly resumed, and failures generated further explanatory turns.

- <https://github.com/multica-ai/multica/issues/8753>

The donor audit records the same causal distinction: ordinary status/failure comments entered the squad-leader fallback and triggered coordination-only model turns.

**ELIOT consequence:**

```text
message/comment
    != work intent
    != task transition
    != model invocation
```

A manager must explicitly turn a coordination result into a Task/Assignment/Operation.

### 2.5 Speaker selection is an unreliable control plane

**ISSUE — AutoGen #842, #6523, #7677/#7678.** Reports include speaker-name normalization failures, graph edges not selected when tools are involved, and round-robin omissions.

- <https://github.com/microsoft/autogen/issues/842>
- <https://github.com/microsoft/autogen/issues/6523>
- <https://github.com/microsoft/autogen/issues/7678>

**ELIOT consequence:** the host determines the exact planned participant and round from durable state. No LLM chooses who may speak next. The model supplies a position; the controller supplies the routing.

### 2.6 One malformed message can poison a group

**ISSUE — AutoGen #3679, #6479.** Invalid tool-call history and one empty message caused failures to cascade through group participants.

- <https://github.com/microsoft/autogen/issues/3679>
- <https://github.com/microsoft/autogen/issues/6479>

**ELIOT consequence:** admission validates each participant response independently. A malformed response becomes that participant's failed/invalid round result. Other positions remain usable, and no malformed body is copied into every participant context.

### 2.7 Goal drift is silent

**ISSUE — AutoGen #7487.** A contributor describes a structural conflict between coordinating work and preserving the original mission. Traffic management can remain apparently successful while the result drifts from the original objective.

- <https://github.com/microsoft/autogen/issues/7487>

**ELIOT consequence:** the immutable Task/Attempt snapshot is the goal-integrity authority. Concilium receives the exact decision question, requirements and non-goals from that snapshot. Peers cannot rewrite the goal through conversation. A manager/independent verifier checks the result afterward.

### 2.8 Backpressure must not be discovered through retry storms

**ISSUE — AutoGen #7321.** The report describes cascading retries when an agent is saturated and capacity is implicit in each caller.

- <https://github.com/microsoft/autogen/issues/7321>

**ELIOT consequence:** coordination admission reads existing capacity/attention state. A saturated recipient produces a durable queued/deferred fact or deadline outcome; senders do not independently spin retry loops. The provider of a communication endpoint owns its declared bounds.

## 3. Debate and consensus research

### 3.1 Homogeneous debate can reduce quality while increasing cost

**PAPER — The Cost of Consensus (arXiv:2605.00914).** In the reported setup, ten homogeneous agents and three rounds showed:

- modal adoption up to 85.5%;
- contextual-fragility vulnerability up to 70%;
- oracle gap up to 32.3 percentage points;
- high conformity even with only two peer rationales;
- 2.1–3.4× token use for equal or lower accuracy than isolated self-correction.

<https://arxiv.org/abs/2605.00914>

**ELIOT consequence:** do not call a Concilium merely to obtain more votes. A bilateral exchange or isolated review is preferred until a material incompatible contract is established.

### 3.2 Answer agreement can hide reasoning disagreement

**PAPER — The Consistency Illusion (arXiv:2606.08457).** The study reports that answer-level consensus can increase while semantic reasoning alignment decreases. Its Grounded Debate Protocol improves alignment by requiring named facts and explicit stances on claims without adding model calls.

<https://arxiv.org/abs/2606.08457>

**ELIOT consequence:** positions use named claims, stances and evidence—not a final “yes/no” vote only.

Proposed position item:

```json
{
  "claim_id": "claim-operation-identity",
  "stance": "support | oppose | uncertain",
  "fact": "The native input ID is deterministic from the Operation ID.",
  "evidence_refs": ["artifact-..."],
  "counterexample": null,
  "falsifier": "A retained Operation maps to a second native input ID.",
  "assumptions": ["one binding generation"],
  "confidence": "medium"
}
```

This is an inspectable decision trace. It is not private chain-of-thought storage.

### 3.3 Consensus itself is not a safe objective

**PAPER — Free-MAD (arXiv:2509.11035).** The paper identifies multi-round token overhead, conformity/error propagation and unfair/random majority voting, then evaluates a consensus-free trajectory-scoring design.

<https://arxiv.org/abs/2509.11035>

**PAPER — Emergence of Biased Consensus (arXiv:2608.02827).** The reported experiments model a conformity threshold beyond which collective bias emerges; heterogeneous agents reduce that effect in the tested settings.

<https://arxiv.org/abs/2608.02827>

**ELIOT consequence:**

- no majority-vote ratification;
- no automatic “consensus reached” authority;
- preserve minority reports;
- record participant/runtime correlation;
- manager chooses against requirements and evidence, not popularity.

### 3.4 Sparse communication is usually cheaper

**PAPER — AgentPrune / Cut the Crap (arXiv:2410.02506).** The paper treats communication redundancy as a graph-pruning problem and reports 28.1–72.8% token reduction in its benchmark setup.

<https://arxiv.org/abs/2410.02506>

**PAPER — Adaptive Graph Pruning (arXiv:2506.02951).** The paper adapts both agent count and topology and reports more than 90% token reduction in the tested benchmarks.

<https://arxiv.org/abs/2506.02951>

**PAPER — CONCAT (arXiv:2605.29612).** The method clusters initial answers, selects leaders and prunes predicted low-benefit interactions; the reported experiments show up to 2.02× efficiency and 50.1% latency reduction on one setup.

<https://arxiv.org/abs/2605.29612>

**PAPER — HCP-MAD (arXiv:2604.09679).** The design starts with a heterogeneous pair, stops easy cases early and escalates only unresolved cases.

<https://arxiv.org/abs/2604.09679>

**ELIOT consequence:**

```text
one exact owner question
  -> bilateral thread
  -> one independent reviewer only when needed
  -> Concilium only for unresolved material conflict
```

Never construct all-to-all agent communication as the default topology.

## 4. Identity and state failures from MCP Agent Mail

MCP Agent Mail is a valuable UX/source donor, but its field reports show why ELIOT must reuse its own durable authority rather than import another product.

### 4.1 Registration is not liveness

**ISSUE #334.** `last_active_ts` was not updated after registration, so the system selected a recently registered but idle agent instead of the active owner. Deferred “touch” work could also be orphaned across pool generations.

<https://github.com/Dicklesworthstone/mcp_agent_mail_rust/issues/334>

**ELIOT consequence:** recipient selection uses current ELIOT assignment/binding/generation and observations. Registration time is never treated as current activity.

### 4.2 Unknown recipient must fail closed

**ISSUE #301.** Sending to an unknown recipient silently created a placeholder identity despite documentation promising fail-fast behavior.

<https://github.com/Dicklesworthstone/mcp_agent_mail_rust/issues/301>

**ELIOT consequence:** coordination cannot create a client, Assignment or participant as a side effect. Unknown/stale generation is `NOT_FOUND`/`STALE_ACTOR`.

### 4.3 “Released” must be read back

**ISSUE #329.** A CLI shape mismatch caused reservation release to do nothing while the agent was told that it had released the reservation.

<https://github.com/Dicklesworthstone/mcp_agent_mail_rust/issues/329>

**ELIOT consequence:** `code.scope.release` returns the exact prior/current scope revision and readback status. Text saying “released” is not evidence.

### 4.4 Humanized timestamps are not machine contracts

**ISSUE #330.** Machine JSON exposed only humanized grant age, preventing reliable age calculations.

<https://github.com/Dicklesworthstone/mcp_agent_mail_rust/issues/330>

**ELIOT consequence:** every deadline, creation, update and expiry is an integer epoch-ms fact. Human rendering is derived separately.

### 4.5 Direct-storage pollers can destroy capacity

**ISSUE #298.** Mixed lock protocols, retained zombie work and direct-storage pollers could reduce admission capacity to zero.

<https://github.com/Dicklesworthstone/mcp_agent_mail_rust/issues/298>

**ELIOT consequence:** clients never open SQLite. All reads go through Store projections. Diagnostic scans are separate, bounded and lower priority than message/reply admission.

### 4.6 Reservation reads and guards must not fail open

**ISSUE #337.** Reservation reads took 5–20 seconds under load; pre-commit guards timed out and effectively failed open. The report also identifies write amplification and stale backup companions.

<https://github.com/Dicklesworthstone/mcp_agent_mail_rust/issues/337>

**ELIOT consequence:**

- scope conflict is a bounded indexed projection;
- optional Git scanning is outside the Store transaction;
- timeout returns `coverage=unknown`, never “no conflict”;
- candidate verification decides whether unknown conflict coverage blocks a project gate;
- no global Git hook is installed automatically.

### 4.7 Read-only status must not be starved by repair scans

**ISSUE #138, #274, #319.** Reports describe expensive overview queries, Doctor scans starving dispatch and circuit-breaker policy that blocked reservation reads despite being documented as write-only.

- <https://github.com/Dicklesworthstone/mcp_agent_mail_rust/issues/138>
- <https://github.com/Dicklesworthstone/mcp_agent_mail_rust/issues/274>
- <https://github.com/Dicklesworthstone/mcp_agent_mail_rust/issues/319>

**ELIOT consequence:** tiny coordination reads and native reply paths have separate bounded work classes. One corrupt optional projection cannot disable exact Task/native response reads.

### 4.8 Storage failures are a reason not to adopt the whole product

**ISSUE #278, #333.** Reports include WAL/storage corruption under concurrency and 2,011 leaked file descriptors to the main SQLite file in 3.5 hours.

- <https://github.com/Dicklesworthstone/mcp_agent_mail_rust/issues/278>
- <https://github.com/Dicklesworthstone/mcp_agent_mail_rust/issues/333>

**ELIOT consequence:** do not add a second database/server stack for mail. Keep one SQLite owner and existing bounded IPC.

## 5. Exact donor adoption map

## 5.1 CCCC v0.4.41

### Take as protocol semantics

| Source unit | Take |
|---|---|
| `crates/cccc-core/src/connect_delivery.rs` | exact delivery ID, sender/recipient generation, absolute deadlines, bounded recipients/attachments, digest-bound cancellation, participant-bound replies, previous-membership rejection |
| `crates/cccc-core/src/inbox.rs` | cursor + pending-read recovery pattern; consumption is separate from storage |
| connect contracts | `stored/claimed/accepted/failed/ambiguous`, reply/cancel reference shapes |

### Do not take

- group/task/runtime authority;
- append-only ledger implementation;
- PTY/TUI delivery topology;
- whole bridge/bootstrap process model;
- default autonomy/approval profiles.

### Translation to ELIOT

```text
CCCC delivery_id              -> existing ELIOT delivery_id
CCCC actor generation         -> existing client/binding generation
CCCC message digest           -> existing payload_digest
CCCC reply/cancel validation  -> extend existing message contract
CCCC mailbox cursor           -> existing observation cursor
CCCC attachments              -> ELIOT immutable artifact refs
```

No CCCC crate is required for C1–C5.

## 5.2 MCP Agent Mail Rust, commit `21a25c2bcfd20c9b31bcb109c294d17411eb5ba1`

### Take as UX/design only

- mail, not chat;
- exact recipients/subject/thread;
- inbox/outbox pull views;
- no default broadcast;
- advisory path reservation with overlap report;
- TTL as reminder, not ownership proof;
- pre-commit conflict-warning concept;
- messages outside model context until requested.

### Do not copy code

- workspace is a large parallel product (`mcp-agent-mail-db`, `server`, `tools`, `guard`, `tui`, search/indexing and more);
- it carries a custom `LicenseRef-MIT-Rider` with OpenAI/Anthropic restrictions;
- current field issues expose a separate storage/liveness/recovery surface;
- ELIOT already owns Operations, clients, SQLite and artifacts.

### Useful source/issue anchors

- <https://github.com/Dicklesworthstone/mcp_agent_mail_rust>
- `README.md`: mail metaphor, threads, reservations, no broadcast
- `crates/mcp-agent-mail-tools/src/reservations.rs`
- `crates/mcp-agent-mail-tools/src/messaging.rs`
- `crates/mcp-agent-mail-db/src/queries.rs`
- `crates/mcp-agent-mail-guard/src/lib.rs`

The paths are research anchors, not authorized donor copies.

## 5.3 Claw Orchestrator, commit `ffa595abfb6f6671ac995041f9c44f5e1a67f50f`

### Take

From `src/kernel/nodes/council.ts`:

- council requires a session manager;
- exact named participants and engine/model configuration;
- `maxRounds`;
- per-agent timeout/turn limits;
- total budget;
- cancellation signal;
- compact context/final summary;
- reaching max rounds is a valid result;
- votes are advisory;
- verification remains downstream.

### Do not take

- its workflow store as a second ELIOT authority;
- automatic node replay for model/external effects;
- regex consensus as an acceptance mechanism;
- project directory write access as a default council privilege.

## 5.4 Multica v0.6.1

### Take

- the negative field evidence from issue #8753;
- exact native Codex steer separately from ordinary communication;
- capability/version distinction;
- structural versus transient failure classification.

### Do not take

- comment/mention as execution routing;
- terminal agent report as Task acceptance;
- automatic approval defaults;
- heavy coding-agent invocation for communication-only transitions.

## 5.5 AutoGen

### Take as field evidence and test cases

- deterministic termination rather than prose `TERMINATE`;
- isolated packets rather than shared full history;
- explicit graph/participant routing;
- capacity/backpressure visibility;
- goal-integrity separation;
- malformed-response isolation.

### Do not take

- free-running GroupChat;
- LLM speaker-selection loop;
- implicit broadcast;
- full transcript in every participant context;
- prompt-only guardrails as application authority.

## 5.6 Google A2A

### Take for future external boundary

- Message is communication, not necessarily Task;
- Task is a stateful work lifecycle;
- Artifact is a work output;
- context IDs correlate without conflating authority.

### Do not take for the local first slice

- another HTTP service/protocol path;
- another Task lifecycle;
- Agent Cards as a replacement for ELIOT client registration.

## 5.7 FIPA ACL

Take a small speech-act vocabulary (`inform`, `query`, `propose`, `accept/reject proposal`, `not-understood`) as semantic inspiration. Do not implement the complete FIPA platform, conversation policy stack or directory service.

## 5.8 Papers

Take design constraints and qualification cases. Do not claim benchmark results transfer directly to ELIOT coding work or to the installed models.

## 6. Behavioral hazard register

| Hazard | Observable symptom | Required ELIOT control |
|---|---|---|
| Politeness loop | repeated agreement/status with no changed fact | deterministic progress fingerprint and no auto-next-round |
| Sycophantic conformity | correct minority adopts confident majority | blind first positions, minority report, no vote authority |
| Reasoning-consensus illusion | same answer, incompatible assumptions | named claims/stances/falsifiers |
| Correlated pseudo-independence | several agents share same model/context lineage | record provider/model/parent context; display correlation |
| Majority dominance | verbose/confident agents overwhelm concise owner | requirements/evidence decide, not token count or votes |
| Goal drift | local solution no longer matches Task | immutable Task packet and independent verification |
| Shared-history pollution | each participant receives all retries/tool chatter | compact packet and pullable history |
| Malformed-message cascade | one empty/invalid response crashes group | per-participant validation and partial result |
| Speaker routing failure | wrong/missing next agent | deterministic plan, no LLM speaker selection |
| Retry/backpressure storm | saturated receiver causes repeated sends | durable queue/deadline, no caller retry loop |
| Stale identity | old client name receives new work | exact generation, no automatic reassignment |
| False release | command reports release without state change | exact readback/revision in result |
| Liveness spam | health chatter dominates mailbox | replaceable observation, not durable mail |
| Placeholder actor creation | typo creates apparent recipient | fail closed on unknown participant |
| Expired TTL treated as safety | resource deleted/reassigned while work lives | TTL is advisory; owner disposition required |
| Diagnostic starvation | Doctor/overview blocks replies | separate bounded lower-priority work class |
| Peer instruction injection | message interpreted as Task order | typed speech acts; no peer assignment method |

## 7. Concilium must optimize for dissent, not consensus

The word Concilium is retained as the user-facing name, but the internal objective is:

```text
structured independent positions
  -> explicit disagreement map
  -> evidence-backed recommendation or unresolved result
```

It is **not**:

```text
majority vote
social consensus
most verbose answer wins
repeat until everyone says yes
```

### Required participant metadata

```json
{
  "participant": "client-id",
  "generation": 7,
  "assignment_role": "producer_owner",
  "runtime": "codex",
  "provider_family": "openai",
  "model_family": "gpt-6",
  "parent_context": "thread-or-session-ref",
  "evidence_scope": ["contract-x", "path-y"]
}
```

This metadata does not rank models. It tells the manager whether three “independent” positions are actually correlated.

### Required result classes

```text
recommended
minority_report
insufficient_evidence
irreconcilable_contract
cancelled
failed
```

`unanimous` may be displayed as a fact but grants no authority.

## 8. What counts as new information

A new round or a continued bilateral exchange requires at least one changed digest:

```text
proposal_digest
evidence_set_digest
counterexample_digest
requirements_revision
scope_revision
manager_question_revision
```

A changed prose paraphrase alone is not progress.

The host records one `coordination_loop` attention item per stable loop fingerprint. It does not generate recurring reminders or model turns.

## 9. Operational metrics

Measure the system by coordination value, not chat volume:

```text
threads opened/resolved/unresolved
median messages to resolution
proposal revisions
new-evidence ratio
repeated-pattern incidents
model turns caused by coordination (target: zero unless manager explicitly requests)
bytes/tokens delivered per participant packet
full-thread reads versus header reads
Concilium proposals/opened/declined
rounds and participants per Concilium
minority reports preserved
scope conflicts detected before integration
false conflict/unknown coverage
stale-generation deliveries rejected
```

A high message count is not throughput. A “consensus rate” is not correctness.

## 10. Acceptance additions derived from field evidence

1. Five `agree/thanks/status` messages with no new digest create no new model turn and one loop attention item.
2. One malformed participant response leaves other positions readable.
3. Three participants from the same parent/model family are shown as correlated, not three independent votes.
4. A correct minority objection remains in the final result even when every other participant supports one proposal.
5. Same final recommendation with contradictory named claims is reported as reasoning misalignment.
6. Unknown recipient/generation fails; no placeholder is created.
7. A release call whose readback still shows active scope is not `Applied`.
8. Absolute timestamps remain machine-readable; humanized age is presentation only.
9. Liveness updates do not create mailbox rows.
10. Diagnostic/Doctor work cannot prevent a pending native reply from admission.
11. Slow reservation/Git inspection yields `coverage=unknown`, never `no conflict`.
12. A full-history request is explicit and paged; it is not automatically included in a participant packet.
13. Concilium cannot continue after max rounds without manager action.
14. No speaker-selection LLM call occurs.
15. A participant timeout produces partial advisory output, not group failure or implied agreement.

## 11. Final donor decision

```text
Build locally on ELIOT:
  typed direct coordination
  immutable contract proposals
  advisory code scopes
  bounded Git inspection
  manager-sponsored Concilium

Reuse conceptually:
  CCCC delivery semantics
  MCP Agent Mail UX
  Claw advisory council boundary
  A2A Message/Task/Artifact distinction
  FIPA speech-act vocabulary
  sparse/debate research constraints

Do not vendor:
  CCCC scheduler/ledger
  MCP Agent Mail code/storage
  Claw workflow store
  AutoGen GroupChat
  any distributed broker for the first local slice
```

The next implementation document specifies exact ELIOT methods, schemas, Rust files and test boundaries.