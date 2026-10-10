# R08 companion. Watch delivery по роли: Participant inbox, Manager/Operator retained list

**Статус:** обязательное уточнение implementation handoff #34. Production-код ещё не изменён. База `40591a295af94b1541ec2ba30afe8e3247701a71`.

## 1. Audit correction

Подозрение «Manager/Operator может создать watch, но никогда не может прочитать matched notice, потому что `notifications()` требует Participant» в такой форме **опровергнуто**.

Существуют два read path:

```text
Participant
  → coordination.inbox
  → coordination_watch::notifications
  → compact watch_notifications headers

Manager / Operator
  → coordination.watch.list with exact Task/Attempt tuple
  → watch_projection
  → matched record.notification
```

`coordination_watch::read` поддерживает Manager/Operator exact current scope и retained historical subject scope. Program test `exact_historical_submission_review_notifies_owner_manager_after_release` уже доказывает:

- Manager создаёт `submission_reviewed` watch;
- Attempt освобождается и Task revision меняется;
- late review result становится historical;
- reconcile matches watch;
- owner Manager читает exact notification через `coordination.watch.list`.

Следовательно:

- не удалять `principal.require_participant()` из `notifications()`;
- не давать Manager/Operator доступ к participant inbox;
- не создавать новый generic notifications endpoint;
- не объявлять manager notices потерянными.

## 2. Подтверждённый избыточный путь

Reconcile сейчас для **каждого** creator независимо от stored role пишет:

1. scope/creator notice index:

```text
coordination:watch:v1:notice:{scope}:{creator}:{time}:{watch}
```

2. owner transition index:

```text
coordination:watch:v1:owner-notice:{creator}:{time}:{watch}
```

3. notification непосредственно в retained watch record;
4. matched state в том же record.

Оба notice-index семейства читаются только `coordination_watch::notifications`, а эта функция немедленно требует `Role::Participant`.

Manager/Operator `coordination.watch.list` использует отдельный persistent list index created at watch admission and reads notification from the watch record. It never reads notice/owner-notice keys.

Therefore manager/operator match currently incurs:

```text
2 unnecessary meta writes
2 permanently unread index rows
larger stale/retention inventory
misleading delivery="mailbox_header" wording
```

This is confirmed by source search: `notice_prefix` and `owner_notice_prefix` have no reader outside participant `notifications()`.

## 3. Correct role contract

Keep one watch record shape but distinguish delivery projection:

| Stored creator role | Notification surface | Transition behavior |
|---|---|---|
| `participant` | `coordination.inbox.watch_notifications` | current-scope notice index + owner-notice fallback after scope transition |
| `manager` | `coordination.watch.list` exact Task/Attempt | retained list index remains readable under current/historical manager authority |
| `operator` | `coordination.watch.list` exact Task/Attempt | retained list index under local Operator authority |

All roles remain:

- exact creator client ID;
- exact retained Task/revision/Attempt;
- current authorization before projection;
- one-shot;
- no model/native wake;
- no work assignment.

## 4. Minimal implementation

### 4.1 One local delivery mode derived from retained creator role

Add a private enum or match local to watch store, not a public framework:

```rust
enum WatchNoticeSurface {
    ParticipantInbox,
    RetainedWatchList,
}

fn notice_surface(record: &Value) -> Result<WatchNoticeSurface>;
```

Mapping uses `record.creator.role`, already retained and verified:

```text
participant → ParticipantInbox
manager/operator → RetainedWatchList
other → damaged
```

Do not infer role from the current Principal during reconcile; creator role is part of the retained watch admission identity. Current authority is checked separately by existing functions.

### 4.2 Reconcile writes only used indexes

Always:

- construct bounded notification;
- write notification/cursor/state/timestamps to record;
- remove active index.

Only for `ParticipantInbox`:

- write scope notice index;
- write owner-notice transition index;
- retain `notice_index_key` string.

For `RetainedWatchList`:

- do not write notice/owner-notice rows;
- retain `notice_index_key = null` or omit it according to one chosen closed record revision;
- existing list index remains the only lookup index.

Because `verify_record` currently does not require `notice_index_key`, no second schema framework is needed. However old matched manager records may contain a string and remain readable; new code must not require null for historical records.

### 4.3 Record versioning

Current watch record has `schema="eliot.coordination.watch.v1"` and a fixed `delivery="mailbox_header"` for all roles.

Two safe options:

**Preferred minimal projection correction:** keep retained v1 readable, but expose role-specific effective delivery in `watch_projection`:

```text
participant → mailbox_header
manager/operator → watch_list
```

Do not mutate historical records. Internally record creator role already determines actual reader.

If exact persisted `delivery` is contractually hashed/compared by external consumers, introduce a new record schema revision for new watches rather than silently changing old bytes. Verify all equality/digest consumers first. Do not create a compatibility union in mutation input: request still names the supported one-shot delivery; Store projects the effective surface.

### 4.4 Participant notifications stay fail-closed

`notifications()` keeps:

```rust
principal.require_participant()?;
```

It validates:

- exact owned watch;
- notice index correspondence;
- retained notification identity;
- current creator authority.

Do not weaken these checks for managers. Manager list path has its own exact scope authorization and retained transition fallback.

### 4.5 Cleanup of historical dead indexes

Do not perform an unbounded migration/sweep in read path.

Options:

- leave old manager/operator notice rows as historical dead data until normal retention tooling exists;
- or bounded cleanup during R08 migration with explicit coverage/cursor.

The correctness fix is stopping new unused writes. A global meta scan is not required to ship it.

## 5. Documentation correction

Current catalog says managers create a bounded `mailbox-header notification`. Clarify:

```text
Participant: compact header appears in coordination.inbox.
Manager/Operator: matched notification appears in coordination.watch.list
for the exact retained Task/Attempt; no participant mailbox is created.
```

`delivery:"mailbox_header"` request naming should not imply every role owns a participant inbox.

Program/module docs must preserve:

- passive metadata only;
- no Task/native/model wake;
- host restart recovery;
- current authority filtering.

## 6. Tests

### T1. Participant current scope

- Participant creates watch;
- reconcile matches;
- two notice indexes exist;
- inbox returns one header;
- watch.list remains consistent;
- no native Operation queued.

### T2. Participant after transition

- current scope becomes unavailable;
- owner-notice index permits compact historical header;
- mail/context are not exposed;
- current authority filtering remains explicit.

### T3. Manager current scope

- Manager creates exact-scope watch;
- reconcile matches;
- no notice/owner-notice keys created;
- `coordination.watch.list` returns notification;
- `coordination.inbox` remains forbidden for Manager.

### T4. Manager historical subject

Retain/update existing `exact_historical_submission_review_notifies_owner_manager_after_release`:

- notification remains readable by exact retained Task/Attempt through list;
- no participant notice-index rows required;
- unrelated manager denied.

### T5. Operator

- local Operator exact watch list read succeeds;
- no participant inbox grant;
- no notice indexes written.

### T6. Historical v1

A previously matched manager watch with old non-null notice index remains readable through list. New code does not classify it damaged.

### T7. Role corruption

Unsupported/malformed retained creator role settles/reads as bounded damaged evidence; it is not guessed from current registration.

## 7. Files

Primary in existing R08/#34:

- `crates/swarm-kernel-host/src/store/coordination_watch.rs`
  - reconcile role projection;
  - local notice surface helper;
  - watch projection/documentation;
  - no change to participant authorization semantics.
- watch program tests, especially submission-reviewed historical manager test.
- MCP catalog/module API wording.

R08 already owns watch ordering/deadline/index semantics. Do not create another PR touching the same reconcile/read functions.

R23/R25 object read scopes do not replace watch-specific exact creator authority. R24 MCP live method authorization does not grant object access. R14 schema extraction later consumes the corrected role-specific documentation.

## 8. What not to do

- remove `require_participant` from notifications;
- expose participant inbox to managers/operators;
- add `coordination.watch.notifications` alias;
- write a second notification Store/table;
- duplicate notification payload in a new Operation;
- auto-wake manager/native session;
- unbounded cleanup of old indexes;
- infer role from display alias or current registration only;
- classify manager notification as lost.

## 9. Minimal gate

After complete R08 code:

```sh
cargo clippy --locked \
  -p swarm-kernel-host \
  -p swarm-mcp \
  --lib --bins -- -D warnings
```

Then exact participant/manager/operator public-path tests. Broad/native/load tests remain the final phase.
