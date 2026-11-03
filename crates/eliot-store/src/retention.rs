//! Bounded physical retention: expired event-prefix pruning and atomic
//! command/child-set cleanup through the one Store writer.
//!
//! Owning contract: docs/spec/core.md#retention. Deletion is silent (no events
//! are emitted) and never replays an effect: event pruning stays below every
//! live rule/Git cursor and advances `rule_history_coverage.pruned_through` in
//! the same transaction, while command/write pruning keeps open work, the
//! retry window, and every row a live projection still references.

use super::*;

const MS_PER_DAY: i64 = 24 * 60 * 60 * 1_000;
/// Traversal backstop for parent/child sets. Parent links are bound once at
/// admission, so sets are acyclic; reaching this fails the pass loudly rather
/// than looping.
const MAX_SET_DEPTH: i64 = 1_000;
/// Candidate roots examined per pass. Total deleted rows stay bounded by
/// `MAX_PAGE_ENTRIES`.
const MAX_PRUNE_ROOTS: i64 = 50;

/// Outcome of one bounded maintenance pass.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RetentionReport {
    pub events_deleted: i64,
    pub pruned_through: i64,
    pub commands_deleted: i64,
    pub writes_deleted: i64,
}

struct SubtreeMember {
    id: String,
    depth: i64,
    state: String,
    created_at: i64,
    turn_bound: bool,
    workspace_ref: bool,
    probe_ref: bool,
}

impl Store {
    /// Delete one bounded batch of expired history. Event pruning removes an
    /// expired contiguous prefix below every live cursor; command pruning
    /// removes whole closed child-sets leaf-first; writes pruning removes
    /// expired mutation bindings. Projections, source recovery state, timers,
    /// and the event id allocator are never touched.
    pub fn prune_retention(
        &mut self,
        events_days: i64,
        commands_days: i64,
        now: i64,
    ) -> Result<RetentionReport> {
        self.ensure_writer_healthy()?;
        let result = self.prune_retention_inner(events_days, commands_days, now);
        self.fault_on_persistence_error(&result);
        result
    }

    fn prune_retention_inner(
        &mut self,
        events_days: i64,
        commands_days: i64,
        now: i64,
    ) -> Result<RetentionReport> {
        if events_days <= 0 || commands_days <= 0 {
            return Err(Error::new(
                "invalid",
                "retention windows must be positive days",
            ));
        }
        let events_cutoff = now.saturating_sub(events_days.saturating_mul(MS_PER_DAY));
        // Command/write bindings must outlive the retry window no matter how
        // short the configured window is: a pruned id must read back as
        // id_expired, never as a new request.
        let commands_cutoff = now.saturating_sub(
            commands_days
                .saturating_mul(MS_PER_DAY)
                .max(RETRY_WINDOW_MS),
        );
        let (events_deleted, pruned_through) = self.prune_events(events_cutoff)?;
        let (commands_deleted, writes_deleted) = self.prune_commands_and_writes(commands_cutoff)?;
        Ok(RetentionReport {
            events_deleted,
            pruned_through,
            commands_deleted,
            writes_deleted,
        })
    }

    fn prune_events(&mut self, cutoff: i64) -> Result<(i64, i64)> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        let pruned_through: i64 = tx
            .query_row(
                "SELECT pruned_through FROM rule_history_coverage WHERE singleton = 1",
                [],
                |row| row.get(0),
            )
            .optional()
            .map_err(sql_error)?
            .ok_or_else(|| Error::new("source_integrity", "rule history coverage is absent"))?;
        let end = match event_prune_end_tx(&tx, pruned_through, cutoff)? {
            Some(end) => end,
            None => {
                commit_transaction(tx, &self.write_faulted)?;
                return Ok((0, pruned_through));
            }
        };
        // Delete the lowest remaining seqs first so coverage stays contiguous.
        let batch_end: Option<i64> = tx
            .query_row(
                "SELECT MAX(seq) FROM (
                     SELECT seq FROM events WHERE seq > ?1 AND seq <= ?2
                     ORDER BY seq LIMIT ?3
                 )",
                params![pruned_through, end, MAX_PAGE_ENTRIES as i64],
                |row| row.get(0),
            )
            .map_err(sql_error)?;
        let Some(batch_end) = batch_end else {
            commit_transaction(tx, &self.write_faulted)?;
            return Ok((0, pruned_through));
        };
        let deleted = db!(tx.execute(
            "DELETE FROM events WHERE seq > ?1 AND seq <= ?2",
            params![pruned_through, batch_end],
        ));
        // The allocator row in sqlite_sequence is deliberately untouched: event
        // ids are never reused and high-water survives prefix deletion.
        let advanced = db!(tx.execute(
            "UPDATE rule_history_coverage SET pruned_through = ?1 WHERE singleton = 1",
            [batch_end],
        ));
        if advanced != 1 {
            return Err(Error::new(
                "source_integrity",
                "rule history coverage disappeared",
            ));
        }
        commit_transaction(tx, &self.write_faulted)?;
        Ok((deleted as i64, batch_end))
    }

    fn prune_commands_and_writes(&mut self, cutoff: i64) -> Result<(i64, i64)> {
        let roots: Vec<String> = self
            .connection
            .prepare(
                "SELECT c.id FROM commands c
                 WHERE c.parent_command IS NULL
                   AND c.state NOT IN ('accepted', 'queued', 'dispatched', 'unknown')
                   AND c.created_at <= ?1
                   AND NOT EXISTS (
                       SELECT 1 FROM turns t
                       WHERE t.agent = c.target AND t.generation = c.generation
                         AND t.turn_id = c.turn_id AND t.state IN ('running', 'unknown')
                   )
                   AND NOT EXISTS (
                       SELECT 1 FROM workspaces w WHERE w.start_command_id = c.id
                   )
                   AND NOT EXISTS (
                       SELECT 1 FROM provider_holds h WHERE h.probe_command = c.id
                   )
                 ORDER BY c.created_at, c.id LIMIT ?2",
            )
            .map_err(sql_error)?
            .query_map(params![cutoff, MAX_PRUNE_ROOTS], |row| row.get(0))
            .map_err(sql_error)?
            .collect::<rusqlite::Result<_>>()
            .map_err(sql_error)?;
        let mut doomed: Vec<SubtreeMember> = Vec::new();
        for root in &roots {
            let members = subtree_members_tx(&self.connection, root)?;
            if members.len() + doomed.len() > MAX_PAGE_ENTRIES && !doomed.is_empty() {
                break;
            }
            if members.iter().any(|member| {
                member.is_open()
                    || member.created_at > cutoff
                    || member.turn_bound
                    || member.workspace_ref
                    || member.probe_ref
            }) {
                continue;
            }
            doomed.extend(members);
            if doomed.len() >= MAX_PAGE_ENTRIES {
                break;
            }
        }
        doomed.sort_by_key(|member| std::cmp::Reverse(member.depth));
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        let mut deleted = 0_i64;
        {
            let mut delete = tx
                .prepare("DELETE FROM commands WHERE id = ?1")
                .map_err(sql_error)?;
            // Children before parents: foreign keys enforce the order.
            for member in &doomed {
                deleted += db!(delete.execute([&member.id])) as i64;
            }
        }
        let writes = prune_writes_tx(&tx, cutoff)?;
        commit_transaction(tx, &self.write_faulted)?;
        Ok((deleted, writes))
    }
}

impl SubtreeMember {
    fn is_open(&self) -> bool {
        matches!(
            self.state.as_str(),
            "accepted" | "queued" | "dispatched" | "unknown"
        )
    }
}

/// Highest seq eligible for event pruning: an expired contiguous prefix below
/// every live cursor, or `None` when nothing is eligible.
fn event_prune_end_tx(
    tx: &Transaction<'_>,
    pruned_through: i64,
    cutoff: i64,
) -> Result<Option<i64>> {
    // Enabled current/retiring cursors pin the events they still need; this is
    // the Store backlog pin model (disabled rules pin nothing). Live Git hint
    // cursors pin the same prefix: hint scans read (cursor, high-water] and
    // must never skip unconsumed history silently.
    let floor: Option<i64> = tx
        .query_row(
            "SELECT MIN(pin) FROM (
                 SELECT current_cursor AS pin FROM rule_state WHERE enabled = 1
                 UNION ALL
                 SELECT retiring_cursor FROM rule_state
                 WHERE enabled = 1
                   AND retiring_cursor IS NOT NULL AND retire_through IS NOT NULL
                 UNION ALL
                 SELECT COALESCE(CAST(s.cursor AS INTEGER), 0) FROM source_state s
                 JOIN agents a ON a.id = s.agent
                 WHERE s.source = 'git' AND s.state = 'open' AND s.generation = a.generation
             )",
            [],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    let ceiling = floor.unwrap_or(i64::MAX);
    if ceiling <= pruned_through {
        return Ok(None);
    }
    // The prefix must be contiguous: stop before the first unexpired row.
    let first_fresh: Option<i64> = tx
        .query_row(
            "SELECT MIN(seq) FROM events WHERE seq > ?1 AND seq <= ?2 AND ts > ?3",
            params![pruned_through, ceiling, cutoff],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    let end = first_fresh.map(|fresh| fresh - 1).unwrap_or(ceiling);
    if end <= pruned_through {
        return Ok(None);
    }
    Ok(Some(end))
}

/// Every member of one command set with its keep signals. The caller deletes
/// the set only when no member asks to be kept.
fn subtree_members_tx(connection: &Connection, root: &str) -> Result<Vec<SubtreeMember>> {
    let mut statement = connection
        .prepare(
            "WITH RECURSIVE subtree(id, depth) AS (
                 SELECT ?1, 0
                 UNION ALL
                 SELECT c.id, s.depth + 1 FROM commands c
                 JOIN subtree s ON c.parent_command = s.id
                 WHERE s.depth < ?2
             )
             SELECT s.id, s.depth, c.state, c.created_at,
                    EXISTS(
                        SELECT 1 FROM turns t
                        WHERE t.agent = c.target AND t.generation = c.generation
                          AND t.turn_id = c.turn_id AND t.state IN ('running', 'unknown')
                    ),
                    EXISTS(SELECT 1 FROM workspaces w WHERE w.start_command_id = c.id),
                    EXISTS(SELECT 1 FROM provider_holds h WHERE h.probe_command = c.id)
             FROM subtree s JOIN commands c ON c.id = s.id",
        )
        .map_err(sql_error)?;
    let rows = statement
        .query_map(params![root, MAX_SET_DEPTH], |row| {
            Ok(SubtreeMember {
                id: row.get(0)?,
                depth: row.get(1)?,
                state: row.get(2)?,
                created_at: row.get(3)?,
                turn_bound: row.get(4)?,
                workspace_ref: row.get(5)?,
                probe_ref: row.get(6)?,
            })
        })
        .map_err(sql_error)?;
    let mut members = Vec::new();
    for row in rows {
        members.push(row.map_err(sql_error)?);
    }
    if members.iter().any(|member| member.depth >= MAX_SET_DEPTH) {
        return Err(Error::new(
            "source_integrity",
            "command set exceeds its traversal bound",
        ));
    }
    Ok(members)
}

fn prune_writes_tx(tx: &Transaction<'_>, cutoff: i64) -> Result<i64> {
    Ok(db!(tx.execute(
        "DELETE FROM writes WHERE id IN (
             SELECT id FROM writes WHERE created_at <= ?1
             ORDER BY created_at, id LIMIT ?2
         )",
        params![cutoff, MAX_PAGE_ENTRIES as i64],
    )) as i64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    const DAY_MS: i64 = 24 * 60 * 60 * 1_000;
    const NOW: i64 = 1_800_000_000_000;

    fn store() -> Store {
        Store::open(Path::new(":memory:")).unwrap()
    }

    fn insert_event(store: &mut Store, ts: i64) -> i64 {
        store
            .connection
            .execute(
                "INSERT INTO events(ts, kind, source, data) VALUES (?1, 'test.event', 'daemon', '{}')",
                [ts],
            )
            .unwrap();
        store.connection.last_insert_rowid()
    }

    fn rule_input(id: &str, enabled: bool) -> RuleDefinitionInput {
        let value = json!({"id": id, "enabled": enabled});
        RuleDefinitionInput {
            id: id.to_owned(),
            canonical_definition: serde_json::to_string(&value).unwrap(),
            definition_digest: core::canonical_json_sha256(&value).unwrap(),
            enabled,
        }
    }

    fn set_current_cursor(store: &mut Store, rule: &str, cursor: i64) {
        assert_eq!(
            store
                .connection
                .execute(
                    "UPDATE rule_state SET current_cursor = ?1 WHERE principal = 'owner' AND rule = ?2",
                    params![cursor, rule],
                )
                .unwrap(),
            1
        );
    }

    fn set_retiring_cursor(store: &mut Store, rule: &str, cursor: i64, through: i64) {
        assert_eq!(
            store
                .connection
                .execute(
                    "UPDATE rule_state
                     SET retiring_revision = 1, retiring_definition = '{}',
                         retiring_cursor = ?1, retire_through = ?2
                     WHERE principal = 'owner' AND rule = ?3",
                    params![cursor, through, rule],
                )
                .unwrap(),
            1
        );
    }

    struct CommandSeed {
        id: String,
        parent: Option<(String, String, i64)>,
        target: Option<i64>,
        generation: Option<i64>,
        turn_id: Option<String>,
        verb: String,
        state: String,
        created_at: i64,
    }

    fn command_seed(id: String, created_at: i64) -> CommandSeed {
        CommandSeed {
            id,
            parent: None,
            target: None,
            generation: None,
            turn_id: None,
            verb: "stop".to_owned(),
            state: "applied".to_owned(),
            created_at,
        }
    }

    fn insert_command(store: &mut Store, seed: CommandSeed) {
        let digest = vec![0_u8; 32];
        let (parent, link_kind, ordinal) = match seed.parent {
            Some((id, kind, ordinal)) => (Some(id), Some(kind), Some(ordinal)),
            None => (None, None, None),
        };
        store
            .connection
            .execute(
                "INSERT INTO commands(id, principal, method, payload_digest, target, generation,
                                      turn_id, parent_command, link_kind, ordinal, verb, args,
                                      state, created_at, updated_at)
                 VALUES (?1, 'owner', 'command.submit', ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, '{}',
                         ?10, ?11, ?11)",
                params![
                    seed.id,
                    digest,
                    seed.target,
                    seed.generation,
                    seed.turn_id,
                    parent,
                    link_kind,
                    ordinal,
                    seed.verb,
                    seed.state,
                    seed.created_at,
                ],
            )
            .unwrap();
    }

    fn insert_agent(store: &mut Store, generation: i64) -> i64 {
        store
            .connection
            .execute(
                "INSERT INTO agents(generation, project, harness, profile, state, created_at, updated_at)
                 VALUES (?1, 'project-test', 'fake', 'fake', 'idle', 1, 1)",
                [generation],
            )
            .unwrap();
        store.connection.last_insert_rowid()
    }

    fn event_count(store: &Store) -> i64 {
        store
            .connection
            .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
            .unwrap()
    }

    fn command_count(store: &Store) -> i64 {
        store
            .connection
            .query_row("SELECT COUNT(*) FROM commands", [], |row| row.get(0))
            .unwrap()
    }

    fn ulid_at(ts_ms: i64, tag: u8) -> String {
        let mut bytes = [0_u8; 16];
        bytes[..6].copy_from_slice(&(ts_ms as u64).to_be_bytes()[2..8]);
        bytes[6..].copy_from_slice(&[tag; 10]);
        Ulid::from_bytes(bytes).to_string()
    }

    #[test]
    fn prune_events_deletes_expired_prefix_and_advances_coverage() {
        let mut store = store();
        let old = NOW - 31 * DAY_MS;
        let mut seqs = Vec::new();
        for _ in 0..5 {
            seqs.push(insert_event(&mut store, old));
        }
        let fresh_first = insert_event(&mut store, NOW);
        insert_event(&mut store, NOW);
        let report = store.prune_retention(30, 30, NOW).unwrap();
        assert_eq!(report.events_deleted, 5);
        assert_eq!(report.pruned_through, seqs[4]);
        assert_eq!(event_count(&store), 2);
        let remaining_min: i64 = store
            .connection
            .query_row("SELECT MIN(seq) FROM events", [], |row| row.get(0))
            .unwrap();
        assert_eq!(remaining_min, fresh_first);
    }

    #[test]
    fn prune_events_stops_before_first_fresh_row() {
        let mut store = store();
        let old = NOW - 31 * DAY_MS;
        let first = insert_event(&mut store, old);
        let second = insert_event(&mut store, old);
        insert_event(&mut store, NOW);
        insert_event(&mut store, old);
        let report = store.prune_retention(30, 30, NOW).unwrap();
        assert_eq!(report.events_deleted, 2);
        assert_eq!(report.pruned_through, second);
        assert_eq!(event_count(&store), 2);
        let remaining_min: i64 = store
            .connection
            .query_row("SELECT MIN(seq) FROM events", [], |row| row.get(0))
            .unwrap();
        assert!(remaining_min > second);
        assert!(first < second);
    }

    #[test]
    fn prune_events_pins_below_enabled_current_cursor() {
        let mut store = store();
        let old = NOW - 31 * DAY_MS;
        for _ in 0..3 {
            insert_event(&mut store, old);
        }
        store
            .ensure_rule_state("owner", &rule_input("pin-rule", true), NOW)
            .unwrap();
        // Expired rows above the cursor are still needed by the rule.
        insert_event(&mut store, old);
        insert_event(&mut store, old);
        let report = store.prune_retention(30, 30, NOW).unwrap();
        assert_eq!(report.events_deleted, 3);
        assert_eq!(report.pruned_through, 3);
        assert_eq!(event_count(&store), 2);
    }

    #[test]
    fn prune_events_pins_below_retiring_cursor() {
        let mut store = store();
        let old = NOW - 31 * DAY_MS;
        for _ in 0..5 {
            insert_event(&mut store, old);
        }
        store
            .ensure_rule_state("owner", &rule_input("retire-rule", true), NOW)
            .unwrap();
        set_current_cursor(&mut store, "retire-rule", 5);
        set_retiring_cursor(&mut store, "retire-rule", 2, 5);
        let report = store.prune_retention(30, 30, NOW).unwrap();
        assert_eq!(report.events_deleted, 2);
        assert_eq!(report.pruned_through, 2);
        assert_eq!(event_count(&store), 3);
    }

    #[test]
    fn prune_events_ignores_disabled_rules() {
        let mut store = store();
        let old = NOW - 31 * DAY_MS;
        for _ in 0..3 {
            insert_event(&mut store, old);
        }
        store
            .ensure_rule_state("owner", &rule_input("off-rule", false), NOW)
            .unwrap();
        set_current_cursor(&mut store, "off-rule", 1);
        let report = store.prune_retention(30, 30, NOW).unwrap();
        assert_eq!(report.events_deleted, 3);
        assert_eq!(report.pruned_through, 3);
        assert_eq!(event_count(&store), 0);
    }

    #[test]
    fn prune_events_pins_below_live_git_cursor_until_it_closes() {
        let mut store = store();
        let old = NOW - 31 * DAY_MS;
        for _ in 0..4 {
            insert_event(&mut store, old);
        }
        let agent = insert_agent(&mut store, 1);
        store
            .connection
            .execute(
                "INSERT INTO source_state(agent, generation, source, cursor, state, health, updated_at)
                 VALUES (?1, 1, 'git', '2', 'open', 'ok', ?2)",
                params![agent, NOW],
            )
            .unwrap();
        let report = store.prune_retention(30, 30, NOW).unwrap();
        assert_eq!(report.events_deleted, 2);
        assert_eq!(report.pruned_through, 2);
        store
            .connection
            .execute(
                "UPDATE source_state SET state = 'closed' WHERE agent = ?1 AND source = 'git'",
                [agent],
            )
            .unwrap();
        let report = store.prune_retention(30, 30, NOW).unwrap();
        assert_eq!(report.events_deleted, 2);
        assert_eq!(report.pruned_through, 4);
        assert_eq!(event_count(&store), 0);
    }

    #[test]
    fn prune_events_ignores_stale_generation_git_cursor() {
        let mut store = store();
        let old = NOW - 31 * DAY_MS;
        for _ in 0..4 {
            insert_event(&mut store, old);
        }
        let agent = insert_agent(&mut store, 2);
        store
            .connection
            .execute(
                "INSERT INTO source_state(agent, generation, source, cursor, state, health, updated_at)
                 VALUES (?1, 1, 'git', '2', 'open', 'ok', ?2)",
                params![agent, NOW],
            )
            .unwrap();
        let report = store.prune_retention(30, 30, NOW).unwrap();
        assert_eq!(report.events_deleted, 4);
        assert_eq!(report.pruned_through, 4);
    }

    #[test]
    fn prune_events_never_resets_the_allocator() {
        let mut store = store();
        let old = NOW - 31 * DAY_MS;
        for _ in 0..3 {
            insert_event(&mut store, old);
        }
        let allocated_before: i64 = store
            .connection
            .query_row(
                "SELECT seq FROM sqlite_sequence WHERE name = 'events'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let report = store.prune_retention(30, 30, NOW).unwrap();
        assert_eq!(report.events_deleted, 3);
        let allocated_after: i64 = store
            .connection
            .query_row(
                "SELECT seq FROM sqlite_sequence WHERE name = 'events'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(allocated_after, allocated_before);
        assert_eq!(store.high_water().unwrap(), allocated_before);
        let next = insert_event(&mut store, NOW);
        assert_eq!(next, allocated_before + 1);
    }

    #[test]
    fn prune_events_is_bounded_per_pass() {
        let mut store = store();
        let old = NOW - 31 * DAY_MS;
        for _ in 0..2500 {
            insert_event(&mut store, old);
        }
        let report = store.prune_retention(30, 30, NOW).unwrap();
        assert_eq!(report.events_deleted, MAX_PAGE_ENTRIES as i64);
        assert_eq!(report.pruned_through, MAX_PAGE_ENTRIES as i64);
        let report = store.prune_retention(30, 30, NOW).unwrap();
        assert_eq!(report.events_deleted, MAX_PAGE_ENTRIES as i64);
        assert_eq!(report.pruned_through, 2 * MAX_PAGE_ENTRIES as i64);
        assert_eq!(event_count(&store), 500);
    }

    #[test]
    fn prune_events_preserves_source_recovery_state() {
        let mut store = store();
        let old = NOW - 31 * DAY_MS;
        for _ in 0..9 {
            insert_event(&mut store, old);
        }
        let agent = insert_agent(&mut store, 1);
        store
            .connection
            .execute(
                "INSERT INTO source_state(agent, generation, source, cursor, last_store_seq,
                                          last_batch_id, last_batch_digest, last_batch_first_seq,
                                          last_batch_last_seq, state, health, updated_at,
                                          gap_id, gap_digest, gap_from, gap_to, gap_seq,
                                          last_handoff_digest, last_handoff)
                 VALUES (?1, 1, 'stream', 'cursor-9', 9, 'batch-9', ?2, 7, 9,
                         'open', 'ok', ?3, 'gap-1', ?4, 'a', 'z', 5, ?5, 'handoff-1')",
                params![agent, vec![9_u8; 32], NOW, vec![8_u8; 32], vec![7_u8; 32]],
            )
            .unwrap();
        let before: Vec<Value> = store
            .connection
            .prepare("SELECT * FROM source_state")
            .unwrap()
            .query_map([], |row| {
                Ok(json!({
                    "cursor": row.get::<_, Option<String>>(3)?,
                    "last_store_seq": row.get::<_, Option<i64>>(4)?,
                    "last_batch_id": row.get::<_, Option<String>>(5)?,
                    "last_batch_digest": row.get::<_, Option<Vec<u8>>>(6)?,
                    "last_batch_first_seq": row.get::<_, Option<i64>>(7)?,
                    "last_batch_last_seq": row.get::<_, Option<i64>>(8)?,
                    "gap_id": row.get::<_, Option<String>>(12)?,
                    "gap_digest": row.get::<_, Option<Vec<u8>>>(13)?,
                    "gap_from": row.get::<_, Option<String>>(14)?,
                    "gap_to": row.get::<_, Option<String>>(15)?,
                    "gap_seq": row.get::<_, Option<i64>>(16)?,
                    "last_handoff_digest": row.get::<_, Option<Vec<u8>>>(17)?,
                    "last_handoff": row.get::<_, Option<String>>(18)?,
                }))
            })
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        let report = store.prune_retention(30, 30, NOW).unwrap();
        assert_eq!(report.events_deleted, 9);
        let after: Vec<Value> = store
            .connection
            .prepare("SELECT * FROM source_state")
            .unwrap()
            .query_map([], |row| {
                Ok(json!({
                    "cursor": row.get::<_, Option<String>>(3)?,
                    "last_store_seq": row.get::<_, Option<i64>>(4)?,
                    "last_batch_id": row.get::<_, Option<String>>(5)?,
                    "last_batch_digest": row.get::<_, Option<Vec<u8>>>(6)?,
                    "last_batch_first_seq": row.get::<_, Option<i64>>(7)?,
                    "last_batch_last_seq": row.get::<_, Option<i64>>(8)?,
                    "gap_id": row.get::<_, Option<String>>(12)?,
                    "gap_digest": row.get::<_, Option<Vec<u8>>>(13)?,
                    "gap_from": row.get::<_, Option<String>>(14)?,
                    "gap_to": row.get::<_, Option<String>>(15)?,
                    "gap_seq": row.get::<_, Option<i64>>(16)?,
                    "last_handoff_digest": row.get::<_, Option<Vec<u8>>>(17)?,
                    "last_handoff": row.get::<_, Option<String>>(18)?,
                }))
            })
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(before, after);
        assert_eq!(before.len(), 1);
    }

    #[test]
    fn catch_up_returns_history_gap_after_pruning() {
        let mut store = store();
        let old = NOW - 31 * DAY_MS;
        for _ in 0..3 {
            insert_event(&mut store, old);
        }
        let state = store
            .ensure_rule_state("owner", &rule_input("gap-rule", true), NOW)
            .unwrap();
        let captured = state.current_cursor;
        let admitted = store
            .admit_rule_catch_up(
                "owner",
                &core::api::RuleCatchUpRequest::new(ulid_at(NOW, 1), "gap-rule", 0, Some(captured)),
                &state,
                captured,
                &[],
                NOW,
            )
            .unwrap();
        assert!(admitted.is_new);
        let report = store.prune_retention(30, 30, NOW).unwrap();
        assert_eq!(report.pruned_through, captured);
        let error = store
            .admit_rule_catch_up(
                "owner",
                &core::api::RuleCatchUpRequest::new(ulid_at(NOW, 2), "gap-rule", 0, Some(captured)),
                &state,
                captured,
                &[],
                NOW,
            )
            .unwrap_err();
        assert_eq!(error.code, "history_gap");
    }

    #[test]
    fn prune_commands_deletes_closed_sets_children_before_parent() {
        let mut store = store();
        let old = NOW - 31 * DAY_MS;
        let parent = Ulid::new().to_string();
        insert_command(&mut store, command_seed(parent.clone(), old));
        let child = Ulid::new().to_string();
        insert_command(
            &mut store,
            CommandSeed {
                parent: Some((parent.clone(), "rule_action".to_owned(), 0)),
                state: "failed".to_owned(),
                ..command_seed(child.clone(), old)
            },
        );
        insert_command(
            &mut store,
            CommandSeed {
                parent: Some((child.clone(), "rule_action".to_owned(), 0)),
                state: "cancelled".to_owned(),
                ..command_seed(Ulid::new().to_string(), old)
            },
        );
        insert_command(
            &mut store,
            CommandSeed {
                parent: Some((parent.clone(), "rule_action".to_owned(), 1)),
                state: "rejected".to_owned(),
                ..command_seed(Ulid::new().to_string(), old)
            },
        );
        let lone = Ulid::new().to_string();
        insert_command(
            &mut store,
            CommandSeed {
                state: "rejected".to_owned(),
                ..command_seed(lone, old)
            },
        );
        assert_eq!(command_count(&store), 5);
        let report = store.prune_retention(30, 30, NOW).unwrap();
        assert_eq!(report.commands_deleted, 5);
        assert_eq!(command_count(&store), 0);
    }

    #[test]
    fn prune_commands_keeps_sets_with_open_work() {
        let mut store = store();
        let old = NOW - 31 * DAY_MS;
        for state in ["accepted", "queued", "dispatched", "unknown"] {
            let parent = Ulid::new().to_string();
            insert_command(&mut store, command_seed(parent.clone(), old));
            insert_command(
                &mut store,
                CommandSeed {
                    parent: Some((parent, "rule_action".to_owned(), 0)),
                    state: state.to_owned(),
                    ..command_seed(Ulid::new().to_string(), old)
                },
            );
        }
        let report = store.prune_retention(30, 30, NOW).unwrap();
        assert_eq!(report.commands_deleted, 0);
        assert_eq!(command_count(&store), 8);
    }

    #[test]
    fn prune_commands_keeps_sets_younger_than_the_window() {
        let mut store = store();
        let old = NOW - 31 * DAY_MS;
        let parent = Ulid::new().to_string();
        insert_command(&mut store, command_seed(parent.clone(), old));
        insert_command(
            &mut store,
            CommandSeed {
                parent: Some((parent, "rule_action".to_owned(), 0)),
                ..command_seed(Ulid::new().to_string(), NOW - 3_600_000)
            },
        );
        let report = store.prune_retention(30, 30, NOW).unwrap();
        assert_eq!(report.commands_deleted, 0);
        assert_eq!(command_count(&store), 2);
        // The retry window holds even for a one-day window.
        let report = store.prune_retention(30, 1, NOW).unwrap();
        assert_eq!(report.commands_deleted, 0);
        assert_eq!(command_count(&store), 2);
    }

    #[test]
    fn prune_commands_keeps_turn_bound_rows_until_the_turn_settles() {
        let mut store = store();
        let old = NOW - 31 * DAY_MS;
        let agent = insert_agent(&mut store, 1);
        let send = Ulid::new().to_string();
        insert_command(
            &mut store,
            CommandSeed {
                target: Some(agent),
                generation: Some(1),
                turn_id: Some("turn-1".to_owned()),
                verb: "send".to_owned(),
                ..command_seed(send.clone(), old)
            },
        );
        store
            .connection
            .execute(
                "INSERT INTO turns(agent, generation, turn_id, state) VALUES (?1, 1, 'turn-1', 'running')",
                [agent],
            )
            .unwrap();
        let report = store.prune_retention(30, 30, NOW).unwrap();
        assert_eq!(report.commands_deleted, 0);
        store
            .connection
            .execute(
                "UPDATE turns SET state = 'unknown' WHERE agent = ?1 AND turn_id = 'turn-1'",
                [agent],
            )
            .unwrap();
        let report = store.prune_retention(30, 30, NOW).unwrap();
        assert_eq!(report.commands_deleted, 0);
        store
            .connection
            .execute(
                "UPDATE turns SET state = 'completed' WHERE agent = ?1 AND turn_id = 'turn-1'",
                [agent],
            )
            .unwrap();
        let report = store.prune_retention(30, 30, NOW).unwrap();
        assert_eq!(report.commands_deleted, 1);
        assert_eq!(command_count(&store), 0);
    }

    #[test]
    fn prune_commands_keeps_workspace_and_probe_references() {
        let mut store = store();
        let old = NOW - 31 * DAY_MS;
        let agent = insert_agent(&mut store, 1);
        let launch = Ulid::new().to_string();
        insert_command(
            &mut store,
            CommandSeed {
                target: Some(agent),
                generation: Some(0),
                verb: "launch".to_owned(),
                ..command_seed(launch.clone(), old)
            },
        );
        store
            .connection
            .execute(
                "INSERT INTO workspaces(agent, target_generation, start_command_id, project,
                                        repo_path, repo_common_dir, requested_branch, base_oid,
                                        worktree_path, frozen_setup, phase, updated_at)
                 VALUES (?1, 1, ?2, 'project-test', '/repo', '/repo/.git', 'main', 'abc',
                         '/worktrees/a1', '[]', 'ready', ?3)",
                params![agent, launch, NOW],
            )
            .unwrap();
        let probe = Ulid::new().to_string();
        insert_command(
            &mut store,
            CommandSeed {
                verb: "send".to_owned(),
                ..command_seed(probe.clone(), old)
            },
        );
        store
            .connection
            .execute(
                "INSERT INTO provider_holds(scope, kind, probe_command, state, updated_at)
                 VALUES ('scope-1', 'rate_limit', ?1, 'probing', ?2)",
                params![probe, NOW],
            )
            .unwrap();
        let report = store.prune_retention(30, 30, NOW).unwrap();
        assert_eq!(report.commands_deleted, 0);
        assert_eq!(command_count(&store), 2);
        store
            .connection
            .execute("DELETE FROM provider_holds WHERE scope = 'scope-1'", [])
            .unwrap();
        let report = store.prune_retention(30, 30, NOW).unwrap();
        assert_eq!(report.commands_deleted, 1);
        // The workspace start stays pinned while its preparation row exists.
        assert_eq!(command_count(&store), 1);
        assert!(store.get_command(&launch).unwrap().is_some());
    }

    #[test]
    fn prune_writes_deletes_only_expired_bindings() {
        let mut store = store();
        for (id, created_at) in [
            (Ulid::new().to_string(), NOW - 31 * DAY_MS),
            (Ulid::new().to_string(), NOW - 3_600_000),
        ] {
            store
                .connection
                .execute(
                    "INSERT INTO writes(id, principal, method, payload_digest, result, created_at)
                     VALUES (?1, 'owner', 'daemon.shutdown', ?2, '{}', ?3)",
                    params![id, vec![0_u8; 32], created_at],
                )
                .unwrap();
        }
        let report = store.prune_retention(30, 30, NOW).unwrap();
        assert_eq!(report.writes_deleted, 1);
        let remaining: i64 = store
            .connection
            .query_row("SELECT COUNT(*) FROM writes", [], |row| row.get(0))
            .unwrap();
        assert_eq!(remaining, 1);
    }

    #[test]
    fn pruned_command_id_resubmits_as_expired_never_new() {
        let mut store = store();
        let submitted_at = NOW - 31 * DAY_MS;
        let mut request = SubmitCommand::new(ulid_at(submitted_at, 1), "launch");
        request.args = json!({
            "project": "retention-project",
            "workspace": "retention-workspace",
        });
        request.expected_generation = Some(0);
        let admission = store
            .submit("owner", request.clone(), submitted_at)
            .unwrap();
        assert!(admission.is_new);
        store
            .connection
            .execute(
                "UPDATE commands SET state = 'applied' WHERE id = ?1",
                [&request.id],
            )
            .unwrap();
        let report = store.prune_retention(30, 30, NOW).unwrap();
        assert_eq!(report.commands_deleted, 1);
        let error = store.submit("owner", request, NOW).unwrap_err();
        assert_eq!(error.code, "id_expired");

        let mut fresh = SubmitCommand::new(ulid_at(NOW, 2), "launch");
        fresh.args = json!({
            "project": "retention-project",
            "workspace": "retention-workspace",
        });
        fresh.expected_generation = Some(0);
        let admission = store.submit("owner", fresh.clone(), NOW).unwrap();
        assert!(admission.is_new);
        let report = store.prune_retention(30, 30, NOW).unwrap();
        assert_eq!(report.commands_deleted, 0);
        let replay = store.submit("owner", fresh, NOW).unwrap();
        assert!(!replay.is_new);
    }

    #[test]
    fn prune_commands_is_bounded_per_pass() {
        let mut store = store();
        let old = NOW - 31 * DAY_MS;
        for _ in 0..1200 {
            insert_command(&mut store, command_seed(Ulid::new().to_string(), old));
        }
        let report = store.prune_retention(30, 30, NOW).unwrap();
        assert_eq!(report.commands_deleted, MAX_PRUNE_ROOTS);
        assert_eq!(command_count(&store), 1200 - MAX_PRUNE_ROOTS);
    }

    #[test]
    fn event_replay_returns_history_gap_after_pruning() {
        let mut store = store();
        let old = NOW - 31 * DAY_MS;
        for _ in 0..3 {
            insert_event(&mut store, old);
        }
        insert_event(&mut store, NOW);
        assert_eq!(store.read_events(0, 100, None).unwrap().len(), 4);
        let report = store.prune_retention(30, 30, NOW).unwrap();
        assert_eq!(report.pruned_through, 3);
        let error = store.read_events(0, 100, None).unwrap_err();
        assert_eq!(error.code, "history_gap");
        let error = store.read_events(2, 100, None).unwrap_err();
        assert_eq!(error.code, "history_gap");
        let page = store.read_events(3, 100, None).unwrap();
        assert_eq!(page.len(), 1);
        assert_eq!(page[0].seq, 4);
    }

    #[test]
    fn prune_retention_rejects_nonpositive_windows() {
        let mut store = store();
        for (events_days, commands_days) in [(0, 30), (30, 0), (-1, 30), (30, -1)] {
            let error = store
                .prune_retention(events_days, commands_days, NOW)
                .unwrap_err();
            assert_eq!(error.code, "invalid");
        }
        assert!(!store.is_write_faulted());
    }
}
