//! Bounded physical retention: expired event-prefix pruning, atomic
//! command/child-set cleanup, and acknowledged-mail pruning through the one
//! Store writer.
//!
//! Owning contract: docs/spec/core.md#retention. Deletion is silent (no events
//! are emitted) and never replays an effect: event pruning stays below every
//! live rule/Git cursor and advances `rule_history_coverage.pruned_through` in
//! the same transaction, while command/write pruning keeps open work, the
//! retry window, and every row a live projection still references. Mail
//! pruning deletes only acknowledged rows older than the mail window; unread
//! mail is never pruned.

use super::*;

const MS_PER_DAY: i64 = 24 * 60 * 60 * 1_000;
/// Traversal backstop for parent/child sets. Parent links are bound once at
/// admission, so sets are acyclic; reaching this fails the pass loudly rather
/// than looping.
const MAX_SET_DEPTH: i64 = 1_000;
/// Members collected per command set. A set larger than this is kept, not an
/// error: fan-out is legitimate, while the depth bound above trips only on
/// corruption. Total deleted rows stay bounded by `MAX_PAGE_ENTRIES`.
const MAX_SET_MEMBERS: usize = MAX_PAGE_ENTRIES;
/// Prunable roots deleted per pass. Kept roots are excluded by the selection
/// query itself, so kept sets never starve the pass.
const MAX_PRUNE_ROOTS: i64 = 50;

/// Outcome of one bounded maintenance pass.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RetentionReport {
    pub events_deleted: i64,
    pub pruned_through: i64,
    pub commands_deleted: i64,
    pub writes_deleted: i64,
    pub mail_deleted: i64,
}

struct SubtreeMember {
    id: String,
    depth: i64,
    state: String,
    created_at: i64,
    turn_bound: bool,
    workspace_ref: bool,
    probe_ref: bool,
    goal_ref: bool,
}

impl Store {
    /// Delete one bounded batch of expired history. Event pruning removes an
    /// expired contiguous prefix below every live cursor; command pruning
    /// removes whole closed child-sets leaf-first; writes pruning removes
    /// expired mutation bindings; mail pruning removes acknowledged messages
    /// older than the mail window, never unread mail. Projections, source
    /// recovery state, timers, and the event id allocator are never touched.
    /// An unset (`None`) window disables pruning for that side: no deletion
    /// without an owner window.
    pub fn prune_retention(
        &mut self,
        events_days: Option<i64>,
        commands_days: Option<i64>,
        mail_days: Option<i64>,
        now: i64,
    ) -> Result<RetentionReport> {
        self.ensure_writer_healthy()?;
        let result = self.prune_retention_inner(events_days, commands_days, mail_days, now);
        self.fault_on_persistence_error(&result);
        result
    }

    fn prune_retention_inner(
        &mut self,
        events_days: Option<i64>,
        commands_days: Option<i64>,
        mail_days: Option<i64>,
        now: i64,
    ) -> Result<RetentionReport> {
        let events_cutoff = match events_days {
            Some(days) => {
                if days <= 0 {
                    return Err(Error::new(
                        "invalid",
                        "retention windows must be positive days",
                    ));
                }
                Some(now.saturating_sub(days.saturating_mul(MS_PER_DAY)))
            }
            None => None,
        };
        let commands_cutoff = match commands_days {
            Some(days) => {
                if days <= 0 {
                    return Err(Error::new(
                        "invalid",
                        "retention windows must be positive days",
                    ));
                }
                // Command/write bindings must outlive the retry window no
                // matter how short the configured window is: a pruned id
                // must read back as id_expired, never as a new request.
                // The floor adds the allowed generator-clock skew, plus one
                // millisecond so the integer-millisecond boundary favors
                // retention: a ULID generated exactly the skew ahead of its
                // row still reads expired after pruning.
                let retry_floor = RETRY_WINDOW_MS + RETRY_CLOCK_SKEW_MS + 1;
                Some(now.saturating_sub(days.saturating_mul(MS_PER_DAY).max(retry_floor)))
            }
            None => None,
        };
        let mail_cutoff = match mail_days {
            Some(days) => {
                if days <= 0 {
                    return Err(Error::new(
                        "invalid",
                        "retention windows must be positive days",
                    ));
                }
                // Mail rows must outlive the retry window plus twice the
                // allowed clock skew no matter how short the configured
                // window is. Two skews chain here: an admitted write-id ULID
                // may run one skew ahead of its row's commit-clock stamps,
                // and the absent-delivery rule itself allows the retry window
                // plus one more skew before it reads `history_gap`. Past this
                // floor a pruned delivery admitted through `mail.send` lands
                // on the gap side of that rule on its own mailbox's read
                // paths, never `not_found`.
                let retry_floor = RETRY_WINDOW_MS + 2 * RETRY_CLOCK_SKEW_MS + 1;
                Some(now.saturating_sub(days.saturating_mul(MS_PER_DAY).max(retry_floor)))
            }
            None => None,
        };
        let (events_deleted, pruned_through) = match events_cutoff {
            Some(cutoff) => self.prune_events(cutoff)?,
            None => (0, coverage_pruned_through(&self.connection)?),
        };
        let (commands_deleted, writes_deleted) = match commands_cutoff {
            Some(cutoff) => self.prune_commands_and_writes(cutoff)?,
            None => (0, 0),
        };
        let mail_deleted = match mail_cutoff {
            Some(cutoff) => self.prune_mail(cutoff)?,
            None => 0,
        };
        Ok(RetentionReport {
            events_deleted,
            pruned_through,
            commands_deleted,
            writes_deleted,
            mail_deleted,
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
        // The open-child / turn / reference / oversize keep checks live in
        // the selection itself: only fully prunable roots are returned, so
        // kept sets never occupy the per-pass root budget and starve pruning.
        let roots: Vec<String> = self
            .connection
            .prepare(
                "WITH RECURSIVE
                 old_sets(root, id, depth) AS (
                     SELECT c.id, c.id, 0 FROM commands c
                     WHERE c.parent_command IS NULL AND c.created_at <= ?1
                     UNION ALL
                     SELECT s.root, ch.id, s.depth + 1 FROM commands ch
                     JOIN old_sets s ON ch.parent_command = s.id
                     WHERE s.depth < ?2
                 ),
                 kept(root) AS (
                     SELECT DISTINCT s.root FROM old_sets s
                     JOIN commands m ON m.id = s.id
                     WHERE m.state IN ('accepted', 'queued', 'dispatched', 'unknown')
                        OR m.created_at > ?1
                        OR s.depth >= ?2
                        OR EXISTS (
                            SELECT 1 FROM turns t
                            WHERE t.agent = m.target AND t.generation = m.generation
                              AND t.turn_id = m.turn_id
                              AND t.state IN ('running', 'unknown')
                        )
                        OR EXISTS (
                            SELECT 1 FROM workspaces w WHERE w.start_command_id = m.id
                        )
                        OR EXISTS (
                            SELECT 1 FROM provider_holds h WHERE h.probe_command = m.id
                        )
                        OR EXISTS (
                            SELECT 1 FROM goals g
                            WHERE g.agent = m.target AND g.mode = 'eliot'
                              AND g.generation = m.generation
                              AND json_extract(g.state, '$.status') IN ('active', 'paused')
                              AND json_extract(g.state, '$.last_continuation_command_id') = m.id
                        )
                 ),
                 oversize(root) AS (
                     SELECT s.root FROM old_sets s
                     GROUP BY s.root HAVING COUNT(*) > ?4
                 )
                 SELECT c.id FROM commands c
                 WHERE c.parent_command IS NULL
                   AND c.created_at <= ?1
                   AND NOT EXISTS (SELECT 1 FROM kept k WHERE k.root = c.id)
                   AND NOT EXISTS (SELECT 1 FROM oversize o WHERE o.root = c.id)
                 ORDER BY c.created_at, c.id LIMIT ?3",
            )
            .map_err(sql_error)?
            .query_map(
                params![
                    cutoff,
                    MAX_SET_DEPTH,
                    MAX_PRUNE_ROOTS,
                    MAX_SET_MEMBERS as i64
                ],
                |row| row.get(0),
            )
            .map_err(sql_error)?
            .collect::<rusqlite::Result<_>>()
            .map_err(sql_error)?;
        let mut doomed: Vec<SubtreeMember> = Vec::new();
        for root in &roots {
            let (members, truncated) = subtree_members_tx(&self.connection, root)?;
            // The selection already excludes oversize sets; this stays as a
            // backstop so one can never be partly pruned.
            if truncated {
                continue;
            }
            if members.len() + doomed.len() > MAX_PAGE_ENTRIES && !doomed.is_empty() {
                break;
            }
            if members.iter().any(|member| {
                member.is_open()
                    || member.created_at > cutoff
                    || member.turn_bound
                    || member.workspace_ref
                    || member.probe_ref
                    || member.goal_ref
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
            // Clear open-notice references to the exact doomed commands
            // first, preserving presentation time; unadmitted reservations
            // have no command row and can never be in this set.
            let mut clear = tx
                .prepare(
                    "UPDATE mail_notices SET open_notice_command = NULL
                     WHERE open_notice_command = ?1",
                )
                .map_err(sql_error)?;
            for member in &doomed {
                db!(clear.execute([&member.id]));
            }
        }
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

    /// Delete one bounded batch of acknowledged messages whose acknowledgement
    /// is older than the mail cutoff, oldest ack first. Unread rows
    /// (`read_at IS NULL`) never match, however old. Deletion is silent: no
    /// events are emitted. Later reads of a removed delivery return
    /// `history_gap` through the shared absent-id age rule, and a retried ack
    /// treats it as already acked.
    fn prune_mail(&mut self, cutoff: i64) -> Result<i64> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        let deleted = db!(tx.execute(
            &format!("DELETE FROM messages WHERE delivery_id IN ({PRUNE_MAIL_SELECT})"),
            params![cutoff, MAX_PAGE_ENTRIES as i64],
        ));
        commit_transaction(tx, &self.write_faulted)?;
        Ok(deleted as i64)
    }
}

/// The prune batch `prune_mail` deletes: acknowledged rows only, oldest ack
/// first. Shared with the plan test so the asserted plan cannot drift from
/// the executed query.
const PRUNE_MAIL_SELECT: &str = "SELECT delivery_id FROM messages
             WHERE read_at IS NOT NULL AND read_at <= ?1
             ORDER BY read_at, delivery_id LIMIT ?2";

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
    let mut end = first_fresh.map(|fresh| fresh - 1).unwrap_or(ceiling);
    // Only the latest batch of a source can be re-delivered idempotently: a
    // new batch replaces only its last-batch acknowledgement, so for an open
    // source pin only events at or after its latest batch start. Older native
    // events of an open source may go, and one old native event of a
    // long-lived source never blocks pruning. Native-bound events of closed
    // sources may go. A NULL batch start (no batch committed yet) pins from
    // the source's first native event instead of pinning nothing: the
    // comparison alone would silently disable the pin for that source.
    let first_pinned: Option<i64> = tx
        .query_row(
            "SELECT MIN(e.seq) FROM events e
             WHERE e.seq > ?1 AND e.seq <= ?2 AND e.native_id IS NOT NULL
               AND EXISTS (
                   SELECT 1 FROM source_state s
                   WHERE s.agent = e.agent AND s.generation = e.generation
                     AND s.source = e.source AND s.state = 'open'
                     AND (s.last_batch_first_seq IS NULL
                          OR e.seq >= s.last_batch_first_seq)
               )",
            params![pruned_through, ceiling],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    if let Some(pinned) = first_pinned {
        end = end.min(pinned - 1);
    }
    // `source.gap` events carry no native_id, so the native pin never covers
    // them — yet the Git coverage check re-reads the retained gap event by
    // `gap_seq` while its boundary is still live. Keep every event at or
    // after the oldest live gap of an open current-generation source: the
    // check reads the gap only while `gap_from` still equals the source
    // cursor (a NULL cursor reads as zero), so a superseded gap pins nothing
    // and can never stall pruning. Stale generations keep their open git rows
    // (generation transitions never close them), but like the Git cursor
    // floor above, only the readable current generation pins. Closed sources
    // pin nothing either: `record_git_observation` refuses them before the
    // coverage check, and gap re-acknowledgement returns the stored seq
    // without reading the event row.
    let first_gap: Option<i64> = tx
        .query_row(
            "SELECT MIN(s.gap_seq) FROM source_state s
             JOIN agents a ON a.id = s.agent
             WHERE s.state = 'open' AND s.gap_seq IS NOT NULL
               AND s.gap_from IS NOT NULL AND s.gap_from = COALESCE(s.cursor, '0')
               AND s.generation = a.generation
               AND s.gap_seq > ?1 AND s.gap_seq <= ?2",
            params![pruned_through, ceiling],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    if let Some(gap) = first_gap {
        end = end.min(gap - 1);
    }
    if end <= pruned_through {
        return Ok(None);
    }
    Ok(Some(end))
}

/// Current `rule_history_coverage.pruned_through` for reports when the events
/// side is disabled.
pub(super) fn coverage_pruned_through(connection: &Connection) -> Result<i64> {
    connection
        .query_row(
            "SELECT pruned_through FROM rule_history_coverage WHERE singleton = 1",
            [],
            |row| row.get(0),
        )
        .optional()
        .map_err(sql_error)?
        .ok_or_else(|| Error::new("source_integrity", "rule history coverage is absent"))
}

/// Every member of one command set with its keep signals. The caller deletes
/// the set only when no member asks to be kept. Collection stops past
/// `MAX_SET_MEMBERS` rows and reports truncation so the caller keeps the set:
/// an oversize set is never partly pruned.
fn subtree_members_tx(connection: &Connection, root: &str) -> Result<(Vec<SubtreeMember>, bool)> {
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
                    EXISTS(SELECT 1 FROM provider_holds h WHERE h.probe_command = c.id),
                    EXISTS(
                        SELECT 1 FROM goals g
                        WHERE g.agent = c.target AND g.mode = 'eliot'
                          AND g.generation = c.generation
                          AND json_extract(g.state, '$.status') IN ('active', 'paused')
                          AND json_extract(g.state, '$.last_continuation_command_id') = c.id
                    )
             FROM subtree s JOIN commands c ON c.id = s.id
             LIMIT ?3",
        )
        .map_err(sql_error)?;
    let rows = statement
        .query_map(
            params![root, MAX_SET_DEPTH, MAX_SET_MEMBERS as i64 + 1],
            |row| {
                Ok(SubtreeMember {
                    id: row.get(0)?,
                    depth: row.get(1)?,
                    state: row.get(2)?,
                    created_at: row.get(3)?,
                    turn_bound: row.get(4)?,
                    workspace_ref: row.get(5)?,
                    probe_ref: row.get(6)?,
                    goal_ref: row.get(7)?,
                })
            },
        )
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
    if members.len() > MAX_SET_MEMBERS {
        members.pop();
        return Ok((members, true));
    }
    Ok((members, false))
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
    use eliot_core::{CommandState, NativeEvent, SourceBatch, SourceGap, parse_agent_id};
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
        let report = store
            .prune_retention(Some(30), Some(30), None, NOW)
            .unwrap();
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
        let report = store
            .prune_retention(Some(30), Some(30), None, NOW)
            .unwrap();
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
        let report = store
            .prune_retention(Some(30), Some(30), None, NOW)
            .unwrap();
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
        let report = store
            .prune_retention(Some(30), Some(30), None, NOW)
            .unwrap();
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
        let report = store
            .prune_retention(Some(30), Some(30), None, NOW)
            .unwrap();
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
        let report = store
            .prune_retention(Some(30), Some(30), None, NOW)
            .unwrap();
        assert_eq!(report.events_deleted, 2);
        assert_eq!(report.pruned_through, 2);
        store
            .connection
            .execute(
                "UPDATE source_state SET state = 'closed' WHERE agent = ?1 AND source = 'git'",
                [agent],
            )
            .unwrap();
        let report = store
            .prune_retention(Some(30), Some(30), None, NOW)
            .unwrap();
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
        let report = store
            .prune_retention(Some(30), Some(30), None, NOW)
            .unwrap();
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
        let report = store
            .prune_retention(Some(30), Some(30), None, NOW)
            .unwrap();
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
        let report = store
            .prune_retention(Some(30), Some(30), None, NOW)
            .unwrap();
        assert_eq!(report.events_deleted, MAX_PAGE_ENTRIES as i64);
        assert_eq!(report.pruned_through, MAX_PAGE_ENTRIES as i64);
        let report = store
            .prune_retention(Some(30), Some(30), None, NOW)
            .unwrap();
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
        let report = store
            .prune_retention(Some(30), Some(30), None, NOW)
            .unwrap();
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
        let report = store
            .prune_retention(Some(30), Some(30), None, NOW)
            .unwrap();
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
        let report = store
            .prune_retention(Some(30), Some(30), None, NOW)
            .unwrap();
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
        let report = store
            .prune_retention(Some(30), Some(30), None, NOW)
            .unwrap();
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
        let report = store
            .prune_retention(Some(30), Some(30), None, NOW)
            .unwrap();
        assert_eq!(report.commands_deleted, 0);
        assert_eq!(command_count(&store), 2);
        // The retry window holds even for a one-day window.
        let report = store.prune_retention(Some(30), Some(1), None, NOW).unwrap();
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
        let report = store
            .prune_retention(Some(30), Some(30), None, NOW)
            .unwrap();
        assert_eq!(report.commands_deleted, 0);
        store
            .connection
            .execute(
                "UPDATE turns SET state = 'unknown' WHERE agent = ?1 AND turn_id = 'turn-1'",
                [agent],
            )
            .unwrap();
        let report = store
            .prune_retention(Some(30), Some(30), None, NOW)
            .unwrap();
        assert_eq!(report.commands_deleted, 0);
        store
            .connection
            .execute(
                "UPDATE turns SET state = 'completed' WHERE agent = ?1 AND turn_id = 'turn-1'",
                [agent],
            )
            .unwrap();
        let report = store
            .prune_retention(Some(30), Some(30), None, NOW)
            .unwrap();
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
        let report = store
            .prune_retention(Some(30), Some(30), None, NOW)
            .unwrap();
        assert_eq!(report.commands_deleted, 0);
        assert_eq!(command_count(&store), 2);
        store
            .connection
            .execute("DELETE FROM provider_holds WHERE scope = 'scope-1'", [])
            .unwrap();
        let report = store
            .prune_retention(Some(30), Some(30), None, NOW)
            .unwrap();
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
        let report = store
            .prune_retention(Some(30), Some(30), None, NOW)
            .unwrap();
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
            "harness": "fake",
            "profile": "fake",
            "profile_snapshot": {},
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
        let report = store
            .prune_retention(Some(30), Some(30), None, NOW)
            .unwrap();
        assert_eq!(report.commands_deleted, 1);
        let error = store.submit("owner", request, NOW).unwrap_err();
        assert_eq!(error.code, "id_expired");

        let mut fresh = SubmitCommand::new(ulid_at(NOW, 2), "launch");
        fresh.args = json!({
            "project": "retention-project",
            "workspace": "retention-workspace",
            "harness": "fake",
            "profile": "fake",
            "profile_snapshot": {},
        });
        fresh.expected_generation = Some(0);
        let admission = store.submit("owner", fresh.clone(), NOW).unwrap();
        assert!(admission.is_new);
        let report = store
            .prune_retention(Some(30), Some(30), None, NOW)
            .unwrap();
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
        let report = store
            .prune_retention(Some(30), Some(30), None, NOW)
            .unwrap();
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
        let report = store
            .prune_retention(Some(30), Some(30), None, NOW)
            .unwrap();
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
        for (events_days, commands_days, mail_days) in [
            (0, 30, 30),
            (30, 0, 30),
            (30, 30, 0),
            (-1, 30, 30),
            (30, -1, 30),
            (30, 30, -1),
        ] {
            let error = store
                .prune_retention(Some(events_days), Some(commands_days), Some(mail_days), NOW)
                .unwrap_err();
            assert_eq!(error.code, "invalid");
        }
        assert!(!store.is_write_faulted());
    }

    #[test]
    fn unset_retention_disables_pruning_per_side() {
        let mut store = store();
        let old = NOW - 31 * DAY_MS;
        for _ in 0..3 {
            insert_event(&mut store, old);
        }
        insert_command(&mut store, command_seed(Ulid::new().to_string(), old));
        let report = store.prune_retention(None, None, None, NOW).unwrap();
        assert_eq!(report.events_deleted, 0);
        assert_eq!(report.commands_deleted, 0);
        assert_eq!(report.writes_deleted, 0);
        assert_eq!(report.pruned_through, 0);
        assert_eq!(event_count(&store), 3);
        assert_eq!(command_count(&store), 1);
        // Each side runs only when its own window is configured.
        let report = store.prune_retention(Some(30), None, None, NOW).unwrap();
        assert_eq!(report.events_deleted, 3);
        assert_eq!(report.pruned_through, 3);
        assert_eq!(report.commands_deleted, 0);
        assert_eq!(command_count(&store), 1);
    }

    #[test]
    fn mail_pruning_removes_only_old_acked_rows() {
        let mut store = store();
        let old = NOW - 31 * DAY_MS;
        let young = NOW - DAY_MS;
        insert_message(&mut store, "old-acked/a1", old, Some(old));
        insert_message(&mut store, "young-acked/a1", old, Some(young));
        insert_message(&mut store, "ancient-unread/a1", old, None);
        insert_message(&mut store, "fresh-unread/a1", NOW, None);

        let report = store.prune_retention(None, None, Some(30), NOW).unwrap();
        assert_eq!(report.mail_deleted, 1);
        assert_eq!(report.events_deleted, 0);
        assert_eq!(report.commands_deleted, 0);
        assert_eq!(report.writes_deleted, 0);
        assert!(!message_exists(&store, "old-acked/a1"));
        assert!(message_exists(&store, "young-acked/a1"));
        // Unread mail is never pruned, however old.
        assert!(message_exists(&store, "ancient-unread/a1"));
        assert!(message_exists(&store, "fresh-unread/a1"));
    }

    #[test]
    fn mail_pruning_honors_the_retry_floor_for_short_windows() {
        let mut store = store();
        // A one-day window still keeps rows acked more recently than the
        // retry window plus twice the allowed clock skew, so a pruned
        // delivery admitted with a skew-bounded write id reads `history_gap`
        // on its own mailbox's read paths.
        let floor = RETRY_WINDOW_MS + 2 * RETRY_CLOCK_SKEW_MS + 1;
        insert_message(
            &mut store,
            "above-cutoff/a1",
            NOW - floor + 1,
            Some(NOW - floor + 1),
        );
        insert_message(&mut store, "at-cutoff/a1", NOW - floor, Some(NOW - floor));
        let report = store.prune_retention(None, None, Some(1), NOW).unwrap();
        assert_eq!(report.mail_deleted, 1);
        assert!(message_exists(&store, "above-cutoff/a1"));
        assert!(!message_exists(&store, "at-cutoff/a1"));
    }

    #[test]
    fn mail_pruning_deletes_one_bounded_batch_per_pass() {
        let mut store = store();
        let old = NOW - 31 * DAY_MS;
        store.connection.execute_batch("BEGIN IMMEDIATE").unwrap();
        for index in 0..MAX_PAGE_ENTRIES + 5 {
            insert_message(&mut store, &format!("batch-{index}/a1"), old, Some(old));
        }
        store.connection.execute_batch("COMMIT").unwrap();

        let report = store.prune_retention(None, None, Some(30), NOW).unwrap();
        assert_eq!(report.mail_deleted, MAX_PAGE_ENTRIES as i64);
        let report = store.prune_retention(None, None, Some(30), NOW).unwrap();
        assert_eq!(report.mail_deleted, 5);
        let report = store.prune_retention(None, None, Some(30), NOW).unwrap();
        assert_eq!(report.mail_deleted, 0);
    }

    #[test]
    fn prune_mail_seeks_the_acked_index_without_scan_or_sort() {
        let store = store();
        let mut statement = store
            .connection
            .prepare(&format!("EXPLAIN QUERY PLAN {PRUNE_MAIL_SELECT}"))
            .unwrap();
        let plan = statement
            .query_map(params![NOW, MAX_PAGE_ENTRIES as i64], |row| {
                row.get::<_, String>(3)
            })
            .unwrap()
            .collect::<rusqlite::Result<Vec<String>>>()
            .unwrap()
            .join("\n");
        assert!(
            plan.contains("USING INDEX messages_prune_acked")
                || plan.contains("USING COVERING INDEX messages_prune_acked"),
            "prune batch should seek the acked-mail index, got:\n{plan}"
        );
        assert!(
            !plan.contains("SCAN messages"),
            "prune batch should not table-scan messages, got:\n{plan}"
        );
        assert!(
            !plan.contains("USE TEMP B-TREE FOR ORDER BY"),
            "prune batch should not sort, got:\n{plan}"
        );
    }

    #[test]
    fn unset_mail_window_deletes_no_mail() {
        let mut store = store();
        let old = NOW - 31 * DAY_MS;
        insert_message(&mut store, "old-acked/a1", old, Some(old));
        let report = store.prune_retention(None, None, None, NOW).unwrap();
        assert_eq!(report.mail_deleted, 0);
        assert!(message_exists(&store, "old-acked/a1"));
    }

    fn launch_agent(store: &mut Store, project: &str, now: i64) -> (String, String) {
        let launch_id = ulid_at(now, 7);
        let mut launch = SubmitCommand::new(launch_id.clone(), "launch");
        launch.args = json!({
            "project": project,
            "workspace": "retention-workspace",
            "harness": "fake",
            "profile": "fake",
            "profile_snapshot": {},
        });
        launch.expected_generation = Some(0);
        let admission = store.submit("owner", launch, now).unwrap();
        let agent = admission.command.target.unwrap();
        store.dispatch(&launch_id, now).unwrap();
        (agent, launch_id)
    }

    fn commit_ready(store: &mut Store, agent: &str, launch_id: &str, batch_id: &str, now: i64) {
        store
            .put_batch(
                SourceBatch {
                    batch_id: batch_id.to_owned(),
                    agent_id: agent.to_owned(),
                    generation: 1,
                    source: "stream".to_owned(),
                    events: vec![NativeEvent {
                        ts: None,
                        kind: "session.ready".to_owned(),
                        native_id: format!("{batch_id}:ready"),
                        data: json!({"command_id": launch_id}),
                    }],
                    cursor: None,
                },
                now,
            )
            .unwrap();
    }

    fn source_state(store: &Store, agent_num: i64) -> String {
        store
            .connection
            .query_row(
                "SELECT state FROM source_state
                 WHERE agent = ?1 AND generation = 1 AND source = 'stream'",
                [agent_num],
                |row| row.get(0),
            )
            .unwrap()
    }

    fn event_exists(store: &Store, seq: i64) -> bool {
        store
            .connection
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM events WHERE seq = ?1)",
                [seq],
                |row| row.get(0),
            )
            .unwrap()
    }

    fn insert_message(
        store: &mut Store,
        delivery_id: &str,
        admitted_at: i64,
        read_at: Option<i64>,
    ) {
        store
            .connection
            .execute(
                "INSERT INTO messages(delivery_id, sender, recipient, recipient_generation,
                                      mode, text, content_digest, admitted_at, read_at)
                 VALUES (?1, 'owner', 1, 1, 'queue', 'seeded', x'00', ?2, ?3)",
                params![delivery_id, admitted_at, read_at],
            )
            .unwrap();
    }

    fn message_exists(store: &Store, delivery_id: &str) -> bool {
        store
            .connection
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM messages WHERE delivery_id = ?1)",
                [delivery_id],
                |row| row.get(0),
            )
            .unwrap()
    }

    fn native_seq(store: &Store, native_id: &str) -> i64 {
        store
            .connection
            .query_row(
                "SELECT seq FROM events WHERE native_id = ?1",
                [native_id],
                |row| row.get(0),
            )
            .unwrap()
    }

    fn insert_native_event(store: &mut Store, agent: i64, ts: i64, native_id: &str) -> i64 {
        store
            .connection
            .execute(
                "INSERT INTO events(ts, kind, agent, generation, source, native_id, data)
                 VALUES (?1, 'message.completed', ?2, 1, 'stream', ?3, '{}')",
                params![ts, agent, native_id],
            )
            .unwrap();
        store.connection.last_insert_rowid()
    }

    /// Commit a `turn.started` batch for a fresh send and return the batch,
    /// so the caller can re-send the identical batch later.
    fn commit_started(store: &mut Store, agent: &str, command_id: &str, now: i64) -> SourceBatch {
        store
            .submit(
                "owner",
                SubmitCommand {
                    id: command_id.to_owned(),
                    target: Some(agent.to_owned()),
                    selector: None,
                    verb: "send".to_owned(),
                    args: json!({"text": "retention second batch"}),
                    expected_generation: Some(1),
                    expected_turn_id: None,
                    request_id: None,
                    probe: false,
                },
                now,
            )
            .unwrap();
        store.dispatch(command_id, now + 1).unwrap();
        let batch = SourceBatch {
            batch_id: format!("{command_id}-started"),
            agent_id: agent.to_owned(),
            generation: 1,
            source: "stream".to_owned(),
            events: vec![NativeEvent {
                ts: None,
                kind: "turn.started".to_owned(),
                native_id: format!("{command_id}-started"),
                data: json!({"command_id": command_id, "turn_id": format!("turn-{command_id}")}),
            }],
            cursor: None,
        };
        store.put_batch(batch.clone(), now + 2).unwrap();
        batch
    }

    #[test]
    fn prune_events_prunes_older_native_events_of_open_source() {
        let mut store = store();
        let old = NOW - 31 * DAY_MS;
        let (agent, launch_id) = launch_agent(&mut store, "retention-native", old);
        let agent_num = parse_agent_id(&agent).unwrap();
        commit_ready(&mut store, &agent, &launch_id, "ready-batch", old);
        store
            .settle(&launch_id, CommandState::Applied, None, old)
            .unwrap();
        let send_id = ulid_at(old + 3, 4);
        commit_started(&mut store, &agent, &send_id, old + 3);
        assert_eq!(source_state(&store, agent_num), "open");
        let ready_seq = native_seq(&store, "ready-batch:ready");
        let started_seq = native_seq(&store, &format!("{send_id}-started"));
        assert!(ready_seq < started_seq);

        // Only the latest batch start pins the open source: the older native
        // event prunes while the source stays open.
        let report = store.prune_retention(Some(30), None, None, NOW).unwrap();
        assert_eq!(report.pruned_through, started_seq - 1);
        assert!(!event_exists(&store, ready_seq));
        assert!(event_exists(&store, started_seq));
        assert_eq!(source_state(&store, agent_num), "open");

        // Native-bound events of closed sources may go.
        store
            .connection
            .execute(
                "UPDATE source_state SET state = 'closed'
                 WHERE agent = ?1 AND generation = 1 AND source = 'stream'",
                [agent_num],
            )
            .unwrap();
        let report = store.prune_retention(Some(30), None, None, NOW).unwrap();
        assert!(report.events_deleted > 0);
        assert!(!event_exists(&store, started_seq));
    }

    #[test]
    fn prune_events_latest_batch_redelivers_idempotently_after_pruning() {
        let mut store = store();
        let old = NOW - 31 * DAY_MS;
        let (agent, launch_id) = launch_agent(&mut store, "retention-redelivery", old);
        commit_ready(&mut store, &agent, &launch_id, "ready-batch", old);
        store
            .settle(&launch_id, CommandState::Applied, None, old)
            .unwrap();
        let send_id = ulid_at(old + 3, 5);
        let batch = commit_started(&mut store, &agent, &send_id, old + 3);
        let first = store.put_batch(batch.clone(), old + 4).unwrap();
        assert!(first.events.is_empty());
        let ready_seq = native_seq(&store, "ready-batch:ready");

        let report = store.prune_retention(Some(30), None, None, NOW).unwrap();
        assert!(report.events_deleted > 0);
        assert!(!event_exists(&store, ready_seq));
        assert!(event_exists(&store, first.ack.first_seq));
        let water_before = store.high_water().unwrap();

        // Re-sending the already committed latest batch (same id + digest)
        // after pruning returns the stored acknowledgement and allocates no
        // new seq.
        let redelivery = store.put_batch(batch, NOW).unwrap();
        assert_eq!(redelivery.ack, first.ack);
        assert!(redelivery.events.is_empty());
        assert_eq!(store.high_water().unwrap(), water_before);
    }

    #[test]
    fn prune_events_pins_live_gap_event_for_git_coverage() {
        let mut store = store();
        let old = NOW - 31 * DAY_MS;
        let (agent, _) = launch_agent(&mut store, "retention-gap", old);
        let agent_num = parse_agent_id(&agent).unwrap();
        store
            .ensure_git_source(&agent, 1, "retention-workspace", old)
            .unwrap();
        let gap = store
            .record_source_gap(
                SourceGap {
                    gap_id: "gap-1".to_owned(),
                    agent_id: agent.clone(),
                    generation: 1,
                    source: "git".to_owned(),
                    families: vec!["file".to_owned(), "git".to_owned()],
                    reason: "retained gap pin".to_owned(),
                    from: Some("0".to_owned()),
                    to: Some("60".to_owned()),
                },
                old,
            )
            .unwrap();
        let gap_seq = gap.value.seq;
        // Advance the cursor past the retained gap while keeping its boundary
        // live: the Git hint cursor pin no longer covers the gap event, so
        // only the gap pin keeps it. The Store accepts any gap boundary
        // strings; the coverage check reads the retained boundary.
        store
            .connection
            .execute(
                "UPDATE source_state SET cursor = '50', gap_from = '50'
                 WHERE agent = ?1 AND generation = 1 AND source = 'git'",
                [agent_num],
            )
            .unwrap();
        assert!(gap_seq < 50);

        let report = store.prune_retention(Some(30), None, None, NOW).unwrap();
        assert_eq!(report.events_deleted, gap_seq - 1);
        assert_eq!(report.pruned_through, gap_seq - 1);
        assert!(event_exists(&store, gap_seq));

        // The Git coverage check still passes afterwards: the retained gap
        // event is present with a matching digest.
        let source = crate::load_source_state(&store.connection, agent_num, 1, "git")
            .unwrap()
            .expect("git source state");
        for family in ["file", "git"] {
            assert!(
                crate::git_interval_gap_covers(
                    &store.connection,
                    agent_num,
                    1,
                    &source,
                    50,
                    60,
                    family
                )
                .unwrap(),
                "coverage still passes for {family}"
            );
        }
    }

    #[test]
    fn prune_events_scopes_gap_pin_to_current_generation() {
        let mut store = store();
        let old = NOW - 31 * DAY_MS;
        let (agent, launch_id) = launch_agent(&mut store, "retention-gap-gen", old);
        let agent_num = parse_agent_id(&agent).unwrap();
        commit_ready(&mut store, &agent, &launch_id, "ready-batch", old);
        store
            .settle(&launch_id, CommandState::Applied, None, old)
            .unwrap();
        store
            .ensure_git_source(&agent, 1, "retention-workspace", old)
            .unwrap();
        // Stale gap: recorded while generation 1 is current, kept live.
        let stale = store
            .record_source_gap(
                SourceGap {
                    gap_id: "gap-stale".to_owned(),
                    agent_id: agent.clone(),
                    generation: 1,
                    source: "git".to_owned(),
                    families: vec!["file".to_owned(), "git".to_owned()],
                    reason: "stale generation gap".to_owned(),
                    from: Some("50".to_owned()),
                    to: Some("60".to_owned()),
                },
                old + 1,
            )
            .unwrap();
        let stale_seq = stale.value.seq;
        store
            .connection
            .execute(
                "UPDATE source_state SET cursor = '50'
                 WHERE agent = ?1 AND generation = 1 AND source = 'git'",
                [agent_num],
            )
            .unwrap();

        // Real lifecycle transition: stop then resume. Resume closes every
        // old-generation source except os/git/workspace_process, so the
        // generation-1 git row stays open with its live gap.
        store
            .connection
            .execute(
                "UPDATE agents SET native_scope = 'retention-gap-gen:scope',
                 native_ref = 'retention-gap-gen:thread' WHERE id = ?1",
                [agent_num],
            )
            .unwrap();
        crate::tests::install_live_custody_observations(&mut store, &agent, 1, old + 2);
        store
            .put_batch(
                SourceBatch {
                    batch_id: "ended-batch".to_owned(),
                    agent_id: agent.clone(),
                    generation: 1,
                    source: "stream".to_owned(),
                    events: vec![NativeEvent {
                        ts: None,
                        kind: "session.ended".to_owned(),
                        native_id: "ended-batch:ended".to_owned(),
                        data: json!({}),
                    }],
                    cursor: None,
                },
                old + 2,
            )
            .unwrap();
        let stop_id = ulid_at(old + 3, 8);
        let mut stop = SubmitCommand::new(stop_id.clone(), "stop");
        stop.target = Some(agent.clone());
        stop.expected_generation = Some(1);
        store.submit("owner", stop, old + 3).unwrap();
        store.dispatch(&stop_id, old + 4).unwrap();
        store
            .settle(
                &stop_id,
                CommandState::Applied,
                Some(json!({"state": "applied"})),
                old + 5,
            )
            .unwrap();
        crate::tests::simulate_normal_stop_proof(&mut store, &agent, 1, old + 5);
        let resume_id = ulid_at(old + 6, 9);
        let mut resume = SubmitCommand::new(resume_id.clone(), "resume");
        resume.target = Some(agent.clone());
        resume.expected_generation = Some(1);
        store.submit("owner", resume, old + 6).unwrap();
        store.dispatch(&resume_id, old + 7).unwrap();
        assert_eq!(store.get_agent(&agent).unwrap().unwrap().generation, 2);

        // The stale row survives the transition open with its live gap.
        let stale_row: (String, Option<i64>, Option<String>, Option<String>) = store
            .connection
            .query_row(
                "SELECT state, gap_seq, gap_from, cursor FROM source_state
                 WHERE agent = ?1 AND generation = 1 AND source = 'git'",
                [agent_num],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(
            stale_row,
            (
                "open".to_owned(),
                Some(stale_seq),
                Some("50".to_owned()),
                Some("50".to_owned())
            )
        );

        // Current-generation gap with its own live boundary.
        store
            .ensure_git_source(&agent, 2, "retention-workspace", old + 8)
            .unwrap();
        let live = store
            .record_source_gap(
                SourceGap {
                    gap_id: "gap-live".to_owned(),
                    agent_id: agent.clone(),
                    generation: 2,
                    source: "git".to_owned(),
                    families: vec!["file".to_owned(), "git".to_owned()],
                    reason: "current generation gap".to_owned(),
                    from: Some("4000".to_owned()),
                    to: Some("4060".to_owned()),
                },
                old + 9,
            )
            .unwrap();
        let live_seq = live.value.seq;
        assert!(stale_seq < live_seq);
        store
            .connection
            .execute(
                "UPDATE source_state SET cursor = '4000'
                 WHERE agent = ?1 AND generation = 2 AND source = 'git'",
                [agent_num],
            )
            .unwrap();
        assert!(live_seq < 4000);

        let report = store.prune_retention(Some(30), None, None, NOW).unwrap();
        assert_eq!(report.pruned_through, live_seq - 1);
        assert!(
            !event_exists(&store, stale_seq),
            "stale generation gap no longer pins"
        );
        assert!(
            event_exists(&store, live_seq),
            "current generation gap still pins"
        );
    }

    #[test]
    fn prune_events_null_batch_start_pins_from_first_native_event() {
        let mut store = store();
        let old = NOW - 31 * DAY_MS;
        let below = insert_event(&mut store, old);
        let agent = insert_agent(&mut store, 1);
        store
            .connection
            .execute(
                "INSERT INTO source_state(agent, generation, source, state, health, updated_at)
                 VALUES (?1, 1, 'stream', 'open', 'ok', ?2)",
                params![agent, NOW],
            )
            .unwrap();
        let first = insert_native_event(&mut store, agent, old, "null-pin-first");
        let second = insert_native_event(&mut store, agent, old, "null-pin-second");

        // No batch committed yet (`last_batch_first_seq IS NULL`): the open
        // source pins from its first native event instead of pinning nothing.
        let report = store.prune_retention(Some(30), None, None, NOW).unwrap();
        assert_eq!(report.pruned_through, first - 1);
        assert!(!event_exists(&store, below));
        assert!(event_exists(&store, first));
        assert!(event_exists(&store, second));
    }

    #[test]
    fn prune_events_pins_minimum_across_two_open_sources() {
        let mut store = store();
        let old = NOW - 31 * DAY_MS;
        // Commit the lower pin first, so the minimum belongs to the earlier
        // source and the later source cannot mask it.
        let (agent_b, launch_b) = launch_agent(&mut store, "retention-min-b", old);
        commit_ready(&mut store, &agent_b, &launch_b, "ready-b", old);
        store
            .settle(&launch_b, CommandState::Applied, None, old)
            .unwrap();
        let send_b = ulid_at(old + 3, 4);
        commit_started(&mut store, &agent_b, &send_b, old + 3);
        let other = old + 1_000;
        let (agent_a, launch_a) = launch_agent(&mut store, "retention-min-a", other);
        commit_ready(&mut store, &agent_a, &launch_a, "ready-a", other);
        store
            .settle(&launch_a, CommandState::Applied, None, other)
            .unwrap();
        let send_a = ulid_at(other + 3, 5);
        commit_started(&mut store, &agent_a, &send_a, other + 3);
        let pin_b = native_seq(&store, &format!("{send_b}-started"));
        let pin_a = native_seq(&store, &format!("{send_a}-started"));
        assert!(pin_b < pin_a);
        let ready_b = native_seq(&store, "ready-b:ready");
        assert!(ready_b < pin_b);

        let report = store.prune_retention(Some(30), None, None, NOW).unwrap();
        assert_eq!(report.pruned_through, pin_b - 1);
        assert!(!event_exists(&store, ready_b));
        assert!(event_exists(&store, pin_b));
        assert!(event_exists(&store, pin_a));
    }

    #[test]
    fn prune_commands_skips_kept_roots_without_starving() {
        let mut store = store();
        let old = NOW - 31 * DAY_MS;
        // More than one root page of old roots whose children stay unknown,
        // all sorting before the prunable roots.
        for index in 0..60 {
            let parent = Ulid::new().to_string();
            insert_command(&mut store, command_seed(parent.clone(), old));
            insert_command(
                &mut store,
                CommandSeed {
                    parent: Some((parent, "rule_action".to_owned(), 0)),
                    state: "unknown".to_owned(),
                    ..command_seed(Ulid::new().to_string(), old + index)
                },
            );
        }
        for _ in 0..5 {
            insert_command(
                &mut store,
                command_seed(Ulid::new().to_string(), old + DAY_MS),
            );
        }
        assert_eq!(command_count(&store), 125);
        let report = store.prune_retention(None, Some(30), None, NOW).unwrap();
        assert_eq!(report.commands_deleted, 5);
        assert_eq!(command_count(&store), 120);
        // Nothing prunable remains; the kept roots are untouched.
        let report = store.prune_retention(None, Some(30), None, NOW).unwrap();
        assert_eq!(report.commands_deleted, 0);
        assert_eq!(command_count(&store), 120);
        let unknown_children: i64 = store
            .connection
            .query_row(
                "SELECT COUNT(*) FROM commands WHERE state = 'unknown'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(unknown_children, 60);
    }

    #[test]
    fn prune_commands_skips_oversize_roots_without_starving() {
        let mut store = store();
        let old = NOW - 31 * DAY_MS;
        // More than one root page of old oversize sets, all sorting before
        // the prunable roots: each holds MAX_SET_MEMBERS + 1 rows.
        let mut oversize_roots = Vec::new();
        for _ in 0..(MAX_PRUNE_ROOTS as usize + 5) {
            let root = Ulid::new().to_string();
            insert_command(&mut store, command_seed(root.clone(), old));
            for ordinal in 0..MAX_SET_MEMBERS as i64 {
                insert_command(
                    &mut store,
                    CommandSeed {
                        parent: Some((root.clone(), "rule_action".to_owned(), ordinal)),
                        state: "applied".to_owned(),
                        ..command_seed(Ulid::new().to_string(), old)
                    },
                );
            }
            oversize_roots.push(root);
        }
        for _ in 0..5 {
            insert_command(
                &mut store,
                command_seed(Ulid::new().to_string(), old + DAY_MS),
            );
        }
        assert_eq!(
            command_count(&store),
            (MAX_PRUNE_ROOTS as usize + 5) as i64 * (MAX_SET_MEMBERS as i64 + 1) + 5
        );
        let report = store.prune_retention(None, Some(30), None, NOW).unwrap();
        assert_eq!(report.commands_deleted, 5);
        for root in &oversize_roots {
            assert!(store.get_command(root).unwrap().is_some());
        }
        // Nothing prunable remains; the oversize sets are untouched.
        let report = store.prune_retention(None, Some(30), None, NOW).unwrap();
        assert_eq!(report.commands_deleted, 0);
        assert_eq!(
            command_count(&store),
            (MAX_PRUNE_ROOTS as usize + 5) as i64 * (MAX_SET_MEMBERS as i64 + 1)
        );
    }

    #[test]
    fn prune_commands_covers_ulid_clock_skew() {
        let mut store = store();
        // The ULID runs the full allowed skew ahead of the commit clock, and
        // the row sits inside the skew-extended floor but past the bare
        // 24-hour floor: without the margin it would prune while its id is
        // still valid.
        let skew = RETRY_CLOCK_SKEW_MS;
        let submitted_at = NOW - RETRY_WINDOW_MS - skew + 60_000;
        let mut request = SubmitCommand::new(ulid_at(submitted_at + skew, 1), "launch");
        request.args = json!({
            "project": "retention-skew-project",
            "workspace": "retention-skew-workspace",
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
        let report = store.prune_retention(None, Some(1), None, NOW).unwrap();
        assert_eq!(report.commands_deleted, 0);
        assert!(store.get_command(&request.id).unwrap().is_some());
        // The id is still live, so it replays instead of expiring.
        let replay = store.submit("owner", request, NOW).unwrap();
        assert!(!replay.is_new);
    }

    fn start_send_turn(
        store: &mut Store,
        agent: &str,
        command_id: &str,
        probe: bool,
        now: i64,
    ) -> String {
        store
            .submit(
                "owner",
                SubmitCommand {
                    id: command_id.to_owned(),
                    target: Some(agent.to_owned()),
                    selector: None,
                    verb: "send".to_owned(),
                    args: json!({"text": "retention probe input"}),
                    expected_generation: Some(1),
                    expected_turn_id: None,
                    request_id: None,
                    probe,
                },
                now,
            )
            .unwrap();
        store.dispatch(command_id, now + 1).unwrap();
        let turn_id = format!("turn-{command_id}");
        store
            .put_batch(
                SourceBatch {
                    batch_id: format!("{command_id}-started"),
                    agent_id: agent.to_owned(),
                    generation: 1,
                    source: "stream".to_owned(),
                    events: vec![NativeEvent {
                        ts: None,
                        kind: "turn.started".to_owned(),
                        native_id: format!("{command_id}-started"),
                        data: json!({"command_id": command_id, "turn_id": turn_id}),
                    }],
                    cursor: None,
                },
                now + 2,
            )
            .unwrap();
        turn_id
    }

    #[test]
    fn provider_probe_scan_fails_loudly_across_pruned_history() {
        let mut store = store();
        let old = NOW - 31 * DAY_MS;
        let (agent, launch_id) = launch_agent(&mut store, "retention-probe", old);
        let agent_num = parse_agent_id(&agent).unwrap();
        commit_ready(&mut store, &agent, &launch_id, "probe-ready", old);
        store
            .settle(&launch_id, CommandState::Applied, None, old)
            .unwrap();
        // A failed turn records a rate-limit hold; the probe send claims it.
        let first_id = ulid_at(old + 3, 2);
        let first_turn = start_send_turn(&mut store, &agent, &first_id, false, old + 3);
        store
            .put_batch(
                SourceBatch {
                    batch_id: format!("{first_id}-completed"),
                    agent_id: agent.clone(),
                    generation: 1,
                    source: "stream".to_owned(),
                    events: vec![NativeEvent {
                        ts: None,
                        kind: "turn.completed".to_owned(),
                        native_id: format!("{first_id}-completed"),
                        data: json!({
                            "command_id": first_id,
                            "turn_id": first_turn,
                            "status": "failed",
                            "provider_restriction": {"kind": "rate_limit", "retry_after": None::<i64>},
                        }),
                    }],
                    cursor: None,
                },
                old + 6,
            )
            .unwrap();
        let probe_id = ulid_at(old + 7, 3);
        let probe_turn = start_send_turn(&mut store, &agent, &probe_id, true, old + 7);
        let accepted_seq: i64 = store
            .connection
            .query_row(
                "SELECT MAX(seq) FROM events
                 WHERE kind = 'command.accepted'
                   AND json_extract(data, '$.command_id') = ?1",
                [&probe_id],
                |row| row.get(0),
            )
            .unwrap();

        // Prune the probe's accepted event while the source is closed, then
        // reopen: the terminal scan must fail loudly instead of clearing the
        // hold on incomplete history.
        store
            .connection
            .execute(
                "UPDATE source_state SET state = 'closed'
                 WHERE agent = ?1 AND generation = 1 AND source = 'stream'",
                [agent_num],
            )
            .unwrap();
        let report = store.prune_retention(Some(30), None, None, NOW).unwrap();
        assert!(report.pruned_through >= accepted_seq);
        let accepted_survives: bool = store
            .connection
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM events WHERE seq = ?1)",
                [accepted_seq],
                |row| row.get(0),
            )
            .unwrap();
        assert!(!accepted_survives);
        store
            .connection
            .execute(
                "UPDATE source_state SET state = 'open'
                 WHERE agent = ?1 AND generation = 1 AND source = 'stream'",
                [agent_num],
            )
            .unwrap();
        let terminal = store.put_batch(
            SourceBatch {
                batch_id: format!("{probe_id}-completed"),
                agent_id: agent.clone(),
                generation: 1,
                source: "stream".to_owned(),
                events: vec![NativeEvent {
                    ts: None,
                    kind: "turn.completed".to_owned(),
                    native_id: format!("{probe_id}-completed"),
                    data: json!({
                        "command_id": probe_id,
                        "turn_id": probe_turn,
                        "status": "completed",
                    }),
                }],
                cursor: None,
            },
            NOW,
        );
        assert_eq!(terminal.unwrap_err().code, "source_integrity");
        let hold_state: String = store
            .connection
            .query_row(
                "SELECT state FROM provider_holds WHERE scope = 'profile:fake'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(hold_state, "probing");
    }

    #[test]
    fn oversize_command_set_is_kept_not_partly_pruned() {
        let mut store = store();
        let old = NOW - 31 * DAY_MS;
        let root = Ulid::new().to_string();
        insert_command(&mut store, command_seed(root.clone(), old));
        for ordinal in 0..MAX_SET_MEMBERS as i64 + 1 {
            insert_command(
                &mut store,
                CommandSeed {
                    parent: Some((root.clone(), "rule_action".to_owned(), ordinal)),
                    state: "applied".to_owned(),
                    ..command_seed(Ulid::new().to_string(), old)
                },
            );
        }
        let (members, truncated) = subtree_members_tx(&store.connection, &root).unwrap();
        assert!(truncated);
        assert_eq!(members.len(), MAX_SET_MEMBERS);
        let prunable = Ulid::new().to_string();
        insert_command(&mut store, command_seed(prunable.clone(), old));
        let report = store.prune_retention(None, Some(30), None, NOW).unwrap();
        assert_eq!(report.commands_deleted, 1);
        assert!(store.get_command(&root).unwrap().is_some());
        assert!(store.get_command(&prunable).unwrap().is_none());
    }
}
