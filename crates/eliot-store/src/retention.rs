//! Bounded physical retention: expired event-prefix pruning, atomic
//! command/child-set cleanup, exited child process pruning, and
//! acknowledged-mail pruning through the one Store writer.
//!
//! Owning contract: docs/spec/core.md#retention. Deletion is silent (no events
//! are emitted) and never replays an effect: event pruning stays below every
//! live rule cursor and before Agent-local source pins, advancing
//! `rule_history_coverage.pruned_through` in
//! the same transaction, while command/write pruning keeps open work, the
//! retry window, and every row a live projection still references. Mail
//! pruning deletes only acknowledged rows older than the mail window; unread
//! mail is never pruned.

use super::*;

const MS_PER_SECOND: i64 = 1_000;
const MS_PER_DAY: i64 = 24 * 60 * 60 * MS_PER_SECOND;
/// Event and process retention delete at most this many rows of each kind per
/// Store-lock acquisition. The daemon releases the writer mutex between calls.
const RETENTION_BATCH_ENTRIES: i64 = 128;
/// Incremental auto-vacuum work is page-bounded for fairness with writers.
const INCREMENTAL_VACUUM_PAGES: i64 = 256;
/// Traversal backstop for parent/child sets. Parent links are bound once at
/// admission, so sets are acyclic; reaching this fails the pass loudly rather
/// than looping.
const MAX_SET_DEPTH: i64 = 1_000;
/// Members collected per command set. A set larger than this is kept, not an
/// error: fan-out is legitimate, while the depth bound above trips only on
/// corruption. Total deleted rows stay bounded by `MAX_PAGE_ENTRIES`.
const MAX_SET_MEMBERS: usize = MAX_PAGE_ENTRIES;
/// Prunable roots deleted per pass. The candidate sweep skips kept roots in
/// place and stops here, so kept sets never starve the pass.
const MAX_PRUNE_ROOTS: i64 = 50;
/// Expired root candidates, oldest first: the first page and the keyed
/// continuation past the last examined `(created_at, id)`. Both ride the
/// `commands_prune_roots` partial index and stop at the batch bound, so no
/// pass scans or materializes the whole expired history to find the next
/// roots. The continuation uses a single row-value lower bound plus the
/// cutoff upper bound so SQLite seeks the index instead of rewinding it.
const PRUNE_COMMAND_CANDIDATES_FIRST: &str =
    "SELECT id, created_at FROM commands INDEXED BY commands_prune_roots
             WHERE parent_command IS NULL AND created_at <= ? ORDER BY created_at, id LIMIT ?";
const PRUNE_COMMAND_CANDIDATES_AFTER: &str =
    "SELECT id, created_at FROM commands INDEXED BY commands_prune_roots
             WHERE parent_command IS NULL AND created_at <= ?
               AND (created_at, id) > (?2, ?3)
             ORDER BY created_at, id LIMIT ?4";

/// SQLite storage facts exposed by the daemon retention status.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RetentionSpaceStatus {
    /// SQLite PRAGMA auto_vacuum: 0=none, 1=full, 2=incremental.
    pub auto_vacuum_mode: i64,
    pub page_count: i64,
    pub freelist_pages: i64,
}

/// Result of an explicitly configured startup conversion for an existing DB.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StoreCompactionReport {
    pub converted: bool,
    pub pages_before: i64,
    pub pages_after: i64,
    pub freelist_pages: i64,
}

/// Outcome of one bounded maintenance pass, including SQLite space handling.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RetentionReport {
    pub events_deleted: i64,
    pub pruned_through: i64,
    pub commands_deleted: i64,
    pub writes_deleted: i64,
    pub mail_deleted: i64,
    pub digests_deleted: i64,
    pub processes_deleted: i64,
    pub processes_examined: i64,
    pub process_scan_more: bool,
    pub pages_reclaimed: i64,
    pub freelist_pages: i64,
    pub auto_vacuum_mode: i64,
    pub wal_frames: i64,
    pub checkpointed_frames: i64,
    pub checkpoint_busy: bool,
}

impl RetentionReport {
    pub fn rows_deleted(&self) -> i64 {
        self.events_deleted
            + self.commands_deleted
            + self.writes_deleted
            + self.mail_deleted
            + self.digests_deleted
            + self.processes_deleted
    }

    pub fn work_count(&self) -> i64 {
        self.rows_deleted() - self.processes_deleted
            + self.processes_examined
            + self.pages_reclaimed
    }

    pub fn accumulate(&mut self, batch: Self) {
        self.events_deleted += batch.events_deleted;
        self.pruned_through = batch.pruned_through;
        self.commands_deleted += batch.commands_deleted;
        self.writes_deleted += batch.writes_deleted;
        self.mail_deleted += batch.mail_deleted;
        self.digests_deleted += batch.digests_deleted;
        self.processes_deleted += batch.processes_deleted;
        self.processes_examined += batch.processes_examined;
        self.process_scan_more = batch.process_scan_more;
        self.pages_reclaimed += batch.pages_reclaimed;
        self.freelist_pages = batch.freelist_pages;
        self.auto_vacuum_mode = batch.auto_vacuum_mode;
        self.wal_frames = batch.wal_frames;
        self.checkpointed_frames = batch.checkpointed_frames;
        self.checkpoint_busy |= batch.checkpoint_busy;
    }
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
    config_ref: bool,
}

impl Store {
    /// Read SQLite's current physical-retention mode and page inventory.
    pub fn retention_space_status(&self) -> Result<RetentionSpaceStatus> {
        Ok(RetentionSpaceStatus {
            auto_vacuum_mode: self
                .connection
                .query_row("PRAGMA auto_vacuum", [], |row| row.get(0))
                .map_err(sql_error)?,
            page_count: self
                .connection
                .query_row("PRAGMA page_count", [], |row| row.get(0))
                .map_err(sql_error)?,
            freelist_pages: self
                .connection
                .query_row("PRAGMA freelist_count", [], |row| row.get(0))
                .map_err(sql_error)?,
        })
    }

    /// Owner-opt-in conversion for an existing database. `VACUUM` runs only
    /// during startup, before the daemon serves requests; ordinary live
    /// retention uses bounded page reuse/checkpointing instead.
    pub fn compact_to_incremental(&mut self) -> Result<StoreCompactionReport> {
        self.ensure_writer_healthy()?;
        let before = self.retention_space_status()?;
        let converted = before.auto_vacuum_mode != 2;
        if converted {
            self.connection
                .execute_batch("PRAGMA auto_vacuum=INCREMENTAL; VACUUM;")
                .map_err(sql_error)?;
        }
        read_passive_checkpoint(&self.connection)?;
        let space = self.retention_space_status()?;
        if space.auto_vacuum_mode != 2 {
            return Err(Error::new(
                "internal",
                "SQLite did not enable incremental auto-vacuum",
            ));
        }
        Ok(StoreCompactionReport {
            converted,
            pages_before: before.page_count,
            pages_after: self.retention_space_status()?.page_count,
            freelist_pages: space.freelist_pages,
        })
    }

    /// Delete one bounded batch of expired history. Event pruning removes an
    /// expired contiguous prefix below every live cursor; exited child rows
    /// use their own shorter cutoff and remain protected while live or while
    /// their generation holds capacity. Command pruning removes whole closed
    /// child-sets leaf-first; mail pruning never removes unread rows. Event
    /// identities, projections, source recovery state, timers, and the event
    /// allocator are preserved.
    pub fn prune_retention(
        &mut self,
        events_seconds: i64,
        processes_seconds: i64,
        commands_days: Option<i64>,
        mail_days: Option<i64>,
        now: i64,
    ) -> Result<RetentionReport> {
        self.ensure_writer_healthy()?;
        let result = self.prune_retention_inner(
            events_seconds,
            processes_seconds,
            commands_days,
            mail_days,
            now,
        );
        self.fault_on_persistence_error(&result);
        result
    }

    fn prune_retention_inner(
        &mut self,
        events_seconds: i64,
        processes_seconds: i64,
        commands_days: Option<i64>,
        mail_days: Option<i64>,
        now: i64,
    ) -> Result<RetentionReport> {
        if events_seconds <= 0 || processes_seconds <= 0 {
            return Err(Error::new(
                "invalid",
                "event and process retention windows must be positive seconds",
            ));
        }
        let events_cutoff = now.saturating_sub(events_seconds.saturating_mul(MS_PER_SECOND));
        let processes_cutoff = now.saturating_sub(processes_seconds.saturating_mul(MS_PER_SECOND));
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
        let (events_deleted, pruned_through) = self.prune_events(events_cutoff)?;
        let (commands_deleted, writes_deleted) = match commands_cutoff {
            Some(cutoff) => self.prune_commands_and_writes(cutoff)?,
            None => (0, 0),
        };
        let mail_deleted = match mail_cutoff {
            Some(cutoff) => self.prune_mail(cutoff)?,
            None => 0,
        };
        let digests_deleted = match commands_cutoff {
            Some(cutoff) => self.prune_delivered_digests(cutoff)?,
            None => 0,
        };
        // Exited child rows have an independent, shorter age window.
        let (processes_deleted, processes_examined, process_scan_more) =
            self.prune_exited_processes(processes_cutoff)?;
        let (pages_reclaimed, space) = self.reclaim_retention_space()?;
        Ok(RetentionReport {
            events_deleted,
            pruned_through,
            commands_deleted,
            writes_deleted,
            mail_deleted,
            digests_deleted,
            processes_deleted,
            processes_examined,
            process_scan_more,
            pages_reclaimed,
            freelist_pages: space.freelist_pages,
            auto_vacuum_mode: space.auto_vacuum_mode,
            ..RetentionReport::default()
        })
    }

    fn reclaim_retention_space(&mut self) -> Result<(i64, RetentionSpaceStatus)> {
        let before = self.retention_space_status()?;
        if before.auto_vacuum_mode != 2 || before.freelist_pages == 0 {
            return Ok((0, before));
        }
        // Incremental vacuum yields a row per step. Finishing the statement
        // commits the bounded set, even when no rows expired in this pass.
        {
            let mut statement = self
                .connection
                .prepare(&format!(
                    "PRAGMA incremental_vacuum({INCREMENTAL_VACUUM_PAGES})"
                ))
                .map_err(sql_error)?;
            let mut rows = statement.query([]).map_err(sql_error)?;
            while rows.next().map_err(sql_error)?.is_some() {}
        }
        let after = self.retention_space_status()?;
        Ok((before.page_count.saturating_sub(after.page_count), after))
    }

    /// Run the passive WAL checkpoint without using the shared Store writer.
    pub fn checkpoint_retention(path: &Path) -> Result<(i64, i64, bool)> {
        let connection =
            Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE)
                .map_err(sql_error)?;
        connection.busy_timeout(Duration::ZERO).map_err(sql_error)?;
        read_passive_checkpoint(&connection)
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
        // Bound the sequence range before applying the age and source pins.
        // A large expired prefix must not be scanned again on every batch.
        let batch_end: Option<i64> = tx
            .query_row(
                "SELECT MAX(seq) FROM (
                     SELECT seq FROM events WHERE seq > ?1 ORDER BY seq LIMIT ?2
                 )",
                params![pruned_through, RETENTION_BATCH_ENTRIES],
                |row| row.get(0),
            )
            .map_err(sql_error)?;
        let Some(batch_end) = batch_end else {
            commit_transaction(tx, &self.write_faulted)?;
            return Ok((0, pruned_through));
        };
        let Some(end) = event_prune_end_tx(&tx, pruned_through, batch_end, cutoff)? else {
            commit_transaction(tx, &self.write_faulted)?;
            return Ok((0, pruned_through));
        };
        let last_deleted: Option<i64> = tx
            .query_row(
                "SELECT MAX(seq) FROM events WHERE seq > ?1 AND seq <= ?2",
                params![pruned_through, end],
                |row| row.get(0),
            )
            .map_err(sql_error)?;
        let Some(last_deleted) = last_deleted else {
            commit_transaction(tx, &self.write_faulted)?;
            return Ok((0, pruned_through));
        };
        let deleted = db!(tx.execute(
            "DELETE FROM events WHERE seq > ?1 AND seq <= ?2",
            params![pruned_through, last_deleted],
        ));
        // The allocator row in sqlite_sequence is deliberately untouched: event
        // ids are never reused and high-water survives prefix deletion.
        let advanced = db!(tx.execute(
            "UPDATE rule_history_coverage SET pruned_through = ?1 WHERE singleton = 1",
            [last_deleted],
        ));
        if advanced != 1 {
            return Err(Error::new(
                "source_integrity",
                "rule history coverage disappeared",
            ));
        }
        commit_transaction(tx, &self.write_faulted)?;
        Ok((deleted as i64, last_deleted))
    }

    fn prune_commands_and_writes(&mut self, cutoff: i64) -> Result<(i64, i64)> {
        // Bounded candidate sweep through the persistent keyset cursor:
        // successive passes resume past the last examined expired root instead
        // of restarting at the oldest, so kept roots are not revisited every
        // pass under the Store lock. Each pass pages expired roots in bounded
        // batches and walks one bounded subtree per candidate. Kept or
        // oversize sets are skipped in place so they never starve later
        // eligible roots, and the pass stops at the root/row/examination
        // budgets with whole sets still deleting atomically. No pass
        // materializes every expired command tree: each candidate costs one
        // bounded subtree walk, and the sweep stops early once the root
        // budget is doomed or the examination bound is reached.
        const CANDIDATE_BATCH: i64 = 64;
        const MAX_CANDIDATES_EXAMINED: i64 = 256;
        let mut doomed: Vec<SubtreeMember> = Vec::new();
        let mut doomed_roots: i64 = 0;
        let mut examined: i64 = 0;
        let mut cursor: Option<(i64, String)> = self.retention_commands_cursor.clone();
        let mut exhausted = false;
        'sweep: loop {
            let (sql, mut candidate_params): (&str, Vec<rusqlite::types::Value>) = match &cursor {
                None => (
                    PRUNE_COMMAND_CANDIDATES_FIRST,
                    vec![rusqlite::types::Value::from(cutoff)],
                ),
                Some((created_at, id)) => (
                    PRUNE_COMMAND_CANDIDATES_AFTER,
                    vec![
                        rusqlite::types::Value::from(cutoff),
                        rusqlite::types::Value::from(*created_at),
                        rusqlite::types::Value::from(id.clone()),
                    ],
                ),
            };
            candidate_params.push(rusqlite::types::Value::from(CANDIDATE_BATCH));
            let mut statement = self.connection.prepare(sql).map_err(sql_error)?;
            let batch: Vec<(String, i64)> = statement
                .query_map(rusqlite::params_from_iter(candidate_params.iter()), |row| {
                    Ok((row.get(0)?, row.get(1)?))
                })
                .map_err(sql_error)?
                .collect::<rusqlite::Result<_>>()
                .map_err(sql_error)?;
            if batch.is_empty() {
                exhausted = true;
                cursor = None;
                break;
            }
            for (root, root_created) in &batch {
                if doomed_roots >= MAX_PRUNE_ROOTS {
                    break 'sweep;
                }
                examined += 1;
                if examined > MAX_CANDIDATES_EXAMINED {
                    break 'sweep;
                }
                // Advance the persistent cursor past every examined root so
                // a budget stop resumes at the first unexamined root instead
                // of skipping the rest of this batch on the next pass.
                cursor = Some((*root_created, root.clone()));
                let (members, truncated) = match subtree_members_tx(&self.connection, root) {
                    Err(error) if error.code == "source_integrity" => {
                        // A set past the traversal bound stays kept, as the
                        // previous selection kept it, instead of failing the
                        // pass on corruption-shaped history.
                        continue;
                    }
                    result => result?,
                };
                // An oversize set is never partly pruned.
                if truncated {
                    continue;
                }
                if members.len() + doomed.len() > MAX_PAGE_ENTRIES && !doomed.is_empty() {
                    break 'sweep;
                }
                if members.iter().any(|member| {
                    member.is_open()
                        || member.created_at > cutoff
                        || member.turn_bound
                        || member.workspace_ref
                        || member.probe_ref
                        || member.goal_ref
                        || member.config_ref
                }) {
                    continue;
                }
                doomed_roots += 1;
                doomed.extend(members);
                if doomed.len() >= MAX_PAGE_ENTRIES {
                    break 'sweep;
                }
            }
            if batch.len() < CANDIDATE_BATCH as usize {
                exhausted = true;
                cursor = None;
                break;
            }
        }
        // Persist the sweep position: the next pass resumes past the last
        // examined root, wrapping to the oldest only once the eligible set is
        // exhausted. Examination stays bounded per pass by the budgets above.
        self.retention_commands_cursor = if exhausted { None } else { cursor };
        doomed.sort_by_key(|member| std::cmp::Reverse(member.depth));
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        let deleted = delete_commands_tx(&tx, doomed.iter().map(|member| member.id.as_str()))
            .map_err(sql_error)?;
        let writes = prune_writes_tx(&tx, cutoff)?;
        commit_transaction(tx, &self.write_faulted)?;
        Ok((deleted, writes))
    }

    /// Delete one bounded batch of delivered digests whose delivery is older
    /// than the commands cutoff, oldest delivery first. Pending rows
    /// (`delivered_at IS NULL`) never match, however old: an undelivered
    /// digest is still live coordination. Deletion is silent and never
    /// replays an effect: the digest mail row is untouched, and a later
    /// re-registration of a pruned id reads `id_expired` through the
    /// write-id age rule instead of a new row.
    fn prune_delivered_digests(&mut self, cutoff: i64) -> Result<i64> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        let deleted = db!(tx.execute(
            "DELETE FROM completion_digests WHERE id IN (
                 SELECT id FROM completion_digests
                 WHERE delivered_at IS NOT NULL AND delivered_at <= ?1
                 ORDER BY delivered_at, id LIMIT ?2
             )",
            params![cutoff, MAX_PAGE_ENTRIES as i64],
        ));
        commit_transaction(tx, &self.write_faulted)?;
        Ok(deleted as i64)
    }

    /// Delete one bounded batch of exited child rows older than the separate
    /// process cutoff, oldest change first, using the schema's partial index.
    /// Plugin and harness rows stay as owned-exit and restart-custody evidence;
    /// live/unknown rows and generations holding capacity never match.
    /// Deletion is silent and never replays a process.
    fn prune_exited_processes(&mut self, cutoff: i64) -> Result<(i64, i64, bool)> {
        let (after_time, after_row) = self
            .retention_processes_cursor
            .unwrap_or((i64::MIN, i64::MIN));
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        // Apply the limit before custody filtering so a retained reservation
        // cannot make this pass scan an arbitrarily long prefix.
        let candidates = {
            let mut statement = tx
                .prepare(
                    "SELECT rowid, updated_at FROM processes
                     INDEXED BY processes_exited_child_updated
                     WHERE state = 'exited' AND role = 'child' AND updated_at < ?1
                       AND (updated_at, rowid) > (?2, ?3)
                     ORDER BY updated_at, rowid LIMIT ?4",
                )
                .map_err(sql_error)?;
            statement
                .query_map(
                    params![cutoff, after_time, after_row, RETENTION_BATCH_ENTRIES],
                    |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
                )
                .map_err(sql_error)?
                .collect::<rusqlite::Result<Vec<_>>>()
                .map_err(sql_error)?
        };
        let examined = candidates.len() as i64;
        let rowids: Vec<i64> = candidates.iter().map(|&(rowid, _)| rowid).collect();
        let deleted = if rowids.is_empty() {
            0
        } else {
            db!(tx.execute(
                "DELETE FROM processes WHERE rowid IN (SELECT value FROM json_each(?1))
                   AND NOT EXISTS (
                       SELECT 1 FROM agents
                       WHERE id = CAST(substr(processes.owner, 8, instr(processes.owner, '#') - 8) AS INTEGER)
                         AND processes.owner = 'agent:a' || id || '#' || capacity_generation
                   )",
                [encode_json(&rowids)?],
            ))
        };
        commit_transaction(tx, &self.write_faulted)?;
        let more = examined == RETENTION_BATCH_ENTRIES;
        self.retention_processes_cursor = candidates
            .last()
            .filter(|_| more)
            .map(|&(rowid, time)| (time, rowid));
        Ok((deleted as i64, examined, more))
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
        // A spent attention wake lives only as long as mail on its thread:
        // once every message on the thread is gone, the bound can go. A new
        // mail on the same thread (same incident re-mailed) then wakes again.
        db!(tx.execute(
            "DELETE FROM attention_wakes WHERE rowid IN (
                 SELECT w.rowid FROM attention_wakes w
                 WHERE NOT EXISTS (SELECT 1 FROM messages m WHERE m.thread = w.thread)
                 LIMIT 512
             )",
            [],
        ));
        commit_transaction(tx, &self.write_faulted)?;
        Ok(deleted as i64)
    }
}

fn read_passive_checkpoint(connection: &Connection) -> Result<(i64, i64, bool)> {
    let (busy, frames, checkpointed): (i64, i64, i64) = connection
        .query_row("PRAGMA wal_checkpoint(PASSIVE)", [], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })
        .map_err(sql_error)?;
    Ok((
        frames.max(0),
        checkpointed.max(0),
        busy != 0 || checkpointed < frames,
    ))
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
    batch_end: i64,
    cutoff: i64,
) -> Result<Option<i64>> {
    // Enabled current/retiring cursors pin the events they still need; this is
    // the Store backlog pin model (disabled rules pin nothing). A live Git
    // cursor protects only unconsumed native file hints from its own Agent
    // generation. A source with no established cursor has no cursor pin.
    let floor: Option<i64> = tx
        .query_row(
            "SELECT MIN(pin) FROM (
                 SELECT current_cursor AS pin FROM rule_state WHERE enabled = 1
                 UNION ALL
                 SELECT retiring_cursor FROM rule_state
                 WHERE enabled = 1
                   AND retiring_cursor IS NOT NULL AND retire_through IS NOT NULL
                 UNION ALL
                 SELECT MIN(e.seq) - 1 FROM events e
                 CROSS JOIN source_state s
                    ON s.agent = e.agent AND s.generation = e.generation AND s.source = 'git'
                 CROSS JOIN agents a ON a.id = e.agent AND a.generation = e.generation
                 WHERE s.state = 'open'
                   AND s.cursor IS NOT NULL AND e.seq > CAST(s.cursor AS INTEGER)
                   AND e.seq > ?1 AND e.seq <= ?2
                   AND e.family = 'tool' AND e.kind = 'tool.completed'
                   AND e.native_id IS NOT NULL AND e.source NOT IN ('git', 'os', 'daemon')
                   AND json_extract(e.data, '$.tool') = 'fileChange'
                   AND json_extract(e.data, '$.status') = 'completed'
                   AND (json_array_length(e.data, '$.changed_paths') > 0
                        OR json_extract(e.data, '$.changed_paths_truncated') = 1
                        OR json_extract(e.data, '$.redacted') = 1)
             )",
            params![pruned_through, batch_end],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    let ceiling = floor.map_or(batch_end, |pin| pin.min(batch_end));
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
            "SELECT MIN(s.gap_seq) FROM source_state s INDEXED BY source_state_open_gap_seq
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
pub(crate) fn coverage_pruned_through(connection: &Connection) -> Result<i64> {
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
/// an oversize set is never partly pruned. Ids are collected breadth-first in
/// Rust with a per-parent `LIMIT` bounded by the remaining budget, so an
/// oversize tree never materializes a recursive result into an automatic
/// index before the outer `LIMIT` applies; the detail fetch below then runs
/// over at most `MAX_SET_MEMBERS + 1` ids.
fn subtree_members_tx(connection: &Connection, root: &str) -> Result<(Vec<SubtreeMember>, bool)> {
    use std::collections::{HashMap, HashSet, VecDeque};
    let cap = MAX_SET_MEMBERS + 1;
    let mut visited: HashSet<String> = HashSet::new();
    let mut depth_of: HashMap<String, i64> = HashMap::new();
    let mut frontier: VecDeque<String> = VecDeque::new();
    visited.insert(root.to_owned());
    depth_of.insert(root.to_owned(), 0);
    frontier.push_back(root.to_owned());
    let mut truncated = false;
    while let Some(parent) = frontier.pop_front() {
        let depth = depth_of[&parent];
        if depth >= MAX_SET_DEPTH {
            return Err(Error::new(
                "source_integrity",
                "command set exceeds its traversal bound",
            ));
        }
        let remaining = cap.saturating_sub(visited.len());
        if remaining == 0 {
            truncated = true;
            break;
        }
        // One bounded child page per parent: an oversize fan-out reports
        // truncation without fetching the whole child list.
        let mut statement = connection
            .prepare("SELECT id FROM commands WHERE parent_command = ?1 LIMIT ?2")
            .map_err(sql_error)?;
        let rows = statement
            .query_map(params![parent, (remaining as i64) + 1], |row| {
                row.get::<_, String>(0)
            })
            .map_err(sql_error)?;
        let mut children = Vec::new();
        for row in rows {
            children.push(row.map_err(sql_error)?);
        }
        if children.len() > remaining {
            truncated = true;
            break;
        }
        for child in children {
            if visited.insert(child.clone()) {
                depth_of.insert(child.clone(), depth + 1);
                frontier.push_back(child);
            }
        }
        if visited.len() > cap {
            truncated = true;
            break;
        }
    }
    if truncated || visited.len() > MAX_SET_MEMBERS {
        return Ok((Vec::new(), true));
    }
    let depth_of_snapshot = depth_of;
    let ids: Vec<String> = visited.into_iter().collect();
    if ids.is_empty() {
        return Ok((Vec::new(), false));
    }
    let placeholders = ids.iter().map(|_| "?").collect::<Vec<_>>().join(",");
    let sql = format!(
        "SELECT c.id, 0, c.state, c.created_at,
                EXISTS(
                    SELECT 1 FROM turns t
                    WHERE t.agent = c.target AND t.generation = c.generation
                      AND t.turn_id = c.turn_id AND t.state IN ('running', 'unknown')
                ),
                (EXISTS(SELECT 1 FROM workspaces w WHERE w.start_command_id = c.id)
                  OR EXISTS(SELECT 1 FROM project_transfers p WHERE p.launch_command = c.id)),
                EXISTS(SELECT 1 FROM provider_holds h WHERE h.probe_command = c.id),
                EXISTS(
                    SELECT 1 FROM goals g
                    WHERE g.agent = c.target AND g.mode = 'eliot'
                      AND g.generation = c.generation
                      AND json_extract(g.state, '$.status') IN ('active', 'paused')
                      AND json_extract(g.state, '$.last_continuation_command_id') = c.id
                ),
                EXISTS(SELECT 1 FROM agent_config a WHERE a.command_id = c.id)
         FROM commands c WHERE c.id IN ({placeholders})"
    );
    let mut statement = connection.prepare(&sql).map_err(sql_error)?;
    let rows = statement
        .query_map(rusqlite::params_from_iter(ids.iter()), |row| {
            Ok(SubtreeMember {
                id: row.get(0)?,
                depth: row.get(1)?,
                state: row.get(2)?,
                created_at: row.get(3)?,
                turn_bound: row.get(4)?,
                workspace_ref: row.get(5)?,
                probe_ref: row.get(6)?,
                goal_ref: row.get(7)?,
                config_ref: row.get(8)?,
            })
        })
        .map_err(sql_error)?;
    let mut members = Vec::new();
    for row in rows {
        members.push(row.map_err(sql_error)?);
    }
    // BFS depths are exact for a parent-linked tree; reuse them for the
    // leaf-first delete ordering below.
    for member in &mut members {
        member.depth = depth_of_snapshot.get(&member.id).copied().unwrap_or(0);
    }
    if members.iter().any(|member| member.depth >= MAX_SET_DEPTH) {
        return Err(Error::new(
            "source_integrity",
            "command set exceeds its traversal bound",
        ));
    }
    Ok((members, false))
}

/// The shared leaf-first physical command cleanup for retention and explicit Agent deletion.
pub(super) fn delete_commands_tx<'a>(
    tx: &Transaction<'_>,
    ids: impl IntoIterator<Item = &'a str>,
) -> rusqlite::Result<i64> {
    let mut clear = tx
        .prepare("UPDATE mail_notices SET open_notice_command=NULL WHERE open_notice_command=?1")?;
    let mut clear_messages = tx.prepare(
        "UPDATE messages SET presentation_command=NULL WHERE presentation_command=?1 AND presented_at IS NULL",
    )?;
    let mut delete = tx.prepare("DELETE FROM commands WHERE id=?1")?;
    let mut deleted = 0;
    for id in ids {
        clear.execute([id])?;
        clear_messages.execute([id])?;
        deleted += delete.execute([id])? as i64;
    }
    Ok(deleted)
}

fn prune_writes_tx(tx: &Transaction<'_>, cutoff: i64) -> Result<i64> {
    // Filter pins before the page limit. Unresolved goal rounds still need
    // their write receipts for the retry fence. Every consumed cleanup plan
    // keeps its apply receipt while the plan row exists, so write retention
    // can never prune the receipt before the ledger reclaims the plan: a
    // consumed plan with a pruned receipt would otherwise become permanently
    // ineligible for reclamation, wedging the ledger. Once the ledger
    // reclaims a settled plan, its receipt becomes eligible for pruning and
    // exact retries read back as retained (plan present) or history-gap
    // (receipt pruned after reclaim).
    Ok(db!(tx.execute(
        "DELETE FROM writes WHERE id IN (
             SELECT id FROM writes WHERE created_at <= ?1
               AND method NOT LIKE 'inference.%'
               AND NOT EXISTS (SELECT 1 FROM launch_intents q WHERE q.id = writes.id)
               AND NOT (method = 'goal.round'
                        AND json_extract(result, '$.outcome') IS json_extract(?3, '$'))
               AND NOT (method = 'cleanup.apply' AND EXISTS(
                   SELECT 1 FROM cleanup_plans p WHERE p.apply_id=writes.id
               ))
             ORDER BY created_at, id LIMIT ?2
         )",
        params![
            cutoff,
            MAX_PAGE_ENTRIES as i64,
            super::goals::goal_round_running_outcome_json()?,
        ],
    )) as i64)
}
