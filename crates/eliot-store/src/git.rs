use super::*;

impl Store {
    /// Commit OIDs already observed and committed for this live Git binding.
    pub fn observed_git_commits(
        &self,
        agent: &str,
        generation: i64,
        limit: usize,
    ) -> Result<Vec<String>> {
        let mut statement = self
            .connection
            .prepare(
                "SELECT oid FROM git_committed_oids
                 WHERE agent = ?1 AND generation = ?2 ORDER BY oid LIMIT ?3",
            )
            .map_err(sql_error)?;
        statement
            .query_map(
                params![
                    parse_agent_id(agent)?,
                    generation,
                    limit.min(MAX_PAGE_ENTRIES) as i64
                ],
                |row| row.get(0),
            )
            .map_err(sql_error)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(sql_error)
    }

    pub(super) fn record_git_observation_inner(
        &mut self,
        agent: &str,
        generation: i64,
        snapshot: &GitSnapshot,
        hints: &[FileChangeHint],
        through: u64,
        now: i64,
    ) -> Result<Vec<Event>> {
        validate_git_snapshot(snapshot)?;
        if hints.len() > MAX_PAGE_ENTRIES || encode_json(&hints)?.len() > MAX_PAGE_BYTES {
            return Err(Error::new(
                "frame_too_large",
                "Git hint observation exceeds its bound",
            ));
        }
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        if retained_git_workspace(&tx, agent, generation)? != snapshot.workspace {
            return Err(Error::new(
                "source_integrity",
                "Git workspace does not match its retained generation",
            ));
        }
        let agent_num = parse_agent_id(agent)?;
        let source = require_git_source(&tx, agent_num, generation)?;
        if source.state != "open" {
            return Err(Error::new("bad_state", "Git source is closed"));
        }
        let (cursor, boundary) = check_git_cursor(&tx, &source, through)?;
        let scanned = scan_git_hints(&tx, agent, generation, cursor, boundary, MAX_PAGE_ENTRIES)?;
        if scanned.through != through {
            return Err(Error::new(
                "invalid",
                "Git hint cursor skips beyond a bounded scanned page",
            ));
        }
        let file_gap =
            git_interval_gap_covers(&tx, agent_num, generation, &source, cursor, through, "file")?;
        let (expected, metadata_uncovered) =
            collect_expected_git_paths(&scanned.events, &snapshot.workspace);
        let baseline = load_and_check_git_baseline(&tx, snapshot)?;
        if self
            .git_observed_at
            .get(&snapshot.workspace)
            .is_some_and(|observed_at| *observed_at > snapshot.observed_at)
        {
            return Err(Error::new(
                "bad_state",
                "Git snapshot predates the latest accepted observation",
            ));
        }
        let (covered, attributed) =
            validate_git_hint_batch(&tx, agent_num, generation, snapshot, hints, through, cursor)?;
        if (covered != expected || metadata_uncovered) && !file_gap {
            return Err(Error::new(
                "source_integrity",
                "Git cursor would skip an unconsumed in-scope native file-change path",
            ));
        }
        let mut events = Vec::new();
        if !git_interval_gap_covers(&tx, agent_num, generation, &source, cursor, through, "git")?
            && let Some(event) = git_history_gap(
                &tx,
                agent_num,
                generation,
                snapshot,
                baseline.as_ref(),
                &source,
            )?
        {
            events.push(event);
        }
        let view = compute_git_view(&tx, agent_num, generation, snapshot)?;
        let work_changed = update_git_work_state(
            &tx,
            snapshot,
            baseline.as_ref(),
            &view.undelivered,
            &attributed,
        )?;
        persist_git_observation_events(
            &tx,
            agent_num,
            generation,
            snapshot,
            view,
            attributed,
            &mut events,
        )?;
        if snapshot.complete {
            // Only explicit clean proofs clear attribution, including older generations.
            if !snapshot.clean_paths.is_empty() {
                let clean_paths = encode_json(&snapshot.clean_paths)?;
                let stale: bool = tx
                    .query_row(
                        "SELECT EXISTS(SELECT 1 FROM agent_edits WHERE workspace = ?1
                         AND path IN (SELECT value FROM json_each(?2)) AND updated_at <= ?3)",
                        params![snapshot.workspace, clean_paths, snapshot.observed_at],
                        |row| row.get(0),
                    )
                    .map_err(sql_error)?;
                if stale {
                    db!(tx.execute(
                        "DELETE FROM agent_edits WHERE workspace = ?1
                         AND path IN (SELECT value FROM json_each(?2)) AND updated_at <= ?3",
                        params![snapshot.workspace, clean_paths, snapshot.observed_at],
                    ));
                }
            }
            // The baseline moves only on a HEAD transition; unchanged probes write nothing here.
            if baseline
                .as_ref()
                .is_none_or(|old| old.head != snapshot.head)
            {
                db!(tx.execute(
                    "INSERT INTO git_workspaces(workspace, head, updated_at) VALUES (?1, ?2, ?3)
                     ON CONFLICT(workspace) DO UPDATE SET head = excluded.head, updated_at = excluded.updated_at",
                    params![snapshot.workspace, snapshot.head, snapshot.observed_at],
                ));
            }
        }
        // Irrelevant global events advance `through` but need no durable cursor write.
        if !scanned.events.is_empty() || !events.is_empty() || work_changed {
            db!(tx.execute(
                "UPDATE source_state SET cursor = ?1, last_store_seq = COALESCE(?2, last_store_seq), updated_at = ?3
                 WHERE agent = ?4 AND generation = ?5 AND source = 'git'",
                params![through.to_string(), events.last().map(|event| event.seq), now, agent_num, generation],
            ));
        }
        commit_transaction(tx, &self.write_faulted)?;
        self.git_observed_at
            .insert(snapshot.workspace.clone(), snapshot.observed_at);
        Ok(events)
    }
}

fn check_git_cursor(
    tx: &Transaction<'_>,
    source: &SourceStateRecord,
    through: u64,
) -> Result<(u64, i64)> {
    let cursor = git_source_cursor(source)?;
    let boundary = git_sequence(through)?;
    let high_water: i64 = tx
        .query_row(
            "SELECT MAX(COALESCE((SELECT MAX(seq) FROM events), 0),
                 COALESCE((SELECT seq FROM sqlite_sequence WHERE name = 'events'), 0))",
            [],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    if through < cursor || boundary > high_water {
        return Err(Error::new(
            "invalid",
            "Git hint cursor is outside the committed interval",
        ));
    }
    Ok((cursor, boundary))
}

fn load_and_check_git_baseline(
    tx: &Transaction<'_>,
    snapshot: &GitSnapshot,
) -> Result<Option<GitBaseline>> {
    let baseline = load_git_baseline(tx, &snapshot.workspace)?;
    if let Some(base) = &snapshot.base_oid {
        let prepared = workspace::load_workspace_by_path(tx, &snapshot.workspace)?;
        if prepared
            .as_ref()
            .is_none_or(|workspace| workspace.base_oid != *base)
        {
            return Err(Error::new(
                "source_integrity",
                "Git base is not the workspace's frozen base OID",
            ));
        }
    }
    if baseline
        .as_ref()
        .is_some_and(|old| old.updated_at > snapshot.observed_at)
    {
        return Err(Error::new(
            "bad_state",
            "Git snapshot predates the stored HEAD baseline",
        ));
    }
    Ok(baseline)
}

fn collect_expected_git_paths(events: &[Event], workspace: &str) -> (HashSet<(u64, String)>, bool) {
    let mut metadata_uncovered = false;
    let mut expected = HashSet::new();
    for event in events {
        metadata_uncovered |= event
            .data
            .get("changed_paths_truncated")
            .and_then(Value::as_bool)
            == Some(true)
            || event.data.get("redacted").and_then(Value::as_bool) == Some(true);
        let Some(paths) = event.data.get("changed_paths").and_then(Value::as_array) else {
            metadata_uncovered = true;
            continue;
        };
        for path in paths {
            let Some(path) = path.as_str() else {
                metadata_uncovered = true;
                continue;
            };
            if let Ok(path) = normalized_native_git_path(workspace, path) {
                expected.insert((event.seq as u64, git_path_key(workspace, &path)));
            } else {
                metadata_uncovered = true;
            }
        }
    }
    (expected, metadata_uncovered)
}

type GitHintBatch<'a> = (HashSet<(u64, String)>, Vec<&'a FileChangeHint>);

fn validate_git_hint_batch<'a>(
    tx: &Transaction<'_>,
    agent_num: i64,
    generation: i64,
    snapshot: &GitSnapshot,
    hints: &'a [FileChangeHint],
    through: u64,
    cursor: u64,
) -> Result<GitHintBatch<'a>> {
    let mut seen = HashSet::with_capacity(hints.len());
    let mut covered = HashSet::with_capacity(hints.len());
    let mut attributed = Vec::new();
    for hint in hints {
        validate_git_path(&hint.path)?;
        if hint.event_seq == 0
            || hint.event_seq > through
            || !seen.insert((
                hint.event_seq,
                git_path_key(&snapshot.workspace, &hint.path),
            ))
        {
            return Err(Error::new(
                "source_integrity",
                "Git hint has an invalid or repeated cause",
            ));
        }
        let event = load_native_git_hint(tx, agent_num, generation, hint.event_seq)?;
        if hint.ts != event.ts {
            return Err(Error::new(
                "source_integrity",
                "Git hint timestamp does not match its native cause",
            ));
        }
        let paths = event
            .data
            .get("changed_paths")
            .and_then(Value::as_array)
            .ok_or_else(|| Error::new("source_integrity", "native file change has no paths"))?;
        let mut belongs = false;
        for path in paths {
            let path = path
                .as_str()
                .ok_or_else(|| Error::new("source_integrity", "native changed path is not text"))?;
            if let Ok(path) = normalized_native_git_path(&snapshot.workspace, path) {
                belongs |= git_path_key(&snapshot.workspace, &path)
                    == git_path_key(&snapshot.workspace, &hint.path);
            }
        }
        if !belongs {
            return Err(Error::new(
                "source_integrity",
                "claimed path is absent from its native file change",
            ));
        }
        // Lost-ack retry: old causes stay historical and never recreate a cleaned row.
        if hint.event_seq <= cursor {
            continue;
        }
        covered.insert((
            hint.event_seq,
            git_path_key(&snapshot.workspace, &hint.path),
        ));
        attributed.push(hint);
    }
    Ok((covered, attributed))
}

struct GitView {
    data: Value,
    changed: bool,
    undelivered: Vec<GitCommit>,
}

/// Code-progress snapshot read by automatic attention without launching Git.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CodeProgressState {
    pub available: bool,
    pub last_code_progress_at: Option<i64>,
    pub tool_calls: u64,
    pub messages: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct GitWorkStateRow {
    base_oid: Option<String>,
    commits_ahead: Option<i64>,
    files_changed: Option<i64>,
    insertions: Option<i64>,
    deletions: Option<i64>,
    diff_digest: Option<String>,
    dirty_digest: Option<String>,
    work_available: bool,
    work_error: Option<String>,
    last_write_at: Option<i64>,
    last_commit_at: Option<i64>,
}

fn load_git_work_state(
    connection: &Connection,
    workspace: &str,
) -> Result<Option<GitWorkStateRow>> {
    connection
        .query_row(
            "SELECT base_oid, commits_ahead, files_changed, insertions, deletions,
                    diff_digest, dirty_digest, work_available, work_error,
                    last_write_at, last_commit_at
             FROM git_work_state WHERE workspace = ?1",
            [workspace],
            |row| {
                Ok(GitWorkStateRow {
                    base_oid: row.get(0)?,
                    commits_ahead: row.get(1)?,
                    files_changed: row.get(2)?,
                    insertions: row.get(3)?,
                    deletions: row.get(4)?,
                    diff_digest: row.get(5)?,
                    dirty_digest: row.get(6)?,
                    work_available: row.get::<_, i64>(7)? != 0,
                    work_error: row.get(8)?,
                    last_write_at: row.get(9)?,
                    last_commit_at: row.get(10)?,
                })
            },
        )
        .optional()
        .map_err(sql_error)
}

/// One short Store fact about a workspace's committed Git work, for a
/// handoff brief: counts when the latest observation is available, the
/// recorded reason otherwise.
pub(super) fn work_fact(connection: &Connection, workspace: &str) -> Result<String> {
    let count = |value: Option<i64>| value.map_or_else(|| "?".to_owned(), |n| n.to_string());
    Ok(match load_git_work_state(connection, workspace)? {
        None => "not observed yet".to_owned(),
        Some(row) if !row.work_available => format!(
            "unavailable ({})",
            row.work_error.as_deref().unwrap_or("unavailable")
        ),
        Some(row) => format!(
            "{} files changed, +{}/-{}, {} commits ahead of the base",
            count(row.files_changed),
            count(row.insertions),
            count(row.deletions),
            count(row.commits_ahead),
        ),
    })
}

fn to_i64_counter(value: Option<u64>) -> Result<Option<i64>> {
    value
        .map(|count| {
            i64::try_from(count).map_err(|_| Error::new("invalid", "Git work counter overflows"))
        })
        .transpose()
}

fn to_u64_counter(value: Option<i64>) -> Option<u64> {
    value.and_then(|count| u64::try_from(count).ok())
}

fn update_git_work_state(
    tx: &Transaction<'_>,
    snapshot: &GitSnapshot,
    baseline: Option<&GitBaseline>,
    undelivered: &[GitCommit],
    attributed: &[&FileChangeHint],
) -> Result<bool> {
    let prior = load_git_work_state(tx, &snapshot.workspace)?;
    let work = snapshot.work.as_ref();
    let work_available = work.is_some_and(|w| w.available);
    let work_error = if work_available {
        None
    } else {
        Some(
            work.and_then(|w| w.error.as_deref())
                .unwrap_or("unavailable")
                .to_owned(),
        )
    };
    let commits_ahead = work.filter(|w| w.available).and_then(|w| w.commits_ahead);
    let files_changed = work.filter(|w| w.available).and_then(|w| w.files_changed);
    let insertions = work.filter(|w| w.available).and_then(|w| w.insertions);
    let deletions = work.filter(|w| w.available).and_then(|w| w.deletions);
    let diff_digest = work.and_then(|w| w.diff_digest.clone());
    let dirty_digest = work.and_then(|w| w.dirty_digest.clone());
    let has_changes = work.is_some_and(|w| w.has_changes);

    let hint_write_ts = attributed.iter().map(|hint| hint.ts).max();
    let dirty_write = !snapshot.dirty.is_empty()
        && work.is_some()
        && dirty_digest.is_some()
        && prior.as_ref().and_then(|row| row.dirty_digest.as_ref()) != dirty_digest.as_ref();
    let diff_write = has_changes
        && work.is_some()
        && diff_digest.is_some()
        && prior.as_ref().and_then(|row| row.diff_digest.as_ref()) != diff_digest.as_ref();
    let observed_write_ts = if let Some(hint_ts) = hint_write_ts {
        Some(hint_ts)
    } else if dirty_write || diff_write {
        Some(snapshot.observed_at)
    } else {
        None
    };
    let last_write_at = match (
        prior.as_ref().and_then(|row| row.last_write_at),
        observed_write_ts,
    ) {
        (Some(prev), Some(curr)) => Some(prev.max(curr)),
        (prev, curr) => curr.or(prev),
    };

    let head_moved = baseline.is_some_and(|old| old.head != snapshot.head);
    let commit_landed = !undelivered.is_empty()
        || (head_moved && commits_ahead.is_some_and(|count| count > 0))
        || (baseline.is_none() && commits_ahead.is_some_and(|count| count > 0))
        || (head_moved && snapshot.base_oid.is_none());
    let last_commit_at = if commits_ahead == Some(0) {
        None
    } else if commit_landed {
        Some(
            prior
                .as_ref()
                .and_then(|row| row.last_commit_at)
                .map_or(snapshot.observed_at, |prev| prev.max(snapshot.observed_at)),
        )
    } else {
        prior.as_ref().and_then(|row| row.last_commit_at)
    };

    let next = GitWorkStateRow {
        base_oid: snapshot.base_oid.clone(),
        commits_ahead: to_i64_counter(commits_ahead)?,
        files_changed: to_i64_counter(files_changed)?,
        insertions: to_i64_counter(insertions)?,
        deletions: to_i64_counter(deletions)?,
        diff_digest,
        dirty_digest,
        work_available,
        work_error,
        last_write_at,
        last_commit_at,
    };
    if prior.as_ref() == Some(&next) {
        return Ok(false);
    }
    db!(tx.execute(
        "INSERT INTO git_work_state(
             workspace, base_oid, commits_ahead, files_changed, insertions, deletions,
             diff_digest, dirty_digest, work_available, work_error,
             last_write_at, last_commit_at, updated_at
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)
         ON CONFLICT(workspace) DO UPDATE SET
             base_oid = excluded.base_oid,
             commits_ahead = excluded.commits_ahead,
             files_changed = excluded.files_changed,
             insertions = excluded.insertions,
             deletions = excluded.deletions,
             diff_digest = excluded.diff_digest,
             dirty_digest = excluded.dirty_digest,
             work_available = excluded.work_available,
             work_error = excluded.work_error,
             last_write_at = excluded.last_write_at,
             last_commit_at = excluded.last_commit_at,
             updated_at = excluded.updated_at",
        params![
            snapshot.workspace,
            next.base_oid,
            next.commits_ahead,
            next.files_changed,
            next.insertions,
            next.deletions,
            next.diff_digest,
            next.dirty_digest,
            i64::from(next.work_available),
            next.work_error,
            next.last_write_at,
            next.last_commit_at,
            snapshot.observed_at,
        ],
    ));
    Ok(true)
}

fn turn_activity_counts(
    connection: &Connection,
    agent_num: i64,
    generation: i64,
    active_turn: Option<&str>,
) -> Result<Option<(u64, u64)>> {
    let Some(turn_id) = active_turn else {
        return Ok(None);
    };
    let counts: Option<(i64, i64)> = connection
        .query_row(
            "SELECT tool_calls, messages FROM turns
             WHERE agent = ?1 AND generation = ?2 AND turn_id = ?3",
            params![agent_num, generation, turn_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(sql_error)?;
    Ok(counts.map(|(tools, messages)| {
        (
            u64::try_from(tools.max(0)).unwrap_or(0),
            u64::try_from(messages.max(0)).unwrap_or(0),
        )
    }))
}

impl Store {
    pub fn work_summary(
        &self,
        agent: &str,
        generation: i64,
        workspace: Option<&str>,
        active_turn: Option<&str>,
    ) -> Result<core::api::WorkSummaryView> {
        let agent_num = parse_agent_id(agent)?;
        self.work_summary_by_num(agent_num, generation, workspace, active_turn)
    }

    pub(super) fn work_summary_by_num(
        &self,
        agent_num: i64,
        generation: i64,
        workspace: Option<&str>,
        active_turn: Option<&str>,
    ) -> Result<core::api::WorkSummaryView> {
        let turn_counts =
            turn_activity_counts(&self.connection, agent_num, generation, active_turn)?;
        let Some(workspace) = workspace else {
            return Ok(core::api::WorkSummaryView::unavailable(
                "no_workspace",
                turn_counts,
            ));
        };
        let source: Option<(String, Option<i64>)> = self
            .connection
            .query_row(
                "SELECT health, last_store_seq FROM source_state
                 WHERE agent = ?1 AND generation = ?2 AND source = 'git'",
                params![agent_num, generation],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(sql_error)?;
        // No Git source row yet: the daemon opens it while preparing the
        // generation, before the Agent starts, so nothing was read yet. A
        // failed read marks the row's health instead.
        let Some((health, last_store_seq)) = source else {
            return Ok(core::api::WorkSummaryView::unavailable(
                "pending",
                turn_counts,
            ));
        };
        if matches!(health.as_str(), "error" | "stale") {
            return Ok(core::api::WorkSummaryView::unavailable(
                "git_unavailable",
                turn_counts,
            ));
        }
        if last_store_seq.is_none() {
            return Ok(core::api::WorkSummaryView::unavailable(
                "pending",
                turn_counts,
            ));
        }
        let Some(row) = load_git_work_state(&self.connection, workspace)? else {
            return Ok(core::api::WorkSummaryView::unavailable(
                "pending",
                turn_counts,
            ));
        };
        if !row.work_available {
            return Ok(core::api::WorkSummaryView::unavailable(
                row.work_error.unwrap_or_else(|| "unavailable".to_owned()),
                turn_counts,
            ));
        }
        let (Some(commits_ahead), Some(files_changed), Some(insertions), Some(deletions)) = (
            to_u64_counter(row.commits_ahead),
            to_u64_counter(row.files_changed),
            to_u64_counter(row.insertions),
            to_u64_counter(row.deletions),
        ) else {
            return Ok(core::api::WorkSummaryView::unavailable(
                "unavailable",
                turn_counts,
            ));
        };
        Ok(core::api::WorkSummaryView {
            available: true,
            reason: None,
            commits_ahead: Some(commits_ahead),
            files_changed: Some(files_changed),
            insertions: Some(insertions),
            deletions: Some(deletions),
            last_write_at: row.last_write_at,
            last_commit_at: row.last_commit_at,
            tool_calls: turn_counts.map(|(tools, _)| tools),
            messages: turn_counts.map(|(_, msgs)| msgs),
        })
    }

    pub fn code_progress_state(
        &self,
        agent: &str,
        generation: i64,
        workspace: Option<&str>,
        active_turn: Option<&str>,
    ) -> Result<CodeProgressState> {
        let agent_num = parse_agent_id(agent)?;
        let (tool_calls, messages) =
            turn_activity_counts(&self.connection, agent_num, generation, active_turn)?
                .unwrap_or((0, 0));
        let Some(workspace) = workspace else {
            return Ok(CodeProgressState {
                available: false,
                last_code_progress_at: None,
                tool_calls,
                messages,
            });
        };
        let source: Option<(String, String, Option<i64>)> = self
            .connection
            .query_row(
                "SELECT state, health, last_store_seq FROM source_state
                 WHERE agent = ?1 AND generation = ?2 AND source = 'git'",
                params![agent_num, generation],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()
            .map_err(sql_error)?;
        let Some((state, health, last_store_seq)) = source else {
            return Ok(CodeProgressState {
                available: false,
                last_code_progress_at: None,
                tool_calls,
                messages,
            });
        };
        if state != "open" || !matches!(health.as_str(), "ok" | "slow") || last_store_seq.is_none()
        {
            return Ok(CodeProgressState {
                available: false,
                last_code_progress_at: None,
                tool_calls,
                messages,
            });
        }
        let Some(row) = load_git_work_state(&self.connection, workspace)? else {
            return Ok(CodeProgressState {
                available: false,
                last_code_progress_at: None,
                tool_calls,
                messages,
            });
        };
        let available = row.work_available
            || (row.work_error.as_deref() == Some("no_base_oid") && row.dirty_digest.is_some());
        let last_code_progress_at = row
            .last_write_at
            .into_iter()
            .chain(row.last_commit_at)
            .max();
        Ok(CodeProgressState {
            available,
            last_code_progress_at,
            tool_calls,
            messages,
        })
    }
}

/// The canonical persisted `git.changed` representation: the full snapshot
/// identity plus the dirty content digest, path coverage and completion
/// flags. Including the content digest makes a new write to an already
/// dirty path observable even if status and the path list stay the same.
/// Built in one place so the Store budget and the persisted bytes share
/// exact serialization rules. `view_digest` is the digest of the full
/// canonical view (untruncated coverage, observed flags), so a change wholly
/// outside the retained prefix still compares different instead of hiding
/// behind an equal truncation.
fn git_changed_data(
    snapshot: &GitSnapshot,
    dirty: &[GitPathStatus],
    committed_paths: &[String],
    complete: bool,
    base_complete: bool,
    view_digest: &str,
) -> Value {
    json!({
        "workspace": snapshot.workspace, "head": snapshot.head, "branch": snapshot.branch,
        "dirty": dirty, "complete": complete,
        "dirty_digest": snapshot.work.as_ref().and_then(|work| work.dirty_digest.as_deref()),
        "history_complete": snapshot.history_complete, "history_gap": snapshot.history_gap,
        "base_oid": snapshot.base_oid, "committed_paths": committed_paths, "base_complete": base_complete,
        "view_digest": view_digest,
    })
}

/// Bound one commit's persisted representation before commit: truncate its
/// paths to fit, marked, so a giant single commit cannot roll back the whole
/// observation. Mirrors the file-change `changed_paths_truncated` marker.
fn bound_git_committed_data(snapshot: &GitSnapshot, commit: &GitCommit) -> Result<Value> {
    let mut paths = commit.paths.clone();
    let mut data = git_committed_data(snapshot, &commit.oid, &paths, false);
    while encode_json(&data)?.len() > MAX_EVENT_BYTES {
        if paths.len() > 1 {
            paths.truncate(paths.len() / 2);
        } else if !paths.is_empty() {
            paths.clear();
        } else {
            return Err(Error::new(
                "frame_too_large",
                "Git commit identity exceeds its bound",
            ));
        }
        data = git_committed_data(snapshot, &commit.oid, &paths, true);
    }
    Ok(data)
}

fn git_committed_data(
    snapshot: &GitSnapshot,
    oid: &str,
    paths: &[String],
    paths_truncated: bool,
) -> Value {
    let mut data = json!({"workspace": snapshot.workspace, "oid": oid, "paths": paths});
    if paths_truncated {
        data["paths_truncated"] = json!(true);
    }
    data
}

fn compute_git_view(
    tx: &Transaction<'_>,
    agent_num: i64,
    generation: i64,
    snapshot: &GitSnapshot,
) -> Result<GitView> {
    let mut dirty = snapshot.dirty.clone();
    dirty.sort_by(|a, b| {
        (&a.path, &a.status, &a.original_path).cmp(&(&b.path, &b.status, &b.original_path))
    });
    let mut committed_paths = snapshot.committed_paths.clone();
    committed_paths.sort();
    committed_paths.dedup();
    // Budget the canonical persisted representation before commit: the
    // accepted snapshot may be up to 900 KiB while one persisted event caps
    // at 64 KiB. Truncate the deterministically sorted dirty list (then
    // committed paths) to fit, and force the persisted view incomplete so a
    // truncated dirty set is never reported as complete. The same snapshot
    // always truncates to the same bytes, so a repeat poll compares equal
    // and writes nothing instead of failing again. Only an identity that
    // alone exceeds the bound still refuses, honestly, as frame_too_large.
    // Digest the full canonical coverage before budgeting: the persisted
    // prefix can drop any suffix, but the digest still witnesses it.
    let view_digest: String = canonical::digest(&git_changed_data(
        snapshot,
        &dirty,
        &committed_paths,
        snapshot.complete,
        snapshot.base_complete,
        "",
    ))?
    .iter()
    .map(|byte| format!("{byte:02x}"))
    .collect();
    let mut complete = snapshot.complete;
    let mut base_complete = snapshot.base_complete;
    let mut data = git_changed_data(
        snapshot,
        &dirty,
        &committed_paths,
        complete,
        base_complete,
        &view_digest,
    );
    while encode_json(&data)?.len() > MAX_EVENT_BYTES {
        if !dirty.is_empty() {
            dirty.truncate(dirty.len() / 2);
        } else if !committed_paths.is_empty() {
            committed_paths.truncate(committed_paths.len() / 2);
            base_complete = false;
        } else {
            return Err(Error::new(
                "frame_too_large",
                "Git observation identity exceeds its bound",
            ));
        }
        complete = false;
        data = git_changed_data(
            snapshot,
            &dirty,
            &committed_paths,
            complete,
            base_complete,
            &view_digest,
        );
    }
    let previous: Option<(String, i64)> = tx
        .query_row(
            "SELECT data,ts FROM events WHERE agent = ?1 AND generation = ?2
             AND source = 'git' AND kind = 'git.changed' ORDER BY seq DESC LIMIT 1",
            params![agent_num, generation],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(sql_error)?;
    if previous
        .as_ref()
        .is_some_and(|(_, observed_at)| *observed_at > snapshot.observed_at)
    {
        return Err(Error::new(
            "bad_state",
            "Git snapshot predates the committed Git view",
        ));
    }
    let previous = previous
        .as_ref()
        .map(|(data, _)| decode_json(data))
        .transpose()?;
    // Keep the exact committed OID in a generation-scoped ledger before
    // emitting its event. Event retention cannot erase this suppression, and
    // rewinding within the same generation does not redeliver an OID.
    let mut undelivered = Vec::new();
    if snapshot.complete {
        for commit in &snapshot.commits {
            let inserted = tx
                .execute(
                    "INSERT OR IGNORE INTO git_committed_oids(agent, generation, oid)
                     VALUES (?1, ?2, ?3)",
                    params![agent_num, generation, commit.oid],
                )
                .map_err(sql_error)?;
            if inserted == 1 {
                undelivered.push(commit.clone());
            }
        }
    }
    let changed = match previous {
        Some(previous) => canonical::digest(&previous)? != canonical::digest(&data)?,
        None => true,
    };
    Ok(GitView {
        data,
        changed,
        undelivered,
    })
}

fn persist_git_observation_events(
    tx: &Transaction<'_>,
    agent_num: i64,
    generation: i64,
    snapshot: &GitSnapshot,
    view: GitView,
    attributed: Vec<&FileChangeHint>,
    events: &mut Vec<Event>,
) -> Result<()> {
    if view.changed {
        events.push(insert_git_event(
            tx,
            agent_num,
            generation,
            snapshot.observed_at,
            "git.changed",
            "git",
            view.data,
        )?);
    }
    if snapshot.complete {
        for commit in &view.undelivered {
            events.push(insert_git_event(
                tx,
                agent_num,
                generation,
                snapshot.observed_at,
                "git.committed",
                "git",
                bound_git_committed_data(snapshot, commit)?,
            )?);
        }
    }
    for hint in attributed {
        db!(tx.execute(
            "INSERT INTO agent_edits(agent, generation, workspace, path, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(agent, generation, path) DO UPDATE
             SET updated_at = MAX(agent_edits.updated_at, excluded.updated_at)",
            params![
                agent_num,
                generation,
                snapshot.workspace,
                hint.path,
                hint.ts
            ],
        ));
        events.push(insert_git_event(tx, agent_num, generation, hint.ts, "file.changed", "file",
            json!({"workspace": snapshot.workspace, "path": hint.path, "cause_seq": hint.event_seq}))?);
    }
    Ok(())
}

// Lexical only: filesystem containment and symlink checks belong to the daemon.
fn normalized_native_git_path(workspace: &str, path: &str) -> Result<String> {
    fn portable(value: &str) -> String {
        let value = value.replace('\\', "/");
        if let Some(rest) = value.strip_prefix("//?/UNC/") {
            format!("//{rest}")
        } else {
            value.strip_prefix("//?/").unwrap_or(&value).to_owned()
        }
    }
    fn parts(value: &str) -> Result<Vec<&str>> {
        let mut parts = Vec::new();
        for part in value
            .split('/')
            .filter(|part| !part.is_empty() && *part != ".")
        {
            if part == ".." {
                if parts.pop().is_none() {
                    return Err(Error::new("invalid", "file hint escapes its workspace"));
                }
            } else {
                parts.push(part);
            }
        }
        Ok(parts)
    }
    let workspace = portable(workspace);
    let path = portable(path);
    let windows = workspace.starts_with("//") || workspace.as_bytes().get(1) == Some(&b':');
    let absolute = path.starts_with('/') || path.as_bytes().get(1) == Some(&b':');
    let base = parts(&workspace)?;
    let candidate = parts(&path)?;
    let relative = if absolute {
        if candidate.len() <= base.len()
            || !base.iter().zip(&candidate).all(|(base, candidate)| {
                if windows {
                    base.eq_ignore_ascii_case(candidate)
                } else {
                    base == candidate
                }
            })
        {
            return Err(Error::new(
                "invalid",
                "native file hint is outside its workspace",
            ));
        }
        candidate[base.len()..].join("/")
    } else {
        candidate.join("/")
    };
    validate_git_path(&relative)?;
    Ok(relative)
}

fn git_path_key(workspace: &str, path: &str) -> String {
    if workspace.starts_with("\\\\")
        || workspace.starts_with("//")
        || workspace.as_bytes().get(1) == Some(&b':')
    {
        path.to_ascii_lowercase()
    } else {
        path.to_owned()
    }
}

fn validate_git_snapshot(snapshot: &GitSnapshot) -> Result<()> {
    require_nonempty(&snapshot.workspace, "Git workspace")?;
    if snapshot.dirty.len() > MAX_PAGE_ENTRIES
        || snapshot.commits.len() > MAX_PAGE_ENTRIES
        || snapshot.clean_paths.len() > MAX_PAGE_ENTRIES
        || snapshot.committed_paths.len() > MAX_PAGE_ENTRIES
        || encode_json(snapshot)?.len() > MAX_PAGE_BYTES
    {
        return Err(Error::new(
            "frame_too_large",
            "Git snapshot exceeds its bound",
        ));
    }
    if let Some(head) = &snapshot.head {
        validate_git_oid(head)?;
    }
    if let Some(base) = &snapshot.base_oid {
        validate_git_oid(base)?;
    } else if snapshot.base_complete || !snapshot.committed_paths.is_empty() {
        return Err(Error::new(
            "invalid",
            "unfrozen Git base cannot report complete base coverage or committed paths",
        ));
    }
    for path in &snapshot.committed_paths {
        validate_git_path(path)?;
    }
    if let Some(branch) = &snapshot.branch {
        require_nonempty(branch, "Git branch")?;
    }
    let mut dirty = HashSet::new();
    for status in &snapshot.dirty {
        validate_git_path(&status.path)?;
        require_nonempty(&status.status, "Git path status")?;
        dirty.insert(git_path_key(&snapshot.workspace, &status.path));
        if let Some(path) = &status.original_path {
            validate_git_path(path)?;
            dirty.insert(git_path_key(&snapshot.workspace, path));
        }
    }
    for commit in &snapshot.commits {
        validate_git_oid(&commit.oid)?;
        for path in &commit.paths {
            validate_git_path(path)?;
        }
    }
    for path in &snapshot.clean_paths {
        validate_git_path(path)?;
        if snapshot.complete && dirty.contains(&git_path_key(&snapshot.workspace, path)) {
            return Err(Error::new(
                "source_integrity",
                "Git path is reported both dirty and clean",
            ));
        }
    }
    Ok(())
}

fn git_history_gap(
    tx: &Transaction<'_>,
    agent: i64,
    generation: i64,
    snapshot: &GitSnapshot,
    baseline: Option<&GitBaseline>,
    source: &SourceStateRecord,
) -> Result<Option<Event>> {
    let moved = baseline.is_none_or(|old| old.head != snapshot.head);
    if baseline.is_some()
        && (snapshot.history_complete && !snapshot.history_gap || !moved && source.gap_id.is_some())
    {
        return Ok(None);
    }
    let from = baseline.and_then(|old| old.head.as_deref());
    let anchor = baseline
        .map(|old| old.updated_at.to_string())
        .unwrap_or_else(|| "missing".to_owned());
    let gap_id = format!(
        "git-history:a{agent}#{generation}:{anchor}:{}:{}",
        from.unwrap_or("unborn"),
        snapshot.head.as_deref().unwrap_or("unborn")
    );
    if source.gap_id.as_deref() == Some(gap_id.as_str()) {
        return Ok(None);
    }
    let data = json!({
        "gap_id": gap_id, "agent_id": format_agent_id(agent), "generation": generation,
        "source": GIT_SOURCE, "families": ["git"],
        "reason": if baseline.is_none() { "initial_baseline" } else { "history_gap" },
        "from": from, "to": snapshot.head,
    });
    let digest = canonical::digest(&data)?;
    let event = insert_git_event(
        tx,
        agent,
        generation,
        snapshot.observed_at,
        "source.gap",
        "git",
        data,
    )?;
    db!(tx.execute(
        "UPDATE source_state SET gap_id = ?1, gap_digest = ?2, gap_from = ?3,
             gap_to = ?4, gap_seq = ?5 WHERE agent = ?6 AND generation = ?7 AND source = 'git'",
        params![
            gap_id,
            digest,
            from,
            snapshot.head,
            event.seq,
            agent,
            generation
        ],
    ));
    Ok(Some(event))
}

fn insert_git_event(
    tx: &Transaction<'_>,
    agent: i64,
    generation: i64,
    ts: i64,
    kind: &str,
    family: &str,
    data: Value,
) -> Result<Event> {
    insert_event(
        tx,
        EventInsert {
            ts,
            kind,
            family: Some(family),
            agent: Some(agent),
            generation: Some(generation),
            source: GIT_SOURCE,
            native_id: None,
            cursor: None,
            payload_digest: None,
            subject: event_subject_for_generation(tx, agent, generation)?,
            cause: None,
            data,
        },
    )
}
