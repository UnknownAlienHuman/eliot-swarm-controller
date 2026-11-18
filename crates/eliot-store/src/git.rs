use super::*;

impl Store {
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
        if !scanned.events.is_empty() || !events.is_empty() {
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
    deliver_commits: bool,
}

/// The canonical persisted `git.changed` representation: the full snapshot
/// identity plus the given dirty and committed-path coverage and completion
/// flags. Built in one place so the Store budget and the persisted bytes
/// share exact serialization rules.
fn git_changed_data(
    snapshot: &GitSnapshot,
    dirty: &[GitPathStatus],
    committed_paths: &[String],
    complete: bool,
    base_complete: bool,
) -> Value {
    json!({
        "workspace": snapshot.workspace, "head": snapshot.head, "branch": snapshot.branch,
        "dirty": dirty, "complete": complete,
        "history_complete": snapshot.history_complete, "history_gap": snapshot.history_gap,
        "base_oid": snapshot.base_oid, "committed_paths": committed_paths, "base_complete": base_complete,
    })
}

/// Bound one commit's persisted representation before commit: truncate its
/// paths to fit, marked, so a giant single commit cannot roll back the whole
/// observation. Mirrors the file-change `changed_paths_truncated` marker.
fn bound_git_committed_data(snapshot: &GitSnapshot, commit: &GitCommit) -> Result<Value> {
    let mut paths = commit.paths.clone();
    let mut truncated = false;
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
        truncated = true;
        data = git_committed_data(snapshot, &commit.oid, &paths, truncated);
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
    let mut complete = snapshot.complete;
    let mut base_complete = snapshot.base_complete;
    let mut data = git_changed_data(snapshot, &dirty, &committed_paths, complete, base_complete);
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
        data = git_changed_data(snapshot, &dirty, &committed_paths, complete, base_complete);
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
    // Delivery belongs to this binding. A partial view is not a commit receipt.
    let delivered = if previous
        .as_ref()
        .is_some_and(|view| view.get("complete").and_then(Value::as_bool) == Some(true))
    {
        previous.clone()
    } else {
        let complete: Option<String> = tx
            .query_row(
                "SELECT data FROM events WHERE agent = ?1 AND generation = ?2
                 AND source = 'git' AND kind = 'git.changed'
                 AND json_extract(data, '$.complete') = 1 ORDER BY seq DESC LIMIT 1",
                params![agent_num, generation],
                |row| row.get(0),
            )
            .optional()
            .map_err(sql_error)?;
        complete.as_deref().map(decode_json).transpose()?
    };
    let deliver_commits = delivered
        .as_ref()
        .is_none_or(|view| view.get("head") != data.get("head"));
    let changed = match previous {
        Some(previous) => canonical::digest(&previous)? != canonical::digest(&data)?,
        None => true,
    };
    Ok(GitView {
        data,
        changed,
        deliver_commits,
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
    if snapshot.complete && view.deliver_commits {
        for commit in &snapshot.commits {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn idle_git_writes_nothing_and_finds_hints_past_unrelated_events() {
        let mut store = Store::open(Path::new(":memory:")).unwrap();
        store.connection.execute("INSERT INTO agents(project,harness,profile,workspace,state,generation,created_at,updated_at) VALUES ('','codex','p','workspace','idle',1,100,100)", []).unwrap();
        store.ensure_git_source("a1", 1, "workspace", 100).unwrap();
        let mut snapshot = GitSnapshot {
            workspace: "workspace".to_owned(),
            head: Some("a".repeat(40)),
            branch: Some("main".to_owned()),
            dirty: Vec::new(),
            commits: Vec::new(),
            clean_paths: vec!["clean.rs".to_owned()],
            observed_at: 100,
            complete: true,
            history_complete: true,
            history_gap: false,
            base_oid: None,
            committed_paths: Vec::new(),
            base_complete: false,
        };
        store
            .record_git_observation("a1", 1, &snapshot, &[], 0, 100)
            .unwrap();
        // More unrelated events than one page used to starve a frozen idle cursor.
        store.connection.execute_batch("WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<1500) INSERT INTO events(ts,kind,agent,generation,source,data) SELECT 101,'tool.completed',1,1,'stream','{\"tool\":\"shell\",\"status\":\"completed\"}' FROM n").unwrap();
        let page = store
            .git_hint_page("a1", 1, store.git_cursor("a1", 1).unwrap(), 10)
            .unwrap();
        assert!(page.events.is_empty());
        assert_eq!(page.through, store.high_water().unwrap() as u64);
        snapshot.observed_at = 200;
        let before = store.connection.total_changes();
        assert!(
            store
                .record_git_observation("a1", 1, &snapshot, &[], page.through, 200)
                .unwrap()
                .is_empty()
        );
        assert_eq!(store.connection.total_changes(), before);

        // A no-op sample still fences a later regressed clock or delayed sample.
        snapshot.observed_at = 150;
        let error = store
            .record_git_observation("a1", 1, &snapshot, &[], page.through, 201)
            .unwrap_err();
        assert_eq!(error.code, "bad_state");
        assert_eq!(store.connection.total_changes(), before);

        // Typed booleans matter: a numeric 1 was never a native true hint.
        for data in [
            json!({"tool":"fileChange","status":"completed","changed_paths_truncated":1}),
            json!({"tool":"fileChange","status":"completed","redacted":1}),
            json!({"tool":"fileChange","status":"completed","changed_paths":"not-an-array"}),
        ] {
            store.connection.execute("INSERT INTO events(ts,kind,family,agent,generation,source,native_id,data) VALUES (201,'tool.completed','tool',1,1,'stream',?1,?2)", params![ulid::Ulid::new().to_string(), data.to_string()]).unwrap();
        }
        assert!(
            store
                .git_hint_page("a1", 1, 0, 10)
                .unwrap()
                .events
                .is_empty()
        );
        assert!(!pending_git_hints(&store.connection, 1, 1, 0).unwrap());
        store.connection.execute("INSERT INTO events(ts,kind,family,agent,generation,source,native_id,data) VALUES (202,'tool.completed','tool',1,1,'stream','real-file-change','{\"tool\":\"fileChange\",\"status\":\"completed\",\"changed_paths\":[\"changed.rs\"]}')", []).unwrap();
        let page = store.git_hint_page("a1", 1, 0, 10).unwrap();
        assert_eq!(page.events.len(), 1);
        assert_eq!(
            page.events[0].native_id.as_deref(),
            Some("real-file-change")
        );
        let hint = FileChangeHint {
            path: "changed.rs".to_owned(),
            event_seq: page.events[0].seq as u64,
            ts: 202,
        };
        snapshot.observed_at = 202;
        let events = store
            .record_git_observation(
                "a1",
                1,
                &snapshot,
                std::slice::from_ref(&hint),
                page.through,
                202,
            )
            .unwrap();
        assert!(events.iter().any(|event| event.kind == "file.changed"));
        assert_eq!(store.git_cursor("a1", 1).unwrap(), page.through);
        snapshot.observed_at = 203;
        let before = store.connection.total_changes();
        assert!(
            store
                .record_git_observation(
                    "a1",
                    1,
                    &snapshot,
                    std::slice::from_ref(&hint),
                    page.through,
                    203
                )
                .unwrap()
                .is_empty()
        );
        assert_eq!(store.connection.total_changes(), before);
        // Actual HEAD movement still commits a view and the new recovery baseline.
        snapshot.head = Some("b".repeat(40));
        snapshot.observed_at = 204;
        assert!(
            store
                .record_git_observation("a1", 1, &snapshot, &[], page.through, 204)
                .unwrap()
                .iter()
                .any(|event| event.kind == "git.changed")
        );
        assert_eq!(
            store.git_baseline("workspace").unwrap().unwrap().updated_at,
            204
        );
        store.connection.execute("INSERT INTO agents(project,harness,profile,workspace,state,generation,created_at,updated_at) VALUES ('','codex','p','workspace','idle',1,100,100)", []).unwrap();
        store.ensure_git_source("a2", 1, "workspace", 205).unwrap();
        store
            .set_git_source_status("a1", 1, "ok", true, 206)
            .unwrap();
        assert!(store.git_observed_at.contains_key("workspace"));
        store
            .set_git_source_status("a2", 1, "ok", true, 207)
            .unwrap();
        assert!(!store.git_observed_at.contains_key("workspace"));
    }
    #[test]
    fn large_dirty_tree_persists_a_bounded_partial_git_changed() {
        let mut store = Store::open(Path::new(":memory:")).unwrap();
        store.connection.execute("INSERT INTO agents(project,harness,profile,workspace,state,generation,created_at,updated_at) VALUES ('','codex','p','workspace','idle',1,100,100)", []).unwrap();
        store.ensure_git_source("a1", 1, "workspace", 100).unwrap();
        // 500 legal untracked paths: the accepted snapshot is far larger
        // than one persisted event, while every producer bound still holds.
        let dirty: Vec<GitPathStatus> = (0..500)
            .map(|i| GitPathStatus {
                path: format!("untracked/{i:03}-{}", "f".repeat(140)),
                status: "??".to_owned(),
                original_path: None,
            })
            .collect();
        let snapshot = GitSnapshot {
            workspace: "workspace".to_owned(),
            head: Some("a".repeat(40)),
            branch: Some("main".to_owned()),
            dirty: dirty.clone(),
            commits: Vec::new(),
            clean_paths: Vec::new(),
            observed_at: 100,
            complete: true,
            history_complete: true,
            history_gap: false,
            base_oid: None,
            committed_paths: Vec::new(),
            base_complete: false,
        };
        assert!(encode_json(&snapshot).unwrap().len() > MAX_EVENT_BYTES);
        let events = store
            .record_git_observation("a1", 1, &snapshot, &[], 0, 100)
            .unwrap();
        let changed = events
            .iter()
            .find(|event| event.kind == "git.changed")
            .expect("large dirty tree commits a git.changed view");
        let data = &changed.data;
        assert!(encode_json(&data).unwrap().len() <= MAX_EVENT_BYTES);
        // A truncated dirty set is never reported as complete, keeps its
        // deterministic sorted prefix, and stays attributable conservatively.
        assert_eq!(data.get("complete"), Some(&Value::Bool(false)));
        let persisted: Vec<String> = data
            .get("dirty")
            .and_then(Value::as_array)
            .expect("dirty list")
            .iter()
            .map(|entry| {
                entry
                    .get("path")
                    .and_then(Value::as_str)
                    .unwrap()
                    .to_owned()
            })
            .collect();
        assert!(!persisted.is_empty());
        let mut sorted: Vec<GitPathStatus> = dirty.clone();
        sorted.sort_by(|a, b| {
            (&a.path, &a.status, &a.original_path).cmp(&(&b.path, &b.status, &b.original_path))
        });
        let expected: Vec<String> = sorted
            .iter()
            .take(persisted.len())
            .map(|status| status.path.clone())
            .collect();
        assert_eq!(persisted, expected);
        // A repeat poll compares equal and writes nothing instead of
        // failing again; a shrunken tree commits a fuller view.
        let replay = store
            .record_git_observation("a1", 1, &snapshot, &[], 0, 101)
            .unwrap();
        assert!(!replay.iter().any(|event| event.kind == "git.changed"));
    }
}
