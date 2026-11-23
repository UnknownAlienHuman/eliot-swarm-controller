use super::*;

const MAX_EDIT_NOTES_PER_TURN: i64 = 4;
const MAX_PATHS_PER_NOTE: usize = 8;

impl Store {
    pub fn context_notes_cursor(&self) -> Result<i64> {
        self.connection
            .query_row(
                "SELECT seq FROM context_notes_cursor WHERE id=1",
                [],
                |row| row.get(0),
            )
            .map_err(sql_error)
    }

    pub fn advance_context_notes_cursor(&mut self, seq: i64) -> Result<()> {
        self.ensure_writer_healthy()?;
        let result = self
            .connection
            .execute(
                "UPDATE context_notes_cursor SET seq=MAX(seq, ?1) WHERE id=1",
                [seq],
            )
            .map(|_| ())
            .map_err(sql_error);
        self.fault_on_persistence_error(&result);
        result
    }

    pub fn task_card_has_handoff(&self, agent: &str, generation: i64) -> Result<bool> {
        let agent = parse_agent_id(agent)?;
        self.connection
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM agent_generation_reasons
                           WHERE agent=?1 AND generation<=?2 AND reason='handoff')
                 OR EXISTS(SELECT 1 FROM commands WHERE target=?1 AND generation<=?2
                           AND link_kind='handoff' AND ordinal=0)",
                params![agent, generation],
                |row| row.get(0),
            )
            .map_err(sql_error)
    }
}

pub(super) fn new_note_paths(
    tx: &Transaction<'_>,
    agent: i64,
    generation: i64,
    turn: &str,
    kind: &str,
    paths: &[ContextEditPath],
) -> Result<Vec<ContextEditPath>> {
    let count: i64 = tx.query_row(
        "SELECT COUNT(DISTINCT note_id) FROM context_noted_paths WHERE agent=?1 AND generation=?2 AND turn_id=?3",
        params![agent, generation, turn], |row| row.get(0),
    ).map_err(sql_error)?;
    if count >= MAX_EDIT_NOTES_PER_TURN {
        return Ok(Vec::new());
    }
    let turn_scoped = kind == "outside_worktree";
    let mut seen = std::collections::HashSet::new();
    let mut new = Vec::new();
    let mut lookup_generation = tx
        .prepare_cached(
            "SELECT EXISTS(SELECT 1 FROM context_noted_paths
                       WHERE agent=?1 AND generation=?2 AND kind=?3 AND path_key IN (?4,?5))",
        )
        .map_err(sql_error)?;
    let mut lookup_turn = tx
        .prepare_cached(
            "SELECT EXISTS(SELECT 1 FROM context_noted_paths
                       WHERE agent=?1 AND generation=?2 AND kind=?3 AND turn_id=?4
                         AND path_key IN (?5,?6,?7,?8))",
        )
        .map_err(sql_error)?;
    for path in paths {
        let key = stored_edit_path_key(kind, turn, path);
        let keys = stored_edit_path_keys(kind, turn, path);
        if keys.iter().any(|key| seen.contains(key)) {
            continue;
        }
        seen.extend(keys);
        let legacy_key = stored_edit_path_legacy_key(kind, turn, path);
        let noted: bool = if turn_scoped {
            lookup_turn
                .query_row(
                    params![
                        agent,
                        generation,
                        kind,
                        turn,
                        key,
                        legacy_key,
                        edit_path_key(path),
                        edit_path_legacy_key(path)
                    ],
                    |row| row.get(0),
                )
                .map_err(sql_error)?
        } else {
            lookup_generation
                .query_row(
                    params![
                        agent,
                        generation,
                        kind,
                        edit_path_key(path),
                        edit_path_legacy_key(path)
                    ],
                    |row| row.get(0),
                )
                .map_err(sql_error)?
        };
        if !noted {
            new.push(path.clone());
        }
        if new.len() == MAX_PATHS_PER_NOTE {
            break;
        }
    }
    Ok(new)
}

pub(super) fn edit_path_key(path: &ContextEditPath) -> &str {
    match path {
        ContextEditPath::Relative { path } => path,
        ContextEditPath::OutsideOrUnverifiable {
            sha256,
            canonical_sha256,
            ..
        } => canonical_sha256.as_deref().unwrap_or(sha256),
    }
}

pub(super) fn edit_path_legacy_key(path: &ContextEditPath) -> &str {
    match path {
        ContextEditPath::Relative { path } => path,
        ContextEditPath::OutsideOrUnverifiable { sha256, .. } => sha256,
    }
}

pub(super) fn stored_edit_path_key(kind: &str, turn: &str, path: &ContextEditPath) -> String {
    let key = edit_path_key(path);
    if kind == "outside_worktree" {
        // Keep the existing key layout. This fixed-width key combines turn
        // and path identity only for this kind; other facts stay generation-scoped.
        turn_scoped_path_key(turn, key)
    } else {
        key.to_owned()
    }
}

pub(super) fn stored_edit_path_legacy_key(
    kind: &str,
    turn: &str,
    path: &ContextEditPath,
) -> String {
    let key = edit_path_legacy_key(path);
    if kind == "outside_worktree" {
        turn_scoped_path_key(turn, key)
    } else {
        key.to_owned()
    }
}

/// Keep canonical and raw aliases so a later unverified observation of the
/// same path still matches the turn's original note.
pub(super) fn stored_edit_path_keys(kind: &str, turn: &str, path: &ContextEditPath) -> Vec<String> {
    let primary = stored_edit_path_key(kind, turn, path);
    let legacy = stored_edit_path_legacy_key(kind, turn, path);
    if primary == legacy {
        vec![primary]
    } else {
        vec![primary, legacy]
    }
}

fn turn_scoped_path_key(turn: &str, path_key: &str) -> String {
    // Length-framed inputs produce one fixed-width per-turn path identity.
    let mut identity = Vec::with_capacity(16 + turn.len() + path_key.len());
    identity.extend_from_slice(&(turn.len() as u64).to_be_bytes());
    identity.extend_from_slice(turn.as_bytes());
    identity.extend_from_slice(&(path_key.len() as u64).to_be_bytes());
    identity.extend_from_slice(path_key.as_bytes());
    eliot_core::api::inference::sha256_hex(&identity)
}

pub(super) fn edit_note_text(kind: &str, paths: &[ContextEditPath]) -> String {
    let mut text = if kind == "outside_worktree" {
        "These edits are outside your managed worktree or could not be verified inside it. Move outside edits into the worktree, or explain why they need to stay outside. Check the location of any unverified path.".to_owned()
    } else {
        "Edits are outside the frozen Goal scope".to_owned()
    };
    for path in paths {
        text.push_str("\n- ");
        match path {
            ContextEditPath::Relative { path } => text.push_str(path),
            ContextEditPath::OutsideOrUnverifiable {
                sha256,
                basename,
                path,
                canonical_sha256,
                ..
            } => {
                if kind == "outside_worktree" {
                    if canonical_sha256.is_some() {
                        text.push_str("verified outside path: ");
                    } else {
                        text.push_str("unverified location: ");
                    }
                }
                if let Some(path) = path {
                    text.push_str(path);
                } else {
                    text.push_str("full path unavailable; reported file ");
                    text.push_str(basename);
                    text.push_str(" (SHA-256 ");
                    text.push_str(sha256);
                    text.push(')');
                }
            }
        }
    }
    text
}
