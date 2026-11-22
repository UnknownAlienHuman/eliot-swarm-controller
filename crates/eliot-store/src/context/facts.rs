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
    let mut seen = std::collections::HashSet::new();
    let mut new = Vec::new();
    let mut lookup = tx.prepare_cached(
        "SELECT EXISTS(SELECT 1 FROM context_noted_paths WHERE agent=?1 AND generation=?2 AND kind=?3 AND path_key=?4)"
    ).map_err(sql_error)?;
    for path in paths {
        let key = edit_path_key(path);
        if !seen.insert(key) {
            continue;
        }
        let noted: bool = lookup
            .query_row(params![agent, generation, kind, key], |row| row.get(0))
            .map_err(sql_error)?;
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
        ContextEditPath::OutsideOrUnverifiable { sha256, .. } => sha256,
    }
}

pub(super) fn edit_note_text(kind: &str, paths: &[ContextEditPath]) -> String {
    let mut text = if kind == "outside_worktree" {
        "Native edit paths are outside or unverifiable".to_owned()
    } else {
        "Edits are outside the frozen Goal scope".to_owned()
    };
    for path in paths {
        text.push_str("\n- ");
        match path {
            ContextEditPath::Relative { path } => text.push_str(path),
            ContextEditPath::OutsideOrUnverifiable { sha256, basename } => {
                text.push_str(basename);
                text.push_str(" (SHA-256 ");
                text.push_str(sha256);
                text.push(')');
            }
        }
    }
    text
}
