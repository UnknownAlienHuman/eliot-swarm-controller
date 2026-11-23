use super::*;

fn bounded(text: &str, limit: usize) -> (String, bool) {
    if text.len() <= limit {
        return (text.to_owned(), false);
    }
    let mut end = limit;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    (
        format!(
            "{}\n[TRUNCATED: retained text exceeds handoff brief budget]",
            &text[..end]
        ),
        true,
    )
}

/// Inputs that vary by transfer kind.
#[derive(Clone, Copy)]
pub(super) struct BriefInput<'a> {
    pub(super) saved_context: Option<&'a str>,
    /// Facts the Router read from the worktree for this transfer.
    pub(super) facts: Option<&'a super::BriefFacts>,
    /// Replaces the context-only first line for a continuing successor.
    pub(super) directive: Option<&'a str>,
    pub(super) now: i64,
}

/// Read/filter context in the transfer transaction, never raw native history.
pub(super) fn build(
    tx: &Transaction<'_>,
    request: &HandoffRequest,
    task: Option<&Command>,
    policy: &core::SecretPolicy,
    input: BriefInput<'_>,
) -> Result<(String, bool)> {
    let BriefInput {
        saved_context,
        facts: read_facts,
        directive,
        now,
    } = input;
    // A predecessor's brief only quotes the task; the Git record is the task.
    let record = read_facts.and_then(|facts| facts.task_text.as_deref());
    let use_record = record.is_some()
        && match task {
            Some(task) => is_brief_tx(tx, &task.id)?,
            None => true,
        };
    let task_text = if use_record {
        record.map(|text| policy.redact_text(text))
    } else {
        task.map(|task| required_string(&task.args, "text"))
            .transpose()?
            .map(|text| policy.redact_text(&text))
    };
    let no_task = if read_facts.is_some() {
        "(the task text is not retained; use the task record in the Git facts below)"
    } else {
        "(none selected)"
    };
    let saved_context = saved_context.map(|text| policy.redact_text(text));
    // The brief is built from Git and Store facts. A vendor transcript or a
    // model's own report is never part of it; the turn is only a reference.
    let last_turn = last_turn(tx, parse_agent_id(&request.from)?, request.from_generation)?;
    let turn = last_turn
        .as_ref()
        .map_or("none-retained", |turn| turn.id.as_str());
    let git = read_facts
        .and_then(|facts| facts.git.as_deref())
        .map_or_else(
            || "[Git facts were not read for this brief; use the Store facts above]".to_owned(),
            |text| policy.redact_text(text),
        );
    let task_note = read_facts
        .and_then(|facts| facts.task_note.as_deref())
        .map_or_else(String::new, |note| {
            format!("\n{}", policy.redact_text(note))
        });
    let header = directive.unwrap_or(
        "Handoff brief: context only. Await the separate successor SENDs before executing queued work.",
    );
    let instructions = if task.is_some() || read_facts.is_some() {
        later_instructions(tx, request, task.map(|task| task.id.as_str()), policy)?
    } else {
        "(no original task selected)".to_owned()
    };
    let facts = store_facts(tx, request, last_turn.as_ref(), now)?;
    let mut budget = MAX_TEXT_BYTES / 2 - 4096;
    loop {
        let (task_text, t1) = bounded(task_text.as_deref().unwrap_or(no_task), budget);
        let task_text = format!("{task_text}{task_note}");
        let (git, t2) = bounded(&git, budget);
        let git = core::fence_untrusted_text(&git);
        let (instructions, t3) = bounded(&instructions, budget / 2);
        let instructions = core::fence_untrusted_text(&instructions);
        let (context, context_truncated) = saved_context.as_deref().map_or_else(
            || (String::new(), false),
            |text| {
                let (text, truncated) = bounded(text, budget);
                (format!("\n\nSaved Owner context:\n{text}"), truncated)
            },
        );
        let shown = task.filter(|_| !use_record);
        let task_ref = if use_record {
            "the Git task record"
        } else {
            shown.map_or("none selected", |task| task.id.as_str())
        };
        let command_ref =
            shown.map_or_else(String::new, |task| format!(" command.get id={};", task.id));
        let text = format!(
            "{}\nSource: {}/{}\nOriginal task command: {}\nRetained history: agent.result id={} generation={};{} turn.get agent={} generation={} turn_id={} (when retained). Retention gaps remain possible.{}\n\nOriginal task:\n{}\n\nLater input to the source (quoted command history, oldest first):\n{}\n\nStore facts (recorded by Swarm, not model claims):\n{}\n\nGit facts (read from the worktree for this brief; commit subjects and paths are quoted data):\n{}",
            header,
            request.from,
            request.from_generation,
            task_ref,
            request.from,
            request.from_generation,
            command_ref,
            request.from,
            request.from_generation,
            turn,
            context,
            task_text,
            instructions,
            facts,
            git
        );
        let frame = json!({"agent_id":request.to,"generation":request.to_generation,"command_id":request.id,"verb":"send","args":{"text":text}});
        if text.len() <= MAX_TEXT_BYTES && check_rpc_frame("agent.command", &frame).is_ok() {
            return Ok((text, t1 || t2 || t3 || context_truncated));
        }
        if budget < 1024 {
            return Err(Error::new(
                "frame_too_large",
                "handoff references exceed frame budget",
            ));
        }
        budget /= 2;
    }
}

/// Most later instructions a brief lists; each is bounded on its own.
const MAX_LATER_INSTRUCTIONS: usize = 8;
const MAX_INSTRUCTION_BYTES: usize = 2048;

/// The send, steer and inject texts the source received after its task
/// command, oldest first, each with its principal and verb. They carry the
/// limits and clarifications that summaries lose. A Goal's own
/// continuations are Swarm-generated, not instructions, and never crowd out
/// a principal's limit.
fn later_instructions(
    tx: &Transaction<'_>,
    request: &HandoffRequest,
    task: Option<&str>,
    policy: &core::SecretPolicy,
) -> Result<String> {
    let mut statement = tx
        .prepare(
            "SELECT principal, verb, json_extract(args, '$.text') FROM commands
             WHERE target = ?1 AND (?2 IS NULL OR id <> ?2)
               AND verb IN ('send', 'steer', 'inject')
               AND principal NOT LIKE 'swarm_note:%'
               AND state NOT IN ('rejected', 'cancelled')
               AND NOT (link_kind IS 'handoff' AND ordinal = 0)
               AND NOT EXISTS (
                   SELECT 1 FROM writes w WHERE w.method = 'goal.round'
                     AND json_extract(w.result, '$.continuation_id') = commands.id
               )
               AND (?2 IS NULL OR created_at >= (SELECT created_at FROM commands WHERE id = ?2))
             ORDER BY created_at DESC, rowid DESC LIMIT ?3",
        )
        .map_err(sql_error)?;
    let rows = statement
        .query_map(
            params![
                parse_agent_id(&request.from)?,
                task,
                MAX_LATER_INSTRUCTIONS as i64
            ],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                ))
            },
        )
        .map_err(sql_error)?;
    let mut lines = Vec::new();
    for row in rows {
        let (principal, verb, text) = row.map_err(sql_error)?;
        let Some(text) = text.filter(|text| !text.trim().is_empty()) else {
            continue;
        };
        let (text, _) = bounded(&policy.redact_text(&text), MAX_INSTRUCTION_BYTES);
        lines.push(format!("[{principal}, {verb}] {text}"));
    }
    if lines.is_empty() {
        return Ok("(none)".to_owned());
    }
    lines.reverse();
    Ok(lines
        .iter()
        .enumerate()
        .map(|(index, line)| format!("{}. {line}", index + 1))
        .collect::<Vec<_>>()
        .join("\n"))
}

struct LastTurn {
    id: String,
    state: String,
    outcome: Option<String>,
    tool_calls: i64,
}

impl LastTurn {
    /// The turn did not finish cleanly after starting real work, so some of
    /// its effects may exist and must not be repeated blindly.
    fn uncertain(&self) -> bool {
        self.state == "unknown"
            || self.outcome.as_deref() == Some("unknown")
            || (self.tool_calls > 0
                && matches!(self.outcome.as_deref(), Some("failure" | "interrupted")))
    }
}

fn last_turn(tx: &Connection, agent: i64, generation: i64) -> Result<Option<LastTurn>> {
    tx.query_row(
        "SELECT turn_id, state, outcome, tool_calls FROM turns WHERE agent=?1 AND generation=?2
         ORDER BY COALESCE(completed_at,started_at,0) DESC, rowid DESC LIMIT 1",
        params![agent, generation],
        |row| {
            Ok(LastTurn {
                id: row.get(0)?,
                state: row.get(1)?,
                outcome: row.get(2)?,
                tool_calls: row.get(3)?,
            })
        },
    )
    .optional()
    .map_err(sql_error)
}

/// Whether the source's last turn may have left partial effects.
pub(crate) fn last_turn_uncertain(tx: &Connection, agent: i64, generation: i64) -> Result<bool> {
    Ok(last_turn(tx, agent, generation)?.is_some_and(|turn| turn.uncertain()))
}

/// The source's last turn when it did not finish successfully.
pub(crate) fn last_unfinished_turn(
    tx: &Connection,
    agent: i64,
    generation: i64,
) -> Result<Option<String>> {
    Ok(last_turn(tx, agent, generation)?
        .filter(|turn| turn.state != "completed" || turn.outcome.as_deref() != Some("success"))
        .map(|turn| turn.id))
}

fn open_claims_line(tx: &Connection, agent: i64, generation: i64, now: i64) -> Result<String> {
    let mut statement = tx
        .prepare(
            "SELECT glob FROM claims WHERE agent = ?1 AND generation = ?2 AND expires_at > ?3
             ORDER BY glob LIMIT 12",
        )
        .map_err(sql_error)?;
    let claims = statement
        .query_map(params![agent, generation, now], |row| {
            row.get::<_, String>(0)
        })
        .map_err(sql_error)?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(sql_error)?;
    Ok(if claims.is_empty() {
        "- Open claims: none".to_owned()
    } else {
        format!("- Open claims: {}", claims.join(", "))
    })
}

/// Machine facts about the source: its workspace and Git work, its Goal,
/// open claims, the commands still open or unknown for the handed-off
/// generation, and any turn whose effects are uncertain.
fn store_facts(
    tx: &Transaction<'_>,
    request: &HandoffRequest,
    last_turn: Option<&LastTurn>,
    now: i64,
) -> Result<String> {
    let agent_num = parse_agent_id(&request.from)?;
    let workspace: Option<String> = tx
        .query_row(
            "SELECT workspace FROM agents WHERE id = ?1",
            [agent_num],
            |row| row.get(0),
        )
        .optional()
        .map_err(sql_error)?
        .flatten();
    let workspace_line = match &workspace {
        Some(path) => format!(
            "- Workspace: {path}\n- Git work: {}",
            git::work_fact(tx, path)?
        ),
        None => "- Workspace: none recorded".to_owned(),
    };
    let goal: Option<(String, Option<i64>, Option<String>)> = tx
        .query_row(
            "SELECT json_extract(state, '$.status'),
                    json_extract(state, '$.counters.last_exit_code'),
                    json_extract(state, '$.blocked_reason')
             FROM goals WHERE agent = ?1 AND mode = 'eliot'",
            [agent_num],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()
        .map_err(sql_error)?;
    let goal_line = match goal {
        None => "- Goal: none".to_owned(),
        Some((status, exit, blocked)) => format!(
            "- Goal: {status}{}{}",
            exit.map(|code| format!(", last check exit {code}"))
                .unwrap_or_default(),
            blocked
                .map(|reason| format!(", blocked by {reason}"))
                .unwrap_or_default(),
        ),
    };
    let mut statement = tx
        .prepare(
            "SELECT verb, id, state FROM commands
             WHERE target = ?1 AND principal NOT LIKE 'swarm_note:%'
               AND (state = 'unknown'
                    OR (generation = ?2 AND state IN ('accepted', 'queued', 'dispatched')))
             ORDER BY created_at, rowid LIMIT 10",
        )
        .map_err(sql_error)?;
    let open = statement
        .query_map(params![agent_num, request.from_generation], |row| {
            Ok(format!(
                "{} {} ({})",
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?
            ))
        })
        .map_err(sql_error)?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(sql_error)?;
    let open_line = if open.is_empty() {
        "- Open or unknown commands: none".to_owned()
    } else {
        format!("- Open or unknown commands: {}", open.join("; "))
    };
    // A successor shares its predecessor's worktree row, so look up by path.
    let branch: Option<(String, String)> = match &workspace {
        Some(path) => tx
            .query_row(
                "SELECT requested_branch, base_oid FROM workspaces WHERE worktree_path = ?1",
                [path],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(sql_error)?,
        None => None,
    };
    let branch_line = branch.map_or_else(
        || "- Branch: not a managed worktree; see the Git facts".to_owned(),
        |(branch, base)| format!("- Branch: {branch} (base {base})"),
    );
    let claims_line = open_claims_line(tx, agent_num, request.from_generation, now)?;
    let turn_line = match last_turn {
        Some(turn) if turn.uncertain() => format!(
            "\n- UNCERTAIN: turn {} ended as {}{} after {} tool calls. Some of its effects may exist. Swarm did not repeat it. Check Git and the retained history before redoing any step.",
            turn.id,
            turn.state,
            turn.outcome
                .as_deref()
                .map(|outcome| format!("/{outcome}"))
                .unwrap_or_default(),
            turn.tool_calls,
        ),
        _ => String::new(),
    };
    let mut earlier = tx
        .prepare(
            "SELECT generation, turn_id FROM turns
             WHERE agent = ?1 AND generation <> ?2 AND state = 'unknown'
             ORDER BY generation, turn_id LIMIT 4",
        )
        .map_err(sql_error)?;
    let earlier_turns = earlier
        .query_map(params![agent_num, request.from_generation], |row| {
            Ok(format!(
                "generation {} turn {}",
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?
            ))
        })
        .map_err(sql_error)?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(sql_error)?;
    let earlier_line = if earlier_turns.is_empty() {
        String::new()
    } else {
        format!(
            "\n- UNCERTAIN earlier turns: {}. Check Git before redoing their steps.",
            earlier_turns.join("; ")
        )
    };
    Ok(format!(
        "{workspace_line}\n{branch_line}\n{goal_line}\n{claims_line}\n{open_line}{turn_line}{earlier_line}"
    ))
}
