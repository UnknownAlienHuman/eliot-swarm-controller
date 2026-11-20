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

/// Read/filter context in the transfer transaction, never raw native history.
pub(super) fn build(
    tx: &Transaction<'_>,
    request: &HandoffRequest,
    task: &Command,
    policy: &core::SecretPolicy,
) -> Result<(String, bool)> {
    let task_text = policy.redact_text(&required_string(&task.args, "text")?);
    let latest: Option<(String,Option<String>)> = tx.query_row(
        "SELECT turn_id,final_text FROM turns WHERE agent=?1 AND generation=?2
         AND state NOT IN ('running') ORDER BY COALESCE(completed_at,started_at,0) DESC,rowid DESC LIMIT 1",
        params![parse_agent_id(&request.from)?,request.from_generation],|row|Ok((row.get(0)?,row.get(1)?)))
        .optional().map_err(sql_error)?;
    let (turn, output) = match latest {
        Some((id, Some(text))) => (id, policy.redact_text(&text)),
        Some((id, None)) => (
            id,
            "[Final output unavailable in retained turn; no success inferred]".into(),
        ),
        None => (
            "none-retained".into(),
            "[No retained final output; absent or pruned history is not complete history]".into(),
        ),
    };
    let instructions = later_instructions(tx, request, policy)?;
    let facts = store_facts(tx, request)?;
    let mut budget = MAX_TEXT_BYTES / 2 - 4096;
    loop {
        let (task_text, t1) = bounded(&task_text, budget);
        let (output, t2) = bounded(&output, budget);
        let (instructions, t3) = bounded(&instructions, budget / 2);
        let text = format!(
            "Handoff brief: context only. Await the separate successor SENDs before executing queued work.\nSource: {}/{}\nOriginal task command: {}\nRetained history: agent.result id={} generation={}; command.get id={}; turn.get agent={} generation={} turn_id={} (when retained). Retention gaps remain possible.\n\nOriginal task:\n{}\n\nLater instructions to the source, oldest first (they narrow or change the task):\n{}\n\nStore facts (recorded by Swarm, not model claims):\n{}\n\nExecutor's last report (model output, not verified by Swarm; turn {}):\n{}",
            request.from,
            request.from_generation,
            request.task_command,
            request.from,
            request.from_generation,
            request.task_command,
            request.from,
            request.from_generation,
            turn,
            task_text,
            instructions,
            facts,
            turn,
            output
        );
        let frame = json!({"agent_id":request.to,"generation":request.to_generation,"command_id":request.id,"verb":"send","args":{"text":text}});
        if text.len() <= MAX_TEXT_BYTES && check_rpc_frame("agent.command", &frame).is_ok() {
            return Ok((text, t1 || t2 || t3));
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
/// limits and clarifications that summaries lose.
fn later_instructions(
    tx: &Transaction<'_>,
    request: &HandoffRequest,
    policy: &core::SecretPolicy,
) -> Result<String> {
    let mut statement = tx
        .prepare(
            "SELECT principal, verb, json_extract(args, '$.text') FROM commands
             WHERE target = ?1 AND id <> ?2
               AND verb IN ('send', 'steer', 'inject')
               AND state NOT IN ('rejected', 'cancelled')
               AND created_at >= (SELECT created_at FROM commands WHERE id = ?2)
             ORDER BY created_at DESC, rowid DESC LIMIT ?3",
        )
        .map_err(sql_error)?;
    let rows = statement
        .query_map(
            params![
                parse_agent_id(&request.from)?,
                request.task_command,
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

/// Machine facts about the source: its workspace and Git work, its Goal,
/// and the commands still open or unknown for the handed-off generation.
fn store_facts(tx: &Transaction<'_>, request: &HandoffRequest) -> Result<String> {
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
             WHERE target = ?1 AND generation = ?2
               AND state IN ('accepted', 'queued', 'dispatched', 'unknown')
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
    Ok(format!("{workspace_line}\n{goal_line}\n{open_line}"))
}
