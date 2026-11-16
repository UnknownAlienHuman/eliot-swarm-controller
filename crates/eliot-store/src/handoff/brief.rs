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
    let mut budget = MAX_TEXT_BYTES / 2 - 4096;
    loop {
        let (task_text, t1) = bounded(&task_text, budget);
        let (output, t2) = bounded(&output, budget);
        let text = format!(
            "Handoff brief: context only. Await the separate successor SENDs before executing queued work.\nSource: {}/{}\nOriginal task command: {}\nRetained history: agent.result id={} generation={}; command.get id={}; turn.get agent={} generation={} turn_id={} (when retained). Retention gaps remain possible.\n\nOriginal task:\n{}\n\nLatest retained final output (turn {}):\n{}",
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
            turn,
            output
        );
        let frame = json!({"agent_id":request.to,"generation":request.to_generation,"command_id":request.id,"verb":"send","args":{"text":text}});
        if text.len() <= MAX_TEXT_BYTES && check_rpc_frame("agent.command", &frame).is_ok() {
            return Ok((text, t1 || t2));
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
