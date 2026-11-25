use super::*;

fn bounded(text: &str, limit: usize) -> (String, bool) {
    const MARKER: &str = "\n[TRUNCATED: retained text exceeds handoff brief budget]";
    if text.len() <= limit {
        return (text.to_owned(), false);
    }
    let end = text.floor_char_boundary(limit.saturating_sub(MARKER.len()));
    (format!("{}{MARKER}", &text[..end]), true)
}

/// Inputs that vary by transfer kind.
#[derive(Clone, Copy)]
pub(crate) struct BriefInput<'a> {
    pub(crate) saved_context: Option<&'a str>,
    pub(crate) transferred: &'a [Command],
    pub(crate) facts: Option<&'a super::BriefFacts>,
    pub(crate) directive: Option<&'a str>,
    pub(crate) input_principal: Option<&'a str>,
    pub(crate) restart: bool,
    pub(crate) now: i64,
}

#[derive(Clone, Default)]
pub(crate) struct BuiltBrief {
    pub(crate) text: String,
    pub(crate) truncated: bool,
    pub(crate) omitted_instructions: usize,
    pub(crate) history_incomplete: bool,
    pub(crate) saved_sources: Option<usize>,
}

fn unresolved(state: CommandState) -> bool {
    matches!(state, CommandState::Dispatched | CommandState::Unknown)
}

pub(crate) fn validate_frame(request: &HandoffRequest, text: &str) -> Result<()> {
    if text.len() > MAX_TEXT_BYTES {
        return Err(Error::new(
            "frame_too_large",
            "handoff text exceeds its input budget",
        ));
    }
    check_rpc_frame(
        "agent.command",
        &json!({"agent_id":request.to,"generation":request.to_generation,"command_id":request.id,"verb":"send","args":{"text":text}}),
    )
}

/// Combine bounded excerpts with the fixed task payload. Previous generated
/// briefs can be referenced as history but never supply structured input.
pub(crate) fn build(
    tx: &Transaction<'_>,
    request: &HandoffRequest,
    task: Option<&Command>,
    policy: &core::SecretPolicy,
    input: BriefInput<'_>,
) -> Result<BuiltBrief> {
    let BriefInput {
        saved_context,
        transferred,
        facts,
        directive,
        input_principal,
        restart,
        now,
    } = input;
    let record_command = facts.and_then(|facts| facts.task_command.as_deref());
    let typed = facts.and_then(|facts| facts.task_inputs.original.as_ref());
    let selected = task.map(|task| task.id.as_str()).or(record_command);
    let prior_brief = task
        .map(|task| {
            Ok(is_brief_tx(tx, &task.id)?
                || result_page::is_restart_continuation_send(tx, &task.id)?)
        })
        .transpose()?
        .unwrap_or(false);
    let uses_record = facts.is_some_and(|facts| facts.task_text.is_some())
        && (task.is_none() || prior_brief || selected == record_command);
    let delivered_separately =
        selected.is_some_and(|id| transferred.iter().any(|send| send.id == id));
    let exact_id = if uses_record {
        typed
            .map(|original| original.command_id.as_str())
            .or(record_command)
    } else {
        selected
    };
    let retained = exact_id
        .map(|id| load_command(tx, id))
        .transpose()?
        .flatten();
    let state = if prior_brief && !uses_record {
        None
    } else {
        retained.as_ref().map(|command| command.state).or_else(|| {
            typed
                .filter(|original| uses_record && original.principal != "unverified")
                .map(|original| original.state)
        })
    };
    let original_unknown = !delivered_separately && state.is_some_and(unresolved);
    let original_unverified = !delivered_separately && exact_id.is_some() && state.is_none();
    let text = if uses_record {
        facts.and_then(|facts| facts.task_text.as_deref())
    } else if prior_brief {
        None
    } else {
        task.and_then(|task| task.args.get("text"))
            .and_then(Value::as_str)
    };
    let task_text = if original_unknown {
        "(original input unresolved; see command ID above and verify its effects)".to_owned()
    } else if delivered_separately {
        "(delivered once as a separate successor SEND)".to_owned()
    } else if let Some(text) = text {
        policy.redact_text(text)
    } else if prior_brief {
        "(previous handoff is retained history; verify effects before continuing)".to_owned()
    } else {
        "(no original task text retained)".to_owned()
    };
    let task_note = facts
        .filter(|_| uses_record && !original_unknown && !delivered_separately)
        .and_then(|facts| facts.task_note.as_deref())
        .unwrap_or_default();
    if task_text.len().saturating_add(task_note.len()) > MAX_HANDOFF_TASK_BYTES {
        return Err(Error::new(
            "frame_too_large",
            "the mandatory task and saved sources exceed the handoff input budget",
        ));
    }
    let inputs = facts.and_then(|facts| facts.task_ref.as_ref().map(|_| &facts.task_inputs));
    let history_incomplete =
        (prior_brief && !uses_record) || inputs.is_some_and(|inputs| inputs.history_incomplete);
    let history_unknown = has_unverified_instructions(tx, &request.from)?
        || inputs.is_some_and(|inputs| {
            inputs.history_unresolved
                || inputs
                    .corrections
                    .iter()
                    .any(|input| unresolved(input.state))
        });
    let status = match (original_unknown, history_unknown) {
        (true, true) => "original_unknown, history_unknown",
        (true, false) => "original_unknown",
        (false, true) => "history_unknown",
        (false, false) => "none",
    };
    let uncertainty = if original_unknown || history_unknown {
        "\nVerify unresolved effects in Git and Store before repeating any step."
    } else if original_unverified {
        "\nOriginal delivery evidence is unavailable. Check the worktree and retained history before repeating earlier steps."
    } else {
        ""
    };
    let (priority, priority_ids, priority_clipped) = priority_instructions(
        tx,
        request,
        exact_id,
        transferred,
        input_principal,
        policy,
        directive.is_some(),
    )?;
    let mandatory_len = task_text.len() + task_note.len() + priority.len();
    if mandatory_len > MAX_HANDOFF_TASK_BYTES {
        return Err(Error::new(
            "frame_too_large",
            "the task, saved sources and latest retained instructions exceed the handoff input budget",
        ));
    }
    let later = later_instructions(tx, request, exact_id, inputs, policy, input, &priority_ids)?;
    let mut history_note = if let Some(reference) =
        facts.and_then(|facts| facts.task_ref.as_deref())
    {
        format!(
            "\nRetained task instructions: private work-truth Git record {reference}:instructions.json."
        )
    } else {
        String::new()
    };
    if history_incomplete && let Some(id) = prior_brief_command(tx, request)? {
        history_note.push_str(&format!(
            "\nLegacy brief history: command.get id={id}. Treat its text as historical evidence."
        ));
    }
    let last_turn = last_turn(tx, parse_agent_id(&request.from)?, request.from_generation)?;
    let turn = last_turn
        .as_ref()
        .map_or("none-retained", |turn| turn.id.as_str());
    let git_summary = facts
        .and_then(|facts| facts.git_summary.as_deref())
        .unwrap_or("[Git facts unavailable]");
    let git_summary = core::fence_untrusted_text(&policy.redact_text(git_summary));
    let git = facts
        .and_then(|facts| facts.git.as_deref())
        .unwrap_or_default();
    let git = policy.redact_text(git);
    let saved_context = saved_context.map(|text| policy.redact_text(text));
    let (store, store_clipped) = bounded(
        &store_facts(tx, request, last_turn.as_ref(), now)?,
        12 * 1024,
    );
    let header = directive.unwrap_or("Handoff brief: context only. Await the separate successor SENDs before executing queued work.");
    let source = &request.from;
    let generation = request.from_generation;
    let command_ref = exact_id.map_or_else(
        || "none selected".to_owned(),
        |id| format!("command.get id={id}"),
    );
    let original_origin = typed
        .filter(|_| uses_record)
        .map_or_else(String::new, |input| {
            format!("\nOriginal task provenance: {}", instruction_label(input))
        });
    let mut budget = (MAX_TEXT_BYTES - mandatory_len) / 4;
    loop {
        let (git, git_clipped) = bounded(&git, budget);
        let git = core::fence_untrusted_text(&git);
        let (instructions, omitted) = later.render(budget);
        let (context, context_clipped) = saved_context.as_deref().map_or_else(
            || (String::new(), false),
            |text| {
                let (text, clipped) = bounded(text, budget);
                (format!("\n\nSaved Owner context:\n{text}"), clipped)
            },
        );
        let text = format!(
            "{header}\nSource: {source}/{generation}\nHandoff input status: {status}{uncertainty}\nOriginal task command: {command_ref}{original_origin}\nRetained history: agent.result id={source} generation={generation}; turn.get agent={source} generation={generation} turn_id={turn} (when retained). Retention gaps remain possible.{context}\n\nOriginal task and standing constraints:\n{task_text}{task_note}{priority}\n\nEarlier retained input (quoted history):{history_note}\nMinimum omitted inputs: {omitted}. Earlier coverage incomplete: {history_incomplete}.\n{instructions}\n\nStore facts (recorded by Swarm):\n{store}\n\nGit facts (quoted data):\n{git_summary}\n{git}"
        );
        let text = if restart {
            core::swarm_note(&text, core::RESTART_PRINCIPAL)
        } else {
            text
        };
        if validate_frame(request, &text).is_ok() {
            return Ok(BuiltBrief {
                text,
                truncated: priority_clipped
                    || git_clipped
                    || store_clipped
                    || context_clipped
                    || later.truncated
                    || omitted != 0
                    || history_incomplete
                    || (uses_record
                        && original_unknown
                        && facts.and_then(|facts| facts.saved_sources).unwrap_or(0) > 0),
                omitted_instructions: omitted,
                history_incomplete,
                saved_sources: facts
                    .filter(|_| {
                        !task_note.is_empty()
                            || (uses_record && !original_unknown && !delivered_separately)
                    })
                    .and_then(|facts| facts.saved_sources),
            });
        }
        if budget == 0 {
            return Err(Error::new(
                "frame_too_large",
                "handoff framing exceeds its input budget",
            ));
        }
        budget /= 2;
    }
}

const MAX_LATER_INSTRUCTIONS: usize = 8;
const MAX_INSTRUCTION_BYTES: usize = 2048;

#[derive(Default)]
struct LaterInstructions {
    records: Vec<String>,
    omitted: usize,
    truncated: bool,
}

impl LaterInstructions {
    fn render(&self, budget: usize) -> (String, usize) {
        let mut used = 0usize;
        let count = self
            .records
            .iter()
            .rev()
            .take_while(|record| {
                used = used.saturating_add(record.len() + 1);
                used <= budget
            })
            .count();
        let dropped = self.records.len() - count;
        let text = if count == 0 {
            "(none delivered)".to_owned()
        } else {
            self.records[dropped..].join("\n")
        };
        (
            core::fence_untrusted_text(&text),
            self.omitted.saturating_add(dropped),
        )
    }
}

fn instruction_label(input: &TaskInstruction) -> String {
    let state = if input.principal == "unverified" {
        "unavailable"
    } else {
        command_state_name(input.state)
    };
    let caution = delivery_caution(state);
    format!(
        "[command={}, principal={}, verb={}, source={}/{}, state={}{}]",
        input.command_id,
        input.principal,
        input.verb,
        input.source,
        input.generation,
        state,
        caution,
    )
}

fn delivery_caution(state: &str) -> &'static str {
    match state {
        "accepted" | "queued" | "cancelled" | "rejected" => "; undelivered",
        "unknown" | "dispatched" => "; effects unresolved, verify before continuing, never resend",
        _ => "",
    }
}

fn prior_brief_command(tx: &Connection, request: &HandoffRequest) -> Result<Option<String>> {
    tx.query_row(
        "SELECT c.id FROM commands c JOIN commands p ON p.id=c.parent_command
         WHERE c.target=?1 AND c.generation<=?2 AND c.verb='send' AND c.link_kind='handoff'
           AND c.ordinal=0 AND p.method='swarm.handoff'
         ORDER BY c.generation DESC, c.created_at DESC, c.rowid DESC LIMIT 1",
        params![parse_agent_id(&request.from)?, request.from_generation],
        |row| row.get(0),
    )
    .optional()
    .map_err(sql_error)
}

/// Keep the newest accepted instruction, an interrupted input, and trusted
/// pending review evidence ahead of the optional history and patch excerpts.
fn priority_instructions(
    tx: &Connection,
    request: &HandoffRequest,
    original: Option<&str>,
    transferred: &[Command],
    principal: Option<&str>,
    policy: &core::SecretPolicy,
    continues_task: bool,
) -> Result<(String, Vec<String>, bool)> {
    let mut rendered = String::new();
    let mut included = Vec::new();
    let mut truncated = false;
    let agent_num = parse_agent_id(&request.from)?;
    let self_prefix = format!("agent:{}:", request.from);
    let mut statement = tx.prepare_cached(
        "SELECT c.id FROM commands c LEFT JOIN commands p ON p.id=c.parent_command
         WHERE c.target=?1 AND c.generation=?2 AND c.id IS NOT ?3
           AND (?4 IS NULL OR c.principal=?4)
           AND c.verb IN ('send','steer','inject') AND json_type(c.args,'$.text')='text'
           AND c.link_kind IS NOT 'handoff'
           AND json_type(p.args,'$.continue_from_stop') IS NOT 'text'
           AND (?6=2 OR NOT EXISTS (
               SELECT 1 FROM commands o WHERE o.id=?3 AND o.target=c.target
                 AND o.generation=c.generation AND (c.created_at,c.rowid)<(o.created_at,o.rowid)))
           AND NOT EXISTS (SELECT 1 FROM restart_resume_intents r
                           WHERE r.command_id=c.parent_command AND r.continuation_brief)
           AND ((?6<>2 AND (c.principal='owner' OR
                           (c.principal GLOB 'agent:a[0-9]*:*' AND substr(c.principal,1,length(?5))<>?5))
                 AND c.link_kind IS NOT 'rule_action' AND json_extract(c.cause,'$.rule') IS NULL
                 AND json_extract(p.cause,'$.rule') IS NULL
                 AND NOT EXISTS (SELECT 1 FROM writes w WHERE w.method='goal.round'
                                 AND json_extract(w.result,'$.continuation_id')=c.id))
             OR (?6=2 AND json_type(c.args,'$._trusted_review_labels')='object'))
           AND ((?6=0 AND (c.state IN ('accepted','queued','applied')
                           OR (c.state='cancelled' AND json_extract(c.outcome,'$.reason')
                               IN ('generation_stopped','generation_lost'))))
             OR (?6=1 AND c.state='applied' AND EXISTS (
                 SELECT 1 FROM turns t WHERE t.agent=c.target AND t.generation=c.generation
                   AND t.turn_id=c.turn_id AND (t.state<>'completed' OR t.outcome IS NOT 'success')))
             OR (?6=2 AND c.state IN ('accepted','queued'))
             OR (?6=3 AND c.state IN ('dispatched','unknown')))
         ORDER BY c.created_at DESC,c.rowid DESC LIMIT 1",
    ).map_err(sql_error)?;
    for (kind, heading) in [
        (1, "Delivered instruction with unfinished turn"),
        (0, "Latest accepted Owner or manager input"),
        (2, "Pending trusted review findings"),
        (3, "Unresolved input"),
    ] {
        let id: Option<String> = statement
            .query_row(
                params![
                    agent_num,
                    request.from_generation,
                    original,
                    principal,
                    self_prefix,
                    kind
                ],
                |row| row.get(0),
            )
            .optional()
            .map_err(sql_error)?;
        let Some(id) = id.filter(|id| !included.contains(id)) else {
            continue;
        };
        let command = load_command(tx, &id)?
            .ok_or_else(|| Error::new("source_integrity", "retained handoff input disappeared"))?;
        if kind == 3 {
            let instruction =
                TaskInstruction::from_command(&command, String::new()).ok_or_else(|| {
                    Error::new("source_integrity", "unresolved handoff input has no source")
                })?;
            rendered.push_str(&format!(
                "\n\n{heading}: {}",
                instruction_label(&instruction)
            ));
            included.push(id);
            continue;
        }
        let text = input_before_owner_rules(&command)
            .ok_or_else(|| Error::new("source_integrity", "retained handoff input has no text"))?;
        let mut text = policy.redact_text(text);
        if kind == 2 {
            let (excerpt, clipped) = bounded(&text, 8 * 1024);
            text = excerpt;
            truncated |= clipped;
        }
        let instruction = TaskInstruction::from_command(&command, text).ok_or_else(|| {
            Error::new("source_integrity", "retained handoff input has no source")
        })?;
        let delivery = if transferred.iter().any(|send| send.id == id) {
            "\nA separate successor SEND will deliver this input. Wait for that SEND before acting on it."
        } else if continues_task
            && kind == 0
            && matches!(
                command.state,
                CommandState::Accepted | CommandState::Queued | CommandState::Cancelled
            )
            && command.verb == "inject"
            && command.args["point"] == "next_input"
        {
            "\nThe source generation did not receive this accepted next-input instruction. The brief carries it forward for the same task. Follow it when continuing, subject to any later applicable instruction."
        } else {
            ""
        };
        rendered.push_str(&format!(
            "\n\n{heading}: {}{delivery}\n{}",
            instruction_label(&instruction),
            core::fence_untrusted_text(&instruction.text)
        ));
        included.push(id);
    }
    Ok((rendered, included, truncated))
}

fn later_instructions(
    tx: &Connection,
    request: &HandoffRequest,
    original: Option<&str>,
    inputs: Option<&TaskInputs>,
    policy: &core::SecretPolicy,
    input: BriefInput<'_>,
    priority: &[String],
) -> Result<LaterInstructions> {
    let BriefInput {
        transferred,
        input_principal,
        ..
    } = input;
    let mut out = LaterInstructions::default();
    let mut records = Vec::new();
    if let Some(inputs) = inputs {
        out.omitted = inputs
            .omitted
            .values()
            .copied()
            .fold(
                inputs.compacted.map_or(0, |compacted| compacted.count),
                usize::saturating_add,
            )
            // Older records did not identify the sources in this aggregate.
            .max(inputs.compacted_omissions);
        for record in inputs.corrections.iter().rev() {
            if input_principal.is_some_and(|principal| principal != record.principal)
                || priority.contains(&record.command_id)
                || transferred.iter().any(|send| send.id == record.command_id)
            {
                continue;
            }
            if records.len() >= MAX_LATER_INSTRUCTIONS {
                out.omitted = out.omitted.saturating_add(1);
                continue;
            }
            let mut rendered = instruction_label(record);
            if record.state == CommandState::Applied {
                let (excerpt, clipped) =
                    bounded(&policy.redact_text(&record.text), MAX_INSTRUCTION_BYTES);
                out.truncated |= clipped;
                rendered.push(' ');
                rendered.push_str(&excerpt);
            }
            records.push(rendered);
        }
    }
    records.reverse();

    let mut query = tx.prepare(
        "SELECT c.id, c.principal, c.verb, c.state, json_extract(c.args, '$.text'), COUNT(*) OVER ()
         FROM commands c WHERE c.target=?1 AND c.generation=?2
           AND c.id IS NOT ?3 AND c.verb IN ('send','steer','inject')
           AND (?4 = 0 OR c.principal <> 'owner')
           AND (?5 IS NULL OR c.principal = ?5)
           AND c.principal NOT LIKE 'swarm_note:%'
           AND c.state <> 'rejected'
           AND NOT (c.link_kind IS 'handoff' AND c.ordinal = 0)
           AND NOT EXISTS (SELECT 1 FROM commands p WHERE p.id=c.parent_command
                           AND json_type(p.args,'$.continue_from_stop')='text')
           AND NOT EXISTS (SELECT 1 FROM restart_resume_intents r
                           WHERE r.command_id=c.parent_command AND r.continuation_brief)
           AND json_type(c.args, '$.text') = 'text'
           AND NOT EXISTS (SELECT 1 FROM commands t WHERE t.id=?3 AND t.target=c.target
             AND t.generation=c.generation AND (c.created_at,c.rowid) <= (t.created_at,t.rowid))
           AND NOT EXISTS (SELECT 1 FROM writes w WHERE w.method='goal.round'
             AND json_extract(w.result, '$.continuation_id')=c.id)
         ORDER BY c.created_at DESC, c.rowid DESC LIMIT 256",
    ).map_err(sql_error)?;
    let mut rows = query
        .query(params![
            parse_agent_id(&request.from)?,
            request.from_generation,
            original,
            inputs.is_some(),
            input_principal,
        ])
        .map_err(sql_error)?;
    let mut current = Vec::new();
    let mut counted = false;
    while let Some(row) = rows.next().map_err(sql_error)? {
        if !counted {
            let total: i64 = row.get(5).map_err(sql_error)?;
            out.omitted = out.omitted.saturating_add(
                usize::try_from(total.saturating_sub(256).max(0)).unwrap_or(usize::MAX),
            );
            counted = true;
        }
        let id: String = row.get(0).map_err(sql_error)?;
        if priority.contains(&id)
            || transferred.iter().any(|send| send.id == id)
            || inputs.is_some_and(|inputs| {
                inputs
                    .corrections
                    .iter()
                    .any(|record| record.command_id == id)
            })
        {
            continue;
        }
        if records.len() + current.len() >= MAX_LATER_INSTRUCTIONS {
            out.omitted = out.omitted.saturating_add(1);
            continue;
        }
        let principal: String = row.get(1).map_err(sql_error)?;
        let verb: String = row.get(2).map_err(sql_error)?;
        let state: String = row.get(3).map_err(sql_error)?;
        let caution = delivery_caution(&state);
        let mut rendered = format!(
            "[command={id}, principal={principal}, verb={verb}, source={}/{}, state={state}{caution}]",
            request.from, request.from_generation
        );
        if state == "applied" {
            let text: String = row.get(4).map_err(sql_error)?;
            let (text, clipped) = bounded(&policy.redact_text(&text), MAX_INSTRUCTION_BYTES);
            out.truncated |= clipped;
            rendered.push(' ');
            rendered.push_str(&text);
        }
        current.push(rendered);
    }
    current.reverse();
    records.extend(current);
    out.records = records;
    Ok(out)
}

pub(super) fn has_unverified_instructions(tx: &Connection, agent: &str) -> Result<bool> {
    tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM commands WHERE target = ?1
           AND verb IN ('send', 'steer', 'inject') AND state IN ('unknown', 'dispatched')
           AND principal NOT LIKE 'swarm_note:%')",
        [parse_agent_id(agent)?],
        |row| row.get(0),
    )
    .map_err(sql_error)
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

fn children_line(tx: &Connection, agent: i64) -> Result<String> {
    const LIMIT: usize = 12;
    let mut statement = tx
        .prepare_cached(
            "SELECT id,generation,state FROM agents WHERE parent=?1 ORDER BY id LIMIT ?2",
        )
        .map_err(sql_error)?;
    let mut children = statement
        .query_map(params![agent, LIMIT as i64 + 1], |row| {
            Ok(format!(
                "a{}#{} ({})",
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, String>(2)?
            ))
        })
        .map_err(sql_error)?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(sql_error)?;
    if children.is_empty() {
        return Ok("- Children: none".to_owned());
    }
    if children.len() > LIMIT {
        children[LIMIT] = "more children omitted".to_owned();
    }
    Ok(format!("- Children: {}", children.join(", ")))
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
        || "- Managed worktree binding: none recorded".to_owned(),
        |(branch, base)| format!("- Managed worktree binding: {branch} (base {base})"),
    );
    let claims_line = open_claims_line(tx, agent_num, request.from_generation, now)?;
    let children_line = children_line(tx, agent_num)?;
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
        "{workspace_line}\n{branch_line}\n{goal_line}\n{children_line}\n{claims_line}\n{open_line}{turn_line}{earlier_line}"
    ))
}
