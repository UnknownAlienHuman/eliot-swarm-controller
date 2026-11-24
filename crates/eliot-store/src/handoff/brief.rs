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
        now,
    } = input;
    let record_command = facts.and_then(|facts| facts.task_command.as_deref());
    let typed = facts.and_then(|facts| facts.task_inputs.original.as_ref());
    let selected = task.map(|task| task.id.as_str()).or(record_command);
    let prior_brief = task
        .map(|task| is_brief_tx(tx, &task.id))
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
    let (task_text, task_clipped) = if delivered_separately {
        (
            "(delivered once as a separate successor SEND)".to_owned(),
            false,
        )
    } else if let Some(text) = text {
        let text = policy.redact_text(text);
        if original_unknown {
            let (excerpt, clipped) = bounded(&text, 8 * 1024);
            (
                format!(
                    "(unresolved original input, quoted only; verify effects before continuing and never resend)\n{}",
                    core::fence_untrusted_text(&excerpt)
                ),
                clipped,
            )
        } else {
            (text, false)
        }
    } else if prior_brief {
        (
            "(previous handoff is retained history; verify effects before continuing)".to_owned(),
            false,
        )
    } else {
        ("(no original task text retained)".to_owned(), false)
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
    let later = later_instructions(tx, request, selected, transferred, inputs, policy)?;
    let mut history_note = if let Some(reference) =
        facts.and_then(|facts| facts.task_ref.as_deref())
    {
        format!(
            "\nExact retained Owner corrections: private work-truth Git record {reference}:instructions.json."
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
    let git = facts
        .and_then(|facts| facts.git.as_deref())
        .unwrap_or("[Git facts unavailable]");
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
    let mut budget = (MAX_TEXT_BYTES - task_text.len() - task_note.len()) / 4;
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
            "{header}\nSource: {source}/{generation}\nHandoff input status: {status}{uncertainty}\nOriginal task command: {command_ref}{original_origin}\nRetained history: agent.result id={source} generation={generation}; turn.get agent={source} generation={generation} turn_id={turn} (when retained). Retention gaps remain possible.{context}\n\nOriginal task:\n{task_text}{task_note}\n\nLater input (quoted history; follow applied Owner corrections in order):{history_note}\nMinimum omitted inputs: {omitted}. Earlier coverage incomplete: {history_incomplete}.\n{instructions}\n\nStore facts (recorded by Swarm):\n{store}\n\nGit facts (quoted data):\n{git}"
        );
        if validate_frame(request, &text).is_ok() {
            return Ok(BuiltBrief {
                text,
                truncated: task_clipped
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

fn later_instructions(
    tx: &Connection,
    request: &HandoffRequest,
    original: Option<&str>,
    transferred: &[Command],
    inputs: Option<&TaskInputs>,
    policy: &core::SecretPolicy,
) -> Result<LaterInstructions> {
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
            if transferred.iter().any(|send| send.id == record.command_id) {
                continue;
            }
            if records.len() >= MAX_LATER_INSTRUCTIONS {
                out.omitted = out.omitted.saturating_add(1);
                continue;
            }
            let mut rendered = instruction_label(record);
            if record.state == CommandState::Applied || unresolved(record.state) {
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
           AND c.principal NOT LIKE 'swarm_note:%'
           AND c.state <> 'rejected'
           AND NOT (c.link_kind IS 'handoff' AND c.ordinal = 0)
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
            inputs.is_some()
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
        if transferred.iter().any(|send| send.id == id) {
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
        if matches!(state.as_str(), "applied" | "unknown" | "dispatched") {
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
