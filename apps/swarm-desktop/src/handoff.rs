//! Hand off / Switch: move an Agent's undispatched work to an existing Agent
//! (`swarm.handoff`) or to a new one (`swarm.switch`), then show what the
//! daemon did and offer only the Owner recovery operations it defines
//! (`command.resolve` for a held or failed brief, `command.cancel` for a
//! queued handoff input, and an ordinary handoff to finish a switch).
//!
//! Every submission is bound to the generations shown when it was built and
//! keeps one mutation id per action: the request (with its id) is retained,
//! so "Refresh" is the documented same-ID readback and never a new effect.
//! Nothing retries with a new id on its own.

use eliot_core::api::{
    HandoffRequest, HandoffResponse, MAX_HANDOFF_SENDS, SwitchLaunch, SwitchRequest, SwitchResponse,
};
use eliot_core::{Command, CommandState, Event};
use serde_json::Value;

/// One retained SEND admitted on the source Agent's bound generation.
#[derive(Clone, Debug, PartialEq)]
pub struct SendRow {
    pub id: String,
    pub state: CommandState,
    pub preview: String,
    /// Admission time (ms since the Unix epoch).
    pub created_at: i64,
}

impl SendRow {
    /// Only plain accepted/queued SENDs can move; the daemon rechecks.
    pub fn transferable(&self) -> bool {
        matches!(self.state, CommandState::Accepted | CommandState::Queued)
    }

    /// What the owner recognises: the text, how long ago, and where it is.
    /// The id stays out of the label (it is bound in the request).
    pub fn label(&self, now_ms: i64) -> String {
        format!(
            "\u{201c}{}\u{201d} · {} · {}",
            self.preview,
            age(now_ms, self.created_at),
            match self.state {
                CommandState::Accepted | CommandState::Queued => "waiting".to_owned(),
                state => state_name(state),
            }
        )
    }
}

/// A one-line preview of a message text: whitespace collapsed, at most
/// `max` characters.
pub fn preview(text: &str, max: usize) -> String {
    let flat: String = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if flat.chars().count() > max {
        format!("{}…", flat.chars().take(max).collect::<String>())
    } else {
        flat
    }
}

/// A coarse relative age: "just now", "4 min ago", "3 h ago", "2 d ago".
pub fn age(now_ms: i64, at_ms: i64) -> String {
    let seconds = (now_ms - at_ms).max(0) / 1000;
    match seconds {
        0..60 => "just now".to_owned(),
        60..3_600 => format!("{} min ago", seconds / 60),
        3_600..86_400 => format!("{} h ago", seconds / 3_600),
        _ => format!("{} d ago", seconds / 86_400),
    }
}

/// The current wall clock in ms since the Unix epoch (0 if unavailable).
pub fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_millis() as i64)
}

pub fn state_name(state: CommandState) -> String {
    format!("{state:?}").to_lowercase()
}

/// Admission events that name a command's verb: an ordinary SEND is
/// `command.accepted`, a launch-linked first SEND is `command.queued`.
pub const ADMISSION_KINDS: [&str; 2] = ["command.accepted", "command.queued"];

/// SEND ids admitted for `agent`, oldest first, from its retained admission
/// events. The generation filter uses the retained command (see
/// [`send_row`]), not the event.
pub fn send_ids(events: &[Event], agent: &str) -> Vec<String> {
    let mut ids: Vec<String> = Vec::new();
    for event in events {
        if ADMISSION_KINDS.contains(&event.kind.as_str())
            && event.agent.as_deref() == Some(agent)
            && event.data.get("verb").and_then(Value::as_str) == Some("send")
            && let Some(id) = event.data.get("command_id").and_then(Value::as_str)
            && !ids.iter().any(|known| known == id)
        {
            ids.push(id.to_owned());
        }
    }
    ids
}

/// The retained command as a row when it is a plain SEND (no request
/// binding, not a probe) bound to exactly `generation`.
pub fn send_row(command: &Command, generation: i64) -> Option<SendRow> {
    let bound = command.generation.or(command.expected_generation);
    if command.verb != "send"
        || command.request_id.is_some()
        || command.probe
        || bound != Some(generation)
    {
        return None;
    }
    let text = command
        .args
        .get("text")
        .and_then(Value::as_str)
        .unwrap_or_default();
    Some(SendRow {
        id: command.id.clone(),
        state: command.state,
        preview: preview(text, 80),
        created_at: command.created_at,
    })
}

/// The exact request last submitted (its id included), kept for same-ID
/// readback.
#[derive(Clone, Debug)]
pub enum Submitted {
    Handoff(HandoffRequest),
    // Boxed: the launch-carrying switch request is much larger than a handoff.
    Switch(Box<SwitchRequest>),
}

impl Submitted {
    pub fn id(&self) -> &str {
        match self {
            Self::Handoff(request) => &request.id,
            Self::Switch(request) => &request.id,
        }
    }

    /// Ledger key of the action: same source, destination and selection
    /// reuse one id; any difference is a new action.
    pub fn key(&self) -> String {
        match self {
            Self::Handoff(request) => format!(
                "handoff:{}:{}:{}:{}:{}:{:?}",
                request.from,
                request.from_generation,
                request.to,
                request.to_generation,
                request.task_command,
                request.commands
            ),
            Self::Switch(request) => format!(
                "switch:{}:{}:{}:{:?}:{}",
                request.from,
                request.from_generation,
                request.task_command,
                request.commands,
                launch_key(&request.launch)
            ),
        }
    }
}

/// Whether a reply applies to the dialog: it must answer the request shown
/// (`shown` is that request's id). The answer to a submission always applies,
/// even after the dialog was closed and reopened; a readback applies only
/// within the opening that started it, so closing ends its poll chain.
pub fn reply_applies(shown: &str, replied: &str, settles: bool, same_opening: bool) -> bool {
    shown == replied && (settles || same_opening)
}

/// Status suffix when older SENDs of the Agent were not read: the list is
/// the newest `limit` only, and the original task may be among the omitted.
pub fn omitted_note(omitted: usize, limit: usize) -> String {
    if omitted == 0 {
        String::new()
    } else {
        format!(" Only the newest {limit} SENDs were read; {omitted} older one(s) are not listed.")
    }
}

fn launch_key(launch: &SwitchLaunch) -> String {
    format!(
        "{:?}/{:?}/{:?}/{:?}",
        launch.profile, launch.harness, launch.model, launch.effort
    )
}

#[derive(Clone, Debug)]
pub enum Outcome {
    Handoff(HandoffResponse),
    Switch(SwitchResponse),
}

impl Outcome {
    /// The applied Handoff parent, directly or inside a switch.
    pub fn handoff(&self) -> Option<&HandoffResponse> {
        match self {
            Self::Handoff(reply) => Some(reply),
            Self::Switch(reply) => reply.handoff.as_ref(),
        }
    }

    /// A switch still orchestrating: read it back again with the same id.
    pub fn switch_pending(&self) -> bool {
        matches!(self, Self::Switch(reply)
            if matches!(reply.state, CommandState::Accepted | CommandState::Queued | CommandState::Dispatched))
    }

    /// The created B to finish with an ordinary handoff, when the daemon
    /// names that next action.
    pub fn finish_target(&self) -> Option<(String, i64)> {
        match self {
            // Only the ready form: `inspect_readiness_then_…` is not ready yet.
            Self::Switch(reply) if reply.next_action == "finish_with_ordinary_swarm.handoff" => {
                reply
                    .created
                    .as_ref()
                    .map(|created| (created.agent.clone(), created.generation))
            }
            _ => None,
        }
    }
}

/// One plain sentence for a definite reply; the facts with ids follow in
/// the readback details.
pub fn outcome_summary(outcome: &Outcome) -> String {
    let handoff = |reply: &HandoffResponse| {
        let brief = state_name(reply.brief_state);
        match reply.transfers.len() {
            0 => format!(
                "Handed off to {}: the brief is {brief}; no waiting SENDs moved.",
                reply.to
            ),
            count => format!(
                "Handed off to {}: the brief is {brief} and {count} waiting SEND(s) moved; {}.",
                reply.to,
                gate_meaning(&reply.successor_gate)
            ),
        }
    };
    match outcome {
        Outcome::Handoff(reply) => handoff(reply),
        Outcome::Switch(reply) => match (&reply.handoff, &reply.created, &reply.error) {
            (Some(done), _, _) => format!("Switched. {}", handoff(done)),
            (None, _, Some(error)) => format!("The switch stopped: {error}"),
            (None, Some(created), None) => format!(
                "Started {} (run {}); next: {}.",
                created.agent,
                created.generation,
                reply.next_action.replace('_', " ")
            ),
            (None, None, None) => format!(
                "The switch is {}; next: {}.",
                state_name(reply.state),
                reply.next_action.replace('_', " ")
            ),
        },
    }
}

/// What a successor gate value means.
pub fn gate_meaning(gate: &str) -> &'static str {
    match gate {
        "queued_until_brief_applied" => "successors wait until the brief is applied",
        "queued_brief_failed" => {
            "the brief failed, was rejected or was cancelled; successors stay queued"
        }
        "queued_brief_unknown" => "the brief outcome is unknown; successors stay queued",
        "released" => "successors may dispatch under the ordinary turn and FIFO gates",
        _ => "unrecognised gate",
    }
}

/// The applied transfer, fact by fact.
pub fn handoff_lines(reply: &HandoffResponse) -> Vec<String> {
    let mut lines = vec![format!(
        "Handoff {} applied (admission, not task completion): {} gen {} → {} gen {}",
        reply.id, reply.from, reply.from_generation, reply.to, reply.to_generation
    )];
    lines.push(format!(
        "Brief {} · {}{}",
        reply.brief_command,
        state_name(reply.brief_state),
        if reply.brief_truncated {
            " · truncated (labelled in the brief)"
        } else {
            ""
        }
    ));
    lines.push(format!(
        "Successor gate: {} — {}",
        reply.successor_gate,
        gate_meaning(&reply.successor_gate)
    ));
    if reply.transfers.is_empty() {
        lines.push("No SENDs selected: context, claims, Goal and children only.".to_owned());
    }
    for transfer in &reply.transfers {
        lines.push(format!(
            "{} cancelled on {} with no effect → successor {} on {}",
            transfer.original, reply.from, transfer.successor, reply.to
        ));
    }
    lines.push(if reply.claims.is_empty() {
        "Claims: none live on the source.".to_owned()
    } else {
        format!("Claims moved: {}", reply.claims.join(", "))
    });
    if !reply.claim_conflicts.is_empty() || reply.conflicts_truncated {
        lines.push(format!(
            "Advisory claim conflicts: {}{}",
            reply.claim_conflicts.len(),
            if reply.conflicts_truncated {
                " (more, truncated)"
            } else {
                ""
            }
        ));
    }
    lines.push(match reply.goal_revision {
        Some(revision) => format!("Goal moved paused to {} at revision {revision}.", reply.to),
        None => "Goal: none transferred.".to_owned(),
    });
    lines.push(if reply.reparented.is_empty() {
        "Children: none moved.".to_owned()
    } else {
        format!(
            "Children moved to {}: {}",
            reply.to,
            reply
                .reparented
                .iter()
                .map(|moved| format!("{} gen {}", moved.agent, moved.generation))
                .collect::<Vec<_>>()
                .join(", ")
        )
    });
    lines
}

/// The switch reservation or result, then its Handoff when admitted.
pub fn switch_lines(reply: &SwitchResponse) -> Vec<String> {
    let mut lines = vec![format!(
        "Switch {} · {} · next action: {}",
        reply.id,
        state_name(reply.state),
        reply.next_action.replace('_', " ")
    )];
    lines.push(format!(
        "Launch {} · {}{}",
        reply.launch_command,
        reply
            .launch_state
            .map(state_name)
            .unwrap_or_else(|| "not admitted yet".to_owned()),
        match &reply.created {
            Some(created) => format!(" · created {} gen {}", created.agent, created.generation),
            None => String::new(),
        }
    ));
    if let Some(error) = &reply.error {
        lines.push(format!("Refusal: {error}"));
    }
    match &reply.handoff {
        Some(handoff) => lines.extend(handoff_lines(handoff)),
        None => lines.push(format!("Handoff {} not admitted.", reply.handoff_command)),
    }
    lines
}

/// The submission, checked before it leaves: at most the handoff bound,
/// a task SEND, and the chosen order preserved.
pub fn check_selection(task: Option<&str>, chosen: &[String]) -> Result<(), String> {
    if task.is_none() {
        return Err("Pick the SEND that carries the original task.".to_owned());
    }
    if chosen.len() > MAX_HANDOFF_SENDS {
        return Err(format!(
            "At most {MAX_HANDOFF_SENDS} SENDs move in one handoff."
        ));
    }
    Ok(())
}
