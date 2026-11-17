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

#[cfg(test)]
mod tests {
    use super::*;
    use eliot_core::api::{SendTransfer, SwitchAgent};
    use serde_json::json;

    fn event(kind: &str, agent: &str, generation: i64, data: Value) -> Event {
        serde_json::from_value(json!({
            "seq": 1, "ts": 1, "kind": kind, "agent": agent, "generation": generation,
            "source": "daemon", "data": data
        }))
        .unwrap()
    }

    fn handoff() -> HandoffResponse {
        HandoffResponse {
            id: "h".into(),
            from: "a1".into(),
            from_generation: 1,
            to: "a2".into(),
            to_generation: 1,
            brief_command: "h/brief".into(),
            children: vec!["h/brief".into(), "h/1".into()],
            transfers: vec![SendTransfer {
                original: "s1".into(),
                successor: "h/1".into(),
            }],
            claims: vec!["src/**".into()],
            claim_conflicts: Vec::new(),
            conflicts_truncated: false,
            goal_revision: None,
            reparented: Vec::new(),
            brief_truncated: false,
            brief_state: CommandState::Queued,
            successor_gate: "queued_until_brief_applied".into(),
        }
    }

    #[test]
    fn sends_are_listed_in_order_and_filtered_by_their_retained_generation() {
        let events = vec![
            event(
                "command.queued",
                "a1",
                1,
                json!({"command_id": "l/send", "verb": "send"}),
            ),
            event(
                "turn.started",
                "a1",
                1,
                json!({"command_id": "x", "verb": "send"}),
            ),
            event(
                "command.accepted",
                "a1",
                1,
                json!({"command_id": "c1", "verb": "configure"}),
            ),
            event(
                "command.accepted",
                "a9",
                1,
                json!({"command_id": "s3", "verb": "send"}),
            ),
            event(
                "command.accepted",
                "a1",
                1,
                json!({"command_id": "s4", "verb": "send"}),
            ),
            event(
                "command.accepted",
                "a1",
                1,
                json!({"command_id": "l/send", "verb": "send"}),
            ),
        ];
        assert_eq!(send_ids(&events, "a1"), vec!["l/send", "s4"]);
        let command = |generation: Option<i64>, expected: Option<i64>| -> Command {
            serde_json::from_value(json!({
                "id": "s4", "principal": "owner", "method": "command.submit", "verb": "send",
                "target": "a1", "expected_generation": expected, "generation": generation,
                "args": {"text": "do  the
            thing"}, "state": "queued", "turn_id": null,
                "outcome": null, "created_at": 1, "updated_at": 1
            }))
            .unwrap()
        };
        let row = send_row(&command(Some(1), Some(1)), 1).unwrap();
        assert_eq!(row.preview, "do the thing");
        assert!(row.transferable());
        assert!(send_row(&command(Some(2), None), 1).is_none());
        assert!(send_row(&command(None, Some(1)), 1).is_some());
    }

    #[test]
    fn outcome_lines_name_every_transfer_fact() {
        let lines = handoff_lines(&handoff());
        let text = lines.join("\n");
        assert!(text.contains("s1 cancelled on a1 with no effect → successor h/1 on a2"));
        assert!(text.contains("Brief h/brief · queued"));
        assert!(text.contains("successors wait until the brief is applied"));
        assert!(text.contains("Claims moved: src/**"));
        assert!(text.contains("Goal: none transferred."));
        assert!(text.contains("Children: none moved."));
    }

    #[test]
    fn finish_is_offered_only_when_the_daemon_names_it() {
        let mut reply = SwitchResponse {
            id: "s".into(),
            from: "a1".into(),
            from_generation: 1,
            state: CommandState::Failed,
            launch_command: "s/launch".into(),
            handoff_command: "s/handoff".into(),
            launch_state: Some(CommandState::Applied),
            created: Some(SwitchAgent {
                agent: "a3".into(),
                generation: 1,
            }),
            handoff: None,
            error: Some("busy: source not quiescent".into()),
            next_action: "finish_with_ordinary_swarm.handoff".into(),
        };
        assert_eq!(
            Outcome::Switch(reply.clone()).finish_target(),
            Some(("a3".into(), 1))
        );
        assert!(!Outcome::Switch(reply.clone()).switch_pending());
        reply.next_action = "inspect_readiness_then_ordinary_swarm.handoff".into();
        assert_eq!(Outcome::Switch(reply.clone()).finish_target(), None);
        reply.next_action = "inspect_created_agent_stop_separately".into();
        assert_eq!(Outcome::Switch(reply.clone()).finish_target(), None);
        reply.state = CommandState::Dispatched;
        assert!(Outcome::Switch(reply).switch_pending());
    }

    #[test]
    fn selection_needs_a_task_and_stays_within_the_bound() {
        assert!(check_selection(None, &[]).is_err());
        assert!(check_selection(Some("t"), &[]).is_ok());
        let many: Vec<String> = (0..=MAX_HANDOFF_SENDS).map(|i| format!("s{i}")).collect();
        assert!(check_selection(Some("t"), &many).is_err());
    }

    #[test]
    fn replies_apply_to_the_request_they_answer() {
        // A submission's answer lands even after the dialog was closed.
        assert!(reply_applies("h1", "h1", true, false));
        // A readback from an earlier opening ends its poll chain.
        assert!(!reply_applies("h1", "h1", false, false));
        assert!(reply_applies("h1", "h1", false, true));
        // A late switch readback never overwrites the finish that replaced it.
        assert!(!reply_applies("finish", "switch", false, true));
        assert!(!reply_applies("finish", "switch", true, true));
    }

    #[test]
    fn a_truncated_send_list_says_so() {
        assert_eq!(omitted_note(0, 200), "");
        assert!(omitted_note(3, 200).contains("newest 200 SENDs"));
        assert!(omitted_note(3, 200).contains("3 older"));
    }

    #[test]
    fn send_labels_show_text_age_and_place_but_no_id() {
        let row = SendRow {
            id: "01ABCDEF/send".to_owned(),
            state: CommandState::Queued,
            preview: "do the thing".to_owned(),
            created_at: 1_000,
        };
        let label = row.label(1_000 + 5 * 60_000);
        assert_eq!(label, "\u{201c}do the thing\u{201d} · 5 min ago · waiting");
        assert!(!label.contains("01ABCDEF"));
        let applied = SendRow {
            state: CommandState::Applied,
            ..row
        };
        assert!(applied.label(1_000).ends_with("just now · applied"));
    }

    #[test]
    fn ages_and_previews_stay_short() {
        assert_eq!(age(0, 0), "just now");
        assert_eq!(age(59_999, 0), "just now");
        assert_eq!(age(60_000, 0), "1 min ago");
        assert_eq!(age(3 * 3_600_000, 0), "3 h ago");
        assert_eq!(age(2 * 86_400_000, 0), "2 d ago");
        // A clock behind the record never shows a negative age.
        assert_eq!(age(0, 10_000), "just now");
        assert_eq!(preview("  a\n b\tc  ", 10), "a b c");
        assert_eq!(preview("abcdef", 3), "abc…");
    }
}
