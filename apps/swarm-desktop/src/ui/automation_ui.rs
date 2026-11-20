//! The Automations page's state and actions. Each automation is one card
//! in plain words: a settings assignment (`[automation.assignments.*]`,
//! which the daemon turns into a launch timer and a completion rule) or a
//! rule/timer from a rules file (`rule.list`, read-only here). A card says
//! what it does, its state, its last and next run, and offers only what the
//! daemon supports: Run now (`timer.run`) for a timer, Pause/Resume (the
//! assignment's `enabled` key) and Edit for an assignment. Raw keys sit in a
//! collapsed "Technical details". The rendering and the settings writes are
//! in `settings_ui::automations`.

use super::*;
use crate::config_client::RuleWorker;
use crate::config_view::{duration_words, local_time, rule_text};
use eliot_core::CommandState;
use eliot_core::api::TimerStateView;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, Write};

#[derive(Clone, Debug)]
pub(super) enum AutomationMsg {
    /// The namespaces and the latest rule worker status.
    RulesLoaded(
        u64,
        Vec<(Option<String>, String, Fallible<RulePage>)>,
        RuleWorker,
    ),
    AutomationRefreshRequested,
    /// Show or hide one card's technical details (the card key).
    TechnicalToggled(String),
    /// Open the form: a new automation, or edit the named assignment.
    FormOpened(Option<String>),
    FormClosed,
    FormText(FormField, String),
    FormBusy(Option<f64>),
    /// The index picked in the form's role list.
    FormRole(Option<usize>),
    /// The index picked in the form's project list.
    FormWorkspace(Option<usize>),
    FormSaved,
    /// Remove the assignment the form edits.
    Removed(String),
    /// Resume (true) or pause an assignment.
    Enabled(String, bool),
    /// Run a timer once now: its project (None: global) and rule id.
    RunNow(Option<String>, String),
    RunFinished(TimerKey, RunOutcome),
    RunReadback(TimerKey),
}

/// The form's text fields.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum FormField {
    Name,
    Folder,
    Interval,
}

/// The short form that creates or edits one assignment.
#[derive(Clone, Debug, Default)]
pub(super) struct AssignmentForm {
    /// The assignment edited; `None` creates one.
    pub(super) editing: Option<String>,
    pub(super) name: String,
    pub(super) folder: String,
    pub(super) workspace: String,
    pub(super) role: String,
    pub(super) busy: u64,
    pub(super) interval: String,
    pub(super) error: Option<String>,
    pub(super) saving: bool,
}

/// What the last Run now of one timer did, in words.
#[derive(Clone, Debug)]
pub(super) struct RunLine {
    pub(super) text: String,
    pub(super) busy: bool,
    pub(super) error: bool,
}

#[derive(Clone, Debug)]
pub(super) enum RunOutcome {
    NoPendingRun,
    Complete(RunLine),
    Retry(RunLine),
    Readback(String),
}

/// Rules and timers read for the Automations page, and its local state.
#[derive(Default)]
pub(super) struct AutomationState {
    /// Replaced only when a read lands, never cleared by a refresh.
    pub(super) rules: Vec<RuleNamespace>,
    pub(super) global_rules_current: bool,
    pub(super) worker: RuleWorker,
    /// Tags rule loads; an older reply is dropped.
    pub(super) epoch: u64,
    /// Cards whose technical details are open.
    pub(super) technical: HashSet<String>,
    pub(super) form: Option<AssignmentForm>,
    /// The last Run now per timer (its run key).
    pub(super) runs: HashMap<TimerKey, RunLine>,
}

impl AutomationState {
    /// A rules read has landed (the global namespace is always in it), so
    /// a background re-read never turns the page back into "Reading…".
    pub(super) fn read(&self) -> bool {
        !self.rules.is_empty()
    }
}

/// One timer within the connected daemon's exact namespace.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(super) struct TimerKey(pub(super) Option<String>, pub(super) String);

impl TimerKey {
    fn card_key(&self) -> String {
        match &self.0 {
            Some(project) => format!("rule:project:{project}/{}", self.1),
            None => format!("rule:global/{}", self.1),
        }
    }

    fn pending_file(&self, data_dir: &Path) -> PathBuf {
        use sha2::{Digest, Sha256};
        // A tag distinguishes the Owner namespace from every project ID.
        let mut digest = Sha256::new();
        digest.update([u8::from(self.0.is_some())]);
        if let Some(project) = &self.0 {
            digest.update((project.len() as u64).to_le_bytes());
            digest.update(project);
        }
        digest.update(&self.1);
        let hash: String = digest
            .finalize()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        data_dir.join(format!("desktop-timer-run-{hash}"))
    }
}

/// A pending action belongs to one timer and survives closing the window.
/// The file stays locked through readback so two desktop instances cannot
/// replace an unresolved action or clear each other's receipts.
struct PendingTimerRun(File);

impl PendingTimerRun {
    fn open(data_dir: &Path, key: &TimerKey, create: bool) -> Fallible<Option<Self>> {
        let file = match OpenOptions::new()
            .read(true)
            .write(true)
            .create(create)
            .truncate(false)
            .open(key.pending_file(data_dir))
        {
            Err(error) if !create && error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(None);
            }
            result => {
                result.map_err(|error| format!("Run now: could not open its saved ID: {error}"))?
            }
        };
        file.try_lock()
            .map_err(|error| format!("Run now: could not lock its saved ID: {error}"))?;
        Ok(Some(Self(file)))
    }

    /// The command ID is the mutation ID, checked by TimerRunRequest.
    fn read_id(&mut self) -> Fallible<Option<(String, bool)>> {
        let mut id = String::new();
        (&mut self.0)
            .take(28)
            .read_to_string(&mut id)
            .map_err(|error| format!("Run now: could not read its saved ID: {error}"))?;
        if id.is_empty() {
            return Ok(None);
        }
        let admitted = id.ends_with('\n');
        if admitted {
            id.pop();
        }
        id.parse::<ulid::Ulid>()
            .map_err(|_| "Run now: its saved ID could not be verified.".to_owned())?;
        Ok(Some((id, admitted)))
    }

    fn start(&mut self) -> Fallible<String> {
        let id = client::new_mutation_id();
        self.0
            .write_all(id.as_bytes())
            .and_then(|()| self.0.sync_all())
            .map_err(|error| format!("Run now: could not save its ID: {error}"))?;
        Ok(id)
    }

    fn mark_admitted(&mut self) -> Fallible<()> {
        self.0
            .write_all(b"\n")
            .and_then(|()| self.0.sync_all())
            .map_err(|error| format!("Run now: could not save its admitted status: {error}"))
    }

    fn clear(&mut self) -> Fallible<()> {
        self.0
            .set_len(0)
            .and_then(|()| self.0.sync_all())
            .and_then(|()| self.0.rewind())
            .map_err(|error| format!("Run now: could not clear its saved ID: {error}"))
    }
}

/// An automation's state, as its card says it.
#[derive(Clone, Debug, PartialEq)]
pub(super) enum CardState {
    Running,
    /// Paused by the owner (an assignment's `enabled = false`).
    Paused,
    /// Off in its rules file (`enabled = false`).
    Off,
    /// The settings say on; the daemon has not loaded it yet.
    Starting,
    Unreadable,
    /// Paused by the daemon, with the reason in words.
    Failed(String),
}

impl CardState {
    pub(super) fn label(&self) -> String {
        match self {
            Self::Running => "Running".to_owned(),
            Self::Paused => "Paused".to_owned(),
            Self::Off => "Off".to_owned(),
            Self::Starting => "Starting…".to_owned(),
            Self::Unreadable => "Unknown".to_owned(),
            Self::Failed(_) => "Failed".to_owned(),
        }
    }

    /// Why it failed, for the line under the card's title.
    pub(super) fn reason(&self) -> Option<&str> {
        match self {
            Self::Failed(reason) => Some(reason),
            Self::Unreadable => Some("Swarm's automation worker status could not be read."),
            _ => None,
        }
    }

    /// The tone of the status dot.
    pub(super) fn tone(&self) -> crate::theme::StatusTone {
        use crate::theme::StatusTone;
        match self {
            Self::Running => StatusTone::Active,
            Self::Failed(_) => StatusTone::Failed,
            Self::Starting => StatusTone::Pending,
            Self::Paused | Self::Off | Self::Unreadable => StatusTone::Neutral,
        }
    }
}

/// One automation's card, in words.
#[derive(Clone, Debug, PartialEq)]
pub(super) struct Card {
    pub(super) key: String,
    pub(super) title: String,
    pub(super) sentence: String,
    pub(super) state: CardState,
    /// "Last checked 12:01:30 · Next check 12:02:00"; empty without a timer.
    pub(super) times: String,
    /// Labelled paths, each opened in Explorer.
    pub(super) paths: Vec<(&'static str, String)>,
    /// The timer Run now fires: its project and rule id.
    pub(super) run: Option<TimerKey>,
    /// The assignment's settings key and whether it is on (Pause/Resume,
    /// Edit).
    pub(super) assignment: Option<(String, bool)>,
    pub(super) technical: Vec<String>,
}

/// One settings assignment as read from the settings draft.
pub(super) struct AssignmentView {
    pub(super) key: String,
    pub(super) folder: String,
    pub(super) workspace: String,
    pub(super) role: String,
    pub(super) busy: Option<u64>,
    pub(super) interval: String,
    pub(super) enabled: bool,
}

/// What the cards are built from.
pub(super) struct CardInputs<'a> {
    pub(super) assignments: Vec<AssignmentView>,
    pub(super) worker: &'a RuleWorker,
    pub(super) namespaces: &'a [RuleNamespace],
    pub(super) global_rules_current: bool,
    /// Registered projects: (id, checkout, label).
    pub(super) projects: &'a [(String, String, String)],
    pub(super) data_dir: Option<&'a Path>,
}

/// An interval as settings write it ("30s", "5m", "1h": one number and
/// its unit, the daemon's humantime form) in milliseconds; `None` for
/// anything else.
pub(super) fn interval_ms(text: &str) -> Option<i64> {
    let text = text.trim();
    let digits = text.find(|c: char| !c.is_ascii_digit())?;
    let value: i64 = text[..digits].parse().ok()?;
    let unit = match &text[digits..] {
        "ms" => 1,
        "s" | "sec" | "secs" | "second" | "seconds" => 1_000,
        "m" | "min" | "mins" | "minute" | "minutes" => 60_000,
        "h" | "hr" | "hrs" | "hour" | "hours" => 3_600_000,
        _ => return None,
    };
    (value > 0).then(|| value.saturating_mul(unit))
}

/// A folder by its last two names (`agent-briefs\queue10`): enough to know
/// it; the full path is the card's link.
fn short_folder(path: &str) -> String {
    let shown = nav::display_path(path).trim_end_matches(['\\', '/']);
    let names: Vec<&str> = shown
        .split(['\\', '/'])
        .filter(|name| !name.is_empty())
        .collect();
    match names.as_slice() {
        [] => shown.to_owned(),
        [.., parent, name] if !parent.ends_with(':') => format!("{parent}\\{name}"),
        [.., name] => (*name).to_owned(),
    }
}

/// A routing role as a kind of task: "worker" is "Worker", "ui" is "UI".
fn role_words(role: &str) -> String {
    if role.chars().count() <= 2 {
        return role.to_uppercase();
    }
    let mut chars = role.chars();
    chars.next().map_or_else(String::new, |first| {
        first.to_uppercase().chain(chars).collect()
    })
}

/// The project a workspace belongs to, by its label; else the folder name.
fn project_words(workspace: &str, projects: &[(String, String, String)]) -> String {
    let key = nav::path_key(workspace);
    projects
        .iter()
        .find(|(_, checkout, _)| nav::path_key(checkout) == key)
        .map(|(_, _, label)| label.clone())
        .unwrap_or_else(|| short_folder(workspace))
}

/// The daemon's pause reason in words.
fn reason_words(reason: &str) -> String {
    match reason {
        "backlog_exceeded" => "too many events were waiting, so Swarm stopped it".to_owned(),
        other => other.replace('_', " "),
    }
}

/// "Every 30 s, take new briefs from agent-briefs\queue10 and start them as
/// Worker tasks in eliot-swarm-controller (at most 2 at once)."
pub(super) fn assignment_sentence(
    assignment: &AssignmentView,
    projects: &[(String, String, String)],
) -> String {
    let every = interval_ms(&assignment.interval)
        .map(duration_words)
        .unwrap_or_else(|| assignment.interval.clone());
    let folder = if assignment.folder.trim().is_empty() {
        "a folder not chosen yet".to_owned()
    } else {
        short_folder(&assignment.folder)
    };
    let at_most = assignment
        .busy
        .map_or_else(String::new, |busy| format!(" (at most {busy} at once)"));
    format!(
        "Every {every}, take new briefs from {folder} and start them as {} tasks in {}{at_most}.",
        role_words(&assignment.role),
        project_words(&assignment.workspace, projects),
    )
}

/// Last and next scheduled run of a timer, on this computer's clock. A
/// Run now is not a scheduled run and moves neither (docs/spec/automation.md).
fn times(timer: &TimerStateView, assignment: bool) -> String {
    let (last, next) = if assignment {
        ("Last scheduled check", "Next check")
    } else {
        ("Last scheduled run", "Next run")
    };
    let mut parts = vec![match timer.last_key {
        Some(at) => format!("{last} {}", local_time(at)),
        None => format!("{last}: not yet"),
    }];
    if timer.enabled
        && let Some(at) = timer.next_due_at
    {
        parts.push(format!("{next} {}", local_time(at)));
    }
    parts.join(" · ")
}

/// Every automation in scope as a card: the settings assignments first,
/// then the rules files' rules and timers.
pub(super) fn cards(inputs: &CardInputs<'_>) -> Vec<Card> {
    let global = inputs
        .namespaces
        .iter()
        .find(|namespace| namespace.project.is_none());
    let rules: &[RuleListEntry] = global
        .and_then(|namespace| namespace.rules.as_ref().ok())
        .map_or(&[], Vec::as_slice);
    let mut cards = Vec::new();
    for assignment in &inputs.assignments {
        let launch_id = format!("assignment-{}", assignment.key);
        let launch = rules.iter().find(|entry| entry.state.id == launch_id);
        let complete = rules
            .iter()
            .find(|entry| entry.state.id == format!("{launch_id}.complete"));
        let timer = launch.and_then(|entry| entry.timer.as_ref());
        let pause_confirmed = timer.map_or_else(
            || global.is_some_and(|namespace| !namespace.more && namespace.rules.is_ok()),
            |timer| !timer.enabled,
        );
        let state = match (assignment.enabled, launch) {
            (false, _) if inputs.global_rules_current && pause_confirmed => CardState::Paused,
            (true, Some(entry)) => match &entry.state.paused_reason {
                Some(reason) => CardState::Failed(reason_words(reason)),
                None if timer.is_some_and(|timer| timer.enabled) => CardState::Running,
                None => CardState::Starting,
            },
            _ => CardState::Starting,
        };
        let mut technical = vec![
            format!("Settings: [automation.assignments.{}]", assignment.key),
            format!(
                "folder = {} · workspace = {}",
                nav::display_path(&assignment.folder),
                nav::display_path(&assignment.workspace)
            ),
            format!(
                "role = {} · max_busy = {} · interval = {} · enabled = {}",
                assignment.role,
                assignment.busy.map_or("?".to_owned(), |busy| busy.to_string()),
                assignment.interval,
                assignment.enabled
            ),
            "Briefs start in filename order; Swarm keeps its place in the folder under this name, so a new brief must sort after the last one started.".to_owned(),
        ];
        for entry in launch.into_iter().chain(complete) {
            technical.push(format!(
                "Rule {} · revision {}",
                entry.state.id, entry.state.revision
            ));
        }
        cards.push(Card {
            key: format!("assignment:{}", assignment.key),
            title: assignment.key.clone(),
            sentence: assignment_sentence(assignment, inputs.projects),
            times: launch
                .and_then(|entry| entry.timer.as_ref())
                .map(|timer| times(timer, true))
                .unwrap_or_default(),
            paths: vec![
                ("Briefs from", assignment.folder.clone()),
                ("Works in", assignment.workspace.clone()),
            ],
            // A paused timer refuses a manual run: no button for it.
            run: (state == CardState::Running).then_some(TimerKey(None, launch_id)),
            assignment: Some((assignment.key.clone(), assignment.enabled)),
            state,
            technical,
        });
    }
    for namespace in inputs.namespaces {
        let Ok(rules) = &namespace.rules else {
            continue;
        };
        // The two rules a settings assignment generates are that
        // assignment's card. Their IDs are exact: the daemon refuses a rules
        // file that reuses one. Once the assignment is gone (a reload
        // pending) they show as rules.
        let generated = |id: &str| {
            namespace.project.is_none()
                && inputs.assignments.iter().any(|assignment| {
                    let launch = format!("assignment-{}", assignment.key);
                    id == launch || id == format!("{launch}.complete")
                })
        };
        let rest = rules
            .iter()
            .filter(|entry| entry.source_present && !generated(&entry.state.id));
        let project = namespace.project.as_deref().map(|id| {
            inputs
                .projects
                .iter()
                .find(|(project, _, _)| project == id)
                .map_or((id.to_owned(), None), |(_, checkout, label)| {
                    (label.clone(), Some(checkout.clone()))
                })
        });
        for entry in rest {
            let rule = &entry.state;
            // A removed assignment's rules until the daemon drops them: they
            // come from no rules file.
            let from_settings = entry.definition.pointer("/do/0/assignment").is_some()
                || entry
                    .definition
                    .pointer("/on/match/event.cause.rule")
                    .and_then(serde_json::Value::as_str)
                    .is_some_and(|cause| cause.starts_with("assignment-"));
            let text = rule_text(entry);
            let state = match &rule.paused_reason {
                Some(reason) => CardState::Failed(reason_words(reason)),
                None if rule.enabled => CardState::Running,
                None => CardState::Off,
            };
            let rules_folder = match &project {
                _ if from_settings => None,
                Some((_, Some(checkout))) => Some(Path::new(checkout).join(".eliot").join("rules")),
                Some((_, None)) => None,
                None => inputs.data_dir.map(|dir| dir.join("rules")),
            };
            let mut technical = vec![format!(
                "Rule {} · revision {} · event cursor {}",
                rule.id, rule.revision, rule.current_cursor
            )];
            if let Some(revision) = rule.retiring_revision {
                technical.push(format!("Retiring revision {revision}"));
            }
            if let Some(timer) = &entry.timer {
                technical.push(format!(
                    "Timer schedule revision {} · enabled = {}",
                    timer.schedule_revision, timer.enabled
                ));
            }
            if state == CardState::Off {
                technical.push("enabled = false in its rules file".to_owned());
            }
            let key = TimerKey(namespace.project.clone(), rule.id.clone());
            cards.push(Card {
                key: key.card_key(),
                title: match &project {
                    Some((label, _)) => format!("{} · {label}", rule.id),
                    None => rule.id.clone(),
                },
                sentence: match text.limit {
                    Some(limit) => format!("{} {limit}", text.sentence),
                    None => text.sentence,
                },
                times: entry
                    .timer
                    .as_ref()
                    .map(|timer| times(timer, false))
                    .unwrap_or_default(),
                paths: rules_folder
                    .map(|folder| ("Rules file in", folder.to_string_lossy().into_owned()))
                    .into_iter()
                    .collect(),
                run: (entry.timer.as_ref().is_some_and(|timer| timer.enabled)
                    && state == CardState::Running)
                    .then_some(key),
                assignment: None,
                state,
                technical,
            });
        }
    }
    // With the rule worker down no rule or timer fires, so no card may say
    // Running, and Run now waits until Swarm restarts.
    for card in &mut cards {
        card.state = match inputs.worker {
            RuleWorker::Running => continue,
            RuleWorker::Down(down) => CardState::Failed(format!(
                "Swarm's automation worker {down}; restart Swarm to run automations again."
            )),
            RuleWorker::Starting if card.state == CardState::Running => CardState::Starting,
            RuleWorker::Unreadable if card.state == CardState::Running => CardState::Unreadable,
            _ => continue,
        };
        card.run = None;
        card.times
            .truncate(card.times.find(" · ").unwrap_or(card.times.len()));
    }
    cards
}

/// The edit form whose assignment no longer has a card (another writer
/// removed it while it was open): render it after the cards so its retained
/// inputs and refusal stay visible.
pub(super) fn unmatched_editing_form<'a>(
    form: Option<&'a AssignmentForm>,
    cards: &'a [Card],
) -> Option<&'a AssignmentForm> {
    let form = form.filter(|form| form.editing.is_some())?;
    let editing = form.editing.as_ref()?;
    let matched = cards
        .iter()
        .any(|card| card.assignment.as_ref().map(|(key, _)| key) == Some(editing));
    (!matched).then_some(form)
}

/// A refused Run now in words. An assignment with no new brief or with its
/// most tasks already running refuses the manual run
/// (`automation::assignment::prepare`): that is an answer, not a failure.
pub(super) fn refusal_words(error: &eliot_core::Error) -> RunLine {
    let quiet = |text: &str| RunLine {
        text: text.to_owned(),
        busy: false,
        error: false,
    };
    match (error.code.as_str(), error.message.as_str()) {
        ("not_found", message) if message.contains("no next brief") => {
            quiet("Run now: nothing to start. There is no new brief in the folder.")
        }
        ("bad_state", message) if message.contains("max_busy") => {
            quiet("Run now: nothing to start. The most tasks at once are already running.")
        }
        (code, message) => RunLine {
            text: format!("Run now didn't run: {message} ({code})."),
            busy: false,
            error: true,
        },
    }
}

/// A settled Run now in words: what started, or why nothing did.
pub(super) fn run_words(parent: &Command, children: &[Command], assignment: bool) -> RunLine {
    let error_words = |command: &Command| {
        let outcome = command.outcome.as_ref();
        let field = |name: &str| {
            outcome
                .and_then(|value| value.get(name))
                .and_then(serde_json::Value::as_str)
        };
        match (field("message"), field("code")) {
            (Some(message), Some(code)) => format!("{message} ({code})"),
            (Some(message), None) => message.to_owned(),
            (None, Some(code)) => code.replace('_', " "),
            (None, None) => format!("{:?}", command.state).to_lowercase(),
        }
    };
    let line = |text: String, error: bool| RunLine {
        text,
        busy: false,
        error,
    };
    match parent.state {
        CommandState::Unknown => {
            return line(
                "Run now: the outcome is unknown. Swarm never runs it a second time on its own."
                    .to_owned(),
                true,
            );
        }
        CommandState::Cancelled => {
            return line("Run now: cancelled.".to_owned(), true);
        }
        CommandState::Rejected | CommandState::Failed => {
            return line(format!("Run now didn't run: {}", error_words(parent)), true);
        }
        _ => {}
    }
    let launches: Vec<&Command> = children
        .iter()
        .filter(|child| child.verb == "launch" && child.state == CommandState::Applied)
        .collect();
    if let Some(cancelled) = children
        .iter()
        .find(|child| child.state == CommandState::Cancelled)
    {
        return line(format!("Run now: {} was cancelled.", cancelled.verb), true);
    }
    if let Some(failed) = children.iter().find(|child| {
        matches!(
            child.state,
            CommandState::Rejected | CommandState::Failed | CommandState::Unknown
        )
    }) {
        return line(
            format!(
                "Run now: {} didn't start: {}",
                failed.verb,
                error_words(failed)
            ),
            true,
        );
    }
    if parent.state != CommandState::Applied
        || children
            .iter()
            .any(|child| child.state != CommandState::Applied)
    {
        return line("Run now: waiting for the result…".to_owned(), false);
    }
    match (launches.first(), children.is_empty()) {
        (Some(launch), _) => {
            let brief = launch
                .args
                .pointer("/assignment/brief")
                .and_then(serde_json::Value::as_str)
                .map(|brief| format!(" from {brief}"))
                .unwrap_or_default();
            let agent = launch
                .target
                .as_deref()
                .map(|agent| format!(" ({agent})"))
                .unwrap_or_default();
            line(format!("Run now: started a task{brief}{agent}."), false)
        }
        (None, true) if assignment => line(
            "Run now: nothing to start. No new brief in the folder, or the most tasks at once are already running."
                .to_owned(),
            false,
        ),
        (None, true) => line("Run now: it ran; there was nothing to do.".to_owned(), false),
        (None, false) => line(
            format!(
                "Run now: it ran {}.",
                eliot_core::count_words(children.len() as u64, "action", "actions")
            ),
            false,
        ),
    }
}

impl DesktopApp {
    pub(super) fn automation_update(
        &mut self,
        msg: AutomationMsg,
        context: &ComponentContext<Self>,
    ) {
        if self
            .automation
            .form
            .as_ref()
            .is_some_and(|form| form.saving)
            && matches!(
                &msg,
                AutomationMsg::FormOpened(_)
                    | AutomationMsg::FormClosed
                    | AutomationMsg::FormText(_, _)
                    | AutomationMsg::FormBusy(_)
                    | AutomationMsg::FormRole(_)
                    | AutomationMsg::FormWorkspace(_)
                    | AutomationMsg::FormSaved
                    | AutomationMsg::Removed(_)
                    | AutomationMsg::Enabled(_, _)
            )
        {
            return;
        }
        match msg {
            AutomationMsg::RulesLoaded(epoch, namespaces, worker) => {
                if epoch != self.automation.epoch {
                    return;
                }
                if !matches!(
                    (&self.automation.worker, &worker),
                    (RuleWorker::Down(_), RuleWorker::Unreadable)
                ) {
                    self.automation.worker = worker;
                }
                self.automation.global_rules_current = namespaces
                    .iter()
                    .any(|(project, _, page)| project.is_none() && page.is_ok());
                let previous = std::mem::take(&mut self.automation.rules);
                self.automation.rules = namespaces
                    .into_iter()
                    .map(|(project, label, page)| {
                        if page.is_err()
                            && let Some(saved) = previous
                                .iter()
                                .find(|row| row.project == project && row.rules.is_ok())
                        {
                            return saved.clone();
                        }
                        RuleNamespace {
                            project,
                            label,
                            more: page.as_ref().is_ok_and(|(_, more)| *more),
                            rules: page.map(|(rules, _)| rules),
                        }
                    })
                    .collect();
                if let Ok(data_dir) = &self.data_dir {
                    let pending: Vec<_> = self
                        .automation
                        .rules
                        .iter()
                        .flat_map(|namespace| {
                            namespace
                                .rules
                                .iter()
                                .flatten()
                                .filter(|entry| entry.timer.is_some())
                                .map(|entry| {
                                    TimerKey(namespace.project.clone(), entry.state.id.clone())
                                })
                        })
                        .filter(|key| {
                            !self.automation.runs.contains_key(key)
                                && key
                                    .pending_file(data_dir)
                                    .metadata()
                                    .is_ok_and(|file| file.len() > 0)
                        })
                        .collect();
                    for key in pending {
                        self.read_timer_run(key, false, context);
                    }
                }
            }
            AutomationMsg::AutomationRefreshRequested => self.load_rules(context),
            AutomationMsg::TechnicalToggled(key) => {
                if !self.automation.technical.remove(&key) {
                    self.automation.technical.insert(key);
                }
            }
            AutomationMsg::FormOpened(key) => self.open_assignment_form(key),
            AutomationMsg::FormClosed => self.automation.form = None,
            AutomationMsg::FormText(field, text) => {
                if let Some(form) = self.automation.form.as_mut() {
                    let slot = match field {
                        FormField::Name => &mut form.name,
                        FormField::Folder => &mut form.folder,
                        FormField::Interval => &mut form.interval,
                    };
                    // A box reports the text it already shows on creation.
                    if *slot != text {
                        *slot = text;
                        form.error = None;
                    }
                }
            }
            AutomationMsg::FormBusy(value) => {
                if let (Some(form), Some(value)) = (
                    self.automation.form.as_mut(),
                    value.filter(|value| value.is_finite() && *value >= 1.0),
                ) {
                    form.busy = value.round() as u64;
                }
            }
            AutomationMsg::FormRole(index) => {
                let roles = self.assignment_roles();
                if let (Some(form), Some(role)) = (
                    self.automation.form.as_mut(),
                    index.and_then(|index| roles.get(index)),
                ) {
                    form.role = role.clone();
                }
            }
            AutomationMsg::FormWorkspace(index) => {
                let workspaces = self.assignment_workspaces();
                if let (Some(form), Some((path, _))) = (
                    self.automation.form.as_mut(),
                    index.and_then(|index| workspaces.get(index)),
                ) {
                    form.workspace = path.clone();
                }
            }
            AutomationMsg::FormSaved => self.save_assignment_form(context),
            AutomationMsg::Removed(key) => {
                self.remove_assignment(&key, context);
                self.automation.form = None;
                self.reload_rules_soon(context);
            }
            AutomationMsg::Enabled(key, on) => {
                self.set_assignment_on(&key, on, context);
                self.reload_rules_soon(context);
            }
            AutomationMsg::RunNow(project, rule) => {
                let key = TimerKey(project, rule);
                if !self.submitting()
                    && !self.automation.runs.get(&key).is_some_and(|line| line.busy)
                {
                    self.read_timer_run(key, true, context);
                }
            }
            AutomationMsg::RunReadback(key) => self.read_timer_run(key, false, context),
            AutomationMsg::RunFinished(key, outcome) => match outcome {
                RunOutcome::NoPendingRun => {
                    self.automation.runs.remove(&key);
                }
                RunOutcome::Complete(line) => {
                    self.automation.runs.insert(key, line);
                    self.load_rules(context);
                }
                RunOutcome::Retry(line) => {
                    self.automation.runs.insert(key, line);
                }
                RunOutcome::Readback(text) => {
                    self.automation.runs.insert(
                        key.clone(),
                        RunLine {
                            text,
                            busy: true,
                            error: true,
                        },
                    );
                    let _task = context.spawn_background(move |cancel| {
                        let _ = feed::backoff(3_000, &|| cancel.is_cancelled());
                        Message::Automation(AutomationMsg::RunReadback(key))
                    });
                }
            },
        }
    }

    fn read_timer_run(&mut self, key: TimerKey, submit: bool, context: &ComponentContext<Self>) {
        let Ok(data_dir) = self.data_dir.clone() else {
            return;
        };
        self.automation
            .runs
            .entry(key.clone())
            .and_modify(|line| {
                if submit {
                    line.text = "Run now: starting…".to_owned();
                    line.error = false;
                }
                line.busy = true;
            })
            .or_insert_with(|| RunLine {
                text: if submit {
                    "Run now: starting…"
                } else {
                    "Run now: waiting for the result…"
                }
                .to_owned(),
                busy: true,
                error: false,
            });
        let _task = context.spawn_background(move |cancel| {
            let outcome = timer_run_action(&data_dir, &key, submit, &|| cancel.is_cancelled());
            Message::Automation(AutomationMsg::RunFinished(key, outcome))
        });
    }

    /// The daemon reloads settings-generated rules within its two-second
    /// reconciliation: read them again after that.
    pub(super) fn reload_rules_soon(&self, context: &ComponentContext<Self>) {
        let _task = context.spawn_background(|cancel| {
            let cancelled = || cancel.is_cancelled();
            let _ = feed::backoff(3_000, &cancelled);
            Message::Automation(AutomationMsg::AutomationRefreshRequested)
        });
    }

    pub(super) fn load_rules(&mut self, context: &ComponentContext<Self>) {
        let Ok(data_dir) = self.data_dir.clone() else {
            return;
        };
        let projects: Vec<String> = self
            .nav
            .projects
            .iter()
            .map(|project| project.id.clone())
            .collect();
        self.automation.epoch += 1;
        let epoch = self.automation.epoch;
        let _task = context.spawn_background(move |_| {
            let mut namespaces = Vec::new();
            let worker = client::block_on(async {
                namespaces.push((
                    None,
                    "Global rules".to_owned(),
                    config_client::list_rules(&data_dir, None)
                        .await
                        .map_err(|error| error.to_string()),
                ));
                for project in projects {
                    let page = config_client::list_rules(&data_dir, Some(project.clone()))
                        .await
                        .map_err(|error| error.to_string());
                    namespaces.push((Some(project.clone()), format!("Project {project}"), page));
                }
                config_client::rule_worker(&data_dir)
                    .await
                    .unwrap_or_default()
            });
            Message::Automation(AutomationMsg::RulesLoaded(epoch, namespaces, worker))
        });
    }
}

fn timer_run_action(
    data_dir: &Path,
    key: &TimerKey,
    submit: bool,
    cancelled: &dyn Fn() -> bool,
) -> RunOutcome {
    let retry = |text| {
        RunOutcome::Retry(RunLine {
            text,
            busy: false,
            error: true,
        })
    };
    let mut pending = match PendingTimerRun::open(data_dir, key, submit) {
        Ok(Some(pending)) => pending,
        Ok(None) => return RunOutcome::NoPendingRun,
        Err(error) if !submit => return RunOutcome::Readback(error),
        Err(error) => return retry(error),
    };
    let (id, admitted, retained) = match pending.read_id() {
        Ok(Some((id, admitted))) => (id, admitted, true),
        Ok(None) if !submit => return RunOutcome::NoPendingRun,
        Ok(None) => match pending.start() {
            Ok(id) => (id, false, false),
            Err(error) => return retry(error),
        },
        Err(error) => return RunOutcome::Readback(error),
    };
    let request = eliot_core::api::TimerRunRequest {
        id: id.clone(),
        project: key.0.clone(),
        rule: key.1.clone(),
    };
    let parent = if retained {
        match client::block_on(client::read_command(data_dir, &id)).and_then(|parent| {
            request.validate_response(&parent)?;
            Ok(parent)
        }) {
            Ok(parent) => Some(parent),
            Err(error) if error.code == "not_found" && !admitted => None,
            Err(error) => {
                return RunOutcome::Readback(format!("Run now: the result was not read: {error}"));
            }
        }
    } else {
        None
    };
    let parent = match parent {
        Some(parent) => parent,
        None if !submit => {
            return retry(
                "Run now: the outcome is unknown; Run now again reuses its ID.".to_owned(),
            );
        }
        None => match client::block_on(config_client::run_timer(
            data_dir,
            id,
            key.0.clone(),
            &key.1,
        )) {
            Ok(parent) => parent,
            Err(error) if crate::state::uncertain(&error) => {
                return retry(format!(
                    "Run now: the outcome is unknown; Run now again reuses its ID ({error})."
                ));
            }
            Err(error) => {
                return match pending.clear() {
                    Ok(()) => RunOutcome::Complete(refusal_words(&error)),
                    Err(error) => RunOutcome::Readback(error),
                };
            }
        },
    };
    if !admitted && let Err(error) = pending.mark_admitted() {
        return RunOutcome::Readback(error);
    }
    let assignment = key.0.is_none() && key.1.starts_with("assignment-");
    match settle_timer_run(data_dir, parent, cancelled) {
        Ok((parent, children)) => match pending.clear() {
            Ok(()) => RunOutcome::Complete(run_words(&parent, &children, assignment)),
            Err(error) => RunOutcome::Readback(error),
        },
        Err(error) => RunOutcome::Readback(format!("Run now: the result was not read: {error}")),
    }
}

/// Wait for a manual timer run and its actions to settle (at most two
/// minutes), then return the parent and its children.
fn settle_timer_run(
    data_dir: &Path,
    parent: Command,
    cancelled: &dyn Fn() -> bool,
) -> Fallible<(Command, Vec<Command>)> {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(120);
    let mut parent = parent;
    loop {
        if cancelled() {
            return Err("stopped waiting; the run goes on in Swarm".to_owned());
        }
        if parent.state == CommandState::Unknown {
            return Err("the run's outcome is unknown; waiting for resolution.".to_owned());
        }
        let open = if parent.state.is_terminal() {
            let children = client::block_on(client::command_children(data_dir, &parent.id))
                .map_err(|error| error.to_string())?;
            if children.iter().all(|child| child.state.is_terminal()) {
                return Ok((parent, children));
            }
            if children
                .iter()
                .any(|child| child.state == CommandState::Unknown)
            {
                return Err("an action's outcome is unknown; waiting for resolution.".to_owned());
            }
            Some(
                children
                    .into_iter()
                    .filter(|child| !child.state.is_terminal())
                    .map(|child| child.id)
                    .take(eliot_core::api::MAX_SWARM_WAIT_TARGETS)
                    .collect::<Vec<_>>(),
            )
        } else {
            None
        };
        if tokio::time::Instant::now() >= deadline {
            return Err(format!(
                "still {} after two minutes (command {})",
                format!("{:?}", parent.state).to_lowercase(),
                parent.id
            ));
        }
        match open {
            None => {
                if let Some(settled) = super::wait_slice(data_dir, &parent.id, deadline)? {
                    parent = settled;
                }
            }
            Some(open) => {
                let slice = (tokio::time::Instant::now() + super::SETTLE_SLICE).min(deadline);
                let settled = client::block_on(client::wait_commands(data_dir, open, slice))
                    .map_err(|error| error.to_string())?;
                if settled {
                    // A turn-bound action reads terminal by its turn first;
                    // let its own row settle before reading the children again.
                    feed::backoff(200, cancelled);
                }
            }
        }
    }
}
