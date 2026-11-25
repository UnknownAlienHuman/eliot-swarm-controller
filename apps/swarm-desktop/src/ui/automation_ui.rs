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
    /// The page's tabs, the rule editor, the rows' switches and menus.
    Rule(RuleMsg),
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
    /// Run now readbacks in a row that could not read the result: the next
    /// waits longer.
    readback_pace: live::Pace,
    /// The Scripts tab is shown.
    pub(super) scripts_tab: bool,
    /// The row whose editor shows (its card key).
    pub(super) open: Option<String>,
    pub(super) editor: Option<RuleEditor>,
    /// Each rule's last outcome and refusal (`rule.get`).
    pub(super) status: HashMap<TimerKey, Fallible<eliot_core::api::RuleShowView>>,
    status_seq: u64,
    /// Rows whose switch is being saved, and why one was refused.
    pub(super) toggling: HashSet<String>,
    pub(super) toggle_errors: HashMap<String, String>,
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
    /// On, but its last scheduled check started nothing, with why in words.
    Blocked(String),
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
            Self::Blocked(_) => "Needs attention".to_owned(),
        }
    }

    /// Whether its timer fires: Run now is offered and a down worker
    /// overrides it.
    pub(super) fn fires(&self) -> bool {
        matches!(self, Self::Running | Self::Blocked(_))
    }

    /// Why it failed, for the line under the card's title.
    pub(super) fn reason(&self) -> Option<&str> {
        match self {
            Self::Failed(reason) | Self::Blocked(reason) => Some(reason),
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
            Self::Blocked(_) => StatusTone::Attention,
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
    /// The rules-file rule it is, if it is one.
    pub(super) rule: Option<RuleRef>,
    /// The page's group it lists under.
    pub(super) group: AutoGroup,
    /// When it fires, in words ("A turn ends", "Every 5m").
    pub(super) trigger: String,
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
/// A rule id as a plain name: "check-after-turn" → "Check after turn".
fn plain_name(id: &str) -> String {
    let words = id.replace(['-', '_'], " ");
    let mut chars = words.chars();
    match chars.next() {
        Some(first) => first.to_uppercase().chain(chars).collect(),
        None => words,
    }
}

fn short_folder(path: &str) -> String {
    let shown = nav::display_path(path);
    let shown = shown.trim_end_matches(['\\', '/']);
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

fn assignment_outcome_label(code: &str) -> Option<&'static str> {
    match code {
        "not_found" => Some("no new brief"),
        "bad_state" | "capacity_unavailable" => Some("its tasks are at the limit"),
        "brief_settling" => Some("a brief is still settling"),
        "brief_requires_include" => {
            Some("this pending legacy brief needs timer run --include NAME")
        }
        _ => None,
    }
}

/// Last and next scheduled run of a timer, on this computer's clock. A
/// Run now is not a scheduled run and moves neither (docs/spec/automation.md).
fn times(timer: &TimerStateView, assignment: bool) -> String {
    let (last, next) = if assignment {
        ("Last scheduled check", "Next check")
    } else {
        ("Last scheduled run", "Next run")
    };
    let note = match timer.last_outcome.as_deref() {
        Some(code) if assignment => assignment_outcome_label(code)
            .map(|label| format!(" ({label})"))
            .unwrap_or_else(|| " (started nothing)".to_owned()),
        Some(_) => " (started nothing)".to_owned(),
        None => String::new(),
    };
    let mut parts = vec![match timer.last_key {
        Some(at) => format!("{last} {}{note}", local_time(at)),
        None => format!("{last}: not yet{note}"),
    }];
    if timer.enabled
        && let Some(at) = timer.next_due_at
    {
        parts.push(format!("{next} {}", local_time(at)));
    }
    parts.join(" · ")
}

/// A firing card: Running while an assignment brief settles, or Blocked when
/// its last scheduled check produced nothing for a reason the Owner must act on.
/// No new brief, or an assignment already at its limit, is normal.
fn firing(timer: Option<&TimerStateView>, assignment: bool) -> CardState {
    let Some(code) = timer.and_then(|timer| timer.last_outcome.as_deref()) else {
        return CardState::Running;
    };
    if assignment
        && matches!(
            code,
            "not_found" | "bad_state" | "brief_settling" | "capacity_unavailable"
        )
    {
        return CardState::Running;
    }
    let what = if assignment {
        assignment_outcome_label(code).map(str::to_owned)
    } else {
        None
    }
    .unwrap_or_else(|| match code {
        "routing_unavailable" => {
            "no model could take the task. Pick one for this kind of task in Routing, or check Usage".to_owned()
        }
        "unavailable" if assignment => "the briefs folder could not be read".to_owned(),
        code => eliot_core::reason_words(code).to_lowercase(),
    });
    CardState::Blocked(format!("Its last check started nothing: {what}."))
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
                None if timer.is_some_and(|timer| timer.enabled) => firing(timer, true),
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
            run: state.fires().then_some(TimerKey(None, launch_id)),
            assignment: Some((assignment.key.clone(), assignment.enabled)),
            state,
            technical,
            rule: None,
            group: AutoGroup::Schedules,
            trigger: format!(
                "Every {} · briefs from {}",
                assignment.interval,
                short_folder(&assignment.folder)
            ),
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
                None if rule.enabled => firing(entry.timer.as_ref(), false),
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
            let name = plain_name(&rule.id);
            cards.push(Card {
                key: key.card_key(),
                title: match &project {
                    Some((label, _)) => format!("{name} · {label}"),
                    None => name,
                },
                rule: Some(RuleRef {
                    key: key.clone(),
                    file: entry.source_file.clone(),
                    enabled: rule.enabled,
                }),
                group: AutoGroup::of(&entry.definition, entry.timer.is_some()),
                trigger: trigger_words(&entry.definition),
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
                run: (entry.timer.as_ref().is_some_and(|timer| timer.enabled) && state.fires())
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
            RuleWorker::Starting if card.state.fires() => CardState::Starting,
            RuleWorker::Unreadable if card.state.fires() => CardState::Unreadable,
            _ => continue,
        };
        card.run = None;
        card.times
            .truncate(card.times.find(" · ").unwrap_or(card.times.len()));
    }
    cards
}

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
        ("bad_state" | "capacity_unavailable", message) if message.contains("max_busy") => {
            quiet("Run now: nothing to start. The most tasks at once are already running.")
        }
        ("brief_settling", _) => quiet("Run now: a brief is still settling."),
        ("capacity_unavailable", _) => quiet("Run now: capacity is currently unavailable."),
        ("brief_requires_include", _) => {
            quiet("Run now: this pending legacy brief needs timer run --include NAME.")
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
            AutomationMsg::Rule(msg) => self.rule_update(msg, context),
            AutomationMsg::RulesLoaded(epoch, namespaces, worker) => {
                if epoch != self.automation.epoch {
                    return;
                }
                let read_status = self.main == Main::Automations;
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
                if read_status {
                    self.read_rule_status(context);
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
            AutomationMsg::RunFinished(key, outcome) => {
                let read = !matches!(outcome, RunOutcome::Readback(_));
                let wait = self.automation.readback_pace.next(read, RUN_READBACK_MS);
                self.run_finished(key, outcome, wait, context);
            }
        }
    }

    /// A Run now's outcome; a readback that failed reads again `wait` ms
    /// later.
    fn run_finished(
        &mut self,
        key: TimerKey,
        outcome: RunOutcome,
        wait: u64,
        context: &ComponentContext<Self>,
    ) {
        match outcome {
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
                    let _ = feed::backoff(wait, &|| cancel.is_cancelled());
                    Message::Automation(AutomationMsg::RunReadback(key))
                });
            }
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
        include: None,
    };
    let parent = if retained {
        match client::block_on(client::read_command(data_dir, &id)).and_then(|parent| {
            request
                .validate_response(&parent)
                .map_err(|error| client::invalid_reply("timer.run", &error.message))?;
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

/// The Automations page's groups (`Automations.dc.html`), in order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum AutoGroup {
    /// A check after a turn, a PR check, a red main: a `run` action.
    Checks,
    /// Everything that answers an event otherwise.
    Reactions,
    /// Merge the branch and check: a Git action.
    AfterWork,
    /// Timers and brief queues.
    Schedules,
}

impl AutoGroup {
    pub(super) const ALL: [Self; 4] = [
        Self::Checks,
        Self::Reactions,
        Self::AfterWork,
        Self::Schedules,
    ];

    pub(super) fn title(self) -> &'static str {
        match self {
            Self::Checks => "Checks",
            Self::Reactions => "Reactions",
            Self::AfterWork => "After work",
            Self::Schedules => "Schedules",
        }
    }

    /// The group of a rule's definition.
    fn of(definition: &serde_json::Value, timer: bool) -> Self {
        let actions: Vec<&str> = definition
            .get("do")
            .and_then(serde_json::Value::as_array)
            .map(|actions| {
                actions
                    .iter()
                    .filter_map(|action| action.get("action").and_then(serde_json::Value::as_str))
                    .collect()
            })
            .unwrap_or_default();
        if timer {
            Self::Schedules
        } else if actions.iter().any(|action| action.starts_with("git.")) {
            Self::AfterWork
        } else if actions.contains(&"run") {
            Self::Checks
        } else {
            Self::Reactions
        }
    }
}

/// A rule from a rules file, as its row and editor reach it.
#[derive(Clone, Debug, PartialEq)]
pub(super) struct RuleRef {
    pub(super) key: TimerKey,
    /// Its top-level rules file; `None` for a rule Swarm generates from
    /// the settings (a monitoring reaction, an assignment's rules).
    pub(super) file: Option<String>,
    pub(super) enabled: bool,
}

/// The events the sentence editor names in words; any other event kind is
/// "Another event…" with its kind typed.
pub(super) const EVENTS: [(&str, &str); 7] = [
    ("turn.completed", "A turn ends"),
    ("request.opened", "An agent asks for approval"),
    ("command.applied", "A command is applied"),
    ("command.failed", "A command fails"),
    ("project.check.passed", "The project check passes"),
    ("hook.before_tool", "A tool is about to run"),
    ("x.user.agent.attention", "A task needs attention"),
];

/// The When choices after the named events.
/// A Run now whose result was not read reads it again after this long
/// (longer after repeated failures, see [`live::Pace`]).
const RUN_READBACK_MS: u64 = 3_000;
const OTHER_EVENT: &str = "Another event…";
const EVERY: &str = "Every…";
const CRON: &str = "On a schedule (cron)…";
const ONCE: &str = "Once at…";

/// The actions the sentence editor names in words (the connected rule
/// actions of `docs/spec/automation.md`).
pub(super) const ACTIONS: [(&str, &str); 10] = [
    ("send", "Send a message"),
    ("steer", "Steer the running turn"),
    ("inject", "Add a note at the next safe point"),
    ("run", "Run a command"),
    ("launch", "Start a task"),
    ("stop", "Stop the Agent"),
    ("resume", "Resume the Agent"),
    ("mail.send", "Send mail"),
    ("var.set", "Set a variable"),
    ("git.integrate", "Merge the branch and check"),
];

/// What a rule's When is.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) enum WhenKind {
    #[default]
    Event,
    Every,
    Cron,
    Once,
}

impl WhenKind {
    fn key(self) -> &'static str {
        match self {
            Self::Event => "event",
            Self::Every => "every",
            Self::Cron => "cron",
            Self::Once => "at",
        }
    }
}

/// The editor's fields that can carry a check's error.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum EditorField {
    Name,
    When,
    OnlyIf,
    Do,
    Repeat,
}

/// One rule as the sentence says it: When · Only if · Do · Repeat.
#[derive(Clone, Debug, Default, PartialEq)]
pub(super) struct Sentence {
    pub(super) when: WhenKind,
    /// The event kind, the interval ("30m"), the cron line or the instant.
    pub(super) when_value: String,
    /// A cron line's time zone (UTC when empty).
    pub(super) zone: String,
    /// `on.match` as the inside of a TOML inline table.
    pub(super) only_if: String,
    /// The first action's verb, and its text when it carries one.
    pub(super) action: String,
    pub(super) text: Option<String>,
    /// More actions after the first (edited as TOML).
    pub(super) more_actions: usize,
    /// `limit.max`, when the rule has a limit, and the limit's other words.
    pub(super) repeat: Option<i64>,
    pub(super) limit_words: Option<String>,
}

impl Sentence {
    /// The When choice's index in [`when_labels`].
    pub(super) fn when_index(&self) -> usize {
        match self.when {
            WhenKind::Event => EVENTS
                .iter()
                .position(|(kind, _)| *kind == self.when_value)
                .unwrap_or(EVENTS.len()),
            WhenKind::Every => EVENTS.len() + 1,
            WhenKind::Cron => EVENTS.len() + 2,
            WhenKind::Once => EVENTS.len() + 3,
        }
    }

    /// Whether the When value is typed (another event, a timer).
    pub(super) fn when_typed(&self) -> bool {
        self.when_index() >= EVENTS.len()
    }

    /// The action choice's index in [`action_labels`] (the rule's own verb
    /// is listed after the named ones when it is another).
    pub(super) fn action_index(&self) -> usize {
        ACTIONS
            .iter()
            .position(|(verb, _)| *verb == self.action)
            .unwrap_or(ACTIONS.len())
    }
}

/// The When dropdown's labels.
pub(super) fn when_labels() -> Vec<String> {
    EVENTS
        .iter()
        .map(|(_, words)| (*words).to_owned())
        .chain([OTHER_EVENT, EVERY, CRON, ONCE].map(str::to_owned))
        .collect()
}

/// The Do dropdown's labels: the named actions, then the rule's own verb
/// when it is another.
pub(super) fn action_labels(sentence: &Sentence) -> Vec<String> {
    let mut labels: Vec<String> = ACTIONS
        .iter()
        .map(|(_, words)| (*words).to_owned())
        .collect();
    if sentence.action_index() == ACTIONS.len() {
        labels.push(sentence.action.clone());
    }
    labels
}

/// A rule's trigger in words, for its row ("A turn ends", "Every 5m").
pub(super) fn trigger_words(definition: &serde_json::Value) -> String {
    let on = definition.get("on");
    let text = |key: &str| {
        on.and_then(|on| on.get(key))
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned)
    };
    if let Some(event) = text("event") {
        return EVENTS
            .iter()
            .find(|(kind, _)| *kind == event)
            .map_or_else(|| format!("On {event}"), |(_, words)| (*words).to_owned());
    }
    if let Some(every) = text("every") {
        return format!("Every {every}");
    }
    if let Some(cron) = text("cron") {
        return match text("tz") {
            Some(zone) => format!("At {cron} ({zone})"),
            None => format!("At {cron} (UTC)"),
        };
    }
    if let Some(at) = text("at") {
        return format!("Once at {at}");
    }
    "When it is run".to_owned()
}

/// The `[[rule]]` table with `id` in a rules file.
fn rule_table<'a>(
    doc: &'a mut toml_edit::DocumentMut,
    id: &str,
) -> Result<&'a mut toml_edit::Table, String> {
    let rules = doc
        .get_mut("rule")
        .and_then(toml_edit::Item::as_array_of_tables_mut)
        .ok_or_else(|| "This file has no [[rule]] tables. Edit it as TOML.".to_owned())?;
    rules
        .iter_mut()
        .find(|table| table.get("id").and_then(toml_edit::Item::as_str) == Some(id))
        .ok_or_else(|| format!("Rule {id} is not in this file any more. Read it again."))
}

fn parse(content: &str) -> Result<toml_edit::DocumentMut, String> {
    content
        .parse::<toml_edit::DocumentMut>()
        .map_err(|error| format!("The file does not parse: {error}"))
}

/// The sentence of rule `id` in a rules file's text.
pub(super) fn read_sentence(content: &str, id: &str) -> Result<Sentence, String> {
    let mut doc = parse(content)?;
    let table = rule_table(&mut doc, id)?;
    let mut sentence = Sentence::default();
    if let Some(on) = table.get("on").and_then(toml_edit::Item::as_table_like) {
        let text = |key: &str| {
            on.get(key)
                .and_then(toml_edit::Item::as_str)
                .map(str::to_owned)
        };
        for kind in [
            WhenKind::Event,
            WhenKind::Every,
            WhenKind::Cron,
            WhenKind::Once,
        ] {
            if let Some(value) = text(kind.key()) {
                sentence.when = kind;
                sentence.when_value = value;
                break;
            }
        }
        sentence.zone = text("tz").unwrap_or_default();
        if let Some(conditions) = on.get("match").and_then(toml_edit::Item::as_table_like) {
            sentence.only_if = conditions
                .iter()
                .map(|(key, item)| {
                    let value = item
                        .as_value()
                        .map(|value| value.to_string().trim().to_owned())
                        .unwrap_or_default();
                    format!("{} = {value}", toml_edit::Key::new(key).display_repr())
                })
                .collect::<Vec<_>>()
                .join(", ");
        }
    }
    if let Some(actions) = table.get("do").and_then(toml_edit::Item::as_array) {
        if let Some(first) = actions.get(0).and_then(toml_edit::Value::as_inline_table) {
            sentence.action = first
                .get("action")
                .and_then(toml_edit::Value::as_str)
                .unwrap_or_default()
                .to_owned();
            sentence.text = first
                .get("text")
                .and_then(toml_edit::Value::as_str)
                .map(str::to_owned);
        }
        sentence.more_actions = actions.len().saturating_sub(1);
    }
    if let Some(limit) = table.get("limit").and_then(toml_edit::Item::as_table_like) {
        sentence.repeat = limit.get("max").and_then(toml_edit::Item::as_integer);
        let words: Vec<String> = ["per", "window"]
            .into_iter()
            .filter_map(|key| {
                limit
                    .get(key)
                    .and_then(toml_edit::Item::as_str)
                    .map(|value| format!("{key} {value}"))
            })
            .collect();
        sentence.limit_words = Some(words.join(" · "));
    }
    Ok(sentence)
}

/// The sentence's own checks, before the daemon's: which field is wrong.
fn check(sentence: &Sentence) -> Result<(), (EditorField, String)> {
    if sentence.when_value.trim().is_empty() {
        return Err((
            EditorField::When,
            match sentence.when {
                WhenKind::Event => "Name the event, for example turn.completed.",
                WhenKind::Every => "Say how often, for example 30m or 1h.",
                WhenKind::Cron => "Write five cron fields, for example 0 9 * * 1-5.",
                WhenKind::Once => "Write the instant, for example 2026-12-01T09:00:00Z.",
            }
            .to_owned(),
        ));
    }
    if sentence.action.trim().is_empty() {
        return Err((EditorField::Do, "Choose what the rule does.".to_owned()));
    }
    if sentence.repeat.is_some_and(|max| max < 1) {
        return Err((EditorField::Repeat, "Repeat at least once.".to_owned()));
    }
    Ok(())
}

/// The `match` inline table the Only if text writes.
fn conditions(only_if: &str) -> Result<Option<toml_edit::InlineTable>, (EditorField, String)> {
    if only_if.trim().is_empty() {
        return Ok(None);
    }
    let doc = format!("x = {{ {only_if} }}")
        .parse::<toml_edit::DocumentMut>()
        .map_err(|error| {
            (
                EditorField::OnlyIf,
                format!(
                    "Write conditions as key = value, separated by commas ({}).",
                    error.message()
                ),
            )
        })?;
    doc.get("x")
        .and_then(toml_edit::Item::as_inline_table)
        .cloned()
        .map(Some)
        .ok_or_else(|| {
            (
                EditorField::OnlyIf,
                "Write conditions as key = value.".to_owned(),
            )
        })
}

/// Rule `id` in `content` with the sentence written into it: its trigger,
/// conditions, first action and repeat limit; every other key stays as it
/// was. Field errors name the field.
pub(super) fn write_sentence(
    content: &str,
    id: &str,
    sentence: &Sentence,
) -> Result<String, (EditorField, String)> {
    check(sentence)?;
    let conditions = conditions(&sentence.only_if)?;
    let mut doc = parse(content).map_err(|error| (EditorField::Name, error))?;
    let table = rule_table(&mut doc, id).map_err(|error| (EditorField::Name, error))?;
    write_into(table, sentence, conditions)?;
    Ok(doc.to_string())
}

fn write_into(
    table: &mut toml_edit::Table,
    sentence: &Sentence,
    conditions: Option<toml_edit::InlineTable>,
) -> Result<(), (EditorField, String)> {
    if table.get("on").is_none() {
        table.insert("on", toml_edit::value(toml_edit::InlineTable::new()));
    }
    let on = table
        .get_mut("on")
        .and_then(toml_edit::Item::as_table_like_mut)
        .ok_or_else(|| {
            (
                EditorField::When,
                "Its trigger is not a table. Edit it as TOML.".to_owned(),
            )
        })?;
    for kind in ["event", "every", "cron", "at"] {
        if kind != sentence.when.key() {
            on.remove(kind);
        }
    }
    on.insert(
        sentence.when.key(),
        toml_edit::value(sentence.when_value.trim()),
    );
    if sentence.when == WhenKind::Cron && !sentence.zone.trim().is_empty() {
        on.insert("tz", toml_edit::value(sentence.zone.trim()));
    } else {
        on.remove("tz");
    }
    match conditions {
        Some(conditions) => {
            on.insert("match", toml_edit::value(conditions));
        }
        None => {
            on.remove("match");
        }
    }
    if table.get("do").is_none() {
        let mut actions = toml_edit::Array::new();
        actions.push(toml_edit::InlineTable::new());
        table.insert("do", toml_edit::value(actions));
    }
    let first = table
        .get_mut("do")
        .and_then(toml_edit::Item::as_array_mut)
        .and_then(|actions| actions.get_mut(0))
        .and_then(toml_edit::Value::as_inline_table_mut)
        .ok_or_else(|| {
            (
                EditorField::Do,
                "Its actions are not a list. Edit it as TOML.".to_owned(),
            )
        })?;
    first.insert("action", sentence.action.trim().into());
    match &sentence.text {
        Some(text) => {
            first.insert("text", text.as_str().into());
        }
        None => {
            first.remove("text");
        }
    }
    if let Some(max) = sentence.repeat {
        let limit = table
            .get_mut("limit")
            .and_then(toml_edit::Item::as_table_like_mut)
            .ok_or_else(|| {
                (
                    EditorField::Repeat,
                    "This rule has no limit. Edit it as TOML to add one.".to_owned(),
                )
            })?;
        limit.insert("max", toml_edit::value(max));
    }
    Ok(())
}

/// A new rules file holding one rule.
pub(super) fn new_rule_file(
    id: &str,
    sentence: &Sentence,
) -> Result<String, (EditorField, String)> {
    let valid = !id.is_empty()
        && id.len() <= 128
        && id
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.'));
    if !valid {
        return Err((
            EditorField::Name,
            "Use letters, digits, -, _ and . for the name.".to_owned(),
        ));
    }
    check(sentence)?;
    let conditions = conditions(&sentence.only_if)?;
    let mut table = toml_edit::Table::new();
    table.insert("id", toml_edit::value(id));
    write_into(&mut table, sentence, conditions)?;
    let mut rules = toml_edit::ArrayOfTables::new();
    rules.push(table);
    let mut doc = toml_edit::DocumentMut::new();
    doc.insert("rule", toml_edit::Item::ArrayOfTables(rules));
    Ok(doc.to_string())
}

/// Rule `id` in `content` switched on or off (its `enabled` key).
pub(super) fn write_enabled(content: &str, id: &str, on: bool) -> Result<String, String> {
    let mut doc = parse(content)?;
    let table = rule_table(&mut doc, id)?;
    table.insert("enabled", toml_edit::value(on));
    Ok(doc.to_string())
}

/// `content` without rule `id`; `None` when it was the file's only rule
/// (the file is removed instead).
pub(super) fn without_rule(content: &str, id: &str) -> Result<Option<String>, String> {
    let mut doc = parse(content)?;
    let rules = doc
        .get_mut("rule")
        .and_then(toml_edit::Item::as_array_of_tables_mut)
        .ok_or_else(|| "This file has no [[rule]] tables. Edit it as TOML.".to_owned())?;
    let before = rules.len();
    rules.retain(|table| table.get("id").and_then(toml_edit::Item::as_str) != Some(id));
    if rules.len() == before {
        return Err(format!(
            "Rule {id} is not in this file any more. Read it again."
        ));
    }
    let empty = rules.is_empty() && doc.iter().count() == 1;
    Ok((!empty).then(|| doc.to_string()))
}

/// The open rule editor (`Automations.dc.html`'s right panel).
#[derive(Clone, Debug, Default)]
pub(super) struct RuleEditor {
    /// The card it edits; `None` for a new rule.
    pub(super) card: Option<String>,
    pub(super) rule: Option<RuleRef>,
    /// A new rule's name (its id and file name).
    pub(super) id: String,
    /// The file as read, its byte revision and the sentence it holds.
    pub(super) content: String,
    pub(super) revision: Option<String>,
    pub(super) base: Sentence,
    pub(super) sentence: Sentence,
    /// The whole file as TOML while Edit as TOML is open.
    pub(super) toml: Option<String>,
    /// A field's check error, or the daemon's refusal or read error.
    pub(super) field_error: Option<(EditorField, String)>,
    pub(super) note: Option<(String, bool)>,
    pub(super) reading: bool,
    pub(super) saving: bool,
}

impl RuleEditor {
    pub(super) fn dirty(&self) -> bool {
        self.sentence != self.base || self.toml.as_ref().is_some_and(|toml| *toml != self.content)
    }
}

/// The Automations page's own messages: its tabs, the rule editor, the
/// rows' switches and menus, and each rule's last outcome.
#[derive(Clone, Debug)]
pub(super) enum RuleMsg {
    /// Show the Scripts tab (true) or Automations.
    TabChosen(bool),
    /// Open a row's editor (its card key).
    Opened(String),
    /// A rules file read for the editor: the card key and the reply.
    Read(String, Box<Fallible<eliot_core::api::RuleReadResponse>>),
    /// A new rule in the global rules folder.
    New,
    Id(String),
    When(Option<usize>),
    WhenValue(String),
    Zone(String),
    OnlyIf(String),
    Action(Option<usize>),
    Text(String),
    Repeat(Option<f64>),
    /// Open or close Edit as TOML.
    TomlToggled,
    TomlText(String),
    Save,
    Cancel,
    Delete,
    /// A save or delete landed: the card key, the content saved (none when
    /// the file was removed) and the reply (the new revision).
    Saved(String, Option<String>, Box<MutationResult<Option<String>>>),
    /// A row's switch: its card key and on.
    Toggled(String, bool),
    /// A rule's switch landed: its card key and what happened.
    ToggleDone(String, Box<MutationResult<()>>),
    /// The rules' last outcomes and refusals (`rule.get`), by read.
    StatusLoaded(
        u64,
        Vec<(TimerKey, Fallible<eliot_core::api::RuleShowView>)>,
    ),
    /// A row's menu: the action, a unit separator, the card key.
    Menu(String),
}

/// The most rules whose last outcome one page read asks for.
const STATUS_READS: usize = 64;

impl DesktopApp {
    /// The cards the page shows now (the same builder as the rows).
    pub(super) fn automation_cards(&self) -> Vec<Card> {
        let projects = self.automation_projects();
        let data_dir = self.data_dir.as_ref().ok().map(PathBuf::as_path);
        cards(&CardInputs {
            assignments: self.assignment_views(),
            worker: &self.automation.worker,
            namespaces: &self.automation.rules,
            global_rules_current: self.automation.global_rules_current,
            projects: &projects,
            data_dir,
        })
    }

    /// Read every listed rule's last outcome and refusal (`rule.get`), at
    /// most [`STATUS_READS`], while the page shows.
    pub(super) fn read_rule_status(&mut self, context: &ComponentContext<Self>) {
        let Ok(data_dir) = self.data_dir.clone() else {
            return;
        };
        let keys: Vec<TimerKey> = self
            .automation
            .rules
            .iter()
            .flat_map(|namespace| {
                namespace
                    .rules
                    .iter()
                    .flatten()
                    .map(|entry| TimerKey(namespace.project.clone(), entry.state.id.clone()))
            })
            .take(STATUS_READS)
            .collect();
        self.automation.status_seq += 1;
        let seq = self.automation.status_seq;
        let _task = context.spawn_background(move |_| {
            let reads = keys
                .into_iter()
                .map(|key| {
                    let read = client::block_on(config_client::rule_status(
                        &data_dir,
                        key.0.clone(),
                        key.1.clone(),
                    ))
                    .map_err(|error| error.user_message());
                    (key, read)
                })
                .collect();
            Message::Automation(AutomationMsg::Rule(RuleMsg::StatusLoaded(seq, reads)))
        });
    }

    fn read_rule_file(&mut self, card: String, rule: &RuleRef, context: &ComponentContext<Self>) {
        let (Ok(data_dir), Some(file)) = (self.data_dir.clone(), rule.file.clone()) else {
            return;
        };
        let project = rule.key.0.clone();
        let _task = context.spawn_background(move |_| {
            let read = client::block_on(config_client::read_rule_file(&data_dir, project, file))
                .map_err(|error| error.user_message());
            Message::Automation(AutomationMsg::Rule(RuleMsg::Read(card, Box::new(read))))
        });
    }

    pub(super) fn rule_update(&mut self, msg: RuleMsg, context: &ComponentContext<Self>) {
        let busy = self
            .automation
            .editor
            .as_ref()
            .is_some_and(|editor| editor.saving);
        match msg {
            RuleMsg::TabChosen(scripts) => {
                self.automation.scripts_tab = scripts;
                if scripts {
                    self.load_scripts(context);
                }
            }
            RuleMsg::Opened(card) => {
                if busy {
                    return;
                }
                self.automation.open = Some(card.clone());
                self.automation.editor = None;
                let cards = self.automation_cards();
                let Some(found) = cards.iter().find(|found| found.key == card) else {
                    return;
                };
                if let Some((key, _)) = &found.assignment {
                    self.automation_update(AutomationMsg::FormOpened(Some(key.clone())), context);
                    return;
                }
                self.automation.form = None;
                if let Some(rule) = found.rule.clone() {
                    let reading = rule.file.is_some();
                    self.automation.editor = Some(RuleEditor {
                        card: Some(card.clone()),
                        rule: Some(rule.clone()),
                        reading,
                        ..RuleEditor::default()
                    });
                    if reading {
                        self.read_rule_file(card, &rule, context);
                    }
                }
            }
            RuleMsg::Read(card, read) => {
                let Some(editor) = self
                    .automation
                    .editor
                    .as_mut()
                    .filter(|editor| editor.card.as_deref() == Some(card.as_str()))
                else {
                    return;
                };
                editor.reading = false;
                let id = editor
                    .rule
                    .as_ref()
                    .map(|rule| rule.key.1.clone())
                    .unwrap_or_default();
                match *read {
                    Ok(reply) => match read_sentence(&reply.content, &id) {
                        Ok(sentence) => {
                            editor.base = sentence.clone();
                            editor.sentence = sentence;
                            editor.content = reply.content;
                            editor.revision = Some(reply.revision);
                            editor.field_error = None;
                        }
                        Err(error) => {
                            editor.content = reply.content.clone();
                            editor.revision = Some(reply.revision);
                            editor.toml = Some(reply.content);
                            editor.note = Some((error, true));
                        }
                    },
                    Err(error) => editor.note = Some((format!("Not read: {error}"), true)),
                }
            }
            RuleMsg::New => {
                if busy {
                    return;
                }
                self.automation.form = None;
                self.automation.open = None;
                self.automation.editor = Some(RuleEditor {
                    sentence: Sentence {
                        when_value: EVENTS[0].0.to_owned(),
                        action: ACTIONS[0].0.to_owned(),
                        text: Some(String::new()),
                        ..Sentence::default()
                    },
                    ..RuleEditor::default()
                });
            }
            RuleMsg::Cancel => {
                if !busy {
                    self.automation.editor = None;
                    self.automation.open = None;
                }
            }
            RuleMsg::Save => self.save_rule(context),
            RuleMsg::Delete => self.delete_rule(context),
            RuleMsg::Saved(card, content, result) => {
                let Some(editor) = self.automation.editor.as_mut() else {
                    return;
                };
                editor.saving = false;
                match *result {
                    Ok(Some(revision)) => {
                        if let Some(content) = content {
                            editor.content = content;
                        }
                        editor.revision = Some(revision);
                        editor.toml = None;
                        editor.base = editor.sentence.clone();
                        editor.note = Some(("Saved.".to_owned(), false));
                        editor.field_error = None;
                        if editor.card.is_none() {
                            // A new rule: its row appears with the next read.
                            self.automation.editor = None;
                        }
                    }
                    Ok(None) => {
                        self.automation.editor = None;
                        if self.automation.open.as_deref() == Some(card.as_str()) {
                            self.automation.open = None;
                        }
                    }
                    // Uncertain: never resent on our own; the file is read
                    // again to show what it holds.
                    Err(error) if crate::state::uncertain(&error) => {
                        editor.note = Some((
                            format!(
                                "Not confirmed ({}). Read the rule again before another save.",
                                error.user_message()
                            ),
                            true,
                        ));
                    }
                    Err(error) => editor.note = Some((error.user_message(), true)),
                }
                self.reload_rules_soon(context);
            }
            RuleMsg::Toggled(card, on) => self.toggle_rule(card, on, context),
            RuleMsg::ToggleDone(card, result) => {
                self.automation.toggling.remove(&card);
                match *result {
                    Ok(()) => {
                        self.automation.toggle_errors.remove(&card);
                        let reread = self.automation.editor.as_ref().and_then(|editor| {
                            (editor.card.as_deref() == Some(card.as_str()) && !editor.dirty())
                                .then(|| editor.rule.clone())
                                .flatten()
                        });
                        if let Some(rule) = reread {
                            if let Some(editor) = self.automation.editor.as_mut() {
                                editor.reading = true;
                            }
                            self.read_rule_file(card.clone(), &rule, context);
                        }
                    }
                    Err(error) => {
                        self.automation
                            .toggle_errors
                            .insert(card, error.user_message());
                    }
                }
                self.reload_rules_soon(context);
            }
            RuleMsg::StatusLoaded(seq, reads) => {
                if seq == self.automation.status_seq {
                    self.automation.status = reads.into_iter().collect();
                }
            }
            RuleMsg::Menu(key) => {
                let Some((action, card)) = key.split_once('\u{1f}') else {
                    return;
                };
                let cards = self.automation_cards();
                let Some(found) = cards.iter().find(|found| found.key == card) else {
                    return;
                };
                match action {
                    "run" => {
                        if let Some(TimerKey(project, rule)) = found.run.clone() {
                            self.automation_update(AutomationMsg::RunNow(project, rule), context);
                        }
                    }
                    "edit" => self.rule_update(RuleMsg::Opened(card.to_owned()), context),
                    "remove" => {
                        if let Some((assignment, _)) = &found.assignment {
                            self.automation_update(
                                AutomationMsg::Removed(assignment.clone()),
                                context,
                            );
                        } else {
                            self.rule_update(RuleMsg::Opened(card.to_owned()), context);
                        }
                    }
                    _ => {}
                }
            }
            edit => {
                let Some(editor) = self.automation.editor.as_mut().filter(|_| !busy) else {
                    return;
                };
                editor.note = None;
                editor.field_error = None;
                let sentence = &mut editor.sentence;
                match edit {
                    RuleMsg::Id(text) => editor.id = text,
                    RuleMsg::When(Some(index)) => {
                        let (kind, value) = match index {
                            index if index < EVENTS.len() => {
                                (WhenKind::Event, EVENTS[index].0.to_owned())
                            }
                            index if index == EVENTS.len() => (WhenKind::Event, String::new()),
                            index if index == EVENTS.len() + 1 => (WhenKind::Every, String::new()),
                            index if index == EVENTS.len() + 2 => (WhenKind::Cron, String::new()),
                            _ => (WhenKind::Once, String::new()),
                        };
                        if sentence.when_index() != index {
                            sentence.when = kind;
                            sentence.when_value = value;
                        }
                    }
                    RuleMsg::WhenValue(text) => sentence.when_value = text,
                    RuleMsg::Zone(text) => sentence.zone = text,
                    RuleMsg::OnlyIf(text) => sentence.only_if = text,
                    RuleMsg::Action(Some(index)) => {
                        if let Some((verb, _)) = ACTIONS.get(index) {
                            sentence.action = (*verb).to_owned();
                            if matches!(*verb, "send" | "steer" | "inject" | "mail.send") {
                                sentence.text.get_or_insert_with(String::new);
                            }
                        }
                    }
                    RuleMsg::Text(text) => sentence.text = Some(text),
                    RuleMsg::Repeat(value) => {
                        if sentence.repeat.is_some() {
                            sentence.repeat = value.map(|value| value.round() as i64);
                        }
                    }
                    RuleMsg::TomlToggled => {
                        editor.toml = match editor.toml.take() {
                            Some(_) => None,
                            None => Some(editor.content.clone()),
                        };
                    }
                    RuleMsg::TomlText(text) => editor.toml = Some(text),
                    _ => {}
                }
            }
        }
    }

    /// Save the editor: the TOML as typed, else the sentence written into
    /// the file read (a new rule: a new file in the global rules folder),
    /// against the revision it was read at.
    fn save_rule(&mut self, context: &ComponentContext<Self>) {
        let Ok(data_dir) = self.data_dir.clone() else {
            return;
        };
        let Some(editor) = self
            .automation
            .editor
            .as_mut()
            .filter(|editor| !editor.saving)
        else {
            return;
        };
        let (project, path, expected, content) = match (&editor.rule, &editor.toml) {
            (Some(rule), Some(toml)) => (
                rule.key.0.clone(),
                rule.file.clone().unwrap_or_default(),
                editor.revision.clone(),
                toml.clone(),
            ),
            (Some(rule), None) => {
                match write_sentence(&editor.content, &rule.key.1, &editor.sentence) {
                    Ok(content) => (
                        rule.key.0.clone(),
                        rule.file.clone().unwrap_or_default(),
                        editor.revision.clone(),
                        content,
                    ),
                    Err(error) => {
                        editor.field_error = Some(error);
                        return;
                    }
                }
            }
            (None, _) => match new_rule_file(editor.id.trim(), &editor.sentence) {
                Ok(content) => (None, format!("{}.toml", editor.id.trim()), None, content),
                Err(error) => {
                    editor.field_error = Some(error);
                    return;
                }
            },
        };
        if path.is_empty() || (editor.rule.is_some() && expected.is_none()) {
            editor.note = Some(("Read the rule again before saving.".to_owned(), true));
            return;
        }
        editor.saving = true;
        editor.note = Some(("Saving…".to_owned(), false));
        let card = editor.card.clone().unwrap_or_default();
        let _task = context.spawn_background(move |_| {
            let result = client::block_on(config_client::save_rule_file(
                &data_dir,
                project,
                path,
                expected,
                content.clone(),
            ))
            .map(|reply| Some(reply.revision));
            Message::Automation(AutomationMsg::Rule(RuleMsg::Saved(
                card,
                Some(content),
                Box::new(result),
            )))
        });
    }

    /// Delete the edited rule: remove its file when it is the file's only
    /// rule, else save the file without it.
    fn delete_rule(&mut self, context: &ComponentContext<Self>) {
        let Ok(data_dir) = self.data_dir.clone() else {
            return;
        };
        let Some(editor) = self
            .automation
            .editor
            .as_mut()
            .filter(|editor| !editor.saving)
        else {
            return;
        };
        let (Some(rule), Some(revision)) = (editor.rule.clone(), editor.revision.clone()) else {
            return;
        };
        let Some(path) = rule.file.clone() else {
            return;
        };
        let rest = match without_rule(&editor.content, &rule.key.1) {
            Ok(rest) => rest,
            Err(error) => {
                editor.note = Some((error, true));
                return;
            }
        };
        editor.saving = true;
        editor.note = Some(("Deleting…".to_owned(), false));
        let card = editor.card.clone().unwrap_or_default();
        let project = rule.key.0.clone();
        let _task = context.spawn_background(move |_| {
            let result = client::block_on(async {
                match &rest {
                    Some(content) => config_client::save_rule_file(
                        &data_dir,
                        project,
                        path,
                        Some(revision),
                        content.clone(),
                    )
                    .await
                    .map(|_| None),
                    None => config_client::delete_rule_file(&data_dir, project, path, revision)
                        .await
                        .map(|_| None),
                }
            });
            Message::Automation(AutomationMsg::Rule(RuleMsg::Saved(
                card,
                None,
                Box::new(result),
            )))
        });
    }

    /// A row's switch: an assignment's `enabled` in the settings, a rule's
    /// `enabled` key in its rules file (read, switched, saved against the
    /// revision read).
    fn toggle_rule(&mut self, card: String, on: bool, context: &ComponentContext<Self>) {
        let cards = self.automation_cards();
        let Some(found) = cards.iter().find(|found| found.key == card) else {
            return;
        };
        if let Some((assignment, _)) = &found.assignment {
            self.automation_update(AutomationMsg::Enabled(assignment.clone(), on), context);
            return;
        }
        let Some(rule) = found.rule.clone().filter(|rule| rule.file.is_some()) else {
            return;
        };
        let Ok(data_dir) = self.data_dir.clone() else {
            return;
        };
        if !self.automation.toggling.insert(card.clone()) {
            return;
        }
        let _task = context.spawn_background(move |_| {
            let result = client::block_on(async {
                let file = rule.file.clone().unwrap_or_default();
                let read =
                    config_client::read_rule_file(&data_dir, rule.key.0.clone(), file.clone())
                        .await?;
                let content = write_enabled(&read.content, &rule.key.1, on)
                    .map_err(|error| eliot_core::Error::new("invalid", error))?;
                config_client::save_rule_file(
                    &data_dir,
                    rule.key.0.clone(),
                    file,
                    Some(read.revision),
                    content,
                )
                .await
                .map(|_| ())
            });
            Message::Automation(AutomationMsg::Rule(RuleMsg::ToggleDone(
                card,
                Box::new(result),
            )))
        });
    }
}
