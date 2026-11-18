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
use crate::config_view::{duration_words, local_time, rule_text};
use eliot_core::CommandState;
use eliot_core::api::TimerStateView;

#[derive(Clone, Debug)]
pub(super) enum AutomationMsg {
    /// The namespaces read, and why the rule worker is down (`Ok(None)`:
    /// it runs).
    RulesLoaded(
        u64,
        Vec<(Option<String>, String, Fallible<RulePage>)>,
        Fallible<Option<String>>,
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
    RunSubmitted(String, Box<MutationResult<Command>>),
    RunSettled(String, Box<Fallible<(Command, Vec<Command>)>>),
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
}

/// What the last Run now of one timer did, in words.
#[derive(Clone, Debug)]
pub(super) struct RunLine {
    pub(super) text: String,
    pub(super) busy: bool,
    pub(super) error: bool,
}

/// Rules and timers read for the Automations page, and its local state.
#[derive(Default)]
pub(super) struct AutomationState {
    /// Replaced only when a read lands, never cleared by a refresh.
    pub(super) rules: Vec<RuleNamespace>,
    /// Why the daemon's rule worker is down, as last read; `None` while it
    /// runs or when its health could not be read.
    pub(super) worker_down: Option<String>,
    /// Tags rule loads; an older reply is dropped.
    pub(super) epoch: u64,
    /// Cards whose technical details are open.
    pub(super) technical: HashSet<String>,
    pub(super) form: Option<AssignmentForm>,
    /// The last Run now per timer (its run key).
    pub(super) runs: HashMap<String, RunLine>,
    /// Distinguishes one Run now click from the next in the mutation ledger.
    pub(super) run_seq: u64,
}

impl AutomationState {
    /// A rules read has landed (the global namespace is always in it), so
    /// a background re-read never turns the page back into "Reading…".
    pub(super) fn read(&self) -> bool {
        !self.rules.is_empty()
    }
}

/// The ledger and Run-now key of one timer.
pub(super) fn run_key(project: Option<&str>, rule: &str) -> String {
    format!("{}/{rule}", project.unwrap_or("global"))
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
            Self::Failed(_) => "Failed".to_owned(),
        }
    }

    /// Why it failed, for the line under the card's title.
    pub(super) fn reason(&self) -> Option<&str> {
        match self {
            Self::Failed(reason) => Some(reason),
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
            Self::Paused | Self::Off => StatusTone::Neutral,
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
    pub(super) run: Option<(Option<String>, String)>,
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
    /// Why the rule worker is down: nothing runs, whatever the rules say.
    pub(super) worker_down: Option<&'a str>,
    pub(super) namespaces: &'a [RuleNamespace],
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
    let global: &[RuleListEntry] = inputs
        .namespaces
        .iter()
        .find(|namespace| namespace.project.is_none())
        .and_then(|namespace| namespace.rules.as_ref().ok())
        .map_or(&[], Vec::as_slice);
    let mut cards = Vec::new();
    for assignment in &inputs.assignments {
        let launch_id = format!("assignment-{}", assignment.key);
        let launch = global.iter().find(|entry| entry.state.id == launch_id);
        let complete = global
            .iter()
            .find(|entry| entry.state.id == format!("{launch_id}.complete"));
        let state = match launch {
            _ if !assignment.enabled => CardState::Paused,
            Some(entry) => match &entry.state.paused_reason {
                Some(reason) => CardState::Failed(reason_words(reason)),
                None if entry.state.enabled => CardState::Running,
                None => CardState::Starting,
            },
            None => CardState::Starting,
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
            run: (state == CardState::Running).then(|| (None, launch_id.clone())),
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
        let rest = rules.iter().filter(|entry| !generated(&entry.state.id));
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
            // A rule removed from its file (or a removed assignment's) stays
            // stored, off, with its definition still on: it no longer exists.
            let defined_on = entry
                .definition
                .get("enabled")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(true);
            if !rule.enabled && rule.paused_reason.is_none() && defined_on {
                continue;
            }
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
            cards.push(Card {
                key: format!("rule:{}", run_key(namespace.project.as_deref(), &rule.id)),
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
                run: (entry.timer.is_some() && state == CardState::Running)
                    .then(|| (namespace.project.clone(), rule.id.clone())),
                assignment: None,
                state,
                technical,
            });
        }
    }
    // With the rule worker down no rule or timer fires, so no card may say
    // Running, and Run now waits until Swarm restarts.
    if let Some(down) = inputs.worker_down {
        for card in &mut cards {
            if matches!(card.state, CardState::Running | CardState::Starting) {
                card.state = CardState::Failed(format!(
                    "Swarm's automation worker {down}; restart Swarm to run automations again."
                ));
                card.run = None;
                // No next run comes while the worker is down.
                card.times = card
                    .times
                    .split(" · ")
                    .next()
                    .unwrap_or_default()
                    .to_owned();
            }
        }
    }
    cards
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
        CommandState::Rejected | CommandState::Failed | CommandState::Cancelled => {
            return line(format!("Run now didn't run: {}", error_words(parent)), true);
        }
        _ => {}
    }
    let launches: Vec<&Command> = children
        .iter()
        .filter(|child| child.verb == "launch")
        .collect();
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
        match msg {
            AutomationMsg::RulesLoaded(epoch, namespaces, worker) => {
                if epoch != self.automation.epoch {
                    return;
                }
                // An unreadable health keeps the cards' own states.
                self.automation.worker_down = worker.ok().flatten();
                self.automation.rules = namespaces
                    .into_iter()
                    .map(|(project, label, page)| RuleNamespace {
                        project,
                        label,
                        more: page.as_ref().is_ok_and(|(_, more)| *more),
                        rules: page.map(|(rules, _)| rules),
                    })
                    .collect();
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
            AutomationMsg::RunNow(project, rule) => self.run_now(project, rule, context),
            AutomationMsg::RunSubmitted(key, result) => {
                let result = *result;
                // Only an uncertain reply keeps the ledger key, so a retry
                // reuses the same id and the daemon deduplicates it.
                let uncertain = result.as_ref().err().is_some_and(crate::state::uncertain);
                self.settle_mutation(!uncertain);
                if !uncertain {
                    self.automation.run_seq += 1;
                }
                match result {
                    Ok(command) => {
                        self.automation.runs.insert(
                            key.clone(),
                            RunLine {
                                text: "Run now: started; waiting for the result…".to_owned(),
                                busy: true,
                                error: false,
                            },
                        );
                        let Ok(data_dir) = self.data_dir.clone() else {
                            return;
                        };
                        let _task = context.spawn_background(move |cancel| {
                            let cancelled = || cancel.is_cancelled();
                            Message::Automation(AutomationMsg::RunSettled(
                                key,
                                Box::new(settle_timer_run(&data_dir, command, &cancelled)),
                            ))
                        });
                    }
                    Err(error) => {
                        let line = if uncertain {
                            RunLine {
                                text: format!(
                                    "Run now: the outcome is unknown; Run now again reuses its id, so it cannot run twice ({error})."
                                ),
                                busy: false,
                                error: true,
                            }
                        } else {
                            refusal_words(&error)
                        };
                        self.automation.runs.insert(key, line);
                    }
                }
            }
            AutomationMsg::RunSettled(key, result) => {
                let assignment = key.starts_with("global/assignment-");
                let line = match *result {
                    Ok((parent, children)) => run_words(&parent, &children, assignment),
                    Err(error) => RunLine {
                        text: format!("Run now: the result was not read: {error}"),
                        busy: false,
                        error: true,
                    },
                };
                self.automation.runs.insert(key, line);
                // The timer's last run and the started task show on reread.
                self.load_rules(context);
            }
        }
    }

    /// Fire one timer occurrence now (`timer.run`) under one mutation id per
    /// click; an uncertain reply keeps the id for the retry.
    fn run_now(&mut self, project: Option<String>, rule: String, context: &ComponentContext<Self>) {
        let key = run_key(project.as_deref(), &rule);
        if self.submitting() || self.automation.runs.get(&key).is_some_and(|line| line.busy) {
            return;
        }
        let Ok(data_dir) = self.data_dir.clone() else {
            return;
        };
        let ledger_key = format!("timer.run:{key}:{}", self.automation.run_seq);
        let mutation_id = self.ledger.id_for(&ledger_key);
        self.in_flight_key = Some(ledger_key);
        self.automation.runs.insert(
            key.clone(),
            RunLine {
                text: "Run now: starting…".to_owned(),
                busy: true,
                error: false,
            },
        );
        let _task = context.spawn_background(move |_| {
            Message::Automation(AutomationMsg::RunSubmitted(
                key,
                Box::new(client::block_on(async {
                    config_client::run_timer(&data_dir, mutation_id, project, &rule).await
                })),
            ))
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
                config_client::rule_worker_down(&data_dir)
                    .await
                    .map_err(|error| error.to_string())
            });
            Message::Automation(AutomationMsg::RulesLoaded(epoch, namespaces, worker))
        });
    }
}

/// Wait for a manual timer run and its actions to settle (at most two
/// minutes), then return the parent and its children.
fn settle_timer_run(
    data_dir: &Path,
    parent: Command,
    cancelled: &dyn Fn() -> bool,
) -> Fallible<(Command, Vec<Command>)> {
    const POLL_MS: u64 = 1_000;
    const MAX_POLLS: usize = 120;
    let mut parent = parent;
    for _ in 0..MAX_POLLS {
        if parent.state.is_terminal() {
            let children = client::block_on(client::command_children(data_dir, &parent.id))
                .map_err(|error| error.to_string())?;
            if children.iter().all(|child| child.state.is_terminal()) {
                return Ok((parent, children));
            }
        }
        if !feed::backoff(POLL_MS, cancelled) {
            return Err("stopped waiting; the run goes on in Swarm".to_owned());
        }
        parent = client::block_on(client::read_command(data_dir, &parent.id))
            .map_err(|error| error.to_string())?;
    }
    Err(format!(
        "still {} after two minutes (command {})",
        format!("{:?}", parent.state).to_lowercase(),
        parent.id
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use eliot_core::api::{RuleStateView, TimerSchedule};
    use serde_json::json;

    fn projects() -> Vec<(String, String, String)> {
        vec![(
            "eliot".to_owned(),
            r"\\?\C:\Development\Rust\projects\eliot-swarm-controller".to_owned(),
            "eliot-swarm-controller".to_owned(),
        )]
    }

    fn queue(enabled: bool) -> AssignmentView {
        AssignmentView {
            key: "queue10".to_owned(),
            folder: r"C:\Development\Rust\agent-briefs\queue10".to_owned(),
            workspace: r"C:\Development\Rust\projects\eliot-swarm-controller".to_owned(),
            role: "worker".to_owned(),
            busy: Some(2),
            interval: "30s".to_owned(),
            enabled,
        }
    }

    fn entry(id: &str, enabled: bool, timer: Option<TimerSchedule>) -> RuleListEntry {
        RuleListEntry {
            state: RuleStateView {
                id: id.to_owned(),
                project: None,
                revision: 3,
                current_cursor: 7,
                retiring_revision: None,
                retiring_cursor: None,
                retire_through: None,
                enabled,
                paused_reason: None,
            },
            timer: timer.map(|schedule| TimerStateView {
                id: id.to_owned(),
                project: None,
                schedule_revision: 1,
                schedule,
                anchor: 0,
                last_key: None,
                next_due_at: Some(1_800_000_000_000),
                enabled,
            }),
            definition: json!({"on": {"every": "1h"}, "do": [{"action": "send", "target": "a1", "text": "hi"}]}),
        }
    }

    #[test]
    fn an_assignment_reads_as_one_sentence() {
        assert_eq!(
            assignment_sentence(&queue(true), &projects()),
            r"Every 30 s, take new briefs from agent-briefs\queue10 and start them as Worker tasks in eliot-swarm-controller (at most 2 at once)."
        );
        let mut ui = queue(true);
        ui.role = "ui".to_owned();
        ui.interval = "5m".to_owned();
        ui.workspace = r"C:\work\other".to_owned();
        assert_eq!(
            assignment_sentence(&ui, &projects()),
            r"Every 5 min, take new briefs from agent-briefs\queue10 and start them as UI tasks in work\other (at most 2 at once)."
        );
    }

    #[test]
    fn intervals_parse_like_settings_write_them() {
        assert_eq!(interval_ms("30s"), Some(30_000));
        assert_eq!(interval_ms("5m"), Some(300_000));
        assert_eq!(interval_ms("2h"), Some(7_200_000));
        assert_eq!(interval_ms("2 h"), None);
        assert_eq!(interval_ms("0s"), None);
        assert_eq!(interval_ms("soon"), None);
        assert_eq!(short_folder(r"C:\briefs"), "briefs");
        assert_eq!(short_folder(r"\\?\C:\a\b\c\"), r"b\c");
    }

    #[test]
    fn cards_show_state_actions_and_hide_raw_keys_in_technical_details() {
        let launch = entry(
            "assignment-queue10",
            true,
            Some(TimerSchedule::Interval {
                period_ms: 30_000,
                anchor: None,
            }),
        );
        let complete = entry("assignment-queue10.complete", true, None);
        let nightly = entry(
            "nightly",
            true,
            Some(TimerSchedule::Interval {
                period_ms: 3_600_000,
                anchor: None,
            }),
        );
        let mut off = entry("old", false, None);
        off.definition = json!({"enabled": false, "on": {"event": "turn.failed"}, "do": [{"action": "stop", "target": "${event.agent}"}]});
        let mut backlog = entry("busy", true, None);
        backlog.state.paused_reason = Some("backlog_exceeded".to_owned());
        // Deleted from its file: stored off, its definition still on.
        let mut removed = entry("gone", false, None);
        removed.definition["enabled"] = json!(true);
        let namespaces = vec![RuleNamespace {
            project: None,
            label: "Global rules".to_owned(),
            rules: Ok(vec![launch, complete, nightly, off, backlog, removed]),
            more: false,
        }];
        let projects = projects();
        let inputs = |enabled| CardInputs {
            assignments: vec![queue(enabled)],
            worker_down: None,
            namespaces: &namespaces,
            projects: &projects,
            data_dir: Some(Path::new(r"C:\data")),
        };
        let cards = cards(&inputs(true));
        let titles: Vec<&str> = cards.iter().map(|card| card.title.as_str()).collect();
        // The generated completion rule is part of the assignment's card.
        assert_eq!(titles, ["queue10", "nightly", "old", "busy"]);
        let queue = &cards[0];
        assert_eq!(queue.state, CardState::Running);
        assert_eq!(queue.run, Some((None, "assignment-queue10".to_owned())));
        assert_eq!(queue.assignment, Some(("queue10".to_owned(), true)));
        assert!(
            queue
                .times
                .starts_with("Last scheduled check: not yet · Next check ")
        );
        assert!(!queue.sentence.contains("max_busy"));
        assert!(
            queue
                .technical
                .iter()
                .any(|line| line.contains("max_busy = 2"))
        );
        assert!(
            queue
                .technical
                .iter()
                .any(|line| line == "Rule assignment-queue10.complete · revision 3")
        );
        assert_eq!(cards[1].run, Some((None, "nightly".to_owned())));
        assert_eq!(
            cards[1].paths,
            [("Rules file in", r"C:\data\rules".to_owned())]
        );
        assert_eq!(cards[2].state, CardState::Off);
        assert_eq!(cards[2].run, None);
        assert_eq!(cards[3].state.label(), "Failed");
        assert_eq!(
            cards[3].state.reason(),
            Some("too many events were waiting, so Swarm stopped it")
        );

        // Paused: no Run now (the daemon refuses a disabled timer).
        let paused = super::cards(&inputs(false));
        assert_eq!(paused[0].state, CardState::Paused);
        assert_eq!(paused[0].run, None);
        assert_eq!(paused[0].assignment, Some(("queue10".to_owned(), false)));

        // The rule worker failed: nothing says Running and Run now waits;
        // Off and Paused stay as they are.
        let down = super::cards(&CardInputs {
            worker_down: Some("stopped after an error (unavailable)"),
            ..inputs(true)
        });
        assert_eq!(
            down[0].state.reason(),
            Some(
                "Swarm's automation worker stopped after an error (unavailable); restart Swarm to run automations again."
            )
        );
        assert_eq!(down[0].run, None);
        assert_eq!(down[0].times, "Last scheduled check: not yet");
        assert_eq!(down[2].state, CardState::Off);
    }

    fn command(verb: &str, state: CommandState, args: serde_json::Value) -> Command {
        serde_json::from_value(json!({
            "id": "01J0000000000000000000000A", "principal": "owner", "method": "command.submit",
            "verb": verb, "target": null, "selector": null, "expected_generation": null,
            "generation": null, "args": args, "state": state, "turn_id": null,
            "request_id": null, "outcome": null, "created_at": 0, "updated_at": 0
        }))
        .unwrap()
    }

    #[test]
    fn run_now_reads_back_in_words() {
        let parent = command("timer.run", CommandState::Applied, json!({}));
        let launch = command(
            "launch",
            CommandState::Applied,
            json!({"assignment": {"key": "queue10", "brief": "03-fix.md"}}),
        );
        let started = run_words(&parent, &[launch], true);
        assert_eq!(started.text, "Run now: started a task from 03-fix.md.");
        assert!(!started.error);
        let idle = run_words(&parent, &[], true);
        assert!(idle.text.starts_with("Run now: nothing to start."));
        let mut refused = command("timer.run", CommandState::Rejected, json!({}));
        refused.outcome =
            Some(json!({"code": "rule_disabled", "message": "the timer is disabled"}));
        let refused = run_words(&refused, &[], true);
        assert_eq!(
            refused.text,
            "Run now didn't run: the timer is disabled (rule_disabled)"
        );
        assert!(refused.error);

        // The daemon's answers when an assignment has nothing to start.
        let empty = refusal_words(&eliot_core::Error::new(
            "not_found",
            "assignment folder has no next brief",
        ));
        assert_eq!(
            empty.text,
            "Run now: nothing to start. There is no new brief in the folder."
        );
        assert!(!empty.error);
        assert!(
            !refusal_words(&eliot_core::Error::new(
                "bad_state",
                "assignment has reached max_busy"
            ))
            .error
        );
        let other = refusal_words(&eliot_core::Error::new("not_allowed", "nope"));
        assert_eq!(other.text, "Run now didn't run: nope (not_allowed).");
        assert!(other.error);
    }
}
