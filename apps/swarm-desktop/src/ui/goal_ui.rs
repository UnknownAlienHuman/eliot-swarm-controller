//! The Goal dialog (brand book §18.1 Goal row, §11.3): one native
//! ContentDialog, opened by the conversation header's Goal, the open
//! chat's composer (+ → Goal…), a task menu's "Goal…", Details › Goal's
//! "Goal…" and Home's Goal chip for the new task. It holds the goal text, the
//! check command (an explicit argv that runs in the task's worktree) and
//! the limits in their source units (rounds, turns, same-result rounds),
//! and offers Save, Pause, Resume and Clear as the daemon allows them for
//! the goal's status (`goal.set`, `goal.pause`, `goal.resume`,
//! `goal.clear`; no other API).
//!
//! The goal shown is the open conversation's (`chat.goal`, read with every
//! detail read); the dialog keeps only the owner's edits, so a goal that
//! changes meanwhile shows as it is until a field is edited. Save checks
//! the request with its own validation before it is sent, and every
//! validation error and refusal shows in the dialog. Save is named and is
//! never the default for Enter: the dialog opens with the keyboard in the
//! goal text, Enter there starts a new line, and Esc and Cancel close it.
//! Clear asks first, in the same dialog.
//!
//! With no goal set, the first message is the objective (brand book §9.1,
//! `docs/spec/automation.md` "Default Goal objective"): the dialog shows
//! it as the current goal text, for the owner to replace.
//!
//! For the new task, the dialog starts from the Goal the launch will
//! install: the project's launch policy (`[projects.ID.policy]`), whose
//! settings stay edited in Settings › Projects alone (the dialog links
//! there). Save keeps only the fields the owner changed, which the launch
//! sends as `args.goal`, each overriding its policy default (Command
//! admission in `docs/spec/api.md`); with no policy check the goal needs
//! its own text and check, as `goal.set` does.

use super::launch_ui::NEW_TASK_COMPOSER;
use super::*;
use crate::settings_model::PolicyFields;
use eliot_core::api::{GoalDefinition, GoalSetRequest, GoalStatus, GoalView};
use serde_json::{Value, json};

/// The most a round, turn or same-result limit may be (`MAX_GOAL_LIMIT`).
const MAX_LIMIT: f64 = eliot_core::api::MAX_GOAL_LIMIT as f64;

#[derive(Clone, Debug)]
pub(super) enum GoalMsg {
    /// Open the dialog for this Agent (its conversation opens first), or
    /// for the new task ([`NEW_TASK_COMPOSER`]).
    Open(String),
    /// The composer's Goal (Home's chip, the open chat's + → Goal…): the
    /// dialog for the composer's task.
    Composer,
    /// The new task's dialog's link: Settings › Projects with the
    /// project's launch policy.
    Policy,
    Text(String),
    Check(String),
    Rounds(Option<f64>),
    Turns(Option<f64>),
    Same(Option<f64>),
    Save,
    Control(client::GoalControl),
    /// Clear's question answered: true clears the goal.
    ClearAnswered(bool),
    /// A call settled: the Agent, what it was, the goal the daemon left.
    Done(String, Step, Box<MutationResult<GoalView>>),
    Closed,
}

/// What the dialog has in flight.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Step {
    Save,
    Control(client::GoalControl),
}

impl Step {
    fn label(self) -> &'static str {
        match self {
            Self::Save => "Save",
            Self::Control(control) => control.label(),
        }
    }
}

/// The owner's edits, in the fields' own terms.
#[derive(Clone, Debug, PartialEq)]
struct Draft {
    text: String,
    check: String,
    rounds: Option<f64>,
    turns: Option<f64>,
    same: Option<f64>,
}

/// A round, turn or same-result limit as the daemon takes it.
fn limit(value: Option<f64>, name: &str) -> Result<Option<i64>, String> {
    match value {
        None => Ok(None),
        Some(value) if value.fract() == 0.0 && (1.0..=MAX_LIMIT).contains(&value) => {
            Ok(Some(value as i64))
        }
        Some(_) => Err(format!("{name} takes a whole number from 1 to 1,000,000.")),
    }
}

/// The check command's requirement, in the owner's words.
const CHECK_REQUIRED: &str =
    "Give the check command: the program that proves the goal is met, such as cargo test.";
/// The bound an idle-driven goal needs, in the owner's words.
const LIMIT_REQUIRED: &str =
    "Set a round or turn limit: Swarm runs the goal on every idle turn until one is reached.";

/// The `args.goal` fields the dialog edits, with the limit each names.
const LIMIT_FIELDS: [(&str, &str); 3] = [
    ("max_rounds", "Rounds"),
    ("max_turns", "Turns"),
    ("max_without_progress", "Same result"),
];

impl Draft {
    fn empty() -> Self {
        Self {
            text: String::new(),
            check: String::new(),
            rounds: None,
            turns: None,
            same: None,
        }
    }

    /// The project policy's Goal as the dialog shows it: its check and
    /// rounds (the policy has no goal text: the daemon names the work).
    fn of_policy(policy: &PolicyFields) -> Self {
        Self {
            check: argv_text(&policy.check_argv()),
            rounds: Some(policy.rounds() as f64),
            ..Self::empty()
        }
    }

    /// These fields with the owner's `args.goal` fields over them.
    fn with(mut self, goal: &client::LaunchGoal) -> Self {
        if let Some(text) = goal.get("text").and_then(Value::as_str) {
            self.text = text.to_owned();
        }
        if let Some(Value::Array(check)) = goal.get("check") {
            let check: Vec<String> = check
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect();
            self.check = argv_text(&check);
        }
        for (field, _) in LIMIT_FIELDS {
            if let Some(value) = goal.get(field) {
                let value = value.as_i64().map(|limit| limit as f64);
                match field {
                    "max_rounds" => self.rounds = value,
                    "max_turns" => self.turns = value,
                    _ => self.same = value,
                }
            }
        }
        self
    }

    fn limits(&self) -> [Option<f64>; 3] {
        [self.rounds, self.turns, self.same]
    }

    /// The new task's `args.goal`: the fields that differ from `base`
    /// (the policy's Goal, or empty without one), checked as the Goal the
    /// launch will install; none when nothing differs.
    fn launch_goal(
        &self,
        base: &Draft,
        policy: bool,
    ) -> Result<Option<client::LaunchGoal>, String> {
        let mut goal = client::LaunchGoal::new();
        let text = self.text.trim();
        if !text.is_empty() && text != base.text.trim() {
            goal.insert("text".to_owned(), json!(text));
        }
        let check = parse_argv(&self.check)?;
        if check != parse_argv(&base.check)? {
            goal.insert("check".to_owned(), json!(check));
        }
        for ((field, name), (mine, theirs)) in LIMIT_FIELDS
            .into_iter()
            .zip(self.limits().into_iter().zip(base.limits()))
        {
            let mine = limit(mine, name)?;
            if mine != limit(theirs, name)? {
                // `null` clears the policy's limit (an Owner override).
                goal.insert(field.to_owned(), json!(mine));
            }
        }
        if goal.is_empty() {
            return Ok(None);
        }
        if !policy && text.is_empty() {
            return Err("Write what the goal is.".to_owned());
        }
        if check.is_empty() {
            return Err(CHECK_REQUIRED.to_owned());
        }
        if self.rounds.is_none() && self.turns.is_none() {
            return Err(LIMIT_REQUIRED.to_owned());
        }
        Ok(Some(goal))
    }

    /// The fields as the goal fills them (empty with no goal).
    fn of(goal: Option<&GoalView>) -> Self {
        match goal {
            Some(goal) => Self {
                text: goal.definition.text.clone(),
                check: argv_text(&goal.definition.check),
                rounds: goal.definition.max_rounds.map(|limit| limit as f64),
                turns: goal.definition.max_turns.map(|limit| limit as f64),
                same: goal
                    .definition
                    .max_without_progress
                    .map(|limit| limit as f64),
            },
            None => Self::empty(),
        }
    }

    /// The definition Save sends: the edited fields over the current
    /// goal's other settings (cadence, scope, cost, escalation), or the
    /// owner's words for what is wrong.
    fn definition(&self, goal: Option<&GoalView>) -> Result<GoalDefinition, String> {
        let text = self.text.trim();
        if text.is_empty() {
            return Err("Write what the goal is.".to_owned());
        }
        let check = parse_argv(&self.check)?;
        if check.is_empty() {
            return Err(CHECK_REQUIRED.to_owned());
        }
        let mut definition = goal.map_or_else(
            || GoalDefinition {
                text: String::new(),
                check: Vec::new(),
                check_cwd: None,
                check_timeout_ms: None,
                check_timeout_retry_safe: false,
                max_turns: None,
                max_rounds: None,
                max_without_progress: None,
                cadence_ms: None,
                scope: Vec::new(),
                policy_scope: Vec::new(),
                check_inputs: Vec::new(),
                max_cost: None,
                escalation: None,
            },
            |goal| goal.definition.clone(),
        );
        definition.text = text.to_owned();
        definition.check = check;
        definition.max_rounds = limit(self.rounds, "Rounds")?;
        definition.max_turns = limit(self.turns, "Turns")?;
        definition.max_without_progress = limit(self.same, "Same result")?;
        if definition.cadence_ms.is_none()
            && definition.max_rounds.is_none()
            && definition.max_turns.is_none()
        {
            return Err(LIMIT_REQUIRED.to_owned());
        }
        Ok(definition)
    }
}

/// The open dialog: the Agent it acts on and what the owner did in it.
pub(super) struct GoalDialog {
    /// The Agent, or [`NEW_TASK_COMPOSER`] for the new task's goal.
    agent: String,
    title: String,
    /// The edits; none shows the current goal's values.
    draft: Option<Draft>,
    busy: Option<Step>,
    /// The last validation error or refusal.
    error: Option<String>,
    /// Clear was pressed: the footer asks first.
    confirm_clear: bool,
    /// A save whose outcome is unknown: its id and definition, reused when
    /// the same goal is saved again so the daemon answers the first one.
    retry: Option<(String, GoalDefinition)>,
}

impl GoalDialog {
    /// Clear's question is showing.
    pub(super) fn confirm_clear(&self) -> bool {
        self.confirm_clear
    }

    /// The dialog sets the new task's goal (no daemon call until Start).
    fn new_task(&self) -> bool {
        self.agent == NEW_TASK_COMPOSER
    }
}

#[derive(Default)]
pub(super) struct GoalState {
    pub(super) open: Option<GoalDialog>,
}

/// The controls the daemon accepts for a goal in `status`
/// (`eliot-store` goal state changes): Pause an active goal, Resume a
/// paused one, Clear any goal; Save (`goal.set`) always replaces it.
fn allowed(status: GoalStatus) -> &'static [client::GoalControl] {
    use client::GoalControl::{Clear, Pause, Resume};
    match status {
        GoalStatus::Active => &[Pause, Clear],
        GoalStatus::Paused => &[Resume, Clear],
        GoalStatus::Blocked | GoalStatus::Completed => &[Clear],
        GoalStatus::Cleared => &[],
    }
}

/// The check command's help: its tooltip and UI Automation help text.
const CHECK_HELP: &str = "Runs in the task's worktree without a shell; exit 0 means met";
/// The limits' help: their tooltip and UI Automation help text.
const LIMITS_HELP: &str = "Rounds of checks, the task's turns, and rounds in a row with the same result; empty means no limit";

/// The goal's state in a few words, for the dialog's context line.
fn status_words(goal: &GoalView) -> String {
    let (state, reason) = match goal.status {
        GoalStatus::Active => ("Active", None),
        // The caller's own words ("Paused by the owner from the desktop.").
        GoalStatus::Paused => ("Paused", goal.paused_reason.clone()),
        GoalStatus::Blocked => (
            "Blocked",
            goal.blocked_reason.as_deref().map(eliot_core::reason_words),
        ),
        GoalStatus::Completed => ("Met", None),
        GoalStatus::Cleared => ("Cleared", None),
    };
    let rounds = match goal.definition.max_rounds {
        Some(max) => format!("round {} of {max}", goal.counters.rounds),
        None => format!("round {}", goal.counters.rounds),
    };
    // The reason is its own sentence after the state and the rounds.
    match reason {
        Some(reason) => format!("{state} · {rounds}. {}.", reason.trim_end_matches('.')),
        None => format!("{state} · {rounds}."),
    }
}

/// An argv as one editable line: arguments with spaces or quotes in
/// double quotes.
fn argv_text(argv: &[String]) -> String {
    argv.iter()
        .map(|arg| {
            if arg.is_empty() || arg.contains(char::is_whitespace) || arg.contains('"') {
                format!("\"{}\"", arg.replace('"', "\\\""))
            } else {
                arg.clone()
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

/// The argv a check line names: words split at spaces, a double-quoted
/// run kept as one argument (`\"` is a quote inside it). Nothing goes
/// through a shell.
fn parse_argv(line: &str) -> Result<Vec<String>, String> {
    let mut argv = Vec::new();
    let mut arg: Option<String> = None;
    let mut quoted = false;
    let mut chars = line.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '\\' if quoted && chars.peek() == Some(&'"') => {
                chars.next();
                arg.get_or_insert_with(String::new).push('"');
            }
            '"' => {
                quoted = !quoted;
                arg.get_or_insert_with(String::new);
            }
            c if c.is_whitespace() && !quoted => {
                argv.extend(arg.take());
            }
            c => arg.get_or_insert_with(String::new).push(c),
        }
    }
    if quoted {
        return Err("The check command has an unclosed quote.".to_owned());
    }
    argv.extend(arg);
    Ok(argv)
}

impl DesktopApp {
    /// The launch policy of the new task's project when it installs a Goal
    /// (a registered project whose policy has a check; never a chat).
    fn launch_policy(&self) -> Option<PolicyFields> {
        let project = self.launch_project_id();
        if project.is_empty() {
            return None;
        }
        Some(self.settings.policy_of(&project)).filter(PolicyFields::installs_goal)
    }

    /// The Goal the new task starts with, as the dialog's fields: the
    /// policy's (or none) and the owner's fields over it.
    fn launch_goal_drafts(&self) -> (Draft, Draft, bool) {
        let policy = self.launch_policy();
        let base = policy.as_ref().map_or_else(Draft::empty, Draft::of_policy);
        let shown = match &self.launch.goal {
            Some(goal) => base.clone().with(goal),
            None => base.clone(),
        };
        (base, shown, policy.is_some())
    }

    /// The new task's `args.goal`, checked against the Goal it overrides
    /// now (the project may have changed since it was set).
    pub(super) fn launch_goal_checked(&self) -> Result<Option<client::LaunchGoal>, String> {
        if self.launch.goal.is_none() {
            return Ok(None);
        }
        let (base, shown, policy) = self.launch_goal_drafts();
        shown
            .launch_goal(&base, policy)
            .map_err(|error| format!("Goal: {error}"))
    }

    /// Home's Goal chip words: the Goal the new task will start with
    /// ("Goal · 3 rounds"), or "Goal" with none.
    pub(super) fn launch_goal_label(&self) -> String {
        let (_, shown, policy) = self.launch_goal_drafts();
        if !policy && self.launch.goal.is_none() {
            return "Goal".to_owned();
        }
        let count = |value: f64, one: &str, many: &str| {
            let value = value as i64;
            format!(
                "Goal \u{b7} {value} {}",
                if value == 1 { one } else { many }
            )
        };
        match (shown.rounds, shown.turns) {
            (Some(rounds), _) => count(rounds, "round", "rounds"),
            (None, Some(turns)) => count(turns, "turn", "turns"),
            (None, None) => "Goal".to_owned(),
        }
    }

    /// The fields the open dialog starts from: the Agent's goal, or with
    /// none the first message as the objective (brand book §9.1); the new
    /// task's Goal for the new task.
    fn goal_base(&self, open: &GoalDialog) -> Draft {
        if open.new_task() {
            return self.launch_goal_drafts().1;
        }
        match self.dialog_goal(&open.agent) {
            Some(None) => Draft {
                text: self
                    .bound_detail()
                    .ok()
                    .filter(|detail| detail.agent == open.agent)
                    // Save checks the goal text limit like any edit.
                    .and_then(|detail| detail.first_owner_message.clone())
                    .unwrap_or_default(),
                ..Draft::empty()
            },
            goal => Draft::of(goal.flatten()),
        }
    }

    /// The open dialog's goal: none while it is read, `Some(None)` with no
    /// goal (a cleared one included).
    fn dialog_goal(&self, agent: &str) -> Option<Option<&GoalView>> {
        self.chat
            .goal
            .as_ref()
            .filter(|(id, _)| id == agent)
            .map(|(_, goal)| {
                goal.as_ref()
                    .filter(|goal| goal.status != GoalStatus::Cleared)
            })
    }

    pub(super) fn goal_update(&mut self, msg: GoalMsg, context: &ComponentContext<Self>) {
        match msg {
            GoalMsg::Composer => {
                let target = self.composer_target();
                self.goal_update(GoalMsg::Open(target), context);
            }
            GoalMsg::Policy => {
                let project = self.launch_project_id();
                if !project.is_empty() {
                    self.goal.open = None;
                    self.open_policy_editor(project, context);
                }
            }
            GoalMsg::Open(agent) if agent == NEW_TASK_COMPOSER => {
                self.close_popover();
                self.goal.open = Some(GoalDialog {
                    agent,
                    title: String::new(),
                    draft: None,
                    busy: None,
                    error: None,
                    confirm_clear: false,
                    retry: None,
                });
            }
            GoalMsg::Open(agent) => {
                // The goal it shows is the open conversation's.
                if self.selected_agent_id().as_deref() != Some(agent.as_str()) {
                    let Some(row) = self.find_agent(&agent).cloned() else {
                        self.alert("That task is not listed any more.");
                        return;
                    };
                    self.shell_update(ShellMsg::AgentOpened(row), context);
                }
                let title = self.agent_title(&agent);
                // The status decides the controls: read it now.
                self.read_goal(context);
                self.goal.open = Some(GoalDialog {
                    agent,
                    title,
                    draft: None,
                    busy: None,
                    error: None,
                    confirm_clear: false,
                    retry: None,
                });
            }
            GoalMsg::Closed => self.goal.open = None,
            GoalMsg::Text(_)
            | GoalMsg::Check(_)
            | GoalMsg::Rounds(_)
            | GoalMsg::Turns(_)
            | GoalMsg::Same(_) => {
                let Some(current) = self.goal.open.as_ref().map(|open| self.goal_base(open)) else {
                    return;
                };
                let Some(open) = self.goal.open.as_mut() else {
                    return;
                };
                let draft = open.draft.get_or_insert(current);
                match msg {
                    GoalMsg::Text(text) => draft.text = text,
                    GoalMsg::Check(check) => draft.check = check,
                    GoalMsg::Rounds(value) => draft.rounds = value,
                    GoalMsg::Turns(value) => draft.turns = value,
                    GoalMsg::Same(value) => draft.same = value,
                    _ => {}
                }
                open.error = None;
            }
            GoalMsg::Save => self.save_goal(context),
            GoalMsg::Control(client::GoalControl::Clear) => {
                if let Some(open) = self.goal.open.as_mut() {
                    open.confirm_clear = true;
                }
            }
            GoalMsg::ClearAnswered(clear) => {
                let Some(open) = self.goal.open.as_mut() else {
                    return;
                };
                open.confirm_clear = false;
                if clear {
                    self.goal_call(Step::Control(client::GoalControl::Clear), None, context);
                }
            }
            GoalMsg::Control(control) => self.goal_call(Step::Control(control), None, context),
            GoalMsg::Done(agent, step, result) => self.goal_done(agent, step, *result, context),
        }
    }

    /// Save: the edited goal, checked here, then `goal.set`; for the new
    /// task, the changed fields kept for its launch.
    fn save_goal(&mut self, context: &ComponentContext<Self>) {
        let Some(open) = self.goal.open.as_ref() else {
            return;
        };
        let draft = open.draft.clone().unwrap_or_else(|| self.goal_base(open));
        if open.new_task() {
            let (base, _, policy) = self.launch_goal_drafts();
            match draft.launch_goal(&base, policy) {
                Ok(goal) => {
                    self.launch.goal = goal;
                    self.goal.open = None;
                }
                Err(error) => {
                    if let Some(open) = self.goal.open.as_mut() {
                        open.error = Some(error);
                    }
                }
            }
            return;
        }
        let goal = self.dialog_goal(&open.agent).flatten();
        let checked = draft.definition(goal).and_then(|definition| {
            let id = open
                .retry
                .as_ref()
                .filter(|(_, sent)| *sent == definition)
                .map_or_else(client::new_mutation_id, |(id, _)| id.clone());
            let request = GoalSetRequest {
                id,
                agent: open.agent.clone(),
                mode: eliot_core::api::GoalMode::Eliot,
                definition,
            };
            request
                .validate()
                .map(|()| request)
                .map_err(|error| error.user_message())
        });
        match checked {
            Ok(request) => self.goal_call(Step::Save, Some(request), context),
            Err(error) => {
                if let Some(open) = self.goal.open.as_mut() {
                    open.error = Some(error);
                }
            }
        }
    }

    /// One goal call at a time, bound to the dialog's Agent.
    fn goal_call(
        &mut self,
        step: Step,
        request: Option<GoalSetRequest>,
        context: &ComponentContext<Self>,
    ) {
        let data_dir = match self.data_dir.clone() {
            Ok(data_dir) => data_dir,
            Err(error) => {
                if let Some(open) = self.goal.open.as_mut() {
                    open.error = Some(format!("{} was not sent: {error}", step.label()));
                }
                return;
            }
        };
        // The controls are disabled while a call is in flight.
        let Some(open) = self.goal.open.as_mut().filter(|open| open.busy.is_none()) else {
            return;
        };
        open.busy = Some(step);
        open.error = None;
        if let Some(request) = &request {
            open.retry = Some((request.id.clone(), request.definition.clone()));
        }
        let agent = open.agent.clone();
        let _task = context.spawn_background(move |_| {
            let result = match (step, request) {
                (Step::Save, Some(request)) => {
                    client::block_on(client::goal_set(&data_dir, &request))
                }
                (Step::Control(control), _) => client::block_on(client::goal_control(
                    &data_dir,
                    client::new_mutation_id(),
                    &agent,
                    control,
                )),
                (Step::Save, None) => Err(eliot_core::Error::new("invalid", "nothing to save")),
            };
            Message::Goal(GoalMsg::Done(agent, step, Box::new(result)))
        });
    }

    /// A goal call settled: the goal the daemon left becomes the open
    /// conversation's (the header and Details show it); a confirmed save
    /// closes the dialog, a refusal stays in it. The goal is read again
    /// either way: a read begun before the call is dropped, and an
    /// unconfirmed call shows what the daemon holds.
    fn goal_done(
        &mut self,
        agent: String,
        step: Step,
        result: MutationResult<GoalView>,
        context: &ComponentContext<Self>,
    ) {
        let open_agent = self.selected_agent_id().as_deref() == Some(agent.as_str());
        let open = self
            .goal
            .open
            .as_mut()
            .filter(|open| open.agent == agent && open.busy == Some(step));
        match result {
            Ok(goal) => {
                if let Some(open) = open {
                    open.busy = None;
                    open.retry = None;
                    if step == Step::Save {
                        self.goal.open = None;
                    }
                }
                if open_agent {
                    self.chat.goal = Some((agent, Some(goal)));
                    self.chat.goal_error = None;
                }
            }
            Err(error) => {
                let uncertain = crate::state::uncertain(&error);
                if let Some(open) = open {
                    open.busy = None;
                    if !uncertain {
                        open.retry = None;
                    }
                    open.error = Some(if uncertain {
                        format!(
                            "{} was not confirmed; saving again reuses its id: {}",
                            step.label(),
                            error.user_message()
                        )
                    } else {
                        format!("{} was refused: {}", step.label(), error.user_message())
                    });
                }
            }
        }
        if open_agent {
            self.read_goal(context);
        }
    }

    /// The Goal dialog: its context line, the fields, the error and the
    /// footer of the controls the goal's status allows.
    pub(super) fn goal_dialog(&self, context: &mut ViewContext<Self>) -> Option<ContentDialog> {
        let open = self.goal.open.as_ref()?;
        if open.new_task() {
            return Some(self.launch_goal_dialog(open, context));
        }
        let goal = self.dialog_goal(&open.agent);
        let current = goal.flatten();
        let draft = open.draft.clone().unwrap_or_else(|| self.goal_base(open));
        let busy = open.busy.is_some();
        // While no daemon answers, no read runs: the goal shown is not
        // confirmed, as after a failed read.
        let read_error = self
            .chat
            .goal_error
            .as_ref()
            .filter(|(id, _)| *id == open.agent)
            .map(|(_, error)| error.as_str())
            .or_else(|| self.daemon_down().then_some("Swarm is not running"));
        // Unknown until a read succeeds: a failed read never reads as "no
        // goal", so Save cannot replace a goal it did not see.
        // A dialog without a goal yet has no status line: its title says it.
        let context_line = match (goal, read_error) {
            (None, Some(error)) => Some(format!("The goal could not be read: {error}")),
            (None, None) => Some("Reading the goal…".to_owned()),
            // Brand book §9.1: the first message serves as the goal.
            (Some(None), None) if !self.goal_base(open).text.is_empty() => Some(
                "No goal set: the first message is the objective. Replace it with your goal."
                    .to_owned(),
            ),
            (Some(None), None) => None,
            (Some(None), Some(error)) => {
                Some(format!("No goal when last read. Not read again: {error}"))
            }
            (Some(Some(goal)), None) => Some(status_words(goal)),
            (Some(Some(goal)), Some(error)) => {
                Some(format!("{} Not read again: {error}", status_words(goal)))
            }
        };
        // The goal as the last read confirmed it: after a failed read the
        // fields show the last known values greyed, and no control acts on
        // them until a read succeeds.
        let stale = read_error.is_some();
        let confirmed = goal.filter(|_| !stale);
        let body = self.goal_fields(
            &draft,
            context_line,
            open.error.clone(),
            stale,
            None,
            context,
        );
        // Keyed by the question, so no button of one footer is reused as
        // another's (a second click on Clear goal never lands on Save).
        let footer = Grid::new().keyed_children(vec![if open.confirm_clear {
            KeyedView::new("ask", self.goal_clear_footer(context))
        } else {
            KeyedView::new("controls", self.goal_footer(open, confirmed, busy, context))
        }]);
        let heading = match current {
            Some(_) => format!("Goal for “{}”", open.title),
            None => format!("Set a goal for “{}”", open.title),
        };
        Some(super::dialog_ui::surface(
            context,
            self.dialog.epoch,
            heading,
            body,
            footer.into(),
            tokens::layout::GOAL_DIALOG_WIDTH,
        ))
    }

    /// The new task's Goal dialog: the Goal its launch will install (the
    /// project's launch policy with the owner's fields over it), a link to
    /// that policy in Settings › Projects, and Cancel and Save goal.
    fn launch_goal_dialog(
        &self,
        open: &GoalDialog,
        context: &mut ViewContext<Self>,
    ) -> ContentDialog {
        let (_, shown, policy) = self.launch_goal_drafts();
        let draft = open.draft.clone().unwrap_or(shown);
        let project = self.launch_project_id();
        let line = if policy {
            "This project's launch policy gives each new task this goal. A field you change applies to this task only."
        } else if project.is_empty() {
            "Without a goal, the first message is the objective."
        } else {
            "This project's launch policy sets no goal. Without one, the first message is the objective."
        };
        let link: Option<View> = (!project.is_empty()).then(|| {
            controls::text_button()
                .horizontal_alignment(HorizontalAlignment::Left)
                .automation_name("Edit the launch policy in Settings › Projects")
                .on_click(context.message(Message::Goal(GoalMsg::Policy)))
                .content(controls::label("Edit the launch policy"))
                .into()
        });
        let body = self.goal_fields(
            &draft,
            Some(line.to_owned()),
            open.error.clone(),
            false,
            link,
            context,
        );
        // Cancel and Save goal at the right, as on a task's dialog.
        let footer = super::dialog_ui::actions(
            controls::text_button()
                .element_ref(&self.dialog.close)
                .automation_name("Cancel goal")
                .on_click(context.message(Message::Goal(GoalMsg::Closed)))
                .content(controls::label("Cancel"))
                .into(),
            Some(
                controls::filled()
                    .automation_name("Save goal")
                    .on_click(context.message(Message::Goal(GoalMsg::Save)))
                    .content(controls::label("Save goal"))
                    .into(),
            ),
        );
        super::dialog_ui::surface(
            context,
            self.dialog.epoch,
            "Goal for the new task".to_owned(),
            body,
            footer,
            tokens::layout::GOAL_DIALOG_WIDTH,
        )
    }

    /// The dialog's body: its context line, an optional link, the goal
    /// text, the check command, the limits and the error.
    fn goal_fields(
        &self,
        draft: &Draft,
        context_line: Option<String>,
        error: Option<String>,
        stale: bool,
        link: Option<View>,
        context: &mut ViewContext<Self>,
    ) -> View {
        let number = |header: &str, value: Option<f64>, message: fn(Option<f64>) -> GoalMsg| {
            NumberBox::new()
                .header(controls::secondary(header))
                .automation_name(header)
                .minimum(1.0)
                .maximum(MAX_LIMIT)
                .value(value)
                .is_enabled(!stale)
                .horizontal_alignment(HorizontalAlignment::Stretch)
                .on_value_changed(context.callback(move |value| Message::Goal(message(value))))
        };
        let limits = Grid::new()
            .columns([GridLength::STAR, GridLength::STAR, GridLength::STAR])
            .column_spacing(tokens::spacing::SM)
            .children((
                number("Rounds", draft.rounds, GoalMsg::Rounds),
                number("Turns", draft.turns, GoalMsg::Turns).grid_column(1),
                number("Same result", draft.same, GoalMsg::Same).grid_column(2),
            ));
        let error: Option<View> = error.map(|error| {
            controls::body(error)
                .foreground(ThemeBrush::SystemCritical)
                .text_wrapping(TextWrapping::Wrap)
                .is_text_selection_enabled(true)
                .into()
        });
        StackPanel::new()
            .spacing(tokens::spacing::MD)
            .keyed_children(crate::views::slots([
                context_line.map(|line| {
                    controls::secondary(line)
                        .text_wrapping(TextWrapping::Wrap)
                        .into()
                }),
                link,
                Some(
                    controls::field()
                        .element_ref(&self.dialog.field)
                        .header(controls::secondary("Goal"))
                        .automation_name("Goal")
                        .is_enabled(!stale)
                        .text(draft.text.clone())
                        .placeholder_text("What is done when the goal is met")
                        .accepts_return(true)
                        .text_wrapping(TextWrapping::Wrap)
                        .min_height(tokens::layout::GOAL_TEXT_MIN_HEIGHT)
                        .max_height(tokens::layout::GOAL_TEXT_MAX_HEIGHT)
                        .on_text_changed(crate::views::text_edits(
                            context.callback(|text| Message::Goal(GoalMsg::Text(text))),
                        ))
                        .into(),
                ),
                Some(
                    controls::field()
                        .header(controls::secondary("Check command"))
                        .automation_name("Check command")
                        .is_enabled(!stale)
                        .text(draft.check.clone())
                        .placeholder_text("cargo test")
                        .on_text_changed(crate::views::text_edits(
                            context.callback(|text| Message::Goal(GoalMsg::Check(text))),
                        ))
                        .automation_help_text(CHECK_HELP)
                        .tooltip(CHECK_HELP),
                ),
                Some(
                    limits
                        .automation_help_text(LIMITS_HELP)
                        .tooltip(LIMITS_HELP),
                ),
                error,
            ]))
            .into()
    }

    /// Clear's question in the footer: Keep goal (the keyboard's) or Clear
    /// goal.
    fn goal_clear_footer(&self, context: &mut ViewContext<Self>) -> View {
        Grid::new()
            .columns([GridLength::STAR, GridLength::Auto])
            .column_spacing(tokens::spacing::SM)
            .children((
                controls::body(
                    "Clear this goal? No more rounds or checks run for it; the task and its work stay.",
                )
                    .vertical_alignment(VerticalAlignment::Center)
                    .text_wrapping(TextWrapping::Wrap),
                Border::new().grid_column(1).content(super::dialog_ui::actions(
                    controls::text_button()
                        .element_ref(&self.dialog.close)
                        .automation_name("Keep goal")
                        .on_click(context.message(Message::Goal(GoalMsg::ClearAnswered(false))))
                        .content(controls::label("Keep goal"))
                        .into(),
                    Some(
                        controls::destructive()
                            .automation_name("Clear goal")
                            .on_click(context.message(Message::Goal(GoalMsg::ClearAnswered(true))))
                            .content(controls::label("Clear goal"))
                            .into(),
                    ),
                )),
            ))
            .into()
    }

    /// The footer: Pause or Resume and Clear at the left, as the goal's
    /// status allows them; Cancel and Save goal at the right.
    fn goal_footer(
        &self,
        open: &GoalDialog,
        goal: Option<Option<&GoalView>>,
        busy: bool,
        context: &mut ViewContext<Self>,
    ) -> View {
        let controls_now = goal.flatten().map_or(&[][..], |goal| allowed(goal.status));
        let working = |step: Step, idle: &str| -> String {
            if open.busy == Some(step) {
                format!("{}…", step.label())
            } else {
                idle.to_owned()
            }
        };
        let left: Vec<KeyedView> = controls_now
            .iter()
            .map(|control| {
                let words = match control {
                    client::GoalControl::Clear => "Clear…".to_owned(),
                    other => working(Step::Control(*other), other.label()),
                };
                KeyedView::new(
                    control.label(),
                    controls::outlined()
                        .is_enabled(!busy)
                        .automation_name(format!("{} goal", control.label()))
                        .on_click(context.message(Message::Goal(GoalMsg::Control(*control))))
                        .content(controls::label(&words)),
                )
            })
            .collect();
        let save = controls::filled()
            .is_enabled(!busy && goal.is_some())
            .automation_name("Save goal")
            .on_click(context.message(Message::Goal(GoalMsg::Save)))
            .content(controls::label(&working(Step::Save, "Save goal")));
        Grid::new()
            .columns([GridLength::Auto, GridLength::STAR, GridLength::Auto])
            .column_spacing(tokens::spacing::SM)
            .children((
                StackPanel::new()
                    .orientation(Orientation::Horizontal)
                    .spacing(tokens::spacing::SM)
                    .keyed_children(left),
                Border::new()
                    .grid_column(2)
                    .content(super::dialog_ui::actions(
                        controls::text_button()
                            .element_ref(&self.dialog.close)
                            .automation_name("Cancel goal")
                            .on_click(context.message(Message::Goal(GoalMsg::Closed)))
                            .content(controls::label("Cancel"))
                            .into(),
                        Some(save.into()),
                    )),
            ))
            .into()
    }
}
