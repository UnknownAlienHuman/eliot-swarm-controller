//! The Goal dialog (brand book §18.1 Goal row, §11.3): one native
//! ContentDialog, opened by a task menu's "Goal…" and Details › Goal's
//! "Set goal…" or "Edit goal…". It holds the goal text, the check command
//! (an explicit argv that runs in the task's worktree) and the limits in
//! their source units (rounds, turns, same-result rounds), and offers Save,
//! Pause, Resume and Clear as the daemon allows them for the goal's status
//! (`goal.set`, `goal.pause`, `goal.resume`, `goal.clear`; no other API).
//!
//! The goal shown is the open conversation's (`chat.goal`, read with every
//! detail read); the dialog keeps only the owner's edits, so a goal that
//! changes meanwhile shows as it is until a field is edited. Save checks
//! the request with its own validation before it is sent, and every
//! validation error and refusal shows in the dialog. Save is named and is
//! never the default for Enter: the dialog opens with the keyboard in the
//! goal text, Enter there starts a new line, and Esc and Cancel close it.
//! Clear asks first, in the same dialog.

use super::*;
use eliot_core::api::{GoalDefinition, GoalSetRequest, GoalStatus, GoalView};

/// The most a round, turn or same-result limit may be (`MAX_GOAL_LIMIT`).
const MAX_LIMIT: f64 = eliot_core::api::MAX_GOAL_LIMIT as f64;

#[derive(Clone, Debug)]
pub(super) enum GoalMsg {
    /// Open the dialog for this Agent (its conversation opens first).
    Open(String),
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

impl Draft {
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
            None => Self {
                text: String::new(),
                check: String::new(),
                rounds: None,
                turns: None,
                same: None,
            },
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
            return Err(
                "Give the check command: the program that proves the goal is met, such as cargo test."
                    .to_owned(),
            );
        }
        let limit = |value: Option<f64>, name: &str| -> Result<Option<i64>, String> {
            match value {
                None => Ok(None),
                Some(value) if value.fract() == 0.0 && (1.0..=MAX_LIMIT).contains(&value) => {
                    Ok(Some(value as i64))
                }
                Some(_) => Err(format!("{name} takes a whole number from 1 to 1,000,000.")),
            }
        };
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
            return Err(
                "Set a round or turn limit: Swarm runs the goal on every idle turn until one is reached."
                    .to_owned(),
            );
        }
        Ok(definition)
    }
}

/// The open dialog: the Agent it acts on and what the owner did in it.
pub(super) struct GoalDialog {
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

/// The goal's state in a few words, for the dialog's context line.
fn status_words(goal: &GoalView) -> String {
    let state = match goal.status {
        GoalStatus::Active => "Active".to_owned(),
        GoalStatus::Paused => match goal.paused_reason.as_deref() {
            Some(reason) => format!("Paused · {reason}"),
            None => "Paused".to_owned(),
        },
        GoalStatus::Blocked => match goal.blocked_reason.as_deref() {
            Some(reason) => format!("Blocked · {}", eliot_core::reason_words(reason)),
            None => "Blocked".to_owned(),
        },
        GoalStatus::Completed => "Met".to_owned(),
        GoalStatus::Cleared => "Cleared".to_owned(),
    };
    let rounds = match goal.definition.max_rounds {
        Some(max) => format!("round {} of {max}", goal.counters.rounds),
        None => format!("round {}", goal.counters.rounds),
    };
    format!("{state} · {rounds}")
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
                let Some(agent) = self.goal.open.as_ref().map(|open| open.agent.clone()) else {
                    return;
                };
                let current = Draft::of(self.dialog_goal(&agent).flatten());
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
            GoalMsg::Done(agent, step, result) => self.goal_done(agent, step, *result),
        }
    }

    /// Save: the edited goal, checked here, then `goal.set`.
    fn save_goal(&mut self, context: &ComponentContext<Self>) {
        let Some(open) = self.goal.open.as_ref() else {
            return;
        };
        let goal = self.dialog_goal(&open.agent).flatten();
        let draft = open.draft.clone().unwrap_or_else(|| Draft::of(goal));
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
        let Ok(data_dir) = self.data_dir.clone() else {
            return;
        };
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
    /// closes the dialog, a refusal stays in it.
    fn goal_done(&mut self, agent: String, step: Step, result: MutationResult<GoalView>) {
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
                if self.selected_agent_id().as_deref() == Some(agent.as_str()) {
                    self.chat.goal = Some((agent, Some(goal)));
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
    }

    /// The Goal dialog: its context line, the fields, the error and the
    /// footer of the controls the goal's status allows.
    pub(super) fn goal_dialog(&self, context: &mut ViewContext<Self>) -> Option<ContentDialog> {
        let open = self.goal.open.as_ref()?;
        let goal = self.dialog_goal(&open.agent);
        let current = goal.flatten();
        let draft = open.draft.clone().unwrap_or_else(|| Draft::of(current));
        let busy = open.busy.is_some();
        let context_line = match goal {
            None => "Reading the goal…".to_owned(),
            Some(None) => {
                "No goal yet. Swarm runs the task in rounds until the check passes or a limit is reached."
                    .to_owned()
            }
            Some(Some(goal)) => status_words(goal),
        };
        let number = |header: &str, value: Option<f64>, message: fn(Option<f64>) -> GoalMsg| {
            NumberBox::new()
                .header(controls::secondary(header))
                .automation_name(header)
                .minimum(1.0)
                .maximum(MAX_LIMIT)
                .value(value)
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
        let error: Option<View> = open.error.clone().map(|error| {
            controls::body(error)
                .foreground(ThemeBrush::SystemCritical)
                .text_wrapping(TextWrapping::Wrap)
                .is_text_selection_enabled(true)
                .into()
        });
        let body = StackPanel::new()
            .spacing(tokens::spacing::MD)
            .keyed_children(crate::views::slots([
                Some(
                    controls::secondary(context_line)
                        .text_wrapping(TextWrapping::Wrap)
                        .into(),
                ),
                Some(
                    controls::field()
                        .element_ref(&self.dialog.field)
                        .header(controls::secondary("Goal"))
                        .automation_name("Goal")
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
                    StackPanel::new()
                        .spacing(tokens::spacing::XS)
                        .children((
                            controls::field()
                                .header(controls::secondary("Check command"))
                                .automation_name("Check command")
                                .text(draft.check.clone())
                                .placeholder_text("cargo test")
                                .on_text_changed(crate::views::text_edits(
                                    context.callback(|text| Message::Goal(GoalMsg::Check(text))),
                                )),
                            controls::secondary(
                                "Runs in the task's worktree without a shell; exit 0 means the goal is met.",
                            )
                            .text_wrapping(TextWrapping::Wrap),
                        ))
                        .into(),
                ),
                Some(
                    StackPanel::new()
                        .spacing(tokens::spacing::XS)
                        .children((
                            limits,
                            controls::secondary(
                                "Limits stop the goal: rounds of checks, the task's turns, and rounds in a row with the same result. Set rounds or turns; empty means no limit.",
                            )
                            .text_wrapping(TextWrapping::Wrap),
                        ))
                        .into(),
                ),
                error,
            ]))
            .into();
        let footer = if open.confirm_clear {
            self.goal_clear_footer(context)
        } else {
            self.goal_footer(open, goal, busy, context)
        };
        let heading = match current {
            Some(_) => format!("Goal for “{}”", open.title),
            None => format!("Set a goal for “{}”", open.title),
        };
        Some(super::dialog_ui::surface(
            context,
            self.dialog.epoch,
            heading,
            body,
            footer,
            tokens::layout::GOAL_DIALOG_WIDTH,
        ))
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
