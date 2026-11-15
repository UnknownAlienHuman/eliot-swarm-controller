//! Glue for the Hand off / Switch dialog: its messages, background reads and
//! submissions, and the `ContentDialog` view. The rules (send selection,
//! result facts, gate meanings, when "finish" applies) live in
//! [`crate::handoff`].
//!
//! Every destination is its own button, bound when it is built to what it
//! shows (B and its generation, the profile, or the ad-hoc harness, model
//! and effort): no picker index decides where work goes.

use super::*;
use crate::handoff::{self, Outcome, SendRow, Submitted};
use crate::theme::winui::Palette;
use eliot_core::CommandState;
use eliot_core::api::{
    CommandResolution, HandoffRequest, HarnessModelsResponse, SwitchLaunch, SwitchRequest,
};

/// Same-ID readbacks of an orchestrating switch: one every two seconds for
/// at most five minutes, then "Refresh the result" reads again on request.
const SWITCH_POLL_MS: u64 = 2_000;
const SWITCH_POLLS: u32 = 150;
const HARNESS_DEFAULT: &str = "Harness default";

/// Where the transfer goes, bound at the button.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Target {
    Existing {
        agent: String,
        generation: i64,
    },
    Profile(String),
    Adhoc {
        harness: String,
        model: Option<String>,
        effort: Option<String>,
    },
}

#[derive(Clone, Debug)]
pub(super) enum HandoffMsg {
    Open {
        agent: String,
        generation: i64,
    },
    /// Epoch, the rows of the bound generation, and how many older SENDs of
    /// the Agent were not read (beyond `client::MAX_LISTED_SENDS`).
    SendsLoaded(u64, Fallible<Vec<SendRow>>, usize),
    Closed,
    TaskPicked(Option<usize>),
    SendToggled(String, bool),
    HarnessPicked(Option<usize>),
    CatalogLoaded(u64, String, Box<Fallible<HarnessModelsResponse>>),
    ModelPicked(Option<usize>),
    EffortPicked(Option<usize>),
    Submit(Target),
    Retry,
    /// Epoch, whether this reply answers a submission (and so settles its
    /// ledger entry, even after the dialog closed), the mutation id it
    /// answers, and the reply.
    Replied(u64, bool, String, Box<Fallible<Outcome>>),
    RefreshRequested,
    ChildrenLoaded(u64, Fallible<Vec<Command>>),
    ReasonChanged(String),
    ResolveRequested(String, CommandResolution),
    CancelRequested(String),
    OwnerDone(u64, String, Fallible<String>),
    Finish {
        agent: String,
        generation: i64,
    },
}

#[derive(Clone, Debug, Default)]
pub(super) struct DialogState {
    open: bool,
    /// Tags one opening; replies from an earlier one are dropped.
    epoch: u64,
    from: String,
    from_generation: i64,
    sends: Option<Fallible<Vec<SendRow>>>,
    task: Option<String>,
    /// Selected SEND ids, kept in admission order.
    chosen: Vec<String>,
    harness: Option<usize>,
    catalog: Option<(String, Fallible<HarnessModelsResponse>)>,
    model: Option<usize>,
    effort: Option<usize>,
    /// The exact request (with its id) last submitted: same-ID readback.
    submitted: Option<Submitted>,
    /// The last submission's reply was uncertain: only "Retry with the same
    /// id" is offered until the daemon answers it.
    uncertain: bool,
    /// The switch request a "Finish" handoff replaced; restored if the
    /// daemon definitely refuses the finish.
    prior: Option<Submitted>,
    outcome: Option<Outcome>,
    polls: u32,
    children: Vec<Command>,
    reason: String,
    status: String,
    owner_status: String,
}

impl DesktopApp {
    pub(super) fn handoff_update(&mut self, msg: HandoffMsg, context: &ComponentContext<Self>) {
        match msg {
            HandoffMsg::Open { agent, generation } => {
                // A request still in flight or left uncertain stays bound to
                // its id across reopening: the dialog comes back with it
                // instead of starting over.
                let pending = self.handoff.submitted.as_ref().is_some_and(|submitted| {
                    self.handoff.uncertain
                        || self.in_flight_key.as_deref() == Some(submitted.key().as_str())
                });
                if pending
                    && self.handoff.from == agent
                    && self.handoff.from_generation == generation
                {
                    self.handoff.open = true;
                    return;
                }
                let epoch = self.handoff.epoch + 1;
                self.handoff = DialogState {
                    open: true,
                    epoch,
                    from: agent.clone(),
                    from_generation: generation,
                    status: format!("Reading the SENDs of {agent} generation {generation}…"),
                    ..DialogState::default()
                };
                let Ok(data_dir) = self.data_dir.clone() else {
                    return;
                };
                let _task = context.spawn_background(move |_| {
                    let read = client::block_on(client::agent_sends(&data_dir, &agent))
                        .map_err(|error| error.to_string());
                    let omitted = read.as_ref().map_or(0, |(_, omitted)| *omitted);
                    let sends = read.map(|(commands, _)| {
                        commands
                            .iter()
                            .filter_map(|command| handoff::send_row(command, generation))
                            .collect()
                    });
                    Message::Handoff(HandoffMsg::SendsLoaded(epoch, sends, omitted))
                });
            }
            HandoffMsg::SendsLoaded(epoch, sends, omitted) => {
                if epoch != self.handoff.epoch {
                    return;
                }
                self.handoff.status = match &sends {
                    Ok(rows) => format!(
                        "{} SEND(s) on this generation, {} undispatched.{}",
                        rows.len(),
                        rows.iter().filter(|row| row.transferable()).count(),
                        handoff::omitted_note(omitted, client::MAX_LISTED_SENDS)
                    ),
                    Err(error) => format!("SENDs not read: {error}"),
                };
                if let Ok(rows) = &sends {
                    self.handoff.task = rows.first().map(|row| row.id.clone());
                }
                self.handoff.sends = Some(sends);
            }
            HandoffMsg::Closed => {
                self.handoff.open = false;
                self.handoff.epoch += 1;
            }
            HandoffMsg::TaskPicked(index) => {
                if let (Some(index), Some(Ok(rows))) = (index, &self.handoff.sends)
                    && let Some(row) = rows.get(index)
                {
                    self.handoff.task = Some(row.id.clone());
                }
            }
            HandoffMsg::SendToggled(id, checked) => {
                let Some(Ok(rows)) = &self.handoff.sends else {
                    return;
                };
                let mut chosen = self.handoff.chosen.clone();
                chosen.retain(|known| known != &id);
                if checked {
                    chosen.push(id);
                }
                // Successor order is admission order on the source.
                self.handoff.chosen = rows
                    .iter()
                    .filter(|row| chosen.contains(&row.id))
                    .map(|row| row.id.clone())
                    .collect();
            }
            HandoffMsg::HarnessPicked(index) => {
                let Some(harness) = index.and_then(|index| ADHOC_HARNESSES.get(index)) else {
                    return;
                };
                if self.handoff.harness == index {
                    return;
                }
                self.handoff.harness = index;
                self.handoff.model = Some(0);
                self.handoff.effort = Some(0);
                self.handoff.catalog = None;
                let Ok(data_dir) = self.data_dir.clone() else {
                    return;
                };
                let (epoch, harness) = (self.handoff.epoch, (*harness).to_owned());
                let _task = context.spawn_background(move |_| {
                    let reply = client::block_on(client::harness_models(&data_dir, &harness, None))
                        .map_err(|error| error.to_string());
                    Message::Handoff(HandoffMsg::CatalogLoaded(epoch, harness, Box::new(reply)))
                });
            }
            HandoffMsg::CatalogLoaded(epoch, harness, reply) => {
                let reply = *reply;
                let current = self
                    .handoff
                    .harness
                    .and_then(|index| ADHOC_HARNESSES.get(index))
                    .is_some_and(|picked| *picked == harness);
                if epoch == self.handoff.epoch && current {
                    self.handoff.catalog = Some((harness, reply));
                }
            }
            HandoffMsg::ModelPicked(index) => {
                if index.is_some() && index != self.handoff.model {
                    self.handoff.model = index;
                    self.handoff.effort = Some(0);
                }
            }
            HandoffMsg::EffortPicked(index) => {
                if index.is_some() {
                    self.handoff.effort = index;
                }
            }
            HandoffMsg::Submit(target) => {
                if self.submitting()
                    || self.handoff.uncertain
                    || self.handoff.submitted.is_some()
                    || self.handoff.outcome.is_some()
                {
                    return;
                }
                match self.handoff_request(target) {
                    Ok(submitted) => self.handoff_send(submitted, context),
                    Err(error) => self.handoff.status = error,
                }
            }
            HandoffMsg::Retry => {
                // Only an uncertain reply keeps the request: the same id.
                if let Some(submitted) = self.handoff.submitted.clone()
                    && self.handoff.uncertain
                    && !self.submitting()
                {
                    self.handoff_send(submitted, context);
                }
            }
            HandoffMsg::Finish { agent, generation } => {
                // An ordinary handoff to the created Agent with the switch's
                // own task and selection: a new action with a new id, never a
                // retry of the switch id.
                let Some(Submitted::Switch(switch)) = self.handoff.submitted.clone() else {
                    return;
                };
                if self.submitting() || self.handoff.uncertain {
                    return;
                }
                let mut submitted = Submitted::Handoff(HandoffRequest {
                    id: String::new(),
                    from: switch.from,
                    from_generation: switch.from_generation,
                    to: agent,
                    to_generation: generation,
                    task_command: switch.task_command,
                    commands: switch.commands,
                });
                let id = self.ledger.id_for(&submitted.key());
                if let Submitted::Handoff(request) = &mut submitted {
                    request.id = id;
                }
                // The switch result stays on screen until the daemon answers.
                self.handoff.prior = self.handoff.submitted.clone();
                self.handoff_send(submitted, context);
            }
            HandoffMsg::Replied(epoch, settles, id, reply) => {
                let reply = *reply;
                let uncertain = reply
                    .as_ref()
                    .err()
                    .is_some_and(|error| crate::state::uncertain(error));
                // Only the reply to a submission settles its ledger entry; a
                // readback never touches another action in flight.
                if settles {
                    self.settle_mutation(!uncertain);
                }
                // A reply belongs to the request it answers, not to a dialog
                // opening: the answer to a submission lands on it even after
                // the dialog was closed, and a reply for a request no longer
                // shown (a switch replaced by its finish) changes nothing.
                let Some(submitted) = self.handoff.submitted.clone() else {
                    return;
                };
                if !handoff::reply_applies(
                    submitted.id(),
                    &id,
                    settles,
                    epoch == self.handoff.epoch,
                ) {
                    return;
                }
                match reply {
                    Ok(outcome) => {
                        // Any definite answer for this id, the submission's or
                        // a same-id readback's, ends the uncertainty.
                        if !settles && self.handoff.uncertain {
                            self.ledger.complete(&submitted.key());
                        }
                        self.handoff.uncertain = false;
                        self.handoff.prior = None;
                        self.handoff.status = "Read back from the daemon.".to_owned();
                        let pending = outcome.switch_pending();
                        let handoff_id = outcome.handoff().map(|reply| reply.id.clone());
                        self.handoff.outcome = Some(outcome);
                        if let Some(id) = handoff_id {
                            self.load_handoff_children(&id, context);
                        }
                        if pending && self.handoff.open && self.handoff.polls < SWITCH_POLLS {
                            self.handoff.polls += 1;
                            self.handoff_readback(SWITCH_POLL_MS, false, context);
                        } else if pending {
                            self.handoff.status = "The switch is still orchestrating; Refresh the result reads it again with the same id.".to_owned();
                        }
                    }
                    Err(error) if uncertain => {
                        if settles {
                            self.handoff.uncertain = true;
                        }
                        self.handoff.status = format!(
                            "Outcome uncertain; Retry sends the same request with the same id, never a new one: {error}"
                        );
                    }
                    Err(error) => {
                        // Definitely refused: a later action gets a new id. A
                        // refused finish brings the switch back for readback.
                        if settles {
                            self.handoff.uncertain = false;
                            if let Some(prior) = self.handoff.prior.take() {
                                self.handoff.submitted = Some(prior);
                            } else if self.handoff.outcome.is_none() {
                                self.handoff.submitted = None;
                            }
                        }
                        self.handoff.status = format!("Refused: {error}");
                    }
                }
            }
            HandoffMsg::RefreshRequested => {
                self.handoff.polls = 0;
                self.handoff_readback(0, false, context);
            }
            HandoffMsg::ChildrenLoaded(epoch, children) => {
                if epoch != self.handoff.epoch {
                    return;
                }
                match children {
                    Ok(children) => self.handoff.children = children,
                    Err(error) => {
                        self.handoff.owner_status = format!("Handoff inputs not read: {error}")
                    }
                }
            }
            HandoffMsg::ReasonChanged(text) => self.handoff.reason = text,
            HandoffMsg::ResolveRequested(command, resolution) => {
                let reason = self.handoff.reason.trim().to_owned();
                if reason.is_empty() {
                    self.handoff.owner_status =
                        "Write the reason for this Owner assertion first.".to_owned();
                    return;
                }
                let key = format!("resolve:{command}:{}", resolution.as_str());
                let label = format!("Resolve {command} as {}", resolution.as_str());
                self.handoff_owner_action(key, label, context, move |data_dir, id| {
                    client::block_on(client::resolve_command(
                        data_dir, id, &command, resolution, reason,
                    ))
                    .map(|reply| {
                        format!(
                            "{} is {}",
                            reply.command_id,
                            handoff::state_name(reply.state)
                        )
                    })
                    .map_err(|error| error.to_string())
                });
            }
            HandoffMsg::CancelRequested(command) => {
                let key = format!("cancel:{command}");
                let label = format!("Withdraw {command}");
                self.handoff_owner_action(key, label, context, move |data_dir, id| {
                    client::block_on(client::cancel_command(data_dir, id, &command))
                        .map(|reply| {
                            format!(
                                "{} is {}",
                                reply.command_id,
                                handoff::state_name(reply.state)
                            )
                        })
                        .map_err(|error| error.to_string())
                });
            }
            HandoffMsg::OwnerDone(epoch, label, result) => {
                let uncertain = result
                    .as_ref()
                    .err()
                    .is_some_and(|error| crate::state::uncertain(error));
                self.settle_mutation(!uncertain);
                if epoch != self.handoff.epoch {
                    return;
                }
                self.handoff.owner_status = match result {
                    Ok(state) => format!("{label}: {state}."),
                    Err(error) if uncertain => format!(
                        "{label}: outcome uncertain (the same button reuses its id): {error}"
                    ),
                    Err(error) => format!("{label} refused: {error}"),
                };
                self.handoff_readback(0, false, context);
            }
        }
    }

    /// Live, non-source Agents of the navigation list.
    fn handoff_targets(&self) -> Vec<&UiAgent> {
        self.nav
            .agents
            .iter()
            .filter(|agent| agent.id != self.handoff.from)
            .filter(|agent| {
                !matches!(
                    agent.state.as_str(),
                    "stopped" | "failed" | "lost" | "unknown"
                )
            })
            .collect()
    }

    fn handoff_models(&self) -> Vec<String> {
        let mut options = vec![HARNESS_DEFAULT.to_owned()];
        if let Some((_, Ok(reply))) = &self.handoff.catalog {
            options.extend(reply.models.iter().map(|model| model.id.clone()));
        }
        options
    }

    fn handoff_efforts(&self) -> Vec<String> {
        let mut options = vec![HARNESS_DEFAULT.to_owned()];
        let model = configure::picked(&self.handoff_models(), self.handoff.model);
        if let (Some((_, Ok(reply))), Some(model)) = (&self.handoff.catalog, model)
            && let Some(entry) = reply.models.iter().find(|entry| entry.id == model)
        {
            options.extend(entry.efforts.iter().flatten().cloned());
        }
        options
    }

    /// The request for `target`, with the task and selection on screen and
    /// one ledger id per distinct action.
    fn handoff_request(&mut self, target: Target) -> Result<Submitted, String> {
        let state = &self.handoff;
        handoff::check_selection(state.task.as_deref(), &state.chosen)?;
        let task_command = state.task.clone().unwrap_or_default();
        let commands = state.chosen.clone();
        let launch = |profile: Option<String>,
                      harness: Option<String>,
                      model: Option<String>,
                      effort: Option<String>| SwitchLaunch {
            profile,
            harness,
            model,
            effort,
            auth: None,
            cmd: None,
            workspace: None,
            worktree: None,
            base: None,
        };
        let switch = |launch: SwitchLaunch| {
            Submitted::Switch(SwitchRequest {
                id: String::new(),
                from: state.from.clone(),
                from_generation: state.from_generation,
                task_command: task_command.clone(),
                commands: commands.clone(),
                launch,
            })
        };
        let mut submitted = match target {
            Target::Existing { agent, generation } => Submitted::Handoff(HandoffRequest {
                id: String::new(),
                from: state.from.clone(),
                from_generation: state.from_generation,
                to: agent,
                to_generation: generation,
                task_command: task_command.clone(),
                commands: commands.clone(),
            }),
            Target::Profile(profile) => switch(launch(Some(profile), None, None, None)),
            Target::Adhoc {
                harness,
                model,
                effort,
            } => switch(launch(None, Some(harness), model, effort)),
        };
        let id = self.ledger.id_for(&submitted.key());
        match &mut submitted {
            Submitted::Handoff(request) => request.id = id,
            Submitted::Switch(request) => request.id = id,
        }
        Ok(submitted)
    }

    fn handoff_send(&mut self, submitted: Submitted, context: &ComponentContext<Self>) {
        self.in_flight_key = Some(submitted.key());
        self.handoff.status = format!("Submitting {}…", submitted.id());
        self.handoff.submitted = Some(submitted);
        self.handoff.polls = 0;
        self.handoff_readback(0, true, context);
    }

    /// Send the retained request again with its own id after `delay_ms`:
    /// the daemon returns the committed result and the current gate readback.
    fn handoff_readback(&mut self, delay_ms: u64, settles: bool, context: &ComponentContext<Self>) {
        let (Some(submitted), Ok(data_dir)) =
            (self.handoff.submitted.clone(), self.data_dir.clone())
        else {
            return;
        };
        let epoch = self.handoff.epoch;
        let id = submitted.id().to_owned();
        let _task = context.spawn_background(move |cancel| {
            let cancelled = || cancel.is_cancelled();
            if delay_ms > 0 && !feed::backoff(delay_ms, &cancelled) {
                return Message::Handoff(HandoffMsg::Replied(
                    epoch,
                    settles,
                    id,
                    Box::new(Err("unknown: readback cancelled".to_owned())),
                ));
            }
            let reply = client::block_on(async {
                match &submitted {
                    Submitted::Handoff(request) => client::handoff(&data_dir, request)
                        .await
                        .map(Outcome::Handoff),
                    Submitted::Switch(request) => client::switch(&data_dir, request)
                        .await
                        .map(Outcome::Switch),
                }
            })
            .map_err(|error| error.to_string());
            Message::Handoff(HandoffMsg::Replied(epoch, settles, id, Box::new(reply)))
        });
    }

    fn load_handoff_children(&self, id: &str, context: &ComponentContext<Self>) {
        let Ok(data_dir) = self.data_dir.clone() else {
            return;
        };
        let (epoch, id) = (self.handoff.epoch, id.to_owned());
        let _task = context.spawn_background(move |_| {
            let children = client::block_on(client::command_children(&data_dir, &id))
                .map_err(|error| error.to_string());
            Message::Handoff(HandoffMsg::ChildrenLoaded(epoch, children))
        });
    }

    fn handoff_owner_action(
        &mut self,
        key: String,
        label: String,
        context: &ComponentContext<Self>,
        work: impl FnOnce(&Path, String) -> Fallible<String> + Send + 'static,
    ) {
        if self.submitting() {
            return;
        }
        let Ok(data_dir) = self.data_dir.clone() else {
            return;
        };
        let mutation_id = self.ledger.id_for(&key);
        self.in_flight_key = Some(key);
        self.handoff.owner_status = format!("{label} ({mutation_id})…");
        let epoch = self.handoff.epoch;
        let _task = context.spawn_background(move |_| {
            Message::Handoff(HandoffMsg::OwnerDone(
                epoch,
                label,
                work(&data_dir, mutation_id),
            ))
        });
    }

    /// The dialog; an empty view while closed.
    pub(super) fn handoff_dialog(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        let state = &self.handoff;
        if !state.open {
            return View::empty();
        }
        let rows: &[SendRow] = match &state.sends {
            Some(Ok(rows)) => rows,
            _ => &[],
        };
        // New actions only from a clean state: no uncertain request pending,
        // no result shown, nothing in flight.
        let can_act = !self.submitting()
            && !state.uncertain
            && state.submitted.is_none()
            && state.outcome.is_none()
            && state.task.is_some();
        let mut sections: Vec<KeyedView> = Vec::new();
        let mut push = |key: &str, view: View| sections.push(KeyedView::new(key.to_owned(), view));
        push("status", small(state.status.clone()).into());
        push(
            "task-label",
            small("Original task SEND (its text opens the brief):").into(),
        );
        let task_labels: Vec<String> = rows.iter().map(SendRow::label).collect();
        push(
            &format!("task:{}", task_labels.join("\u{1f}")),
            ComboBox::new()
                .items_source(task_labels.clone())
                .selected_index(
                    state
                        .task
                        .as_ref()
                        .and_then(|task| rows.iter().position(|row| &row.id == task)),
                )
                .is_enabled(can_act)
                .automation_name("Original task SEND")
                .on_selection_changed(
                    context.callback(|index| Message::Handoff(HandoffMsg::TaskPicked(index))),
                )
                .into(),
        );
        let transferable: Vec<&SendRow> = rows.iter().filter(|row| row.transferable()).collect();
        push(
            "sends-label",
            small(format!(
                "Undispatched SENDs to move (at most {}; the rest stay on {}):",
                eliot_core::api::MAX_HANDOFF_SENDS,
                state.from
            ))
            .into(),
        );
        if transferable.is_empty() {
            push(
                "no-sends",
                small("No accepted or queued SENDs on this generation: the transfer carries context, claims, Goal and children only.")
                    .opacity(theme::MUTED_OPACITY)
                    .into(),
            );
        }
        for row in transferable {
            let id = row.id.clone();
            push(
                &format!("send-{}", row.id),
                CheckBox::new()
                    .is_checked(state.chosen.contains(&row.id))
                    .is_enabled(can_act)
                    .automation_name(format!("Move {}", row.label()))
                    .on_is_checked_changed(context.callback(move |checked| {
                        Message::Handoff(HandoffMsg::SendToggled(id.clone(), checked))
                    }))
                    .content(small(row.label())),
            );
        }
        push(
            "summary",
            small(format!(
                "Bound: {} generation {} · task {} · moving {}",
                state.from,
                state.from_generation,
                state.task.as_deref().unwrap_or("not chosen"),
                if state.chosen.is_empty() {
                    "no SENDs".to_owned()
                } else {
                    state.chosen.join(", ")
                }
            ))
            .into(),
        );
        push(
            "existing-label",
            heading("Hand off to an existing Agent").into(),
        );
        let targets: Vec<KeyedView> = self
            .handoff_targets()
            .into_iter()
            .map(|agent| {
                let label = format!(
                    "Hand off to {} ({}, {}, generation {})",
                    agent.id, agent.state, agent.harness, agent.generation
                );
                KeyedView::new(
                    agent.id.clone(),
                    action(
                        label,
                        context.message(Message::Handoff(HandoffMsg::Submit(Target::Existing {
                            agent: agent.id.clone(),
                            generation: agent.generation,
                        }))),
                        can_act,
                        palette,
                    ),
                )
            })
            .collect();
        push(
            "existing",
            StackPanel::new()
                .spacing(theme::spacing::XS)
                .keyed_children(targets),
        );
        push(
            "profile-label",
            heading("Switch to a new Agent from a profile").into(),
        );
        let profiles: Vec<KeyedView> = self
            .launch
            .profile_summaries
            .iter()
            .map(|profile| {
                KeyedView::new(
                    profile.name.clone(),
                    action(
                        format!(
                            "Switch to a new {} Agent ({})",
                            profile.name, profile.harness
                        ),
                        context.message(Message::Handoff(HandoffMsg::Submit(Target::Profile(
                            profile.name.clone(),
                        )))),
                        can_act,
                        palette,
                    ),
                )
            })
            .collect();
        push(
            "profiles",
            StackPanel::new()
                .spacing(theme::spacing::XS)
                .keyed_children(profiles),
        );
        push(
            "adhoc-label",
            heading("Switch to a new ad-hoc Agent").into(),
        );
        let models = self.handoff_models();
        let efforts = self.handoff_efforts();
        let harness = state
            .harness
            .and_then(|index| ADHOC_HARNESSES.get(index))
            .map(|harness| (*harness).to_owned());
        let model = configure::picked(&models, state.model);
        let effort = configure::picked(&efforts, state.effort);
        let adhoc_label = match &harness {
            Some(harness) => format!(
                "Switch to a new ad-hoc {harness} Agent · model {} · effort {}",
                model.as_deref().unwrap_or("harness default"),
                effort.as_deref().unwrap_or("harness default")
            ),
            None => "Pick a harness for the ad-hoc Agent".to_owned(),
        };
        let adhoc_target = harness.clone().map(|harness| Target::Adhoc {
            harness,
            model: model.clone(),
            effort: effort.clone(),
        });
        let catalog_line = match &state.catalog {
            Some((harness, Ok(reply))) => format!(
                "{harness} catalogue: {} model(s){}",
                reply.models.len(),
                reply
                    .note
                    .as_deref()
                    .map(|note| format!(" · {note}"))
                    .unwrap_or_default()
            ),
            Some((harness, Err(error))) => format!("{harness} catalogue unavailable: {error}"),
            None => "Pick a harness to read its catalogue.".to_owned(),
        };
        push(
            "adhoc",
            StackPanel::new()
                .orientation(Orientation::Horizontal)
                .spacing(theme::spacing::XS)
                .keyed_children(vec![
                    KeyedView::new(
                        "harness",
                        ComboBox::new()
                            .items_source(ADHOC_HARNESSES.map(str::to_owned).to_vec())
                            .selected_index(state.harness)
                            .is_enabled(can_act)
                            .placeholder_text("Harness")
                            .automation_name("Switch harness")
                            .on_selection_changed(context.callback(|index| {
                                Message::Handoff(HandoffMsg::HarnessPicked(index))
                            })),
                    ),
                    // Option lists that change get a new control (see
                    // `configure::panel`).
                    KeyedView::new(
                        format!("model:{}", models.join("\u{1f}")),
                        ComboBox::new()
                            .items_source(models.clone())
                            .selected_index(state.model)
                            .is_enabled(can_act)
                            .automation_name("Switch model")
                            .on_selection_changed(context.callback(|index| {
                                Message::Handoff(HandoffMsg::ModelPicked(index))
                            })),
                    ),
                    KeyedView::new(
                        format!("effort:{}", efforts.join("\u{1f}")),
                        ComboBox::new()
                            .items_source(efforts.clone())
                            .selected_index(state.effort)
                            .is_enabled(can_act)
                            .automation_name("Switch effort")
                            .on_selection_changed(context.callback(|index| {
                                Message::Handoff(HandoffMsg::EffortPicked(index))
                            })),
                    ),
                ]),
        );
        push(
            "catalog",
            small(catalog_line).opacity(theme::MUTED_OPACITY).into(),
        );
        push(
            "adhoc-submit",
            match adhoc_target {
                Some(target) => action(
                    adhoc_label,
                    context.message(Message::Handoff(HandoffMsg::Submit(target))),
                    can_act,
                    palette,
                ),
                None => small(adhoc_label).opacity(theme::MUTED_OPACITY).into(),
            },
        );
        if let Some(submitted) = &state.submitted {
            let retry = state.uncertain;
            push(
                "readback",
                StackPanel::new()
                    .orientation(Orientation::Horizontal)
                    .spacing(theme::spacing::SM)
                    .children((
                        action(
                            format!("Retry {} with the same id", submitted.id()),
                            context.message(Message::Handoff(HandoffMsg::Retry)),
                            retry && !self.submitting(),
                            palette,
                        ),
                        action(
                            "Refresh the result".to_owned(),
                            context.message(Message::Handoff(HandoffMsg::RefreshRequested)),
                            !self.submitting(),
                            palette,
                        ),
                    )),
            );
        }
        if let Some(outcome) = &state.outcome {
            let lines = match outcome {
                Outcome::Handoff(reply) => handoff::handoff_lines(reply),
                Outcome::Switch(reply) => handoff::switch_lines(reply),
            };
            push("result", small(lines.join("\n")).into());
            if let Some((agent, generation)) = outcome.finish_target() {
                push(
                    "finish",
                    action(
                        format!(
                            "Finish switch with ordinary handoff to {agent} (generation {generation})"
                        ),
                        context.message(Message::Handoff(HandoffMsg::Finish { agent, generation })),
                        !self.submitting(),
                        palette,
                    ),
                );
            }
        }
        if !state.children.is_empty() {
            let brief = state
                .outcome
                .as_ref()
                .and_then(Outcome::handoff)
                .map(|reply| reply.brief_command.clone());
            let lines: Vec<String> = state
                .children
                .iter()
                .map(|child| {
                    format!(
                        "{} {} · {}",
                        if Some(&child.id) == brief.as_ref() {
                            "brief"
                        } else {
                            "successor"
                        },
                        child.id,
                        handoff::state_name(child.state)
                    )
                })
                .collect();
            push("children", small(lines.join("\n")).into());
            push(
                "reason",
                TextBox::new()
                    .text(state.reason.clone())
                    .placeholder_text("Reason for an Owner assertion (required)")
                    .automation_name("Owner reason")
                    .on_text_changed(
                        context.callback(|text| Message::Handoff(HandoffMsg::ReasonChanged(text))),
                    )
                    .into(),
            );
            let mut owner: Vec<KeyedView> = Vec::new();
            for child in &state.children {
                let is_brief = Some(&child.id) == brief.as_ref();
                let mut add = |label: String, message: HandoffMsg| {
                    owner.push(KeyedView::new(
                        label.clone(),
                        action(
                            label,
                            context.message(Message::Handoff(message)),
                            !self.submitting(),
                            palette,
                        ),
                    ));
                };
                if is_brief && child.state == CommandState::Unknown {
                    add(
                        format!("Resolve brief {} applied", child.id),
                        HandoffMsg::ResolveRequested(child.id.clone(), CommandResolution::Applied),
                    );
                    add(
                        format!("Resolve brief {} failed", child.id),
                        HandoffMsg::ResolveRequested(child.id.clone(), CommandResolution::Failed),
                    );
                }
                if is_brief && child.state == CommandState::Failed {
                    add(
                        format!("Assert failed brief {} applied", child.id),
                        HandoffMsg::ResolveRequested(child.id.clone(), CommandResolution::Applied),
                    );
                }
                if matches!(child.state, CommandState::Accepted | CommandState::Queued) {
                    add(
                        format!("Withdraw {}", child.id),
                        HandoffMsg::CancelRequested(child.id.clone()),
                    );
                }
            }
            push(
                "owner-actions",
                StackPanel::new()
                    .spacing(theme::spacing::XS)
                    .keyed_children(owner),
            );
            push("owner-status", small(state.owner_status.clone()).into());
        }
        ContentDialog::new()
            .is_open(true)
            .title(format!(
                "Hand off or switch {} (generation {})",
                state.from, state.from_generation
            ))
            .close_button_text("Close")
            .on_closed(context.callback(|_| Message::Handoff(HandoffMsg::Closed)))
            .content(
                ScrollViewer::new()
                    .vertical_scroll_bar_visibility(ScrollBarVisibility::Auto)
                    .max_height(560.0)
                    .content(
                        StackPanel::new()
                            .spacing(theme::spacing::SM)
                            .keyed_children(sections),
                    ),
            )
    }
}

fn action(label: String, on_click: Callback<()>, enabled: bool, palette: Palette) -> View {
    m3::tonal(palette)
        .is_enabled(enabled)
        .automation_name(label.clone())
        .on_click(on_click)
        .content(
            TextBlock::new()
                .text(label)
                .text_wrapping(TextWrapping::Wrap),
        )
}

fn heading(text: &str) -> TextBlock {
    TextBlock::new()
        .text(text.to_owned())
        .font_weight(FontWeight::SEMI_BOLD)
        .automation_heading_level(AutomationHeadingLevel::Level3)
}

fn small(text: impl Into<String>) -> TextBlock {
    TextBlock::new()
        .text(text.into())
        .font_size(theme::type_scale::LABEL_MEDIUM.size)
        .text_wrapping(TextWrapping::Wrap)
        .is_text_selection_enabled(true)
}
