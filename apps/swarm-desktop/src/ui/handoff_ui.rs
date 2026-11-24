//! Glue for the Hand off / Switch dialog: its messages, background reads and
//! submissions, and the shared M3 dialog view. The rules (send selection,
//! result facts, gate meanings, when "finish" applies) live in
//! [`crate::handoff`].
//!
//! The destination is one radio group whose options carry what they show
//! (B and its generation, or the profile); the ad-hoc harness, model and
//! effort are read from the shown pickers when the one primary action is
//! clicked, so no index decides where work goes.

use super::*;
use crate::handoff::{self, Outcome, SendRow, Submitted};
use eliot_core::CommandState;
use eliot_core::api::{
    CommandResolution, HandoffRequest, HarnessModelsResponse, SwitchLaunch, SwitchRequest,
};

/// Same-ID readbacks of an orchestrating switch: one every two seconds for
/// at most five minutes, then "Refresh the result" reads again on request.
const SWITCH_POLL_MS: u64 = 2_000;
const SWITCH_POLLS: u32 = 150;
const HARNESS_DEFAULT: &str = "Harness default";

/// The destination picked in the dialog's radio group.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Choice {
    Existing { agent: String, generation: i64 },
    Profile { name: String, harness: String },
    Adhoc,
}

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
    /// A destination was checked, or unchecked by another one.
    Chose(Choice),
    Unchose(Choice),
    DetailsToggled(bool),
    Submit(Target),
    Retry,
    /// Epoch, whether this reply answers a submission (and so settles its
    /// ledger entry, even after the dialog closed), the mutation id it
    /// answers, and the reply.
    Replied(u64, bool, String, Box<MutationResult<Outcome>>),
    RefreshRequested,
    ChildrenLoaded(u64, Fallible<Vec<Command>>),
    ReasonChanged(String),
    ResolveRequested(String, CommandResolution),
    CancelRequested(String),
    OwnerDone(u64, String, MutationResult<String>),
    Finish {
        agent: String,
        generation: i64,
    },
}

#[derive(Clone, Debug, Default)]
pub(super) struct DialogState {
    pub(super) open: bool,
    /// Tags one opening; replies from an earlier one are dropped.
    epoch: u64,
    from: String,
    from_generation: i64,
    sends: Option<Fallible<Vec<SendRow>>>,
    task: Option<String>,
    /// Selected SEND ids, kept in admission order.
    chosen: Vec<String>,
    /// The destination radio choice.
    choice: Option<Choice>,
    /// "Readback details (ids)" expanded.
    details_open: bool,
    /// The last definite refusal, until another destination or submission.
    refusal: Option<String>,
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
                    status: format!("Reading the SENDs of {agent} run {generation}…"),
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
                        "{} SEND(s) in this run, {} undispatched.{}",
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
            HandoffMsg::Chose(choice) => {
                if self.handoff.submitted.is_none() && !self.handoff.uncertain {
                    if self.handoff.choice.as_ref() != Some(&choice) {
                        self.handoff.refusal = None;
                    }
                    self.handoff.choice = Some(choice);
                }
            }
            HandoffMsg::Unchose(choice) => {
                // WinUI unchecks the previous option of the group; only the
                // option still recorded is cleared.
                if self.handoff.choice.as_ref() == Some(&choice) {
                    self.handoff.choice = None;
                }
            }
            HandoffMsg::DetailsToggled(open) => self.handoff.details_open = open,
            HandoffMsg::HarnessPicked(index) => {
                let Some(harness) = index.and_then(|index| ADHOC_HARNESSES.get(index)) else {
                    return;
                };
                // Picking the ad-hoc harness picks the ad-hoc destination.
                if self.handoff.submitted.is_none() && !self.handoff.uncertain {
                    self.handoff.choice = Some(Choice::Adhoc);
                }
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
                // Re-check an existing-Agent destination against the current
                // candidates: the row may have left the scope, ended, or
                // moved generation since the choice was saved.
                if let Target::Existing { agent, generation } = &target
                    && !Self::existing_choice_fresh(agent, *generation, &self.handoff_targets())
                {
                    let refused = format!(
                        "{agent} can no longer take this work (it left the scope, ended, or moved on); pick another destination."
                    );
                    self.handoff.status = refused.clone();
                    self.handoff.refusal = Some(refused);
                    return;
                }
                self.handoff.refusal = None;
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
                let uncertain = reply.as_ref().err().is_some_and(crate::state::uncertain);
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
                    Err(error) if uncertain && settles => {
                        self.handoff.uncertain = true;
                        self.handoff.status = format!(
                            "Outcome uncertain; Retry sends the same request with the same id, never a new one: {error}"
                        );
                    }
                    Err(error) if uncertain => {
                        // A readback that failed changes nothing it had read.
                        self.handoff.status = format!(
                            "The readback did not answer; Refresh the result reads it again with the same id: {error}"
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
                        // Shared words lead (a draining project transfer
                        // reads "A project transfer is in progress"), the
                        // daemon's reason follows intact.
                        let refusal = format!("Refused: {}", error.user_message());
                        self.handoff.status = refusal.clone();
                        self.handoff.refusal = Some(refusal);
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
                // The reason is part of the assertion: another reason is a
                // new action with a new id.
                let key = format!(
                    "resolve:{command}:{}:{:x}",
                    resolution.as_str(),
                    crate::chat::text_hash(&reason)
                );
                let label = format!("Resolve the brief as {}", resolution.as_str());
                self.handoff_owner_action(key, label, context, move |data_dir, id| {
                    client::block_on(client::resolve_command(
                        data_dir, id, &command, resolution, reason,
                    ))
                    .map(|reply| format!("now {}", handoff::state_name(reply.state)))
                });
            }
            HandoffMsg::CancelRequested(command) => {
                let key = format!("cancel:{command}");
                let label = "Withdraw the input".to_owned();
                self.handoff_owner_action(key, label, context, move |data_dir, id| {
                    client::block_on(client::cancel_command(data_dir, id, &command))
                        .map(|reply| format!("now {}", handoff::state_name(reply.state)))
                });
            }
            HandoffMsg::OwnerDone(epoch, label, result) => {
                let uncertain = result.as_ref().err().is_some_and(crate::state::uncertain);
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

    /// Live, non-source Agents of the navigation list in the source's
    /// authorization scope (the daemon requires a same-scope B and checks
    /// again). A missing source, an empty scope or an unknown one (a
    /// pending launch whose daemon row has not arrived) offers nothing,
    /// and pending rows are never offered: handoff stays in one scope,
    /// never across projects.
    fn handoff_targets(&self) -> Vec<&UiAgent> {
        handoff_candidates(&self.handoff.from, &self.nav.agents, &self.nav.branches)
    }

    /// Whether a saved existing-Agent destination is still among the
    /// current candidates with the same generation and a known scope. A
    /// row that left the scope, ended, or moved generation since it was
    /// picked no longer qualifies, nor does a pending row whose daemon row
    /// has not arrived; the submitter must refuse the stale choice and the
    /// daemon would refuse it again.
    fn existing_choice_fresh(agent: &str, generation: i64, targets: &[&UiAgent]) -> bool {
        targets
            .iter()
            .any(|row| row.id == agent && row.generation == generation && row.scope().is_some())
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
            role: None,
            harness,
            model,
            effort,
            auth: None,
            cmd: None,
            project: None,
            workspace: None,
            worktree: None,
            base: None,
            access: None,
        };
        let switch = |launch: SwitchLaunch| {
            Submitted::Switch(Box::new(SwitchRequest {
                id: String::new(),
                from: state.from.clone(),
                from_generation: state.from_generation,
                task_command: task_command.clone(),
                commands: commands.clone(),
                launch,
            }))
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
        self.handoff.status = match &submitted {
            Submitted::Handoff(request) => format!("Handing off to {}…", request.to),
            Submitted::Switch(_) => "Switching…".to_owned(),
        };
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
                    Box::new(Err(eliot_core::Error::new("unknown", "readback cancelled"))),
                ));
            }
            let reply = client::block_on(async {
                match &submitted {
                    Submitted::Handoff(request) => client::handoff(&data_dir, request)
                        .await
                        .map(|reply| Outcome::Handoff(Box::new(reply))),
                    Submitted::Switch(request) => client::switch(&data_dir, request)
                        .await
                        .map(|reply| Outcome::Switch(Box::new(reply))),
                }
            });
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
        work: impl FnOnce(&Path, String) -> MutationResult<String> + Send + 'static,
    ) {
        if self.submitting() {
            return;
        }
        let Ok(data_dir) = self.data_dir.clone() else {
            return;
        };
        let mutation_id = self.ledger.id_for(&key);
        self.in_flight_key = Some(key);
        self.handoff.owner_status = format!("{label}…");
        let epoch = self.handoff.epoch;
        let _task = context.spawn_background(move |_| {
            Message::Handoff(HandoffMsg::OwnerDone(
                epoch,
                label,
                work(&data_dir, mutation_id),
            ))
        });
    }

    /// The dialog; an empty view while closed. The destination is picked
    /// from one radio group (existing Agents, profiles, ad-hoc); the one
    /// primary action at the bottom right submits what is shown, bound when
    /// clicked. Ids stay out of the main view: SENDs show their text and age,
    /// and the readback with ids sits under "Readback details".
    ///
    pub(super) fn handoff_dialog(&self, context: &mut ViewContext<Self>) -> View {
        let state = &self.handoff;
        let palette = self.palette();
        if !state.open {
            return View::empty();
        }
        let now = handoff::now_ms();
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
        if state.outcome.is_none() {
            push(
                "status",
                small(state.status.clone())
                    .foreground(palette.muted())
                    .into(),
            );
        }

        let models = self.handoff_models();
        let efforts = self.handoff_efforts();
        let harness = state
            .harness
            .and_then(|index| ADHOC_HARNESSES.get(index))
            .map(|harness| (*harness).to_owned());
        let model = configure::picked(&models, state.model);
        let effort = configure::picked(&efforts, state.effort);
        // Once the daemon answered, the dialog shows the result instead of
        // the (then fixed) choices.
        if state.outcome.is_none() {
            push("task-head", heading("Original task").into());
            let task_labels: Vec<String> = rows.iter().map(|row| row.label(now)).collect();
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
                    .placeholder_text("The SEND whose text opens the brief")
                    .automation_name("Original task SEND")
                    .horizontal_alignment(HorizontalAlignment::Stretch)
                    .on_selection_changed(
                        context.callback(|index| Message::Handoff(HandoffMsg::TaskPicked(index))),
                    )
                    .into(),
            );

            push("sends-head", heading("Waiting SENDs to move").into());
            let transferable: Vec<&SendRow> =
                rows.iter().filter(|row| row.transferable()).collect();
            if transferable.is_empty() {
                push(
                "no-sends",
                small("None are waiting in this run: the transfer carries context, claims, Goal and children only.")
                    .foreground(palette.muted())
                    .into(),
            );
            } else {
                push(
                    "sends-note",
                    small(format!(
                        "At most {}; the rest stay on {}.",
                        eliot_core::api::MAX_HANDOFF_SENDS,
                        state.from
                    ))
                    .foreground(palette.muted())
                    .into(),
                );
            }
            for row in transferable {
                let id = row.id.clone();
                let label = row.label(now);
                push(
                    &format!("send-{}", row.id),
                    CheckBox::new()
                        .is_checked(state.chosen.contains(&row.id))
                        .is_enabled(can_act)
                        .automation_name(format!("Move {label}"))
                        .on_is_checked_changed(context.callback(move |checked| {
                            Message::Handoff(HandoffMsg::SendToggled(id.clone(), checked))
                        }))
                        .content(small(label)),
                );
            }

            push(
                "existing-head",
                heading("Hand off to an existing Agent").into(),
            );
            let targets = self.handoff_targets();
            if targets.is_empty() {
                push(
                    "existing-none",
                    small("No other live Agent can take this work.")
                        .foreground(palette.muted())
                        .into(),
                );
            }
            for agent in targets {
                let choice = Choice::Existing {
                    agent: agent.id.clone(),
                    generation: agent.generation,
                };
                push(
                    &format!("existing-{}", agent.id),
                    destination(
                        format!(
                            "{} · {} · {}",
                            agent.id,
                            eliot_core::api::agent_state_name(agent.state),
                            agent.harness
                        ),
                        &choice,
                        state.choice.as_ref(),
                        can_act,
                        context,
                    ),
                );
            }

            push(
                "profile-head",
                heading("Switch to a new Agent from a profile").into(),
            );
            if self.launch.profile_summaries.is_empty() {
                push(
                    "profile-none",
                    small("No configured profile was read.")
                        .foreground(palette.muted())
                        .into(),
                );
            }
            for profile in &self.launch.profile_summaries {
                let choice = Choice::Profile {
                    name: profile.name.clone(),
                    harness: profile.harness.clone(),
                };
                push(
                    &format!("profile-{}", profile.name),
                    destination(
                        format!("New {} Agent ({})", profile.name, profile.harness),
                        &choice,
                        state.choice.as_ref(),
                        can_act,
                        context,
                    ),
                );
            }

            push("adhoc-head", heading("Switch to a new ad-hoc Agent").into());
            push(
                "adhoc-choice",
                destination(
                    match &harness {
                        Some(harness) => format!("New ad-hoc {harness} Agent"),
                        None => "New ad-hoc Agent (pick its harness)".to_owned(),
                    },
                    &Choice::Adhoc,
                    state.choice.as_ref(),
                    can_act,
                    context,
                ),
            );
            let labelled = |label: &str, control: View| -> View {
                StackPanel::new()
                    .spacing(tokens::spacing::XS)
                    .children((small(label.to_owned()).foreground(palette.muted()), control))
            };
            // The effort steps as segments (the effort popover's control):
            // each shows the short word and is named in full words, and its
            // position is the index into `efforts` (0: the harness default).
            let effort_segments: Vec<KeyedView> =
                efforts
                    .iter()
                    .enumerate()
                    .map(|(index, effort)| {
                        let (label, name) = if index == 0 {
                            ("default", "Harness default effort".to_owned())
                        } else {
                            (
                                crate::picker::effort_label(effort),
                                crate::picker::effort_name(effort),
                            )
                        };
                        let selected = state.effort == Some(index);
                        KeyedView::new(
                            format!("{index}:{effort}:{selected}"),
                            m3::segment(palette, selected)
                                .is_enabled(can_act && harness.is_some())
                                .automation_name(format!(
                                    "Ad-hoc effort: {name}{}",
                                    if selected { " (chosen)" } else { "" }
                                ))
                                .on_click(context.message(Message::Handoff(
                                    HandoffMsg::EffortPicked(Some(index)),
                                )))
                                .content(m3::segment_label(label)),
                        )
                    })
                    .collect();
            push(
                "adhoc",
                StackPanel::new()
                    .spacing(tokens::spacing::SM)
                    .margin(Thickness::new(RADIO_INDENT, 0.0, 0.0, 0.0))
                    .keyed_children(vec![
                        KeyedView::new(
                            "pickers",
                            StackPanel::new()
                                .orientation(Orientation::Horizontal)
                                .spacing(tokens::spacing::SM)
                                .keyed_children(vec![
                                    KeyedView::new(
                                        "harness",
                                        labelled(
                                            "Harness",
                                            ComboBox::new()
                                                .items_source(
                                                    ADHOC_HARNESSES.map(str::to_owned).to_vec(),
                                                )
                                                .selected_index(state.harness)
                                                .is_enabled(can_act)
                                                .placeholder_text("Pick one")
                                                .automation_name("Ad-hoc harness")
                                                .min_width(tokens::layout::DIALOG_PICKER_WIDTH)
                                                .on_selection_changed(context.callback(|index| {
                                                    Message::Handoff(HandoffMsg::HarnessPicked(
                                                        index,
                                                    ))
                                                }))
                                                .into(),
                                        ),
                                    ),
                                    // Option lists that change get a new
                                    // control (see `configure::panel`).
                                    KeyedView::new(
                                        format!("model:{}", models.join("\u{1f}")),
                                        labelled(
                                            "Model",
                                            ComboBox::new()
                                                .items_source(models.clone())
                                                .selected_index(state.model)
                                                .is_enabled(can_act && harness.is_some())
                                                .automation_name("Ad-hoc model")
                                                .min_width(tokens::layout::DIALOG_PICKER_WIDTH)
                                                .on_selection_changed(context.callback(|index| {
                                                    Message::Handoff(HandoffMsg::ModelPicked(index))
                                                }))
                                                .into(),
                                        ),
                                    ),
                                ]),
                        ),
                        KeyedView::new(
                            format!("effort:{}", efforts.join("\u{1f}")),
                            labelled("Effort", m3::segmented(palette, effort_segments)),
                        ),
                    ]),
            );
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
                None if harness.is_some() => "Reading the catalogue…".to_owned(),
                None => "Picking a harness reads its model catalogue.".to_owned(),
            };
            push(
                "catalog",
                small(catalog_line)
                    .foreground(palette.muted())
                    .margin(Thickness::new(RADIO_INDENT, 0.0, 0.0, 0.0))
                    .into(),
            );
        }

        if let Some(outcome) = &state.outcome {
            push("result-head", heading("Result").into());
            push("result", small(handoff::outcome_summary(outcome)).into());
            let lines = match outcome {
                Outcome::Handoff(reply) => handoff::handoff_lines(reply),
                Outcome::Switch(reply) => handoff::switch_lines(reply),
            };
            push(
                "details",
                Expander::new()
                    .is_expanded(state.details_open)
                    .horizontal_alignment(HorizontalAlignment::Stretch)
                    .on_is_expanded_changed(
                        context.callback(|open| Message::Handoff(HandoffMsg::DetailsToggled(open))),
                    )
                    .slots([
                        SlotView::new(ExpanderSlot::Header, small("Readback details (ids)")),
                        SlotView::new(
                            ExpanderSlot::Content,
                            small(lines.join("\n")).foreground(palette.muted()),
                        ),
                    ]),
            );
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
                    if Some(&child.id) == brief.as_ref() {
                        format!("The brief · {}", handoff::state_name(child.state))
                    } else {
                        format!(
                            "\u{201c}{}\u{201d} · {}",
                            child_preview(child),
                            handoff::state_name(child.state)
                        )
                    }
                })
                .collect();
            push("children-head", heading("Moved inputs").into());
            push("children", small(lines.join("\n")).into());
            let mut owner: Vec<KeyedView> = Vec::new();
            for child in &state.children {
                let is_brief = Some(&child.id) == brief.as_ref();
                let mut add = |key: String, label: String, message: HandoffMsg| {
                    owner.push(KeyedView::new(
                        key,
                        m3::text_button(palette)
                            .is_enabled(!self.submitting())
                            .automation_name(label.clone())
                            .on_click(context.message(Message::Handoff(message)))
                            .content(m3::label(&label)),
                    ));
                };
                if is_brief && child.state == CommandState::Unknown {
                    add(
                        format!("applied:{}", child.id),
                        "Resolve the brief as applied".to_owned(),
                        HandoffMsg::ResolveRequested(child.id.clone(), CommandResolution::Applied),
                    );
                    add(
                        format!("failed:{}", child.id),
                        "Resolve the brief as failed".to_owned(),
                        HandoffMsg::ResolveRequested(child.id.clone(), CommandResolution::Failed),
                    );
                }
                if is_brief && child.state == CommandState::Failed {
                    add(
                        format!("assert:{}", child.id),
                        "Assert the failed brief applied".to_owned(),
                        HandoffMsg::ResolveRequested(child.id.clone(), CommandResolution::Applied),
                    );
                }
                if matches!(child.state, CommandState::Accepted | CommandState::Queued) {
                    add(
                        format!("withdraw:{}", child.id),
                        format!("Withdraw \u{201c}{}\u{201d}", child_preview(child)),
                        HandoffMsg::CancelRequested(child.id.clone()),
                    );
                }
            }
            // An Owner assertion needs a reason; without one to make, no box.
            if !owner.is_empty() {
                push(
                    "reason",
                    m3::field()
                        .text(state.reason.clone())
                        .placeholder_text("Reason for an Owner assertion (required)")
                        .automation_name("Owner reason")
                        .on_text_changed(
                            context
                                .callback(|text| Message::Handoff(HandoffMsg::ReasonChanged(text))),
                        )
                        .into(),
                );
                push(
                    "owner-actions",
                    StackPanel::new()
                        .spacing(tokens::spacing::XS)
                        .keyed_children(owner),
                );
            }
            push("owner-status", small(state.owner_status.clone()).into());
        }

        // The one primary action follows the dialog's state.
        let choice_label = match state.choice {
            Some(Choice::Existing { .. }) | None => "Hand off",
            Some(_) => "Switch",
        };
        let pending = state.outcome.as_ref().is_some_and(Outcome::switch_pending);
        let (primary_label, primary_message, primary_enabled) = if state.uncertain {
            (
                "Retry with the same id".to_owned(),
                Some(HandoffMsg::Retry),
                !self.submitting(),
            )
        } else if let Some((agent, generation)) =
            state.outcome.as_ref().and_then(Outcome::finish_target)
        {
            (
                format!("Finish: hand off to {agent}"),
                Some(HandoffMsg::Finish { agent, generation }),
                !self.submitting(),
            )
        } else if pending {
            (
                "Refresh the result".to_owned(),
                Some(HandoffMsg::RefreshRequested),
                !self.submitting(),
            )
        } else if state.outcome.is_some() {
            // Done: Close is the only action left (no primary below).
            (String::new(), None, false)
        } else if state.submitted.is_some() {
            // In flight: the action keeps its name until the daemon answers.
            (choice_label.to_owned(), None, false)
        } else {
            let target = self.handoff_target(harness.clone(), model.clone(), effort.clone());
            let enabled = can_act && target.is_some();
            (
                choice_label.to_owned(),
                target.map(HandoffMsg::Submit),
                enabled,
            )
        };
        let primary: View = match primary_message {
            _ if primary_label.is_empty() => View::empty(),
            Some(message) => m3::filled(palette)
                .element_ref(&self.dialog.primary)
                .is_enabled(primary_enabled)
                .on_click(context.message(Message::Handoff(message)))
                .automation_name(primary_label.clone())
                .content(TextBlock::new().text(primary_label)),
            None => m3::filled(palette)
                .is_enabled(false)
                .automation_name(primary_label.clone())
                .content(TextBlock::new().text(primary_label)),
        };
        // The same Cancel/Close then primary order as every M3 dialog.
        let close = m3::text_button(palette)
            .element_ref(&self.dialog.close)
            .automation_name("Close")
            .on_click(context.message(Message::Handoff(HandoffMsg::Closed)))
            .content(TextBlock::new().text("Close"));
        // Definite refusals use the shared window palette's error role.
        let summary = self.handoff_summary(harness, model, effort);
        let summary: View = match &state.refusal {
            Some(refusal) => StackPanel::new()
                .spacing(tokens::spacing::XS)
                .vertical_alignment(VerticalAlignment::Center)
                .children((
                    small(refusal.clone()).foreground(palette.attention()),
                    small(summary),
                )),
            None => small(summary)
                .vertical_alignment(VerticalAlignment::Center)
                .into(),
        };
        let footer = Grid::new()
            .grid_row(1)
            .columns([GridLength::STAR, GridLength::Auto])
            .column_spacing(tokens::spacing::LG)
            .children((
                summary,
                StackPanel::new()
                    .grid_column(1)
                    .orientation(Orientation::Horizontal)
                    .spacing(tokens::spacing::SM)
                    .vertical_alignment(VerticalAlignment::Bottom)
                    .children((close, primary)),
            ));
        super::dialog_ui::surface(
            context,
            self.dialog.epoch,
            palette,
            format!("Hand off or switch {}", state.from),
            StackPanel::new()
                .spacing(tokens::spacing::XS)
                .keyed_children(sections),
            footer,
            640.0,
        )
    }

    /// The submission for the chosen destination, from what is shown: the
    /// existing Agent and its generation, the profile, or the ad-hoc
    /// harness/model/effort on screen. None until a destination is complete.
    /// A saved existing-Agent choice that is no longer among the current
    /// candidates (same id and generation) is stale and submits nothing.
    fn handoff_target(
        &self,
        harness: Option<String>,
        model: Option<String>,
        effort: Option<String>,
    ) -> Option<Target> {
        match self.handoff.choice.as_ref()? {
            Choice::Existing { agent, generation } => {
                if !Self::existing_choice_fresh(agent, *generation, &self.handoff_targets()) {
                    return None;
                }
                Some(Target::Existing {
                    agent: agent.clone(),
                    generation: *generation,
                })
            }
            Choice::Profile { name, .. } => Some(Target::Profile(name.clone())),
            Choice::Adhoc => harness.map(|harness| Target::Adhoc {
                harness,
                model,
                effort,
            }),
        }
    }

    /// What will move, in plain language, beside the primary action.
    fn handoff_summary(
        &self,
        harness: Option<String>,
        model: Option<String>,
        effort: Option<String>,
    ) -> String {
        let state = &self.handoff;
        let from = &state.from;
        if state.outcome.is_some() {
            // The body states the result.
            return String::new();
        }
        if state.submitted.is_some() || state.uncertain {
            return state.status.clone();
        }
        let sends = match state.chosen.len() {
            0 => String::new(),
            1 => ", plus 1 waiting SEND".to_owned(),
            count => format!(", plus {count} waiting SENDs"),
        };
        let carried = format!(
            "a brief from the original task, {from}'s context, claims, Goal and children{sends}"
        );
        match &state.choice {
            None => format!("Choose where {from}'s work goes."),
            Some(Choice::Existing { agent, .. }) => format!(
                "Moves {from}'s task to {agent}: {carried}. Other work stays on {from}, which keeps running."
            ),
            Some(Choice::Profile { name, harness }) => format!(
                "Starts a new {name} Agent ({harness}) in {from}'s workspace and moves the task to it: {carried}. {from} keeps running."
            ),
            Some(Choice::Adhoc) => match harness {
                Some(harness) => format!(
                    "Starts a new ad-hoc {harness} Agent (model {}, effort {}) in {from}'s workspace and moves the task to it: {carried}. {from} keeps running.",
                    model.as_deref().unwrap_or("harness default"),
                    effort
                        .as_deref()
                        .map_or("harness default", crate::picker::effort_label)
                ),
                None => "Pick the ad-hoc Agent's harness.".to_owned(),
            },
        }
    }
}

/// One destination of the dialog's single radio group, bound to `choice`.
fn destination(
    label: String,
    choice: &Choice,
    chosen: Option<&Choice>,
    enabled: bool,
    context: &mut ViewContext<DesktopApp>,
) -> View {
    let message = choice.clone();
    RadioButton::new()
        .group_name("handoff-destination")
        .is_checked(chosen == Some(choice))
        .is_enabled(enabled)
        .automation_name(label.clone())
        .on_checked(context.callback(move |checked: bool| {
            Message::Handoff(if checked {
                HandoffMsg::Chose(message.clone())
            } else {
                HandoffMsg::Unchose(message.clone())
            })
        }))
        .content(small(label))
}

/// The text of a moved input, previewed (never its id).
fn child_preview(command: &Command) -> String {
    handoff::preview(
        command
            .args
            .get("text")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("(no text)"),
        60,
    )
}

/// Options under a radio button line up with its label (WinUI's 20-DIP
/// radio glyph and its 8-DIP gap).
const RADIO_INDENT: f64 = tokens::icon::ROW + tokens::spacing::SM;

fn heading(text: &str) -> TextBlock {
    m3::section_title(text.to_owned()).margin(Thickness::new(0.0, tokens::spacing::SM, 0.0, 0.0))
}

fn small(text: impl Into<String>) -> TextBlock {
    m3::body(text)
        .text_wrapping(TextWrapping::Wrap)
        .is_text_selection_enabled(true)
}

/// Live, non-source Agents of the navigation list in the source's
/// authorization scope (the handoff picker flow): a missing source, an
/// empty scope or an unknown one (a pending launch whose daemon row has
/// not arrived) offers nothing, and pending rows are never offered.
fn handoff_candidates<'a>(
    from: &str,
    agents: &'a [UiAgent],
    branches: &'a std::collections::HashMap<String, nav::Branch>,
) -> Vec<&'a UiAgent> {
    let listed = || {
        agents
            .iter()
            .chain(branches.values().flat_map(|branch| match branch {
                nav::Branch::Loaded { children, .. } => children.as_slice(),
                _ => &[],
            }))
    };
    let scope = listed()
        .find(|agent| agent.id == from)
        .and_then(|agent| agent.scope())
        .map(str::to_owned)
        .filter(|scope| !scope.is_empty());
    let mut seen = HashSet::new();
    listed()
        .filter(|agent| seen.insert(agent.id.as_str()))
        .filter(|agent| agent.id != from)
        .filter(|agent| {
            scope
                .as_ref()
                .is_some_and(|scope| agent.scope() == Some(scope.as_str()))
        })
        .filter(|agent| {
            !matches!(
                agent.state,
                eliot_core::AgentState::Stopped
                    | eliot_core::AgentState::Failed
                    | eliot_core::AgentState::Lost
                    | eliot_core::AgentState::Unknown
            )
        })
        .collect()
}
