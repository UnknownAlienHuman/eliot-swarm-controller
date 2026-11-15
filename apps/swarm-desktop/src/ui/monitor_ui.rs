//! The openable swarm monitor: refresh chain, selection, detail controls and manager moves.

use super::*;
use crate::theme::winui::Palette;
use eliot_core::AgentState;

#[derive(Clone, Debug)]
pub(super) enum MonitorMsg {
    MonitorToggled(bool),
    MonitorLoaded(u64, Option<Box<MonitorSnapshot>>),
    MonitorSelected(Option<usize>),
    ReparentRequested(reparent::MoveBinding),
    ReparentSettled(reparent::MoveBinding, Fallible<ReparentResponse>),
    MonitorOpenConversation(String),
    MonitorStop(BoundAction, bool),
    /// Force stop was clicked: confirm this exact Agent and generation.
    ForceStopAsked(BoundAction),
    ForceStopCancelled,
    MonitorInterrupt(BoundAction),
    MonitorResume(BoundAction),
    MonitorDetach(BoundAction),
}

/// The monitor pane: open state, refresh chain, rows and selection.
pub(super) struct MonitorState {
    pub(super) data: Monitor,
    // Monitor: refreshes only while open, one task chain, epoch-gated.
    pub(super) open: bool,
    pub(super) epoch: u64,
    pub(super) task: Option<ComponentTask>,
    pub(super) selected: Option<String>,
    /// Monitor rows published before the latest refresh; the native
    /// selection never points past them (see `monitor_view`).
    pub(super) published: usize,
    pub(super) reparent_status: String,
    /// The Force stop awaiting confirmation, bound at its first click.
    pub(super) confirm_force: Option<BoundAction>,
    /// Row ids in the order the list showed before the latest refresh, and
    /// the render count when that refresh was applied.
    pub(super) on_screen: Vec<String>,
    pub(super) applied_at: u64,
}

impl DesktopApp {
    pub(super) fn monitor_update(&mut self, msg: MonitorMsg, context: &ComponentContext<Self>) {
        match msg {
            MonitorMsg::MonitorToggled(open) => {
                self.monitor.open = open;
                if open {
                    // A cancelled chain may still finish its last reads; wait
                    // a moment instead of overlapping them.
                    let previous_running = self
                        .monitor
                        .task
                        .as_ref()
                        .is_some_and(|task| task.status() == ComponentTaskStatus::Running);
                    self.spawn_monitor(context, if previous_running { 1_000 } else { 0 });
                } else {
                    // Hidden: no refresh, no render work; essential command
                    // and conversation delivery continue elsewhere.
                    self.monitor.epoch += 1;
                    if let Some(task) = self.monitor.task.take() {
                        task.cancel();
                    }
                }
            }
            MonitorMsg::MonitorLoaded(epoch, snapshot) => {
                if epoch != self.monitor.epoch || !self.monitor.open {
                    return;
                }
                if let Some(snapshot) = snapshot {
                    self.monitor.published = self.monitor.data.rows.len();
                    // The order on screen until the next render (rows are
                    // attention-first, so a refresh can reorder them).
                    self.monitor.on_screen = self
                        .monitor
                        .data
                        .rows
                        .iter()
                        .map(|row| row.id.clone())
                        .collect();
                    self.monitor.applied_at = self.renders.get();
                    self.monitor.data.apply(*snapshot);
                    self.spawn_monitor(context, monitor::PERIOD_MS);
                }
            }
            MonitorMsg::MonitorSelected(index) => {
                // The index counts rows as the list shows them: before the
                // first render after a refresh, that is the previous order.
                let id = index.and_then(|index| {
                    if self.renders.get() == self.monitor.applied_at {
                        self.monitor.on_screen.get(index).cloned()
                    } else {
                        self.monitor.data.rows.get(index).map(|row| row.id.clone())
                    }
                });
                if id.is_some() && id != self.monitor.selected {
                    // The running chain reads the new selection on its next
                    // cycle (within one period): still one refresh in flight.
                    self.monitor.selected = id;
                    self.monitor.reparent_status.clear();
                    self.monitor.confirm_force = None;
                }
            }
            MonitorMsg::ReparentRequested(binding) => {
                if self.submitting() {
                    return;
                }
                let Ok(data_dir) = self.data_dir.clone() else {
                    return;
                };
                let key = binding.key();
                let mutation_id = self.ledger.id_for(&key);
                self.in_flight_key = Some(key);
                self.monitor.reparent_status =
                    format!("Moving {} ({mutation_id})…", binding.describe());
                let request = binding.request(mutation_id);
                let _task = context.spawn_background(move |_| {
                    let result = client::block_on(client::reparent(&data_dir, &request))
                        .map_err(|error| error.to_string());
                    Message::Monitor(MonitorMsg::ReparentSettled(binding, result))
                });
            }
            MonitorMsg::ReparentSettled(binding, result) => {
                let uncertain = result
                    .as_ref()
                    .err()
                    .is_some_and(|error| crate::state::uncertain(error));
                self.settle_mutation(!uncertain);
                self.monitor.reparent_status = match result {
                    Ok(reply) => reparent::result_lines(&binding, &reply).join("\n"),
                    Err(error) if uncertain => {
                        format!("Move outcome uncertain (the same click reuses its id): {error}")
                    }
                    Err(error) => reparent::refusal_line(&binding, &error),
                };
                if self.monitor.open {
                    self.spawn_monitor(context, 0);
                }
            }
            MonitorMsg::MonitorOpenConversation(id) => {
                let agent = self
                    .nav
                    .agents
                    .iter()
                    .find(|agent| agent.id == id)
                    .cloned()
                    .or_else(|| {
                        self.monitor
                            .data
                            .rows
                            .iter()
                            .find(|row| row.id == id)
                            .map(|row| UiAgent {
                                id: row.id.clone(),
                                generation: row.generation,
                                harness: row.harness.clone(),
                                state: monitor::state_name(row.state).to_owned(),
                                project: String::new(),
                                parent: row.parent.clone(),
                            })
                    });
                if let Some(agent) = agent {
                    // The rows are unchanged in this update: the index is
                    // already published.
                    self.nav.agent_idx = self
                        .nav
                        .rows
                        .iter()
                        .position(|row| row.agent.id == agent.id);
                    if self.chat.convo.as_ref().map(Conversation::agent) != Some(&agent.id) {
                        self.open_conversation(&agent, context);
                    }
                }
            }
            MonitorMsg::ForceStopAsked(bound) => self.monitor.confirm_force = Some(bound),
            MonitorMsg::ForceStopCancelled => self.monitor.confirm_force = None,
            MonitorMsg::MonitorResume(bound) => {
                let key = format!("resume:{}:{}", bound.agent, bound.generation);
                self.submit_control(context, "Resume", key, move |data_dir, mutation_id| {
                    client::block_on(client::submit_resume(
                        data_dir,
                        mutation_id,
                        &bound.agent,
                        bound.generation,
                        None,
                    ))
                    .map(|reply| submit_note(&reply))
                    .map_err(|error| error.to_string())
                });
            }
            MonitorMsg::MonitorDetach(bound) => {
                let key = format!("detach:{}:{}", bound.agent, bound.generation);
                self.submit_control(context, "Detach", key, move |data_dir, mutation_id| {
                    client::block_on(client::submit_detach(
                        data_dir,
                        mutation_id,
                        &bound.agent,
                        bound.generation,
                    ))
                    .map(|reply| submit_note(&reply))
                    .map_err(|error| error.to_string())
                });
            }
            MonitorMsg::MonitorStop(bound, force) => {
                if force {
                    // Only the confirmed binding, never a newer selection.
                    if self.monitor.confirm_force.as_ref() != Some(&bound) {
                        return;
                    }
                    self.monitor.confirm_force = None;
                }
                let what = if force { "Force stop" } else { "Stop" };
                let key = format!("stop:{}:{}:{force}", bound.agent, bound.generation);
                self.submit_control(context, what, key, move |data_dir, mutation_id| {
                    client::block_on(client::submit_stop(
                        data_dir,
                        mutation_id,
                        &bound.agent,
                        bound.generation,
                        force,
                    ))
                    .map(|reply| submit_note(&reply))
                    .map_err(|error| error.to_string())
                });
            }
            MonitorMsg::MonitorInterrupt(bound) => {
                let turn = bound.turn.clone().unwrap_or_default();
                let key = format!("interrupt:{}:{}:{turn}", bound.agent, bound.generation);
                self.submit_control(context, "Interrupt", key, move |data_dir, mutation_id| {
                    client::block_on(client::submit_interrupt(
                        data_dir,
                        mutation_id,
                        &bound.agent,
                        bound.generation,
                        turn,
                    ))
                    .map(|reply| submit_note(&reply))
                    .map_err(|error| error.to_string())
                });
            }
        }
    }

    /// Schedule the next monitor refresh: one chain, one refresh in flight,
    /// none while the monitor is hidden.
    pub(super) fn spawn_monitor(&mut self, context: &ComponentContext<Self>, delay_ms: u64) {
        if let Some(task) = self.monitor.task.take() {
            task.cancel();
        }
        let data_dir = match self.data_dir.clone() {
            Ok(data_dir) => data_dir,
            Err(error) => {
                self.monitor.data.errors = vec![format!("no data directory: {error}")];
                return;
            }
        };
        self.monitor.epoch += 1;
        let epoch = self.monitor.epoch;
        let selected = self.monitor.selected.clone();
        self.monitor.task = Some(context.spawn_background(move |cancel| {
            let cancelled = || cancel.is_cancelled();
            if delay_ms > 0 && !feed::backoff(delay_ms, &cancelled) {
                return Message::Monitor(MonitorMsg::MonitorLoaded(epoch, None));
            }
            let snapshot = client::block_on(monitor::refresh(&data_dir, selected, &cancelled));
            Message::Monitor(MonitorMsg::MonitorLoaded(epoch, snapshot.map(Box::new)))
        }));
    }

    /// Monitor column. Control messages carry the Agent, generation and turn
    /// of the detail shown on screen: what is clicked is what was displayed.
    pub(super) fn monitor_view(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        let shown = self
            .monitor
            .data
            .detail
            .as_ref()
            .and_then(|detail| detail.as_ref().ok())
            .filter(|read| Some(&read.agent.agent.id) == self.monitor.selected.as_ref())
            .map(|read| &read.agent.agent);
        let controllable = shown.filter(|agent| !agent.observe_only);
        // Lifecycle stop applies to a controlled Agent that has not ended.
        let live = controllable.filter(|agent| {
            !matches!(
                agent.state,
                AgentState::Stopped | AgentState::Failed | AgentState::Lost
            )
        });
        // A pending confirmation shows only for the Agent it was asked for.
        let confirm = self
            .monitor
            .confirm_force
            .clone()
            .filter(|bound| shown.is_some_and(|agent| agent.id == bound.agent));
        let selected = self.monitor.selected.as_deref().unwrap_or_default();
        // Each button is bound now, from what is on screen: the shown source
        // generation and that row's generation.
        let rows = reparent::candidates(&self.monitor.data.rows, selected)
            .into_iter()
            .map(|manager| {
                let bind = |form| {
                    shown.map(|agent| {
                        context.message(Message::Monitor(MonitorMsg::ReparentRequested(
                            reparent::MoveBinding {
                                form,
                                source: agent.id.clone(),
                                source_generation: agent.generation,
                                parent: manager.id.clone(),
                                parent_generation: manager.generation,
                            },
                        )))
                    })
                };
                monitor_view::MoveRow {
                    id: manager.id.clone(),
                    label: reparent::candidate_label(manager),
                    move_agent: bind(reparent::MoveForm::Agent),
                    move_children: bind(reparent::MoveForm::Children),
                }
            })
            .collect();
        let reparent_data = monitor_view::ReparentData {
            rows,
            status: &self.monitor.reparent_status,
        };
        let actions = MonitorActions {
            select: context.callback(|value| Message::Monitor(MonitorMsg::MonitorSelected(value))),
            open: self.monitor.selected.clone().map(|id| {
                context.message(Message::Monitor(MonitorMsg::MonitorOpenConversation(id)))
            }),
            interrupt: controllable.and_then(|agent| {
                agent.active_turn.as_ref().map(|turn| {
                    context.message(Message::Monitor(MonitorMsg::MonitorInterrupt(
                        BoundAction::turn_control(&agent.id, agent.generation, turn),
                    )))
                })
            }),
            stop: live.map(|agent| {
                context.message(Message::Monitor(MonitorMsg::MonitorStop(
                    BoundAction::agent_control(&agent.id, agent.generation),
                    false,
                )))
            }),
            force_stop: live
                .filter(|_| confirm.is_none())
                .map(|agent| {
                    context.message(Message::Monitor(MonitorMsg::ForceStopAsked(
                        BoundAction::agent_control(&agent.id, agent.generation),
                    )))
                }),
            confirm_force: confirm.map(|bound| monitor_view::ForceConfirm {
                question: format!(
                    "Force stop {} (generation {})? It ends the session at once instead of a clean stop; an active turn is cut off.",
                    bound.agent, bound.generation
                ),
                confirm: context.message(Message::Monitor(MonitorMsg::MonitorStop(
                    bound.clone(),
                    true,
                ))),
                cancel: context.message(Message::Monitor(MonitorMsg::ForceStopCancelled)),
            }),
            resume: controllable
                .filter(|agent| {
                    matches!(
                        agent.state,
                        AgentState::Stopped | AgentState::Failed | AgentState::Lost
                    ) && agent.active_turn.is_none()
                })
                .map(|agent| {
                    context.message(Message::Monitor(MonitorMsg::MonitorResume(
                        BoundAction::agent_control(&agent.id, agent.generation),
                    )))
                }),
            detach: shown.filter(|agent| agent.observe_only).map(|agent| {
                context.message(Message::Monitor(MonitorMsg::MonitorDetach(
                    BoundAction::agent_control(&agent.id, agent.generation),
                )))
            }),
        };
        monitor_view::monitor_pane(
            &self.monitor.data,
            self.monitor.selected.as_deref(),
            self.monitor.published,
            !self.submitting(),
            reparent_data,
            actions,
            palette,
        )
    }
}
