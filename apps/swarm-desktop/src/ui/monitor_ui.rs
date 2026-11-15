//! The openable swarm monitor: refresh chain, selection, detail controls and manager moves.

use super::*;

#[derive(Clone, Debug)]
pub(super) enum MonitorMsg {
    MonitorToggled(bool),
    MonitorLoaded(u64, Option<Box<MonitorSnapshot>>),
    MonitorSelected(Option<usize>),
    ReparentRequested(reparent::MoveBinding),
    ReparentSettled(reparent::MoveBinding, Fallible<ReparentResponse>),
    MonitorOpenConversation(String),
    MonitorStop(BoundAction, bool),
    MonitorInterrupt(BoundAction),
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
                    self.monitor.data.apply(*snapshot);
                    self.spawn_monitor(context, monitor::PERIOD_MS);
                }
            }
            MonitorMsg::MonitorSelected(index) => {
                let id = index
                    .and_then(|index| self.monitor.data.rows.get(index))
                    .map(|row| row.id.clone());
                if id.is_some() && id != self.monitor.selected {
                    // The running chain reads the new selection on its next
                    // cycle (within one period): still one refresh in flight.
                    self.monitor.selected = id;
                    self.monitor.reparent_status.clear();
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
                            })
                    });
                if let Some(agent) = agent {
                    self.nav.agent_idx = self
                        .nav
                        .agents
                        .iter()
                        .position(|known| known.id == agent.id);
                    if self.chat.convo.as_ref().map(Conversation::agent) != Some(&agent.id) {
                        self.open_conversation(&agent, context);
                    }
                }
            }
            MonitorMsg::MonitorStop(bound, force) => {
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
    pub(super) fn monitor_view(&self, context: &mut ViewContext<Self>) -> View {
        let shown = self
            .monitor
            .data
            .detail
            .as_ref()
            .and_then(|detail| detail.as_ref().ok())
            .filter(|read| Some(&read.agent.agent.id) == self.monitor.selected.as_ref())
            .map(|read| &read.agent.agent);
        let controllable = shown.filter(|agent| !agent.observe_only);
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
            stop: controllable.map(|agent| {
                context.message(Message::Monitor(MonitorMsg::MonitorStop(
                    BoundAction {
                        agent: agent.id.clone(),
                        generation: agent.generation,
                        turn: None,
                        request: None,
                    },
                    false,
                )))
            }),
            force_stop: controllable.map(|agent| {
                context.message(Message::Monitor(MonitorMsg::MonitorStop(
                    BoundAction {
                        agent: agent.id.clone(),
                        generation: agent.generation,
                        turn: None,
                        request: None,
                    },
                    true,
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
        )
    }
}
