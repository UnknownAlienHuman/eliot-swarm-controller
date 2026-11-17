//! Model and effort of the open Agent: the catalogue read under its own
//! route, and `configure` from the composer's model chip. The requested
//! versus effective readback lives in the details panel.

use super::*;
use crate::configure::Pick;

#[derive(Clone, Debug)]
pub(super) enum ConfigureMsg {
    CatalogLoaded(Box<configure::Catalog>),
    /// A model-chip menu entry (its label), for the Agent generation the
    /// chip shows when the click is handled.
    Chosen(String),
    /// The configured Agent; outer error: the submission itself; inner: the
    /// settled command read.
    ConfigureSettled(String, Box<Result<Fallible<Command>, eliot_core::Error>>),
}

/// The catalogue of the exact Agent generation and the last `configure`
/// outcome.
#[derive(Clone, Debug, Default)]
pub(super) struct ConfigureState {
    pub(super) catalog: Option<configure::Catalog>,
    /// Agent and generation whose catalogue read is in flight.
    pub(super) loading: Option<(String, i64)>,
    pub(super) status: String,
}

impl DesktopApp {
    pub(super) fn configure_update(&mut self, msg: ConfigureMsg, context: &ComponentContext<Self>) {
        match msg {
            ConfigureMsg::CatalogLoaded(catalog) => {
                let catalog = *catalog;
                let loaded = (catalog.agent.clone(), catalog.generation);
                if self.configure.loading.as_ref() == Some(&loaded) {
                    self.configure.loading = None;
                }
                let current = self
                    .selected_agent_id()
                    .zip(self.chat.detail.as_ref().map(|detail| detail.generation));
                if current == Some(loaded) {
                    self.configure.catalog = Some(catalog);
                }
            }
            ConfigureMsg::Chosen(label) => {
                let (Some(agent), Some(generation)) = (
                    self.selected_agent_id(),
                    self.chat.detail.as_ref().map(|detail| detail.generation),
                ) else {
                    return;
                };
                // Only the catalogue read for this exact generation applies.
                let (model, effort) = match self.configure_pick(&agent, generation, &label) {
                    Some(Pick::Model(Some(model))) => (Some(model), None),
                    Some(Pick::Effort(Some(effort))) => (None, Some(effort)),
                    _ => return,
                };
                if self.submitting() {
                    return;
                }
                let Ok(data_dir) = self.data_dir.clone() else {
                    return;
                };
                // Bound at click time: exact Agent generation and values, one
                // ULID per action, reused only when the reply was uncertain.
                let key = format!("configure:{agent}:{generation}:{model:?}:{effort:?}");
                let mutation_id = self.ledger.id_for(&key);
                self.in_flight_key = Some(key);
                self.alert = None;
                self.configure.status = format!("Configure {mutation_id}…");
                let _task = context.spawn_background(move |cancel| {
                    let cancelled = || cancel.is_cancelled();
                    let submitted = client::block_on(client::submit_configure(
                        &data_dir,
                        mutation_id,
                        &agent,
                        generation,
                        model,
                        effort,
                    ));
                    Message::Configure(ConfigureMsg::ConfigureSettled(
                        agent.clone(),
                        Box::new(submitted.map(|reply| {
                            settle_command(&data_dir, &reply.command_id, 90, &cancelled)
                        })),
                    ))
                });
            }
            ConfigureMsg::ConfigureSettled(agent, result) => {
                let result = *result;
                let uncertain = result.as_ref().err().is_some_and(crate::state::uncertain);
                self.settle_mutation(!uncertain);
                // A reply for an Agent no longer shown changes nothing on screen.
                if self.selected_agent_id().as_deref() != Some(agent.as_str()) {
                    return;
                }
                match result {
                    Ok(Ok(command)) => {
                        self.configure.status = configure::outcome_line(&command);
                        if command.state != eliot_core::CommandState::Applied {
                            self.alert(format!(
                                "Model or effort not changed: {}",
                                self.configure.status
                            ));
                        }
                    }
                    Ok(Err(error)) => {
                        self.configure.status =
                            format!("Configure admitted; outcome not read: {error}");
                        self.alert(self.configure.status.clone());
                    }
                    Err(error) if uncertain => {
                        self.configure.status = format!(
                            "Configure not confirmed (choosing it again reuses its id): {error}"
                        );
                        self.alert(self.configure.status.clone());
                    }
                    Err(error) => {
                        self.configure.status = format!("Configure refused: {error}");
                        self.alert(self.configure.status.clone());
                    }
                }
                self.queue_reread(
                    Invalidation {
                        detail: true,
                        ..Invalidation::default()
                    },
                    context,
                );
            }
        }
    }

    /// Read the catalogue for the open Agent's exact generation and route
    /// once.
    pub(super) fn ensure_catalog(&mut self, context: &ComponentContext<Self>) {
        let (Some(agent), Some(detail), Ok(data_dir)) = (
            self.selected_agent_id(),
            self.chat.detail.as_ref(),
            self.data_dir.clone(),
        ) else {
            return;
        };
        let key = (agent, detail.generation);
        let current = self
            .configure
            .catalog
            .as_ref()
            .is_some_and(|catalog| catalog.is_for(&key.0, key.1));
        if current || self.configure.loading.as_ref() == Some(&key) {
            return;
        }
        self.configure.loading = Some(key.clone());
        let harness = detail.harness.clone();
        let _task = context.spawn_background(move |_| {
            let reply = client::block_on(client::harness_models(&data_dir, &harness, Some(&key.0)))
                .map_err(|error| error.to_string());
            Message::Configure(ConfigureMsg::CatalogLoaded(Box::new(configure::Catalog {
                agent: key.0,
                generation: key.1,
                reply,
            })))
        });
    }
}
