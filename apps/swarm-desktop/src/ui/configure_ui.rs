//! Model and effort for the selected Agent: catalogue read under its route, configure, requested versus effective.

use super::*;
use crate::theme::winui::Palette;

#[derive(Clone, Debug)]
pub(super) enum ConfigureMsg {
    CatalogLoaded(Box<configure::Catalog>),
    CatalogRefreshRequested,
    ConfigureModelPicked(Option<usize>),
    ConfigureEffortPicked(Option<usize>),
    ConfigureExpanded(bool),
    ConfigureRequested,
    /// The configured Agent; outer error: the submission itself; inner: the
    /// settled command read.
    ConfigureSettled(String, Box<Result<Fallible<Command>, String>>),
}

/// Model/effort section of the Agent detail: the catalogue of the exact
/// Agent generation, the picks (index 0 keeps the field) and the last
/// `configure` outcome.
#[derive(Clone, Debug, Default)]
pub(super) struct ConfigureState {
    pub(super) catalog: Option<configure::Catalog>,
    /// Agent and generation whose catalogue read is in flight.
    pub(super) loading: Option<(String, i64)>,
    pub(super) model_pick: Option<usize>,
    pub(super) effort_pick: Option<usize>,
    pub(super) status: String,
    pub(super) expanded: bool,
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
                    self.configure.model_pick = Some(0);
                    self.configure.effort_pick = Some(0);
                }
            }
            ConfigureMsg::CatalogRefreshRequested => {
                self.configure.catalog = None;
                self.configure.loading = None;
                self.ensure_catalog(context);
            }
            ConfigureMsg::ConfigureModelPicked(index) => {
                if index.is_some() && index != self.configure.model_pick {
                    self.configure.model_pick = index;
                    // Efforts belong to the picked model.
                    self.configure.effort_pick = Some(0);
                }
            }
            ConfigureMsg::ConfigureEffortPicked(index) => {
                if index.is_some() {
                    self.configure.effort_pick = index;
                }
            }
            ConfigureMsg::ConfigureExpanded(open) => {
                self.configure.expanded = open;
                if open {
                    self.ensure_catalog(context);
                }
            }
            ConfigureMsg::ConfigureRequested => {
                let (Some(agent), Some(detail)) =
                    (self.selected_agent_id(), self.chat.detail.clone())
                else {
                    self.configure.status =
                        "Select an Agent and wait for its catalogue.".to_owned();
                    return;
                };
                // Only the catalogue read for this exact generation applies.
                let Some(catalog) = self
                    .configure
                    .catalog
                    .clone()
                    .filter(|catalog| catalog.is_for(&agent, detail.generation))
                else {
                    self.configure.status =
                        "Select an Agent and wait for its catalogue.".to_owned();
                    return;
                };
                let (model, effort) = self.configure_picks(&catalog, &detail);
                if model.is_none() && effort.is_none() {
                    self.configure.status = "Pick a model or an effort to change.".to_owned();
                    return;
                }
                if self.submitting() {
                    return;
                }
                let Ok(data_dir) = self.data_dir.clone() else {
                    return;
                };
                // Bound at click time: exact Agent generation and values, one
                // ULID per action, reused only when the reply was uncertain.
                let generation = detail.generation;
                let key = format!("configure:{agent}:{generation}:{model:?}:{effort:?}");
                let mutation_id = self.ledger.id_for(&key);
                self.in_flight_key = Some(key);
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
                    ))
                    .map_err(|error| error.to_string());
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
                let uncertain = result
                    .as_ref()
                    .err()
                    .is_some_and(|error| crate::state::uncertain(error));
                self.settle_mutation(!uncertain);
                // A reply for an Agent no longer shown changes nothing on screen.
                if self.selected_agent_id().as_deref() != Some(agent.as_str()) {
                    return;
                }
                self.configure.status = match result {
                    Ok(Ok(command)) => {
                        self.configure.model_pick = Some(0);
                        self.configure.effort_pick = Some(0);
                        configure::outcome_line(&command)
                    }
                    Ok(Err(error)) => format!("Configure admitted; outcome not read: {error}"),
                    Err(error) if uncertain => {
                        format!("Configure outcome uncertain (Apply again reuses its id): {error}")
                    }
                    Err(error) => format!("Configure refused: {error}"),
                };
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

    /// Read the catalogue for the selected Agent's exact generation and
    /// route once, while the section is open.
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
        if !self.configure.expanded || current || self.configure.loading.as_ref() == Some(&key) {
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

    /// The picked values that would change something; refused fields never
    /// submit.
    pub(super) fn configure_picks(
        &self,
        catalog: &configure::Catalog,
        detail: &UiDetail,
    ) -> (Option<String>, Option<String>) {
        let models = catalog.models();
        let model = configure::picked(&models, self.configure.model_pick)
            .filter(|_| catalog.model_refusal().is_none());
        let efforts = catalog.efforts(model.as_deref().or(detail.settings.next_model()));
        let effort = configure::picked(&efforts, self.configure.effort_pick)
            .filter(|_| catalog.effort_refusal().is_none());
        (model, effort)
    }

    /// The Agent detail's model/effort section; empty without a readback.
    pub(super) fn configure_view(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        let Some(detail) = self
            .chat
            .detail
            .as_ref()
            .filter(|_| self.selected_agent_id().is_some())
        else {
            return View::empty();
        };
        let catalog = self.configure.catalog.as_ref().filter(|catalog| {
            self.selected_agent_id()
                .is_some_and(|agent| catalog.is_for(&agent, detail.generation))
        });
        let models = catalog.map(configure::Catalog::models).unwrap_or_default();
        let model = configure::picked(&models, self.configure.model_pick);
        let efforts = catalog
            .map(|catalog| catalog.efforts(model.as_deref().or(detail.settings.next_model())))
            .unwrap_or_default();
        let (picked_model, picked_effort) = catalog
            .map(|catalog| self.configure_picks(catalog, detail))
            .unwrap_or_default();
        let can_apply = (picked_model.is_some() || picked_effort.is_some()) && !self.submitting();
        let apply_label = configure::apply_label(picked_model.as_deref(), picked_effort.as_deref());
        let catalog_line = match (catalog, &self.configure.loading) {
            (Some(catalog), _) => catalog.line(),
            (None, Some(_)) => format!("Reading the {} catalogue for this Agent…", detail.harness),
            (None, None) => "Catalogue not read yet.".to_owned(),
        };
        configure::panel(
            configure::ConfigureData {
                settings: &detail.settings,
                catalog_line,
                models,
                model_pick: self.configure.model_pick,
                model_refusal: catalog.and_then(configure::Catalog::model_refusal),
                efforts,
                effort_pick: self.configure.effort_pick,
                effort_refusal: catalog.and_then(configure::Catalog::effort_refusal),
                can_apply,
                apply_label,
                status: &self.configure.status,
                expanded: self.configure.expanded,
            },
            configure::ConfigureActions {
                model: context.callback(|value| {
                    Message::Configure(ConfigureMsg::ConfigureModelPicked(value))
                }),
                effort: context.callback(|value| {
                    Message::Configure(ConfigureMsg::ConfigureEffortPicked(value))
                }),
                apply: context.message(Message::Configure(ConfigureMsg::ConfigureRequested)),
                refresh: context.message(Message::Configure(ConfigureMsg::CatalogRefreshRequested)),
                expanded: context
                    .callback(|value| Message::Configure(ConfigureMsg::ConfigureExpanded(value))),
            },
            palette,
        )
    }
}
