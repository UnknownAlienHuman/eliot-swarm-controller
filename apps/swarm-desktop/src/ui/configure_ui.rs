//! Model and effort of the open Agent: the catalogue read under its own
//! route, and `configure` from the composer's effort popover and model
//! picker. The requested versus effective readback lives in the details
//! panel.

use super::*;

/// Reads after which a failed catalogue stops re-reading on every
/// open: further tries need an explicit retry or a refresh (#681).
const MAX_CATALOG_ATTEMPTS: u8 = 3;

/// Whether a catalogue read may start: never over a cached success or a
/// read in flight; a cached failure retries boundedly unless forced.
fn catalog_read_allowed(
    success: bool,
    loading: bool,
    failed_here: bool,
    attempts: u8,
    retry: bool,
) -> bool {
    if success || loading {
        return false;
    }
    if !failed_here {
        return true;
    }
    retry || attempts < MAX_CATALOG_ATTEMPTS
}

#[derive(Clone, Debug)]
pub(super) enum ConfigureMsg {
    CatalogLoaded(Box<configure::Catalog>),
    /// Re-read the open Agent's catalogue after a failure (the effort
    /// error view's Retry, the Agent picker's Retry while it shows the
    /// Agent's route): one more bounded round for the same generation.
    CatalogRetry,
    /// A model and/or effort for the next turn (an effort step, a model
    /// picker row), for the Agent generation shown when the click is
    /// handled; only what differs and the route lists is sent.
    Chosen {
        model: Option<String>,
        effort: Option<String>,
    },
    /// The configured Agent; outer error: the submission itself; inner: the
    /// settled command read.
    ConfigureSettled(String, Box<Result<Fallible<Command>, eliot_core::Error>>),
}

/// The catalogue of the exact Agent generation and the last `configure`
/// outcome. Successes live in `catalog`; failures in `failed`: a transient
/// error never disables the model/effort actions past its bounded retries,
/// and a refresh drops it while the success (if any) stays.
#[derive(Clone, Debug, Default)]
pub(super) struct ConfigureState {
    pub(super) catalog: Option<configure::Catalog>,
    /// The last failed catalogue read, with its Agent and generation.
    pub(super) failed: Option<configure::Catalog>,
    /// Reads started for the failed generation (bounded by
    /// [`MAX_CATALOG_ATTEMPTS`], reset by success, retry or refresh).
    pub(super) failed_attempts: u8,
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
                    if catalog.reply.is_ok() {
                        self.configure.catalog = Some(catalog);
                        self.configure.failed = None;
                        self.configure.failed_attempts = 0;
                    } else {
                        self.configure.failed = Some(catalog);
                    }
                    match self.launch.open {
                        Some(launch_ui::Pop::Model) if self.popover_agent().is_some() => {
                            if self.agent_model_allowed()
                                || self
                                    .popover_agent()
                                    .is_some_and(|detail| detail.harness == "opencode")
                            {
                                self.keep_highlight();
                            } else {
                                self.launch_update(
                                    LaunchMsg::Toggle(launch_ui::Pop::Effort),
                                    context,
                                );
                            }
                        }
                        Some(launch_ui::Pop::Effort)
                            if self
                                .launch
                                .effort_highlight
                                .as_ref()
                                .is_some_and(|effort| !self.agent_efforts().contains(effort)) =>
                        {
                            self.launch.effort_highlight = None;
                        }
                        _ => {}
                    }
                }
            }
            ConfigureMsg::CatalogRetry => self.retry_catalog(context),
            ConfigureMsg::Chosen { model, effort } => {
                let (Some(agent), Some(detail)) =
                    (self.selected_agent_id(), self.chat.detail.as_ref())
                else {
                    return;
                };
                let generation = detail.generation;
                // Only the catalogue read for this exact generation applies.
                let Some(Ok(reply)) = self
                    .configure
                    .catalog
                    .as_ref()
                    .filter(|catalog| catalog.is_for(&agent, generation))
                    .map(|catalog| &catalog.reply)
                else {
                    return;
                };
                let Some((model, effort)) =
                    configure::change(reply, &detail.settings, model.as_deref(), effort.as_deref())
                else {
                    return;
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
                self.configure.status = "Changing the model or effort…".to_owned();
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
                        self.configure.status = configure::outcome_words(&command);
                        if command.state != eliot_core::CommandState::Applied {
                            self.alert(configure::failure_alert(&command, &self.configure.status));
                        }
                    }
                    Ok(Err(error)) => {
                        self.configure.status = format!(
                            "The change was accepted, but its outcome could not be read: {}",
                            crate::state::plain_error(&error)
                        );
                        self.alert(self.configure.status.clone());
                    }
                    Err(error) if uncertain => {
                        self.configure.status = format!(
                            "The change may not have reached Swarm; choosing it again repeats the same request. {}",
                            error.user_message()
                        );
                        self.alert(self.configure.status.clone());
                    }
                    Err(error) => {
                        self.configure.status =
                            format!("Model or effort not changed: {}", error.user_message());
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

    /// Read the catalogue for the open Agent's exact generation and
    /// route once; a cached failure retries boundedly on opens.
    pub(super) fn ensure_catalog(&mut self, context: &ComponentContext<Self>) {
        self.read_catalog(context, false);
    }

    /// Re-read the catalogue after a failure: one more bounded round for
    /// the same generation.
    pub(super) fn retry_catalog(&mut self, context: &ComponentContext<Self>) {
        self.read_catalog(context, true);
    }

    fn read_catalog(&mut self, context: &ComponentContext<Self>, retry: bool) {
        let (Some(agent), Some(detail), Ok(data_dir)) = (
            self.selected_agent_id(),
            self.chat.detail.as_ref(),
            self.data_dir.clone(),
        ) else {
            return;
        };
        let key = (agent, detail.generation);
        let success = self
            .configure
            .catalog
            .as_ref()
            .is_some_and(|catalog| catalog.is_for(&key.0, key.1) && catalog.reply.is_ok());
        let loading = self.configure.loading.as_ref() == Some(&key);
        let failed_here = self
            .configure
            .failed
            .as_ref()
            .is_some_and(|catalog| catalog.is_for(&key.0, key.1));
        if !failed_here {
            // A failure for another Agent or generation never blocks.
            self.configure.failed = None;
            self.configure.failed_attempts = 0;
        } else if retry {
            self.configure.failed_attempts = 0;
        }
        if !catalog_read_allowed(
            success,
            loading,
            failed_here,
            self.configure.failed_attempts,
            retry,
        ) {
            return;
        }
        self.configure.failed_attempts += 1;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_cached_failure_retries_boundedly_until_forced() {
        // #681: successes and in-flight reads never re-read; a failure
        // retries a few times on opens, then needs an explicit retry.
        assert!(!catalog_read_allowed(true, false, false, 0, false));
        assert!(!catalog_read_allowed(true, false, true, 0, false));
        assert!(!catalog_read_allowed(false, true, false, 0, false));
        assert!(catalog_read_allowed(false, false, false, 0, false));
        assert!(catalog_read_allowed(false, false, true, 0, false));
        assert!(catalog_read_allowed(
            false,
            false,
            true,
            MAX_CATALOG_ATTEMPTS - 1,
            false
        ));
        assert!(!catalog_read_allowed(
            false,
            false,
            true,
            MAX_CATALOG_ATTEMPTS,
            false
        ));
        assert!(catalog_read_allowed(
            false,
            false,
            true,
            MAX_CATALOG_ATTEMPTS,
            true
        ));
    }
}
