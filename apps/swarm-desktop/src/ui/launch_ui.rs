//! A new task: the project it starts in, Auto / a profile / an ad-hoc
//! harness with its model and effort, and the first message, all set from
//! the composer's chips.
//!
//! "Auto" stands for daemon routing (k55). Until routing lands it launches
//! the default profile from Settings, else the first launchable profile the
//! project allows, and the chip says which one.

use super::*;
use crate::configure::Pick;
use crate::theme::winui::Palette;
use eliot_core::api::HarnessModelsResponse;

/// The project chip's entry that shows the folder field.
const OTHER_FOLDER: &str = "Other folder…";
/// The profile chip's routing entry.
const AUTO: &str = "Auto";

#[derive(Clone, Debug)]
pub(super) enum LaunchMsg {
    WorkspaceChanged(String),
    /// A project chip entry: a registered project id or [`OTHER_FOLDER`].
    ProjectMenu(String),
    /// A profile chip entry: [`AUTO`], a profile name or "<harness> harness".
    ProfileMenu(String),
    /// A model-and-effort chip entry (ad-hoc harness).
    ModelMenu(String),
    LaunchRequested,
    LaunchSubmitted(Fallible<LaunchOutcome>),
    ProfilesListed(Fallible<ProfileListResponse>),
    HarnessCatalogLoaded(String, Box<Fallible<HarnessModelsResponse>>),
}

/// What a new task launches.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(super) enum Choice {
    /// Routing (until it lands: the default profile).
    #[default]
    Auto,
    Profile(String),
    /// An Owner ad-hoc launch of one harness.
    Harness(String),
}

/// The new-task draft and its context.
#[derive(Default)]
pub(super) struct LaunchState {
    /// The selected project's profile allowlist (empty: any profile).
    pub(super) profiles: Vec<String>,
    pub(super) workspace: String,
    /// The folder field is shown (a path outside the registered projects).
    pub(super) custom_workspace: bool,
    pub(super) launch_text: String,
    pub(super) launch_rev: u64,
    /// Daemon `profile.list` summaries.
    pub(super) profile_summaries: Vec<ProfileSummary>,
    pub(super) profile_list: Option<ProfileListResponse>,
    pub(super) choice: Choice,
    pub(super) adhoc_model: Option<String>,
    pub(super) adhoc_effort: Option<String>,
    /// The ad-hoc harness's catalogue, once read.
    pub(super) catalog: Option<(String, Fallible<HarnessModelsResponse>)>,
}

impl LaunchState {
    /// Profiles a new task may use: the daemon's summaries, narrowed to the
    /// project's allowlist when it has one.
    pub(super) fn allowed(&self) -> Vec<&ProfileSummary> {
        self.profile_summaries
            .iter()
            .filter(|profile| self.profiles.is_empty() || self.profiles.contains(&profile.name))
            .collect()
    }

    /// A profile choice the project no longer allows falls back to Auto.
    pub(super) fn reconcile_choice(&mut self) {
        if let Choice::Profile(name) = &self.choice
            && !self.profile_summaries.is_empty()
            && !self.allowed().iter().any(|profile| &profile.name == name)
        {
            self.choice = Choice::Auto;
        }
    }

    /// The profile Auto launches now: the preferred one when the project
    /// allows it and it is launchable, else the first launchable allowed.
    pub(super) fn auto_profile(&self, preferred: Option<&str>) -> Option<&ProfileSummary> {
        let allowed = self.allowed();
        preferred
            .and_then(|name| {
                allowed
                    .iter()
                    .find(|profile| profile.name == name && profile.launchable)
            })
            .or_else(|| allowed.iter().find(|profile| profile.launchable))
            .copied()
    }

    /// The profile this task launches (Auto resolved), if any.
    pub(super) fn profile(&self, preferred: Option<&str>) -> Option<&ProfileSummary> {
        match &self.choice {
            Choice::Auto => self.auto_profile(preferred),
            Choice::Profile(name) => self
                .profile_summaries
                .iter()
                .find(|profile| &profile.name == name),
            Choice::Harness(_) => None,
        }
    }

    /// Model and effort options of the ad-hoc harness's catalogue.
    fn adhoc_options(&self) -> (Vec<String>, Vec<String>) {
        match (&self.choice, &self.catalog) {
            (Choice::Harness(harness), Some((read, Ok(reply)))) if harness == read => {
                configure::options(reply, self.adhoc_model.as_deref())
            }
            _ => (Vec::new(), Vec::new()),
        }
    }
}

impl DesktopApp {
    pub(super) fn launch_update(&mut self, msg: LaunchMsg, context: &ComponentContext<Self>) {
        match msg {
            LaunchMsg::WorkspaceChanged(text) => self.launch.workspace = text,
            LaunchMsg::ProjectMenu(label) => {
                if label == OTHER_FOLDER {
                    self.launch.custom_workspace = true;
                    self.launch.workspace.clear();
                    self.launch.profiles.clear();
                    self.nav.selection.project = None;
                } else if let Some(project) = self
                    .nav
                    .projects
                    .iter()
                    .find(|project| project.id == label)
                    .cloned()
                {
                    self.launch.workspace = project.workspace.clone();
                    self.launch.custom_workspace = false;
                    self.launch.profiles = project.profiles.clone();
                    self.nav.selection.project = Some(project.id.clone());
                    self.launch.reconcile_choice();
                }
            }
            LaunchMsg::ProfileMenu(label) => {
                let choice = if label == AUTO {
                    Choice::Auto
                } else if let Some(harness) = label
                    .strip_suffix(" harness")
                    .filter(|harness| ADHOC_HARNESSES.contains(harness))
                {
                    Choice::Harness(harness.to_owned())
                } else {
                    Choice::Profile(label)
                };
                if let Choice::Harness(harness) = &choice {
                    if self.launch.choice != choice {
                        self.launch.adhoc_model = None;
                        self.launch.adhoc_effort = None;
                    }
                    let held =
                        matches!(&self.launch.catalog, Some((read, Ok(_))) if read == harness);
                    if !held {
                        self.read_harness_catalog(harness.clone(), context);
                    }
                }
                self.nav.selection.profile = match &choice {
                    Choice::Profile(name) => Some(name.clone()),
                    _ => None,
                };
                self.launch.choice = choice;
            }
            LaunchMsg::ModelMenu(label) => {
                let (models, efforts) = self.launch.adhoc_options();
                match configure::resolve(&label, &models, &efforts) {
                    Some(Pick::Model(model)) => {
                        self.launch.adhoc_model = model;
                        // Efforts belong to the model.
                        self.launch.adhoc_effort = None;
                    }
                    Some(Pick::Effort(effort)) => self.launch.adhoc_effort = effort,
                    None => {}
                }
            }
            LaunchMsg::HarnessCatalogLoaded(harness, reply) => {
                // A reply for a harness no longer chosen is dropped.
                if self.launch.choice == Choice::Harness(harness.clone()) {
                    self.launch.catalog = Some((harness, *reply));
                }
            }
            LaunchMsg::LaunchRequested => {
                if self.submitting() {
                    return;
                }
                let Ok(data_dir) = self.data_dir.clone() else {
                    return;
                };
                let target = match self.launch_target() {
                    Ok(target) => target,
                    Err(error) => {
                        self.alert(format!("Cannot start: {error}."));
                        return;
                    }
                };
                let text = profiles::normalize_newlines(&self.launch.launch_text);
                if text.trim().is_empty() {
                    self.alert("Type the first message.");
                    return;
                }
                if self.launch.workspace.trim().is_empty() {
                    self.alert("Choose a project or a folder for the task.");
                    return;
                }
                let key = format!(
                    "launch:{target:?}:{}:{}",
                    self.launch.workspace, self.launch.launch_rev
                );
                let mutation_id = self.ledger.id_for(&key);
                self.in_flight_key = Some(key);
                let workspace = self.launch.workspace.clone();
                let launch_rev = self.launch.launch_rev;
                self.alert = None;
                self.set_status(format!("Launching ({mutation_id})…"));
                let _task = context.spawn_background(move |_| {
                    Message::Launch(LaunchMsg::LaunchSubmitted(client::block_on(async {
                        let reply = client::submit_launch(
                            &data_dir,
                            mutation_id,
                            target,
                            &workspace,
                            Some(text.clone()),
                        )
                        .await
                        .map_err(|error| error.to_string())?;
                        Ok(LaunchOutcome {
                            // Admitted without an Agent id: the outcome is
                            // unknown, so the id is kept for a same-id retry.
                            agent: reply.agent.clone().ok_or_else(|| {
                                "unknown: the launch was admitted without an Agent id".to_owned()
                            })?,
                            generation: reply.generation.unwrap_or_default(),
                            command_id: reply.command_id.clone(),
                            linked_command: reply.linked_command.clone(),
                            text,
                            launch_rev,
                        })
                    })))
                });
            }
            LaunchMsg::LaunchSubmitted(outcome) => {
                // Keyboard focus goes back to the composer (no-op while it is unbound).
                let _ = self.chat.composer_ref.request_focus();
                self.settle_mutation(
                    outcome
                        .as_ref()
                        .err()
                        .is_none_or(|error| !crate::state::uncertain(error)),
                );
                match outcome {
                    Ok(outcome) => {
                        // The first prompt rode the launch admission; it is
                        // never sent again. Clear only the submitted draft.
                        if self.launch.launch_rev == outcome.launch_rev {
                            self.launch.launch_text.clear();
                        }
                        if let Some(linked) = outcome.linked_command.clone() {
                            self.chat.pending_local =
                                Some((outcome.agent.clone(), linked, outcome.text.clone()));
                        }
                        // The title is the first message, known now.
                        self.nav
                            .titles
                            .insert(outcome.agent.clone(), nav::title(&outcome.text));
                        self.set_status(format!(
                            "Launched {} ({}); its first input is {}.",
                            outcome.agent,
                            outcome.command_id,
                            outcome.linked_command.as_deref().unwrap_or("not linked")
                        ));
                        self.nav.pending_select = Some(UiAgent {
                            id: outcome.agent.clone(),
                            generation: outcome.generation,
                            harness: String::new(),
                            state: "created".to_owned(),
                            project: self.launch.workspace.clone(),
                            parent: None,
                            updated_at: handoff_now_ms(),
                        });
                        self.nav.selection.select_agent(Some(outcome.agent));
                        self.load_agents(context, None);
                    }
                    Err(error) if crate::state::uncertain(&error) => {
                        self.alert(format!(
                            "Start not confirmed (sending again reuses its id): {error}"
                        ));
                    }
                    Err(error) => self.alert(format!("Start refused: {error}")),
                }
            }
            LaunchMsg::ProfilesListed(result) => match result {
                Ok(list) => {
                    self.launch.profile_summaries = list.profiles.clone();
                    self.launch.profile_list = Some(list);
                    self.launch.reconcile_choice();
                }
                Err(error) => self.alert(format!("Profiles not read: {error}")),
            },
        }
    }

    /// Read the ad-hoc harness's model catalogue (models and efforts).
    fn read_harness_catalog(&self, harness: String, context: &ComponentContext<Self>) {
        let Ok(data_dir) = self.data_dir.clone() else {
            return;
        };
        let _task = context.spawn_background(move |_| {
            let reply = client::block_on(client::harness_models(&data_dir, &harness, None))
                .map_err(|error| error.to_string());
            Message::Launch(LaunchMsg::HarnessCatalogLoaded(harness, Box::new(reply)))
        });
    }

    pub(super) fn launch_target(&self) -> Result<LaunchTarget, String> {
        match &self.launch.choice {
            Choice::Harness(harness) => Ok(LaunchTarget::Adhoc {
                harness: harness.clone(),
                model: self.launch.adhoc_model.clone(),
                effort: self.launch.adhoc_effort.clone(),
            }),
            Choice::Profile(name) => Ok(LaunchTarget::Profile(name.clone())),
            Choice::Auto => self
                .launch
                .auto_profile(None)
                .map(|profile| LaunchTarget::Profile(profile.name.clone()))
                .ok_or_else(|| {
                    "no launchable profile is allowed here; add one in Settings, or pick a harness"
                        .to_owned()
                }),
        }
    }

    pub(super) fn load_profile_list(&mut self, context: &ComponentContext<Self>) {
        let Ok(data_dir) = self.data_dir.clone() else {
            return;
        };
        let _task = context.spawn_background(move |_| {
            Message::Launch(LaunchMsg::ProfilesListed(client::block_on(async {
                config_client::list_profiles(&data_dir)
                    .await
                    .map_err(|error| error.to_string())
            })))
        });
    }

    /// The composer's context row for a new task: the project chip (or the
    /// folder field) and the Auto / profile / harness chip.
    pub(super) fn launch_context(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        let project_label = self
            .selected_project()
            .map(|project| project.id.clone())
            .or_else(|| {
                (!self.launch.workspace.is_empty())
                    .then(|| nav::project_name(&self.launch.workspace, &[]))
            })
            .unwrap_or_else(|| "Choose a project".to_owned());
        let mut project_items: Vec<MenuItem> = self
            .nav
            .projects
            .iter()
            .map(|project| MenuItem::item(format!("p:{}", project.id), project.id.clone()))
            .collect();
        if !project_items.is_empty() {
            project_items.push(MenuItem::separator("sep"));
        }
        project_items.push(MenuItem::item("other", OTHER_FOLDER));
        let project = m3::chip(palette)
            .automation_name(format!("Project: {project_label}"))
            .content(m3::chip_label(m3::glyph::FOLDER, &project_label))
            .menu(Menu::new(
                project_items,
                context.callback(|label: String| Message::Launch(LaunchMsg::ProjectMenu(label))),
            ));
        let folder: View = if self.launch.custom_workspace || self.nav.projects.is_empty() {
            m3::field()
                .text(self.launch.workspace.clone())
                .placeholder_text("Folder path")
                .automation_name("Folder")
                .width(280.0)
                .vertical_alignment(VerticalAlignment::Center)
                .on_text_changed(
                    context.callback(|value| Message::Launch(LaunchMsg::WorkspaceChanged(value))),
                )
                .into()
        } else {
            View::empty()
        };

        let auto = self.launch.auto_profile(None);
        let profile_label = match &self.launch.choice {
            Choice::Auto => match auto {
                Some(profile) => format!("Auto · {}", profile.name),
                None => "Auto".to_owned(),
            },
            Choice::Profile(name) => name.clone(),
            Choice::Harness(harness) => format!("{harness} harness"),
        };
        let mut profile_items = vec![MenuItem::item("auto", AUTO)];
        let allowed = self.launch.allowed();
        if !allowed.is_empty() {
            profile_items.push(MenuItem::separator("sep-profiles"));
            profile_items.extend(allowed.iter().map(|profile| {
                if profile.launchable {
                    MenuItem::item(format!("p:{}", profile.name), profile.name.clone())
                } else {
                    MenuItem::disabled(format!("p:{}", profile.name), profile.name.clone())
                }
            }));
        }
        profile_items.push(MenuItem::separator("sep-harness"));
        profile_items.push(MenuItem::submenu(
            "harness",
            "Harness",
            ADHOC_HARNESSES.iter().map(|harness| {
                MenuItem::item(format!("h:{harness}"), format!("{harness} harness"))
            }),
        ));
        let profile = m3::chip(palette)
            .automation_name(format!("Profile: {profile_label}"))
            .content(m3::chip_label(m3::glyph::PROFILE, &profile_label))
            .menu(Menu::new(
                profile_items,
                context.callback(|label: String| Message::Launch(LaunchMsg::ProfileMenu(label))),
            ));
        StackPanel::new()
            .orientation(Orientation::Horizontal)
            .spacing(theme::spacing::SM)
            .children((project, folder, profile))
    }

    /// The model-and-effort chip and the access chip of a new task: read
    /// from the profile, or chosen from the ad-hoc harness's catalogue.
    pub(super) fn launch_chips(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        let profile = self.launch.profile(None);
        let (model_chip, access): (View, String) = match (&self.launch.choice, profile) {
            (Choice::Harness(harness), _) => {
                let (models, efforts) = self.launch.adhoc_options();
                let label = format!(
                    "{} · {}",
                    self.launch
                        .adhoc_model
                        .as_deref()
                        .unwrap_or("Default model"),
                    self.launch
                        .adhoc_effort
                        .as_deref()
                        .unwrap_or("default effort")
                );
                let loading = !matches!(&self.launch.catalog, Some((read, _)) if read == harness);
                let items = match &self.launch.catalog {
                    Some((read, Err(error))) if read == harness => {
                        vec![MenuItem::disabled(
                            "error",
                            format!("Catalogue unavailable: {error}"),
                        )]
                    }
                    _ if loading => vec![MenuItem::disabled("loading", "Reading the catalogue…")],
                    _ => configure::menu_items(&models, &efforts, true, None, None),
                };
                (
                    m3::chip(palette)
                        .automation_name(format!("Model and effort: {label}"))
                        .content(m3::chip_label(m3::glyph::MODEL, &label))
                        .menu(Menu::new(
                            items,
                            context.callback(|label: String| {
                                Message::Launch(LaunchMsg::ModelMenu(label))
                            }),
                        )),
                    access_label(None),
                )
            }
            (_, Some(profile)) => (
                m3::chip_static(
                    palette,
                    m3::glyph::MODEL,
                    &format!(
                        "{} · {}",
                        profile.model.as_deref().unwrap_or("default model"),
                        profile.effort.as_deref().unwrap_or("default effort")
                    ),
                ),
                access_label(profile.permissions.as_deref()),
            ),
            (_, None) => (
                m3::chip_static(palette, m3::glyph::MODEL, "No profile"),
                access_label(None),
            ),
        };
        StackPanel::new()
            .orientation(Orientation::Horizontal)
            .spacing(theme::spacing::SM)
            .children((
                model_chip,
                m3::chip_static(palette, m3::glyph::SHIELD, &access),
            ))
    }
}

/// The access chip's words for a profile's permissions.
pub(super) fn access_label(permissions: Option<&str>) -> String {
    match permissions {
        None => "Default access".to_owned(),
        Some("danger-full-access") => "Full access".to_owned(),
        Some("read-only") => "Read only".to_owned(),
        Some("workspace-write") => "Workspace write".to_owned(),
        Some(other) => other.replace(['-', '_'], " "),
    }
}

/// Now in ms since the epoch, for a just-launched row's Recents order.
fn handoff_now_ms() -> i64 {
    crate::handoff::now_ms()
}
