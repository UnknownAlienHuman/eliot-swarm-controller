//! A new task (owner review 2026-11-16, the Codex, OpenCode and Antigravity
//! composers as references). The composer's top row is the project chip;
//! the bottom row is the model chip (`Harness · Model · effort`), the
//! access chip and Send. There are no profiles in it: the model chip opens
//! the picker ([`crate::picker`], fed by `models.catalog`) with Auto, the
//! favorites, Recent, every harness's models and the saved presets; the
//! access chip opens the Codex access menu (Ask for approval / Approve for
//! me / Full access → launch `access` ask / auto / full, remembered in
//! `[desktop] last_access`).
//!
//! "Auto" is the daemon's routing: the launch names no profile and admission
//! picks the first available candidate of the default role. Before launch
//! the picker shows `routing.preview`'s pick and why; after launch the
//! Agent details show the recorded decision. A model choice is an Owner
//! ad-hoc launch (`harness`, `model`, `effort`, `access`), which the daemon
//! validates and freezes; a preset launches its profile as configured.
//!
//! The popovers are drawn in the window above the composer, not as WinUI
//! flyouts: Reactor 0.100.0 cannot close a `Flyout` it opened, and a menu
//! whose callback captures data is replaced (dropping the click) on every
//! render. Their keys come from the window's keyboard hook
//! ([`crate::shortcuts`]) while one is open.

use super::*;
use crate::picker::{self, Choice, Item, Pick, Row};
use crate::settings::Access;
use crate::shortcuts::{Popover as HookPopover, PopoverKey};
use eliot_core::api::ModelsCatalogResponse;

/// A popover above the composer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Pop {
    Model,
    Access,
    Project,
}

/// The project popover's row that shows the folder field.
pub(super) const OTHER_FOLDER: &str = "other-folder";
/// Focus attempts for a popover's search field, and the wait between them.
const FIELD_FOCUS_TRIES: u8 = 5;
const FIELD_FOCUS_RETRY_MS: u64 = 50;
/// Re-check for the render that bound the highlighted row, one frame later.
const REVEAL_RETRY_MS: u64 = 16;

#[derive(Clone, Debug)]
pub(super) enum LaunchMsg {
    WorkspaceChanged(String),
    LaunchRequested,
    LaunchSubmitted(Fallible<LaunchOutcome>),
    ProfilesListed(Fallible<ProfileListResponse>),
    /// `routing.preview` for the default role.
    PreviewLoaded(Box<Fallible<eliot_core::RoutingDecision>>),
    /// `models.catalog`, every page.
    CatalogLoaded(Box<Fallible<ModelsCatalogResponse>>),
    /// Open a popover, or close it when it is the one open.
    Toggle(Pop),
    /// Close the open popover (a click outside it, Esc).
    Close,
    /// The model search field's text.
    PickerQuery(String),
    /// A picker row was clicked (its key).
    PickerChosen(String),
    /// An effort on the highlighted row was clicked: choose the row with it.
    PickerEffort(String, String),
    /// The pointer entered a picker row: highlight it.
    PickerHover(String),
    /// The highlighted row's star.
    PickerFavorite(String),
    AccessChosen(Access),
    /// The pointer entered an access row.
    AccessHover(Access),
    /// The project search field's text.
    ProjectQuery(String),
    /// A project row (its checkout) or [`OTHER_FOLDER`].
    ProjectChosen(String),
    /// A popover key from the keyboard hook.
    Key(PopoverKey),
    /// After the render that bound the highlighted row: bring it into view.
    Reveal(u64),
    /// Put the keyboard in the open popover's search field (tries left).
    FocusField(u8),
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
    /// Daemon `profile.list` summaries (the presets).
    pub(super) profile_summaries: Vec<ProfileSummary>,
    pub(super) profile_list: Option<ProfileListResponse>,
    pub(super) choice: Choice,
    /// The owner chose in this window (a later preference read keeps it).
    pub(super) chosen: bool,
    pub(super) access: Option<Access>,
    /// The unified catalogue, once read.
    pub(super) catalog: Option<Fallible<ModelsCatalogResponse>>,
    pub(super) catalog_loading: bool,
    /// What routing would pick for a new task now (advisory).
    pub(super) preview: Option<Fallible<eliot_core::RoutingDecision>>,
    pub(super) preview_loading: bool,
    /// A refresh was asked for while one ran: read again when it lands.
    pub(super) preview_again: bool,
    /// The open popover.
    pub(super) open: Option<Pop>,
    pub(super) query: String,
    pub(super) project_query: String,
    /// The highlighted picker or project row (its key).
    pub(super) highlight: Option<String>,
    pub(super) access_highlight: Option<Access>,
    /// Efforts chosen on picker rows with ←/→ (row key → effort).
    pub(super) efforts: HashMap<String, String>,
    /// The open popover's search field.
    pub(super) field_ref: ElementRef<TextBox>,
    /// The highlighted picker row (brought into view by focusing it).
    pub(super) row_ref: ElementRef<Button>,
    pub(super) reveal_epoch: u64,
    pub(super) reveal_after: u64,
}

impl LaunchState {
    /// Presets a new task may use: the daemon's summaries, narrowed to the
    /// project's allowlist when it has one.
    pub(super) fn allowed(&self) -> Vec<&ProfileSummary> {
        self.profile_summaries
            .iter()
            .filter(|profile| self.profiles.is_empty() || self.profiles.contains(&profile.name))
            .collect()
    }

    /// A preset the project no longer allows falls back to Auto.
    pub(super) fn reconcile_choice(&mut self) {
        if let Choice::Preset(name) = &self.choice
            && !self.profile_summaries.is_empty()
            && !self.allowed().iter().any(|profile| &profile.name == name)
        {
            self.choice = Choice::Auto;
        }
    }

    /// The profile routing would pick now, from the last preview.
    pub(super) fn auto_profile(&self) -> Option<&str> {
        self.preview.as_ref()?.as_ref().ok()?.profile.as_deref()
    }

    /// The preset (or Auto's previewed profile) this task launches, if any.
    pub(super) fn profile(&self) -> Option<&ProfileSummary> {
        let name = match &self.choice {
            Choice::Auto => self.auto_profile()?,
            Choice::Preset(name) => name.as_str(),
            Choice::Model(_) => return None,
        };
        self.profile_summaries
            .iter()
            .find(|profile| profile.name == name)
    }

    /// The access level a model choice launches with.
    pub(super) fn access(&self) -> Access {
        self.access.unwrap_or(Access::Ask)
    }

    /// Auto's line in the picker: what Swarm picks now, and why.
    pub(super) fn auto_detail(&self) -> String {
        match &self.preview {
            Some(Ok(decision)) => {
                let (headline, _) = crate::settings_model::decision_lines(decision);
                match decision.profile.as_deref() {
                    Some(profile) => format!(
                        "Swarm picks {profile}: {}",
                        headline
                            .strip_prefix("Routing picks ")
                            .and_then(|rest| rest.split_once(": ").map(|(_, why)| why))
                            .unwrap_or(&headline)
                    ),
                    None => format!("Swarm has no preset to pick: {headline}"),
                }
            }
            Some(Err(error)) => format!("Swarm's pick is unavailable: {error}"),
            None => "Swarm picks when the task starts".to_owned(),
        }
    }
}

impl DesktopApp {
    /// The picker's rows for the current state.
    pub(super) fn picker_rows(&self) -> Vec<Row> {
        let presets = self.launch.allowed();
        let auto_detail = self.launch.auto_detail();
        picker::rows(&picker::Inputs {
            catalog: self.launch.catalog.as_ref(),
            query: &self.launch.query,
            favorites: &self.prefs.favorites,
            recents: &self.prefs.recents,
            presets: &presets,
            current: &self.launch.choice,
            auto_detail: &auto_detail,
            efforts: &self.launch.efforts,
            claude_efforts: eliot_daemon::CLAUDE_EFFORT_LEVELS,
        })
    }

    /// The project popover's rows: `(checkout, name, path)`, filtered.
    pub(super) fn project_rows(&self) -> Vec<(String, String, String)> {
        let query = self.launch.project_query.trim().to_lowercase();
        let mut rows: Vec<(String, String, String)> = self
            .nav
            .projects
            .iter()
            .filter(|project| {
                query.is_empty()
                    || project.name.to_lowercase().contains(&query)
                    || project.workspace.to_lowercase().contains(&query)
            })
            .map(|project| {
                (
                    project.workspace.clone(),
                    project.name.clone(),
                    nav::display_path(&project.workspace).to_owned(),
                )
            })
            .collect();
        rows.push((
            OTHER_FOLDER.to_owned(),
            "Other folder…".to_owned(),
            "Type a folder path".to_owned(),
        ));
        rows
    }

    pub(super) fn launch_update(&mut self, msg: LaunchMsg, context: &ComponentContext<Self>) {
        match msg {
            LaunchMsg::WorkspaceChanged(text) => self.launch.workspace = text,
            LaunchMsg::CatalogLoaded(reply) => {
                self.launch.catalog_loading = false;
                self.launch.catalog = Some(*reply);
                self.keep_highlight();
            }
            LaunchMsg::Toggle(pop) => {
                if self.launch.open == Some(pop) {
                    self.close_popover();
                } else {
                    self.open_popover(pop, context);
                }
            }
            LaunchMsg::Close => self.close_popover(),
            LaunchMsg::PickerQuery(text) => {
                if self.launch.query != text {
                    self.launch.query = text;
                    self.launch.highlight = picker::moved(&self.picker_rows(), None, 0);
                }
            }
            LaunchMsg::PickerChosen(key) => {
                let rows = self.picker_rows();
                if let Some(item) = picker::find(&rows, Some(&key)).cloned() {
                    self.choose(item.choice(), context);
                }
            }
            LaunchMsg::PickerEffort(key, effort) => {
                let rows = self.picker_rows();
                if let Some(item) = picker::find(&rows, Some(&key)).cloned() {
                    self.launch.efforts.insert(key, effort.clone());
                    self.choose(item.choice_with(Some(effort)), context);
                }
            }
            LaunchMsg::PickerHover(key) => {
                if self.launch.highlight.as_deref() != Some(key.as_str()) {
                    self.launch.highlight = Some(key);
                }
            }
            LaunchMsg::PickerFavorite(key) => self.toggle_favorite(&key, context),
            LaunchMsg::AccessChosen(access) => self.choose_access(access, context),
            LaunchMsg::AccessHover(access) => self.launch.access_highlight = Some(access),
            LaunchMsg::ProjectQuery(text) => {
                if self.launch.project_query != text {
                    self.launch.project_query = text;
                    self.launch.highlight = self.project_rows().first().map(|row| row.0.clone());
                }
            }
            LaunchMsg::ProjectChosen(checkout) => {
                self.close_popover();
                if checkout == OTHER_FOLDER {
                    self.launch.custom_workspace = true;
                    self.launch.workspace.clear();
                    self.launch.profiles.clear();
                    self.nav.selection.project = None;
                } else {
                    self.pick_project(&checkout);
                }
            }
            LaunchMsg::Key(key) => self.popover_key(key, context),
            LaunchMsg::Reveal(epoch) => {
                if epoch != self.launch.reveal_epoch || self.launch.open != Some(Pop::Model) {
                    return;
                }
                // The highlighted row's reference binds when it renders.
                if self.renders.get() <= self.launch.reveal_after {
                    self.post_reveal(epoch, REVEAL_RETRY_MS, context);
                    return;
                }
                // Focusing the row brings it into view (WinUI scrolls a
                // focused element in); the keyboard then goes back to the
                // search field, so typing still filters.
                let later = context.sender();
                let accepted = self.launch.row_ref.request_focus_result(move |_| {
                    let _ = later.send(Message::Launch(LaunchMsg::FocusField(1)));
                });
                if !accepted {
                    self.focus_field(1, context);
                }
            }
            LaunchMsg::FocusField(tries) => self.focus_field(tries, context),
            LaunchMsg::LaunchRequested => self.request_launch(context),
            LaunchMsg::LaunchSubmitted(outcome) => self.launch_submitted(outcome, context),
            LaunchMsg::ProfilesListed(result) => match result {
                Ok(list) => {
                    self.launch.profile_summaries = list.profiles.clone();
                    self.launch.profile_list = Some(list);
                    self.launch.reconcile_choice();
                }
                Err(error) => self.alert(format!("Presets not read: {error}")),
            },
            LaunchMsg::PreviewLoaded(result) => {
                self.launch.preview_loading = false;
                self.launch.preview = Some(*result);
                if std::mem::take(&mut self.launch.preview_again) {
                    self.load_routing_preview(context);
                }
            }
        }
    }

    /// The `[desktop]` preferences arrived: a new task starts with the most
    /// recent model and the last access level, unless the owner already
    /// chose in this window.
    pub(super) fn launch_preferences_loaded(&mut self) {
        if self.launch.chosen {
            return;
        }
        self.launch.access = self.prefs.last_access;
        if let Some(recent) = self.prefs.recents.first() {
            self.launch.choice = Choice::Model(Pick {
                harness: recent.harness.clone(),
                model: Some(recent.model.clone()),
                effort: recent.effort.clone(),
            });
        }
    }

    fn open_popover(&mut self, pop: Pop, context: &ComponentContext<Self>) {
        // The composer for a new task only.
        if self.selected_agent_id().is_some() || self.main != Main::Conversation {
            return;
        }
        self.launch.open = Some(pop);
        match pop {
            Pop::Model => {
                self.launch.query.clear();
                self.launch.highlight = picker::initial(&self.picker_rows());
                if self.launch.catalog.is_none() || matches!(self.launch.catalog, Some(Err(_))) {
                    self.load_catalog(context);
                }
                crate::shortcuts::set_popover(HookPopover::Picker);
                self.post_focus_field(FIELD_FOCUS_TRIES, 0, context);
                self.reveal(context);
            }
            Pop::Access => {
                self.launch.access_highlight = Some(self.launch.access());
                crate::shortcuts::set_popover(HookPopover::Menu);
            }
            Pop::Project => {
                self.launch.project_query.clear();
                self.launch.highlight = Some(
                    self.selected_project()
                        .map_or_else(|| OTHER_FOLDER.to_owned(), |p| p.workspace.clone()),
                );
                crate::shortcuts::set_popover(HookPopover::Menu);
                self.post_focus_field(FIELD_FOCUS_TRIES, 0, context);
            }
        }
    }

    /// Close the open popover; its keys go back to the focused control.
    pub(super) fn close_popover(&mut self) {
        if self.launch.open.take().is_some() {
            crate::shortcuts::set_popover(HookPopover::None);
            // Keyboard focus goes back to the composer (no-op while unbound).
            let _ = self.chat.composer_ref.request_focus();
        }
    }

    /// A choice from the picker: it sets the launch's harness, model and
    /// effort (or Auto, or a preset) and closes the picker.
    fn choose(&mut self, choice: Choice, context: &ComponentContext<Self>) {
        self.nav.selection.profile = match &choice {
            Choice::Preset(name) => Some(name.clone()),
            _ => None,
        };
        self.launch.choice = choice;
        self.launch.chosen = true;
        self.close_popover();
        if self.launch.choice == Choice::Auto {
            self.load_routing_preview(context);
        }
    }

    fn choose_access(&mut self, access: Access, context: &ComponentContext<Self>) {
        if access == Access::Full && self.launch.choice.harness() == Some("opencode") {
            // The daemon refuses it (no full-access mapping); say so here.
            return;
        }
        self.launch.access = Some(access);
        self.launch.chosen = true;
        self.close_popover();
        if self.prefs.last_access != Some(access) {
            self.prefs.last_access = Some(access);
            self.prefs_rev += 1;
            self.save_preferences(context);
        }
    }

    fn toggle_favorite(&mut self, key: &str, context: &ComponentContext<Self>) {
        let rows = self.picker_rows();
        let Some(model) = picker::find(&rows, Some(key)).and_then(Item::model_ref) else {
            return;
        };
        self.prefs.toggle_favorite(model);
        self.prefs_rev += 1;
        self.save_preferences(context);
        self.keep_highlight();
    }

    /// A row the highlight names may be gone (a read, a favorite): keep it
    /// when shown, else the first row.
    fn keep_highlight(&mut self) {
        if self.launch.open != Some(Pop::Model) {
            return;
        }
        let rows = self.picker_rows();
        if picker::find(&rows, self.launch.highlight.as_deref()).is_none() {
            self.launch.highlight = picker::initial(&rows);
        }
    }

    fn popover_key(&mut self, key: PopoverKey, context: &ComponentContext<Self>) {
        let Some(pop) = self.launch.open else {
            return;
        };
        if key == PopoverKey::Escape {
            self.close_popover();
            return;
        }
        match pop {
            Pop::Model => {
                let rows = self.picker_rows();
                let highlight = self.launch.highlight.clone();
                match key {
                    PopoverKey::Up | PopoverKey::Down => {
                        let step = if key == PopoverKey::Up { -1 } else { 1 };
                        self.launch.highlight = picker::moved(&rows, highlight.as_deref(), step);
                        self.reveal(context);
                    }
                    PopoverKey::Left | PopoverKey::Right => {
                        if let Some(item) = picker::find(&rows, highlight.as_deref())
                            && let Some(effort) = item.step_effort(key == PopoverKey::Right)
                        {
                            self.launch.efforts.insert(item.key.clone(), effort);
                        }
                    }
                    PopoverKey::Enter => {
                        if let Some(item) = picker::find(&rows, highlight.as_deref()).cloned() {
                            self.choose(item.choice(), context);
                        }
                    }
                    PopoverKey::Favorite => {
                        if let Some(key) = highlight {
                            self.toggle_favorite(&key, context);
                        }
                    }
                    PopoverKey::Escape => {}
                }
            }
            Pop::Access => {
                let levels = self.access_levels();
                let at = self
                    .launch
                    .access_highlight
                    .and_then(|access| levels.iter().position(|(level, _)| *level == access))
                    .unwrap_or(0);
                match key {
                    PopoverKey::Up => {
                        self.launch.access_highlight = Some(levels[at.saturating_sub(1)].0);
                    }
                    PopoverKey::Down => {
                        self.launch.access_highlight =
                            Some(levels[(at + 1).min(levels.len() - 1)].0);
                    }
                    PopoverKey::Enter => {
                        if let Some((access, None)) = levels.get(at).cloned() {
                            self.choose_access(access, context);
                        }
                    }
                    _ => {}
                }
            }
            Pop::Project => {
                let rows = self.project_rows();
                let at = self
                    .launch
                    .highlight
                    .as_deref()
                    .and_then(|key| rows.iter().position(|row| row.0 == key))
                    .unwrap_or(0);
                match key {
                    PopoverKey::Up => {
                        self.launch.highlight = Some(rows[at.saturating_sub(1)].0.clone());
                    }
                    PopoverKey::Down => {
                        self.launch.highlight = Some(rows[(at + 1).min(rows.len() - 1)].0.clone());
                    }
                    PopoverKey::Enter => {
                        let chosen = rows[at].0.clone();
                        self.launch_update(LaunchMsg::ProjectChosen(chosen), context);
                    }
                    _ => {}
                }
            }
        }
    }

    /// The access levels and, for each, why it cannot be chosen now.
    pub(super) fn access_levels(&self) -> Vec<(Access, Option<&'static str>)> {
        Access::ALL
            .into_iter()
            .map(|access| {
                let refused = (access == Access::Full
                    && self.launch.choice.harness() == Some("opencode"))
                .then_some("OpenCode has no full-access mode");
                (access, refused)
            })
            .collect()
    }

    /// After the next render, bring the highlighted picker row into view.
    fn reveal(&mut self, context: &ComponentContext<Self>) {
        self.launch.reveal_epoch += 1;
        self.launch.reveal_after = self.renders.get();
        self.post_reveal(self.launch.reveal_epoch, 0, context);
    }

    fn post_reveal(&self, epoch: u64, delay_ms: u64, context: &ComponentContext<Self>) {
        let _task = context.spawn_background(move |cancel| {
            let cancelled = || cancel.is_cancelled();
            if delay_ms > 0 {
                let _ = feed::backoff(delay_ms, &cancelled);
            }
            Message::Launch(LaunchMsg::Reveal(epoch))
        });
    }

    fn post_focus_field(&self, tries: u8, delay_ms: u64, context: &ComponentContext<Self>) {
        let _task = context.spawn_background(move |cancel| {
            let cancelled = || cancel.is_cancelled();
            if delay_ms > 0 {
                let _ = feed::backoff(delay_ms, &cancelled);
            }
            Message::Launch(LaunchMsg::FocusField(tries))
        });
    }

    /// Focus the open popover's search field; a refused or unbound focus
    /// is tried again, `tries` times in all.
    fn focus_field(&mut self, tries: u8, context: &ComponentContext<Self>) {
        if !matches!(self.launch.open, Some(Pop::Model | Pop::Project)) {
            return;
        }
        let Some(left) = tries.checked_sub(1) else {
            return;
        };
        let later = context.sender();
        let accepted = self.launch.field_ref.request_focus_result(move |result| {
            if !matches!(result, Ok(true)) && left > 0 {
                let _ = later.send(Message::Launch(LaunchMsg::FocusField(left)));
            }
        });
        if !accepted && left > 0 {
            self.post_focus_field(left, FIELD_FOCUS_RETRY_MS, context);
        }
    }

    /// Read the unified model catalogue (every page) on a worker.
    pub(super) fn load_catalog(&mut self, context: &ComponentContext<Self>) {
        if self.launch.catalog_loading {
            return;
        }
        let Ok(data_dir) = self.data_dir.clone() else {
            return;
        };
        self.launch.catalog_loading = true;
        let _task = context.spawn_background(move |_| {
            Message::Launch(LaunchMsg::CatalogLoaded(Box::new(
                client::block_on(config_client::models_catalog(&data_dir))
                    .map_err(|error| error.to_string()),
            )))
        });
    }

    fn request_launch(&mut self, context: &ComponentContext<Self>) {
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
            self.alert("Describe the task first.");
            return;
        }
        if self.launch.workspace.trim().is_empty() {
            self.alert("Choose a project or a folder for the task.");
            return;
        }
        self.close_popover();
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
                .map_err(|error| crate::settings_model::launch_error(&error))?;
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

    fn launch_submitted(
        &mut self,
        outcome: Fallible<LaunchOutcome>,
        context: &ComponentContext<Self>,
    ) {
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
                // The first prompt rode the launch admission; it is never
                // sent again. Clear only the submitted draft.
                if self.launch.launch_rev == outcome.launch_rev {
                    self.launch.launch_text.clear();
                }
                if let Some(linked) = outcome.linked_command.clone() {
                    self.chat.pending_local =
                        Some((outcome.agent.clone(), linked, outcome.text.clone()));
                }
                self.set_status(format!(
                    "Launched {} ({}); its first input is {}.",
                    outcome.agent,
                    outcome.command_id,
                    outcome.linked_command.as_deref().unwrap_or("not linked")
                ));
                // The model goes first in Recent.
                if let Choice::Model(pick) = &self.launch.choice
                    && let Some(model) = pick.model_ref()
                {
                    self.prefs.remember(model);
                    self.prefs_rev += 1;
                    self.save_preferences(context);
                }
                // Until the list reads it back, the title is the first
                // message, known now.
                self.nav.pending_select = Some(UiAgent {
                    id: outcome.agent.clone(),
                    generation: outcome.generation,
                    harness: self.launch.choice.harness().unwrap_or_default().to_owned(),
                    state: "created".to_owned(),
                    project: self.launch.workspace.clone(),
                    parent: None,
                    updated_at: crate::handoff::now_ms(),
                    title: nav::title(&outcome.text).unwrap_or_else(|| outcome.agent.clone()),
                    native_child: false,
                    tokens: None,
                });
                self.nav.selection.select_agent(Some(outcome.agent));
                self.load_agents(context, None);
                // Capacity changed: the next Auto pick may differ.
                self.load_routing_preview(context);
            }
            Err(error) if crate::state::uncertain(&error) => {
                self.alert(format!(
                    "Start not confirmed (sending again reuses its id): {error}"
                ));
            }
            Err(error) => self.alert(format!("Start refused: {error}")),
        }
    }

    /// Ask routing what it would pick for a new task now.
    pub(super) fn load_routing_preview(&mut self, context: &ComponentContext<Self>) {
        if self.launch.preview_loading {
            // The running read may predate the change that asked.
            self.launch.preview_again = true;
            return;
        }
        let Ok(data_dir) = self.data_dir.clone() else {
            return;
        };
        self.launch.preview_loading = true;
        let _task = context.spawn_background(move |_| {
            Message::Launch(LaunchMsg::PreviewLoaded(Box::new(
                client::block_on(config_client::routing_preview(&data_dir, None))
                    .map_err(|error| error.to_string()),
            )))
        });
    }

    pub(super) fn launch_target(&self) -> Result<LaunchTarget, String> {
        match &self.launch.choice {
            Choice::Model(pick) => {
                let access = self.launch.access();
                if access == Access::Full && pick.harness == "opencode" {
                    return Err(
                        "OpenCode has no full-access mode; choose Ask for approval or Approve for me"
                            .to_owned(),
                    );
                }
                Ok(LaunchTarget::Adhoc {
                    harness: pick.harness.clone(),
                    model: pick.model.clone(),
                    effort: pick.effort.clone(),
                    access: Some(access.wire().to_owned()),
                })
            }
            Choice::Preset(name) => Ok(LaunchTarget::Profile(name.clone())),
            // Admission routes; a miss is refused there with its reasons.
            Choice::Auto => Ok(LaunchTarget::Routed),
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
}

/// What a conversation's chip says the Agent was launched as: the harness
/// for an ad-hoc launch (no internal `adhoc-*` name), else the preset and
/// its harness.
pub(super) fn launched_as(profile: &str, harness: &str) -> String {
    let label = picker::harness_label(harness);
    if profile == harness || profile.starts_with("adhoc-") {
        label.to_owned()
    } else {
        format!("{profile} · {label}")
    }
}

/// A running Agent's access chip: its frozen access level when the launch
/// named one, else its profile's permissions.
pub(super) fn agent_access_label(settings: &crate::configure::Settings) -> String {
    match settings.access.as_deref() {
        Some("ask") => Access::Ask.label().to_owned(),
        Some("auto") => Access::Auto.label().to_owned(),
        Some("full") => Access::Full.label().to_owned(),
        _ => access_label(settings.permissions.as_deref()),
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
