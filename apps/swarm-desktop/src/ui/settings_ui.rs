//! Settings: one page with sections, the one place for everything that
//! needs adjusting. Every section reads and writes the daemon's unified
//! settings (`settings.read` / `settings.save`, one store with a revision
//! and author history) through [`crate::config_client`]:
//! General (`[desktop]`), Models and profiles (`[profiles]`), Routing
//! (`[routing]`), Projects (`[projects]`), Automation (`[automation]`
//! assignments above the read-only rules), Scripts (their own files and
//! API), Cloud MCP (`[cloud_mcp]`) and Advanced (every section as JSON, and
//! the settings history). Each save shows its result and revision. The
//! pages replace the sidebar with their own navigation (`settings_nav`) and
//! show one centred column, one filled Save per page.

use std::collections::{BTreeMap, HashMap};

use super::*;
use crate::settings::{Startup, ThemeChoice};
use crate::settings_model::{self as model, AssignmentField};
use crate::theme::winui::Palette;
use eliot_core::api::{
    SettingsClientSource, SettingsHistoryEntry, SettingsReadResponse, SettingsSaveRequest,
    SettingsSaveResponse, SettingsSection as Section,
};
use serde_json::{Value, json};

/// The page's sections, in order.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(super) enum Page {
    General,
    Models,
    Routing,
    Projects,
    Automation,
    Scripts,
    CloudMcp,
    Advanced,
}

impl Page {
    const ALL: [Self; 8] = [
        Self::General,
        Self::Models,
        Self::Routing,
        Self::Projects,
        Self::Automation,
        Self::Scripts,
        Self::CloudMcp,
        Self::Advanced,
    ];

    fn label(self) -> &'static str {
        match self {
            Self::General => "General",
            Self::Models => "Models and profiles",
            Self::Routing => "Routing",
            Self::Projects => "Projects",
            Self::Automation => "Automation",
            Self::Scripts => "Scripts",
            Self::CloudMcp => "Cloud MCP",
            Self::Advanced => "Advanced",
        }
    }
}

#[derive(Clone, Debug)]
pub(super) enum SettingsMsg {
    PageSelected(Page),
    Loaded(Fallible<SettingsReadResponse>),
    Reload,
    /// Validate (true) or save the page's section.
    Save(Page, bool),
    Saved(Page, bool, Fallible<SettingsSaveResponse>),
    /// Put the page's draft back to the saved values.
    Discard(Page),
    /// Role, candidate index, up (true) or down.
    RoutingMoved(String, usize, bool),
    RoutingRemoved(String, usize),
    /// Role and the index picked in its "Add a profile" list.
    RoutingAdded(String, Option<usize>),
    NewRoleChanged(String),
    RoleAdded,
    /// Show (or hide) one project's editor.
    ProjectOpened(String),
    ProjectPath(String, String),
    ProjectProfile(String, String, bool),
    ProjectActions(String, String),
    ProjectRemoved(String),
    NewProjectId(String),
    NewProjectPath(String),
    ProjectAdded,
    AssignmentText(String, AssignmentField, String),
    AssignmentBusy(String, Option<f64>),
    /// Assignment key and the index picked in its role list.
    AssignmentRole(String, Option<usize>),
    AssignmentRemoved(String),
    NewAssignment(String),
    AssignmentAdded,
    /// Index into [`model::CLOUD_FIELDS`] and its text.
    CloudField(usize, String),
    /// Configure (true) or remove Cloud MCP.
    CloudEnabled(bool),
    /// The Advanced JSON draft of every section.
    AllDraft(String),
    HistoryToggled,
}

/// The read settings, the drafts of the page's sections and the last
/// save result of each section.
#[derive(Default)]
pub(super) struct SettingsState {
    pub(super) loaded: bool,
    pub(super) reading: bool,
    /// A read was asked for while one ran (after a save): read again.
    reread: bool,
    /// [`DesktopApp::prefs_rev`] when the running read was issued.
    read_prefs_rev: u64,
    /// The section whose save is running (one save at a time).
    saving: Option<Page>,
    revision: Option<String>,
    base: BTreeMap<Section, Value>,
    restart_required: Vec<Section>,
    history: Vec<SettingsHistoryEntry>,
    error: Option<String>,
    pub(super) notes: HashMap<Page, String>,
    routing: Value,
    projects: Value,
    automation: Value,
    cloud: Value,
    /// Advanced: every section as pretty JSON, and its read form.
    all: String,
    all_base: String,
    new_role: String,
    project_open: Option<String>,
    new_project_id: String,
    new_project_path: String,
    new_assignment: String,
    history_open: bool,
}

impl SettingsState {
    fn base(&self, section: Section) -> Value {
        self.base
            .get(&section)
            .cloned()
            .unwrap_or_else(|| json!({}))
    }

    fn busy(&self) -> bool {
        self.reading || self.saving.is_some()
    }

    /// Take a fresh read: drafts without unsaved edits follow it, edited
    /// drafts are kept.
    fn take(&mut self, read: SettingsReadResponse) {
        let old = std::mem::take(&mut self.base);
        let fresh = !self.loaded;
        let follow = |draft: &mut Value, section: Section, new: &BTreeMap<Section, Value>| {
            let before = old.get(&section).cloned().unwrap_or_else(|| json!({}));
            if fresh || *draft == before {
                *draft = new.get(&section).cloned().unwrap_or_else(|| json!({}));
            }
        };
        follow(&mut self.routing, Section::Routing, &read.sections);
        follow(&mut self.projects, Section::Projects, &read.sections);
        follow(&mut self.automation, Section::Automation, &read.sections);
        follow(&mut self.cloud, Section::CloudMcp, &read.sections);
        let all = serde_json::to_string_pretty(&read.sections).unwrap_or_default();
        if fresh || self.all == self.all_base {
            self.all = all.clone();
        }
        self.all_base = all;
        self.base = read.sections;
        self.revision = read.revision;
        self.restart_required = read.restart_required;
        self.history = read.history;
        self.loaded = true;
        self.error = None;
    }

    /// The page's sections to send, or why there is nothing to send.
    fn request_sections(
        &self,
        page: Page,
        profiles_draft: &str,
    ) -> Result<BTreeMap<Section, Value>, String> {
        let one = |section: Section, value: Value| Ok(BTreeMap::from([(section, value)]));
        match page {
            Page::Models => one(
                Section::Profiles,
                model::profiles_from_toml(profiles_draft)?,
            ),
            Page::Routing => one(Section::Routing, model::routing_for_save(&self.routing)),
            Page::Projects => one(Section::Projects, self.projects.clone()),
            Page::Automation => one(Section::Automation, self.automation.clone()),
            Page::CloudMcp => one(Section::CloudMcp, self.cloud.clone()),
            Page::Advanced => {
                let mut sections: BTreeMap<Section, Value> = serde_json::from_str(&self.all)
                    .map_err(|_| "The JSON does not parse; the draft is kept.".to_owned())?;
                sections.retain(|section, value| self.base.get(section) != Some(value));
                if sections.is_empty() {
                    return Err("No section changed.".to_owned());
                }
                Ok(sections)
            }
            Page::General => Err("General saves as you choose.".to_owned()),
            Page::Scripts => Err("Scripts save from their own editor.".to_owned()),
        }
    }

    fn dirty(&self, page: Page) -> bool {
        match page {
            Page::Routing => {
                model::routing_for_save(&self.routing)
                    != model::routing_for_save(&self.base(Section::Routing))
            }
            Page::Projects => self.projects != self.base(Section::Projects),
            Page::Automation => self.automation != self.base(Section::Automation),
            Page::CloudMcp => self.cloud != self.base(Section::CloudMcp),
            Page::Advanced => self.all != self.all_base,
            Page::General | Page::Models | Page::Scripts => false,
        }
    }
}

impl DesktopApp {
    pub(super) fn settings_update(&mut self, msg: SettingsMsg, context: &ComponentContext<Self>) {
        let s = &mut self.settings;
        match msg {
            SettingsMsg::PageSelected(page) => {
                if self.settings_page != page {
                    self.settings_page = page;
                    self.reprime_fields(context);
                    self.settings_entered(context);
                }
            }
            SettingsMsg::Loaded(result) => {
                self.settings.reading = false;
                match result {
                    Ok(read) => {
                        // A choice made after this read was issued (or still
                        // being written) wins over the read.
                        if !self.prefs_saving && self.prefs_rev == self.settings.read_prefs_rev {
                            self.prefs = crate::settings::Preferences::from_reply(&read);
                        }
                        self.take_profiles(&read);
                        self.settings.take(read);
                        // Only the Settings page has primed editors to refill;
                        // the composer keeps its draft untouched.
                        if self.main == Main::Settings {
                            self.reprime_fields(context);
                        }
                    }
                    Err(error) => self.settings.error = Some(format!("Settings not read: {error}")),
                }
                if std::mem::take(&mut self.settings.reread) {
                    self.load_settings(context);
                }
            }
            SettingsMsg::Reload => {
                self.settings.notes.clear();
                self.load_settings(context);
            }
            SettingsMsg::Save(page, dry_run) => self.save_page(page, dry_run, context),
            SettingsMsg::Saved(page, dry_run, result) => {
                self.settings.saving = None;
                self.settings
                    .notes
                    .insert(page, model::save_note(dry_run, &result));
                if matches!(&result, Ok(reply) if reply.saved && !dry_run) {
                    self.after_settings_saved(context);
                }
            }
            SettingsMsg::Discard(page) => {
                match page {
                    Page::Models => {
                        self.editor.draft = self.editor.base.clone();
                        self.editor.view = self.editor.base.clone();
                    }
                    Page::Routing => s.routing = s.base(Section::Routing),
                    Page::Projects => s.projects = s.base(Section::Projects),
                    Page::Automation => s.automation = s.base(Section::Automation),
                    Page::CloudMcp => s.cloud = s.base(Section::CloudMcp),
                    Page::Advanced => s.all = s.all_base.clone(),
                    Page::General | Page::Scripts => {}
                }
                self.settings.notes.remove(&page);
                self.reprime_fields(context);
            }
            SettingsMsg::RoutingMoved(role, index, up) => {
                model::routing_move(&mut s.routing, &role, index, up)
            }
            SettingsMsg::RoutingRemoved(role, index) => {
                model::routing_remove(&mut s.routing, &role, index)
            }
            SettingsMsg::RoutingAdded(role, index) => {
                // A ComboBox reports None when its items are replaced.
                if let Some(profile) =
                    index.and_then(|index| self.addable_profiles(&role).into_iter().nth(index))
                {
                    model::routing_add(&mut self.settings.routing, &role, &profile);
                }
            }
            SettingsMsg::NewRoleChanged(text) => s.new_role = text,
            SettingsMsg::RoleAdded => {
                let role = s.new_role.trim().to_owned();
                match model::routing_add_role(&mut s.routing, &role) {
                    Ok(()) => s.new_role.clear(),
                    Err(error) => {
                        s.notes.insert(Page::Routing, error);
                    }
                }
            }
            SettingsMsg::ProjectOpened(id) => {
                s.project_open = if s.project_open.as_deref() == Some(id.as_str()) {
                    None
                } else {
                    Some(id)
                };
            }
            SettingsMsg::ProjectPath(id, path) => {
                model::set_project_path(&mut s.projects, &id, &path)
            }
            SettingsMsg::ProjectProfile(id, profile, on) => {
                model::set_project_profile(&mut s.projects, &id, &profile, on)
            }
            SettingsMsg::ProjectActions(id, text) => {
                model::set_project_actions(&mut s.projects, &id, &text)
            }
            SettingsMsg::ProjectRemoved(id) => {
                model::remove_entry(&mut s.projects, &id);
                s.project_open = None;
            }
            SettingsMsg::NewProjectId(text) => s.new_project_id = text,
            SettingsMsg::NewProjectPath(text) => s.new_project_path = text,
            SettingsMsg::ProjectAdded => {
                let profiles = self.known_profiles();
                let s = &mut self.settings;
                let id = s.new_project_id.trim().to_owned();
                match model::add_project(&mut s.projects, &id, &s.new_project_path, &profiles) {
                    Ok(()) => {
                        s.project_open = Some(id);
                        s.new_project_id.clear();
                        s.new_project_path.clear();
                    }
                    Err(error) => {
                        s.notes.insert(Page::Projects, error);
                    }
                }
            }
            SettingsMsg::AssignmentText(key, field, text) => {
                model::set_assignment_text(&mut s.automation, &key, field, &text)
            }
            SettingsMsg::AssignmentBusy(key, value) => {
                if let Some(value) = value.filter(|value| value.is_finite() && *value >= 1.0) {
                    model::set_assignment_busy(&mut s.automation, &key, value.round() as u64);
                }
            }
            SettingsMsg::AssignmentRole(key, index) => {
                let mut roles = model::routing_roles(&s.routing);
                let current = model::assignment_role(&s.automation, &key);
                if !roles.contains(&current) {
                    roles.push(current);
                }
                if let Some(role) = index.and_then(|index| roles.get(index)) {
                    model::set_assignment_role(&mut s.automation, &key, role);
                }
            }
            SettingsMsg::AssignmentRemoved(key) => {
                model::remove_assignment(&mut s.automation, &key)
            }
            SettingsMsg::NewAssignment(text) => s.new_assignment = text,
            SettingsMsg::AssignmentAdded => {
                let workspace = self
                    .nav
                    .projects
                    .first()
                    .map(|project| nav::display_path(&project.workspace).to_owned())
                    .unwrap_or_default();
                let s = &mut self.settings;
                let key = s.new_assignment.trim().to_owned();
                match model::add_assignment(&mut s.automation, &key, &workspace) {
                    Ok(()) => s.new_assignment.clear(),
                    Err(error) => {
                        s.notes.insert(Page::Automation, error);
                    }
                }
            }
            SettingsMsg::CloudField(index, text) => {
                if let Some((key, _)) = model::CLOUD_FIELDS.get(index) {
                    model::set_cloud_field(&mut s.cloud, key, &text);
                }
            }
            SettingsMsg::CloudEnabled(on) => {
                s.cloud = if on {
                    model::cloud_template()
                } else {
                    json!({})
                };
            }
            SettingsMsg::AllDraft(text) => {
                if !s.busy() {
                    s.all = profiles::normalize_newlines(&text);
                }
            }
            SettingsMsg::HistoryToggled => s.history_open = !s.history_open,
        }
    }

    /// The profile editor follows a read unless it has unsaved edits.
    fn take_profiles(&mut self, read: &SettingsReadResponse) {
        let section = read
            .sections
            .get(&Section::Profiles)
            .cloned()
            .unwrap_or_else(|| json!({}));
        let editor = &mut self.editor;
        editor.loaded = true;
        editor.revision = read.revision.clone();
        match model::profiles_toml(&section) {
            Ok(text) => {
                if editor.draft == editor.base {
                    editor.draft = text.clone();
                    editor.view = text.clone();
                }
                editor.base = text;
                editor.editable = true;
                editor.reason = None;
            }
            Err(error) => {
                editor.editable = false;
                editor.reason = Some(error);
            }
        }
        let names = profiles::profile_names(&editor.draft);
        if editor
            .selected
            .as_ref()
            .is_none_or(|name| !names.contains(name))
        {
            editor.selected = names.first().cloned();
        }
    }

    /// Validate or save one section from its draft.
    pub(super) fn save_page(
        &mut self,
        page: Page,
        dry_run: bool,
        context: &ComponentContext<Self>,
    ) {
        if self.settings.busy() || !self.settings.loaded {
            return;
        }
        let Ok(data_dir) = self.data_dir.clone() else {
            return;
        };
        let sections = match self.settings.request_sections(page, &self.editor.draft) {
            Ok(sections) => sections,
            Err(note) => {
                self.settings.notes.insert(page, note);
                return;
            }
        };
        let request = SettingsSaveRequest {
            expected_revision: self.settings.revision.clone(),
            sections,
            dry_run,
            client: SettingsClientSource::Desktop,
        };
        self.settings.saving = Some(page);
        self.settings.notes.insert(
            page,
            if dry_run {
                "Validating…"
            } else {
                "Saving…"
            }
            .to_owned(),
        );
        let _task = context.spawn_background(move |_| {
            Message::Settings(SettingsMsg::Saved(
                page,
                dry_run,
                client::block_on(config_client::save_settings(&data_dir, &request))
                    .map_err(|error| error.to_string()),
            ))
        });
    }

    /// A saved change: read the settings again (unsaved drafts stay), and
    /// what depends on them (profiles, projects, the routing preview).
    pub(super) fn after_settings_saved(&mut self, context: &ComponentContext<Self>) {
        self.load_settings(context);
        self.load_profile_list(context);
        self.load_routing_preview(context);
        if let Ok(data_dir) = self.data_dir.clone() {
            let _task = context.spawn_background(move |_| {
                Message::Shell(ShellMsg::ProjectsLoaded(load_projects_blocking(&data_dir)))
            });
        }
    }

    pub(super) fn load_settings(&mut self, context: &ComponentContext<Self>) {
        if self.settings.reading {
            // The running read may predate the save that asked.
            self.settings.reread = true;
            return;
        }
        let Ok(data_dir) = self.data_dir.clone() else {
            return;
        };
        self.settings.reading = true;
        self.settings.read_prefs_rev = self.prefs_rev;
        let _task = context.spawn_background(move |_| {
            Message::Settings(SettingsMsg::Loaded(
                client::block_on(config_client::read_settings(&data_dir))
                    .map_err(|error| error.to_string()),
            ))
        });
    }

    /// Read what the shown section needs.
    pub(super) fn settings_entered(&mut self, context: &ComponentContext<Self>) {
        if !self.settings.loaded {
            self.load_settings(context);
        }
        match self.settings_page {
            Page::Models | Page::Routing | Page::Projects => self.load_profile_list(context),
            Page::Automation => self.load_rules(context),
            Page::Scripts if !self.scripts.listed => {
                self.load_scripts(context);
            }
            _ => {}
        }
    }

    /// Profiles a role does not list yet, in profile order.
    fn addable_profiles(&self, role: &str) -> Vec<String> {
        let listed = model::candidates(&self.settings.routing, role);
        self.known_profiles()
            .into_iter()
            .filter(|name| !listed.contains(name))
            .collect()
    }

    /// Profile names from the saved profiles, else the daemon's summaries.
    fn known_profiles(&self) -> Vec<String> {
        let saved = model::project_ids(&self.settings.base(Section::Profiles));
        if saved.is_empty() {
            self.launch
                .profile_summaries
                .iter()
                .map(|profile| profile.name.clone())
                .collect()
        } else {
            saved
        }
    }

    /// The Settings navigation's page rows (the sidebar's place, see
    /// `settings_nav`).
    pub(super) fn settings_rows(
        &self,
        context: &mut ViewContext<Self>,
        palette: Palette,
    ) -> Vec<KeyedView> {
        Page::ALL
            .iter()
            .map(|page| {
                let selected = *page == self.settings_page;
                KeyedView::new(
                    format!("{}:{selected}", page.label()),
                    m3::row(palette, selected)
                        .automation_name(if selected {
                            format!("{} (shown)", page.label())
                        } else {
                            page.label().to_owned()
                        })
                        .on_click(
                            context.message(Message::Settings(SettingsMsg::PageSelected(*page))),
                        )
                        .content(
                            TextBlock::new()
                                .text(page.label())
                                .vertical_alignment(VerticalAlignment::Center)
                                .text_trimming(TextTrimming::CharacterEllipsis),
                        ),
                )
            })
            .collect()
    }

    /// The shown Settings page: its title, then its groups, in one centred
    /// column of at most 840 DIPs with the 24-DIP page padding.
    pub(super) fn settings_view(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        let page = self.settings_page;
        let body: View = if let Some(error) = &self.settings.error {
            self.read_error(error, context, palette)
        } else if !self.settings.loaded {
            m3::secondary("Reading settings…").into()
        } else {
            match page {
                Page::General => self.general_section(context, palette),
                Page::Models => self.models_section(context, palette),
                Page::Routing => self.routing_section(context, palette),
                Page::Projects => self.projects_section(context, palette),
                Page::Automation => self.automation_section(context, palette),
                Page::Scripts => self.scripts_view(context, palette),
                Page::CloudMcp => self.cloud_section(context, palette),
                Page::Advanced => self.advanced_section(context, palette),
            }
        };
        // One explicit column width, so every page's groups line up
        // (content-sized until the window size is observed).
        let width = self.main_width().map(|width| {
            (width - 2.0 * tokens::spacing::LG)
                .clamp(tokens::layout::MIN_COLUMN, tokens::layout::SETTINGS_WIDTH)
        });
        // The page's tree is keyed by page, so its editors are new controls
        // (see `reprime_fields`).
        ScrollViewer::new()
            .vertical_scroll_bar_visibility(ScrollBarVisibility::Auto)
            .content(
                StackPanel::new()
                    .width(width)
                    .horizontal_alignment(HorizontalAlignment::Center)
                    .margin(Thickness::uniform(tokens::spacing::LG))
                    .spacing(tokens::spacing::MD)
                    .keyed_children(vec![
                        KeyedView::new(
                            format!("title:{}", page.label()),
                            m3::page_title(page.label()),
                        ),
                        KeyedView::new(format!("body:{}", page.label()), body),
                    ]),
            )
    }

    fn read_error(&self, error: &str, context: &mut ViewContext<Self>, palette: Palette) -> View {
        StackPanel::new().spacing(tokens::spacing::SM).children((
            m3::body(error.to_owned())
                .text_wrapping(TextWrapping::Wrap)
                .is_text_selection_enabled(true),
            m3::outlined(palette)
                .horizontal_alignment(HorizontalAlignment::Left)
                .is_enabled(!self.settings.reading)
                .on_click(context.message(Message::Settings(SettingsMsg::Reload)))
                .content(m3::label("Read again")),
        ))
    }

    /// Save / Validate / Discard and the page's last result, under every
    /// editable page: the page's one filled action, then outlined, then
    /// text.
    fn save_bar(
        &self,
        page: Page,
        dirty: bool,
        context: &mut ViewContext<Self>,
        palette: Palette,
    ) -> View {
        let busy = self.settings.busy();
        let saving = self.settings.saving == Some(page);
        let note = self.settings.notes.get(&page).cloned().unwrap_or_else(|| {
            if dirty {
                "Unsaved changes.".to_owned()
            } else {
                format!(
                    "Saved settings, revision {}.",
                    self.settings
                        .revision
                        .as_deref()
                        .map_or("none yet", |revision| &revision[..revision.len().min(12)])
                )
            }
        });
        StackPanel::new().spacing(tokens::spacing::SM).children((
            StackPanel::new()
                .orientation(Orientation::Horizontal)
                .spacing(tokens::spacing::SM)
                .children((
                    m3::filled(palette)
                        .is_enabled(dirty && !busy)
                        .automation_name(format!("Save {}", page.label()))
                        .on_click(
                            context.message(Message::Settings(SettingsMsg::Save(page, false))),
                        )
                        .content(m3::label(if saving { "Saving…" } else { "Save" })),
                    m3::outlined(palette)
                        .is_enabled(!busy)
                        .automation_name(format!("Validate {}", page.label()))
                        .on_click(context.message(Message::Settings(SettingsMsg::Save(page, true))))
                        .content(m3::label("Validate")),
                    m3::text_button(palette)
                        .is_enabled(dirty && !busy)
                        .automation_name(format!("Discard changes to {}", page.label()))
                        .on_click(context.message(Message::Settings(SettingsMsg::Discard(page))))
                        .content(m3::label("Discard")),
                )),
            m3::secondary(note)
                .text_wrapping(TextWrapping::Wrap)
                .is_text_selection_enabled(true),
        ))
    }

    fn general_section(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        let segments: Vec<KeyedView> = [ThemeChoice::System, ThemeChoice::Light, ThemeChoice::Dark]
            .into_iter()
            .map(|choice| {
                let selected = self.prefs.theme == choice;
                KeyedView::new(
                    format!("{}:{selected}", choice.label()),
                    m3::segment(palette, selected)
                        .automation_name(format!(
                            "Theme {}{}",
                            choice.label(),
                            if selected { " (current)" } else { "" }
                        ))
                        .on_click(context.message(Message::Shell(ShellMsg::ThemeChosen(choice))))
                        .content(m3::segment_label(choice.label())),
                )
            })
            .collect();
        let reopen = self.prefs.startup == Startup::LastConversation;
        // A check box rather than a switch: its shape stays on the scale (a
        // switch is a pill with a round thumb).
        let switch = Grid::new().keyed_children(vec![KeyedView::new(
            format!("reopen:{reopen}"),
            CheckBox::new()
                .is_checked(reopen)
                .automation_name("Reopen the last conversation at start")
                .on_is_checked_changed(context.callback(|on: bool| {
                    Message::Shell(ShellMsg::StartupChosen(if on {
                        Startup::LastConversation
                    } else {
                        Startup::NewTask
                    }))
                }))
                .content(m3::label("Reopen it")),
        )]);
        let note = self
            .settings
            .notes
            .get(&Page::General)
            .cloned()
            .unwrap_or_else(|| "Changes save as you choose them.".to_owned());
        StackPanel::new().spacing(tokens::spacing::SM).children((
            m3::group(
                palette,
                vec![
                    KeyedView::new(
                        "theme",
                        m3::form_row("Theme", m3::segmented(palette, segments), None),
                    ),
                    KeyedView::new(
                        "theme-help",
                        m3::help("System follows the Windows setting."),
                    ),
                    KeyedView::new("reopen", m3::form_row("Reopen last task", switch, None)),
                    KeyedView::new(
                        "reopen-help",
                        m3::help(
                            "At start, open the most recently active task instead of a new one.",
                        ),
                    ),
                ],
            ),
            m3::secondary(note).text_wrapping(TextWrapping::Wrap),
        ))
    }

    fn models_section(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        let dirty = self.editor.draft != self.editor.base;
        StackPanel::new().spacing(tokens::spacing::MD).children((
            self.profiles_view(context, palette),
            self.save_bar(Page::Models, dirty, context, palette),
        ))
    }

    fn routing_section(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        let busy = self.settings.busy();
        let mut groups: Vec<KeyedView> = vec![KeyedView::new(
            "intro",
            m3::secondary(
                "When a task starts on Auto, Swarm tries the role's profiles in order and runs the first one that is installed, not on hold, not out of quota and within capacity. The choice and the skipped profiles are recorded on the task.",
            )
            .text_wrapping(TextWrapping::Wrap),
        )];
        // What Auto picks right now, from the saved policy.
        if let Some(Ok(decision)) = &self.launch.preview {
            let (headline, skipped) = model::decision_lines(decision);
            let now = std::iter::once(format!(
                "Right now Auto {}.",
                headline.replacen("Routing picks", "picks", 1)
            ))
            .chain(skipped)
            .collect::<Vec<_>>()
            .join("\n");
            groups.push(KeyedView::new(
                format!("now:{now}"),
                m3::body(now)
                    .automation_name("What Auto picks now")
                    .text_wrapping(TextWrapping::Wrap),
            ));
        }
        for role in model::routing_roles(&self.settings.routing) {
            let list = model::candidates(&self.settings.routing, &role);
            let headline = match role.as_str() {
                "default" => "Default",
                "manager" => "Manager",
                "worker" => "Worker",
                "audit" => "Audit",
                other => other,
            };
            let supporting = match (role.as_str(), list.is_empty()) {
                ("default", true) => "No profiles: tasks on Auto cannot start.",
                ("default", false) => "New tasks on Auto.",
                (_, true) => "Not set: launches that ask for this role are refused.",
                (_, false) => "Launches that ask for this role.",
            };
            let mut rows: Vec<KeyedView> = Vec::new();
            let count = list.len();
            for (index, name) in list.iter().enumerate() {
                let first = index == 0;
                let last = index + 1 == count;
                let buttons = StackPanel::new()
                    .orientation(Orientation::Horizontal)
                    .children((
                        m3::icon_button(palette)
                            .is_enabled(!busy && !first)
                            .automation_name(format!("Move {name} earlier in {role}"))
                            .on_click(context.message(Message::Settings(
                                SettingsMsg::RoutingMoved(role.clone(), index, true),
                            )))
                            .content(m3::icon(m3::glyph::ARROW_UP)),
                        m3::icon_button(palette)
                            .is_enabled(!busy && !last)
                            .automation_name(format!("Move {name} later in {role}"))
                            .on_click(context.message(Message::Settings(
                                SettingsMsg::RoutingMoved(role.clone(), index, false),
                            )))
                            .content(m3::icon(m3::glyph::ARROW_DOWN)),
                        m3::icon_button(palette)
                            .is_enabled(!busy)
                            .automation_name(format!("Remove {name} from {role}"))
                            .on_click(context.message(Message::Settings(
                                SettingsMsg::RoutingRemoved(role.clone(), index),
                            )))
                            .content(m3::icon(m3::glyph::CLOSE)),
                    ));
                rows.push(KeyedView::new(
                    format!("{role}:{index}:{name}:{first}:{last}"),
                    candidate_row(index + 1, name, buttons),
                ));
            }
            let addable = self.addable_profiles(&role);
            if !addable.is_empty() {
                let key = format!("{role}:add:{}", addable.join("\u{1f}"));
                let callback_role = role.clone();
                rows.push(KeyedView::new(
                    key.clone(),
                    Grid::new()
                        .min_height(tokens::height::FORM_ROW)
                        .keyed_children(vec![KeyedView::new(
                            key,
                            m3::enum_width(ComboBox::new())
                                .items_source(addable)
                                .selected_index(None)
                                .placeholder_text("Add a profile")
                                .is_enabled(!busy)
                                .automation_name(format!("Add a profile to {role}"))
                                .vertical_alignment(VerticalAlignment::Center)
                                .on_selection_changed(context.callback(move |index| {
                                    Message::Settings(SettingsMsg::RoutingAdded(
                                        callback_role.clone(),
                                        index,
                                    ))
                                })),
                        )]),
                ));
            }
            groups.push(KeyedView::new(
                format!("role:{role}"),
                StackPanel::new().spacing(tokens::spacing::SM).children((
                    m3::section_heading(headline, supporting, None),
                    m3::group(palette, rows),
                )),
            ));
        }
        groups.push(KeyedView::new(
            "new-role",
            m3::group(
                palette,
                vec![KeyedView::new(
                    "new-role",
                    m3::form_row(
                        "Another role",
                        m3::field()
                            .text(self.settings.new_role.clone())
                            .placeholder_text("Role name, for example review")
                            .automation_name("New role name")
                            .on_text_changed(context.callback(|text| {
                                Message::Settings(SettingsMsg::NewRoleChanged(text))
                            }))
                            .into(),
                        Some(
                            m3::outlined(palette)
                                .is_enabled(!busy && !self.settings.new_role.trim().is_empty())
                                .automation_name("Add role")
                                .on_click(
                                    context.message(Message::Settings(SettingsMsg::RoleAdded)),
                                )
                                .content(m3::label("Add role")),
                        ),
                    ),
                )],
            ),
        ));
        let dirty = self.settings.dirty(Page::Routing);
        groups.push(KeyedView::new(
            "save",
            self.save_bar(Page::Routing, dirty, context, palette),
        ));
        StackPanel::new()
            .spacing(tokens::spacing::MD)
            .keyed_children(groups)
    }

    fn projects_section(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        let busy = self.settings.busy();
        let drafts = &self.settings.projects;
        let profiles = self.known_profiles();
        let mut rows: Vec<KeyedView> = Vec::new();
        for id in model::project_ids(drafts) {
            let open = self.settings.project_open.as_deref() == Some(id.as_str());
            let path = model::project_path(drafts, &id);
            let allowed = model::project_profiles(drafts, &id);
            let summary = if allowed.is_empty() {
                "no profiles allowed".to_owned()
            } else {
                allowed.join(", ")
            };
            let name = nav::project_display(&id, &path);
            rows.push(KeyedView::new(
                format!("project:{id}:{open}"),
                m3::row(palette, open)
                    .automation_name(format!("Project {name}: {path}"))
                    .on_click(
                        context.message(Message::Settings(SettingsMsg::ProjectOpened(id.clone()))),
                    )
                    .content(m3::row_content(
                        Some(m3::glyph::FOLDER),
                        &name,
                        &format!("{} · {summary}", nav::display_path(&path)),
                        View::empty(),
                    )),
            ));
            if !open {
                continue;
            }
            let path_id = id.clone();
            let actions_id = id.clone();
            let checks: Vec<KeyedView> = profiles
                .iter()
                .map(|profile| {
                    let on = allowed.contains(profile);
                    let (check_id, check_profile) = (id.clone(), profile.clone());
                    KeyedView::new(
                        format!("{profile}:{on}"),
                        CheckBox::new()
                            .is_checked(on)
                            .is_enabled(!busy)
                            .automation_name(format!("Allow {profile} for rules of {name}"))
                            .on_is_checked_changed(context.callback(move |on: bool| {
                                Message::Settings(SettingsMsg::ProjectProfile(
                                    check_id.clone(),
                                    check_profile.clone(),
                                    on,
                                ))
                            }))
                            .content(TextBlock::new().text(profile.clone())),
                    )
                })
                .collect();
            // The list's 8-DIP padding plus these 8 put the labels at the
            // 16-DIP inset of every other group.
            let editor = StackPanel::new()
                .margin(Thickness::uniform(tokens::spacing::SM))
                .children((
                    m3::form_row(
                        "Checkout folder",
                        m3::field()
                            .text(nav::display_path(&path).to_owned())
                            .automation_name(format!("Checkout folder of {name}"))
                            .is_enabled(!busy)
                            .on_text_changed(context.callback(move |text| {
                                Message::Settings(SettingsMsg::ProjectPath(path_id.clone(), text))
                            }))
                            .into(),
                        None,
                    ),
                    m3::form_row(
                        "Rule profiles",
                        StackPanel::new()
                            .orientation(Orientation::Horizontal)
                            .spacing(tokens::spacing::MD)
                            .keyed_children(checks),
                        None,
                    ),
                    m3::form_row(
                        "Rule actions",
                        m3::field()
                            .text(model::project_actions(drafts, &id))
                            .placeholder_text("For example send, stop, git.fetch")
                            .automation_name(format!("Rule actions of {name}"))
                            .is_enabled(!busy)
                            .on_text_changed(context.callback(move |text| {
                                Message::Settings(SettingsMsg::ProjectActions(
                                    actions_id.clone(),
                                    text,
                                ))
                            }))
                            .into(),
                        None,
                    ),
                    m3::destructive(palette)
                        .margin(Thickness::new(
                            tokens::layout::LABEL_WIDTH + tokens::spacing::SM,
                            tokens::spacing::XS,
                            0.0,
                            0.0,
                        ))
                        .is_enabled(!busy)
                        .automation_name(format!("Remove project {name}"))
                        .on_click(
                            context.message(Message::Settings(SettingsMsg::ProjectRemoved(
                                id.clone(),
                            ))),
                        )
                        .content(m3::label("Remove project")),
                ));
            rows.push(KeyedView::new(format!("editor:{id}"), editor));
        }
        if rows.is_empty() {
            rows.push(KeyedView::new(
                "none",
                m3::secondary(
                    "No projects yet. Opening Swarm in a Git checkout registers it here.",
                )
                .text_wrapping(TextWrapping::Wrap)
                .margin(Thickness::uniform(tokens::spacing::SM)),
            ));
        }
        let add = m3::group(
            palette,
            vec![
                KeyedView::new(
                    "id",
                    m3::form_row(
                        "Project id",
                        m3::field()
                            .text(self.settings.new_project_id.clone())
                            .placeholder_text("Letters, digits, - and _")
                            .automation_name("New project id")
                            .on_text_changed(context.callback(|text| {
                                Message::Settings(SettingsMsg::NewProjectId(text))
                            }))
                            .into(),
                        None,
                    ),
                ),
                KeyedView::new(
                    "path",
                    m3::form_row(
                        "Checkout folder",
                        m3::field()
                            .text(self.settings.new_project_path.clone())
                            .placeholder_text("C:\\path\\to\\checkout")
                            .automation_name("New project folder")
                            .on_text_changed(context.callback(|text| {
                                Message::Settings(SettingsMsg::NewProjectPath(text))
                            }))
                            .into(),
                        Some(
                            m3::outlined(palette)
                                .is_enabled(
                                    !busy && !self.settings.new_project_id.trim().is_empty(),
                                )
                                .automation_name("Add project")
                                .on_click(
                                    context.message(Message::Settings(SettingsMsg::ProjectAdded)),
                                )
                                .content(m3::label("Add")),
                        ),
                    ),
                ),
            ],
        );
        let dirty = self.settings.dirty(Page::Projects);
        StackPanel::new().spacing(tokens::spacing::MD).children((
            m3::list(palette, rows),
            StackPanel::new()
                .spacing(tokens::spacing::SM)
                .children((m3::section_heading("Add a project", "", None), add)),
            self.save_bar(Page::Projects, dirty, context, palette),
        ))
    }

    fn automation_section(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        StackPanel::new().spacing(tokens::spacing::MD).children((
            self.assignments_view(context, palette),
            config_view::automation_screen(
                &self.automation.rules,
                &self.automation.status,
                context.message(Message::Automation(
                    AutomationMsg::AutomationRefreshRequested,
                )),
                palette,
            ),
        ))
    }

    /// "Keep N busy from folder X": the `[automation.assignments.*]` entries.
    fn assignments_view(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        let busy = self.settings.busy();
        let automation = &self.settings.automation;
        let roles = model::routing_roles(&self.settings.routing);
        let mut groups: Vec<KeyedView> = vec![KeyedView::new(
            "title",
            m3::section_heading(
                "Assignments",
                "Keep a number of Agents busy with the briefs (.md files) of a folder, in filename order. Each brief starts one Agent through routing; a finished Agent is retired and the next brief starts.",
                None,
            ),
        )];
        for key in model::assignment_keys(automation) {
            let busy_count = model::assignment_busy(automation, &key);
            let role = model::assignment_role(automation, &key);
            let mut role_items = roles.clone();
            if !role_items.contains(&role) {
                role_items.push(role.clone());
            }
            let role_index = role_items.iter().position(|item| *item == role);
            let text_field = |field: AssignmentField, label: &str, hint: &str| {
                let callback_key = key.clone();
                m3::form_row(
                    label,
                    m3::field()
                        .text(
                            nav::display_path(&model::assignment_text(automation, &key, field))
                                .to_owned(),
                        )
                        .placeholder_text(hint)
                        .automation_name(format!("{label} of {key}"))
                        .is_enabled(!busy)
                        .on_text_changed(context.callback(move |text| {
                            Message::Settings(SettingsMsg::AssignmentText(
                                callback_key.clone(),
                                field,
                                text,
                            ))
                        }))
                        .into(),
                    None,
                )
            };
            let folder = text_field(
                AssignmentField::Folder,
                "Briefs folder",
                "C:\\briefs\\backlog",
            );
            let workspace = text_field(
                AssignmentField::Workspace,
                "Work in",
                "A project's checkout folder",
            );
            let interval = text_field(AssignmentField::Interval, "Check every", "30s");
            let (busy_key, role_key) = (key.clone(), key.clone());
            let rows = vec![
                KeyedView::new(
                    "title",
                    m3::section_title(format!(
                        "{key} · keep {} busy",
                        busy_count.map_or("?".to_owned(), |count| count.to_string())
                    ))
                    .height(tokens::height::FORM_ROW)
                    .vertical_alignment(VerticalAlignment::Center),
                ),
                KeyedView::new(
                    "busy",
                    m3::form_row(
                        "Agents kept busy",
                        NumberBox::new()
                            .width(tokens::layout::NUMBER_WIDTH)
                            .horizontal_alignment(HorizontalAlignment::Left)
                            .minimum(1.0)
                            .maximum(1000.0)
                            .value(busy_count.map(|count| count as f64))
                            .is_enabled(!busy)
                            .automation_name(format!("Agents kept busy by {key}"))
                            .on_value_changed(context.callback(move |value| {
                                Message::Settings(SettingsMsg::AssignmentBusy(
                                    busy_key.clone(),
                                    value,
                                ))
                            }))
                            .into(),
                        None,
                    ),
                ),
                KeyedView::new("folder", folder),
                KeyedView::new("workspace", workspace),
                KeyedView::new(
                    "role",
                    m3::form_row(
                        "Routing role",
                        Grid::new().keyed_children(vec![KeyedView::new(
                            format!("{}:{role}", role_items.join("\u{1f}")),
                            m3::enum_width(ComboBox::new())
                                .items_source(role_items)
                                .selected_index(role_index)
                                .is_enabled(!busy)
                                .automation_name(format!("Routing role of {key}"))
                                .on_selection_changed(context.callback(move |index| {
                                    Message::Settings(SettingsMsg::AssignmentRole(
                                        role_key.clone(),
                                        index,
                                    ))
                                })),
                        )]),
                        None,
                    ),
                ),
                KeyedView::new("interval", interval),
                KeyedView::new(
                    "remove",
                    m3::destructive(palette)
                        .margin(Thickness::new(
                            tokens::layout::LABEL_WIDTH + tokens::spacing::SM,
                            tokens::spacing::XS,
                            0.0,
                            0.0,
                        ))
                        .is_enabled(!busy)
                        .automation_name(format!("Remove assignment {key}"))
                        .on_click(context.message(Message::Settings(
                            SettingsMsg::AssignmentRemoved(key.clone()),
                        )))
                        .content(m3::label("Remove assignment")),
                ),
            ];
            groups.push(KeyedView::new(
                format!("assignment:{key}"),
                m3::group(palette, rows),
            ));
        }
        groups.push(KeyedView::new(
            "add",
            m3::group(
                palette,
                vec![KeyedView::new(
                    "add",
                    m3::form_row(
                        "New assignment",
                        m3::field()
                            .text(self.settings.new_assignment.clone())
                            .placeholder_text("Name, for example backlog")
                            .automation_name("New assignment name")
                            .on_text_changed(context.callback(|text| {
                                Message::Settings(SettingsMsg::NewAssignment(text))
                            }))
                            .into(),
                        Some(
                            m3::outlined(palette)
                                .is_enabled(
                                    !busy && !self.settings.new_assignment.trim().is_empty(),
                                )
                                .automation_name("Add assignment")
                                .on_click(
                                    context
                                        .message(Message::Settings(SettingsMsg::AssignmentAdded)),
                                )
                                .content(m3::label("Add")),
                        ),
                    ),
                )],
            ),
        ));
        let dirty = self.settings.dirty(Page::Automation);
        groups.push(KeyedView::new(
            "save",
            self.save_bar(Page::Automation, dirty, context, palette),
        ));
        StackPanel::new()
            .spacing(tokens::spacing::MD)
            .keyed_children(groups)
    }

    fn cloud_section(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        let busy = self.settings.busy();
        let enabled = model::cloud_enabled(&self.settings.cloud);
        let switch = Grid::new().keyed_children(vec![KeyedView::new(
            format!("cloud:{enabled}"),
            CheckBox::new()
                .is_checked(enabled)
                .is_enabled(!busy)
                .automation_name("Cloud MCP configured")
                .on_is_checked_changed(
                    context.callback(|on: bool| Message::Settings(SettingsMsg::CloudEnabled(on))),
                )
                .content(m3::label("Configured")),
        )]);
        let mut rows: Vec<KeyedView> = vec![
            KeyedView::new("enabled", m3::form_row("Cloud MCP", switch, None)),
            KeyedView::new(
                "help",
                m3::help(
                    "Serve Swarm's MCP endpoint through a Cloudflare tunnel behind Cloudflare Access. The daemon applies a change at its next start.",
                ),
            ),
        ];
        if enabled {
            rows.extend(
                model::CLOUD_FIELDS
                    .iter()
                    .enumerate()
                    .map(|(index, (key, label))| {
                        KeyedView::new(
                            *key,
                            m3::form_row(
                                label,
                                m3::field()
                                    .text(model::cloud_field(&self.settings.cloud, key))
                                    .automation_name(*label)
                                    .is_enabled(!busy)
                                    .on_text_changed(context.callback(move |text| {
                                        Message::Settings(SettingsMsg::CloudField(index, text))
                                    }))
                                    .into(),
                                None,
                            ),
                        )
                    }),
            );
        }
        let dirty = self.settings.dirty(Page::CloudMcp);
        StackPanel::new().spacing(tokens::spacing::MD).children((
            m3::group(palette, rows),
            self.save_bar(Page::CloudMcp, dirty, context, palette),
        ))
    }

    fn advanced_section(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        let data_dir = match &self.data_dir {
            Ok(path) => nav::display_path(&path.display().to_string()).to_owned(),
            Err(error) => format!("unavailable: {error}"),
        };
        let open = self.settings.history_open;
        let history: Vec<KeyedView> = self
            .settings
            .history
            .iter()
            .map(|entry| {
                let (headline, supporting) = model::history_lines(entry);
                KeyedView::new(
                    format!("history:{}", entry.revision),
                    Border::new()
                        .height(tokens::height::CONTROL)
                        .automation_name(format!("{headline}, {supporting}"))
                        .content(m3::row_content(None, &headline, &supporting, View::empty())),
                )
            })
            .collect();
        let history_view: View = if !open {
            View::empty()
        } else if history.is_empty() {
            m3::secondary("No settings changes recorded yet.").into()
        } else {
            m3::group(palette, history)
        };
        let restart = if self.settings.restart_required.is_empty() {
            "Every section applies without a restart.".to_owned()
        } else {
            format!(
                "Changes to {} apply at the daemon's next start.",
                self.settings
                    .restart_required
                    .iter()
                    .map(|section| model::section_label(*section))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        };
        let dirty = self.settings.dirty(Page::Advanced);
        StackPanel::new()
            .spacing(tokens::spacing::MD)
            .children((
                m3::group(
                    palette,
                    vec![
                        KeyedView::new(
                            "data",
                            m3::form_row(
                                "Daemon data",
                                m3::body(data_dir)
                                    .text_trimming(TextTrimming::CharacterEllipsis)
                                    .is_text_selection_enabled(true)
                                    .into(),
                                Some(
                                    m3::outlined(palette)
                                        .automation_name("Reload everything")
                                        .on_click(context.message(Message::Shell(
                                            ShellMsg::RefreshRequested,
                                        )))
                                        .content(m3::label("Reload")),
                                ),
                            ),
                        ),
                        KeyedView::new(
                            "data-help",
                            m3::help(
                                "Settings live in its config.toml; every change goes through the daemon's checks.",
                            ),
                        ),
                        KeyedView::new(
                            "history",
                            m3::form_row(
                                "History",
                                m3::body(format!(
                                    "{} recent change(s): who changed which section, and when.",
                                    self.settings.history.len()
                                ))
                                .text_trimming(TextTrimming::CharacterEllipsis)
                                .into(),
                                Some(
                                    m3::text_button(palette)
                                        .automation_name(if open {
                                            "Hide history"
                                        } else {
                                            "Show history"
                                        })
                                        .on_click(context.message(Message::Settings(
                                            SettingsMsg::HistoryToggled,
                                        )))
                                        .content(m3::label(if open { "Hide" } else { "Show" })),
                                ),
                            ),
                        ),
                    ],
                ),
                history_view,
                m3::section_heading(
                    "All settings",
                    &format!(
                        "Every section of config.toml as JSON, including daemon, mail, accounts and proxies. {restart}"
                    ),
                    None,
                ),
                m3::field()
                    .accepts_return(true)
                    .text_wrapping(TextWrapping::NoWrap)
                    .min_height(tokens::layout::SOURCE_MIN_HEIGHT)
                    .max_height(tokens::layout::SOURCE_MAX_HEIGHT)
                    .automation_name("Settings sections JSON")
                    .text(self.primed(&self.settings.all))
                    .is_enabled(!self.settings.busy())
                    .on_text_changed(
                        context.callback(|text| Message::Settings(SettingsMsg::AllDraft(text))),
                    ),
                self.save_bar(Page::Advanced, dirty, context, palette),
            ))
    }
}

/// One routing candidate row: its place in the order, its name and the
/// reorder / remove buttons.
fn candidate_row(place: usize, name: &str, buttons: View) -> View {
    Grid::new()
        .height(tokens::height::CONTROL)
        .columns([
            GridLength::Pixel(tokens::layout::TREE_INDENT),
            GridLength::STAR,
            GridLength::Auto,
        ])
        .children((
            m3::secondary(format!("{place}")).vertical_alignment(VerticalAlignment::Center),
            m3::body(name.to_owned())
                .grid_column(1)
                .vertical_alignment(VerticalAlignment::Center)
                .text_trimming(TextTrimming::CharacterEllipsis),
            Border::new().grid_column(2).content(buttons),
        ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn read_with(revision: &str, routing: Value, desktop: Value) -> SettingsReadResponse {
        SettingsReadResponse {
            revision: Some(revision.to_owned()),
            sections: BTreeMap::from([(Section::Routing, routing), (Section::Desktop, desktop)]),
            restart_required: Vec::new(),
            history: Vec::new(),
        }
    }

    #[test]
    fn desktop_only_reread_keeps_edited_drafts() {
        let mut state = SettingsState::default();
        state.take(read_with(
            "rev1",
            json!({"default": ["a"]}),
            json!({"theme": "system"}),
        ));
        // The owner edits Routing without saving; a window-size write moves
        // the revision and the desktop section only.
        state.routing = json!({"default": ["a", "b"]});
        state.take(read_with(
            "rev2",
            json!({"default": ["a"]}),
            json!({"theme": "system", "window": {"width": 900, "height": 700}}),
        ));
        assert_eq!(state.routing, json!({"default": ["a", "b"]}));
        assert!(state.dirty(Page::Routing));
        assert_eq!(state.revision.as_deref(), Some("rev2"));
    }
}
