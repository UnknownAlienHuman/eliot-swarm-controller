//! Settings: one page with sections, the one place for everything that
//! needs adjusting. Every section reads and writes the daemon's unified
//! settings (`settings.read` / `settings.save`, one store with a revision
//! and author history) through [`crate::config_client`]:
//! General (`[desktop]`), Models and profiles (`[profiles]`), Routing
//! (`[routing]`), Projects (`[projects]`), Automation and scripts
//! (`[automation]` assignments; rules and scripts keep their own files and
//! APIs), Cloud MCP (`[cloud_mcp]`) and Advanced (every section as JSON,
//! and the settings history). Each save shows its result and revision.

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
    CloudMcp,
    Advanced,
}

impl Page {
    const ALL: [Self; 7] = [
        Self::General,
        Self::Models,
        Self::Routing,
        Self::Projects,
        Self::Automation,
        Self::CloudMcp,
        Self::Advanced,
    ];

    fn label(self) -> &'static str {
        match self {
            Self::General => "General",
            Self::Models => "Models and profiles",
            Self::Routing => "Routing",
            Self::Projects => "Projects",
            Self::Automation => "Automation and scripts",
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
            Page::General | Page::Models => false,
        }
    }
}

/// The section list beside the shown section.
const SECTION_LIST_WIDTH: f64 = 280.0;
/// The label column of the forms.
const LABEL_WIDTH: f64 = 180.0;

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
                        self.prefs = crate::settings::Preferences::from_reply(&read);
                        self.preferred_window = crate::settings::window(&read);
                        self.take_profiles(&read);
                        self.settings.take(read);
                        self.reprime_fields(context);
                    }
                    Err(error) => self.settings.error = Some(format!("Settings not read: {error}")),
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
                    Page::General => {}
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
                    .map(|project| project.workspace.clone())
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
            return;
        }
        let Ok(data_dir) = self.data_dir.clone() else {
            return;
        };
        self.settings.reading = true;
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
            Page::Automation => {
                if !self.scripts.listed {
                    self.load_scripts(context);
                }
                self.load_rules(context);
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

    /// The Settings page: the section list (navigation items), then the
    /// shown section under its title-large title.
    pub(super) fn settings_view(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        let rows: Vec<KeyedView> = Page::ALL
            .iter()
            .map(|page| {
                let selected = *page == self.settings_page;
                KeyedView::new(
                    format!("{}:{selected}", page.label()),
                    m3::nav_item(palette, selected)
                        .automation_name(if selected {
                            format!("{} (shown)", page.label())
                        } else {
                            page.label().to_owned()
                        })
                        .on_click(
                            context.message(Message::Settings(SettingsMsg::PageSelected(*page))),
                        )
                        .content(m3::nav_content(
                            TextBlock::new()
                                .text(page.label())
                                .text_trimming(TextTrimming::CharacterEllipsis),
                        )),
                )
            })
            .collect();
        let nav = StackPanel::new()
            .margin(Thickness::new(
                12.0,
                theme::spacing::LG,
                theme::spacing::SM,
                theme::spacing::MD,
            ))
            .children((
                m3::title("Settings", theme::type_scale::TITLE_LARGE).margin(Thickness::new(
                    theme::spacing::MD,
                    0.0,
                    0.0,
                    theme::spacing::MD,
                )),
                StackPanel::new().keyed_children(rows),
            ));
        let page = self.settings_page;
        let body: View = if let Some(error) = &self.settings.error {
            scroll(self.read_error(error, context, palette))
        } else if !self.settings.loaded {
            m3::muted("Reading settings…", theme::type_scale::BODY_MEDIUM).into()
        } else {
            match page {
                Page::General => scroll(self.general_section(context, palette)),
                Page::Models => self.models_section(context, palette),
                Page::Routing => scroll(self.routing_section(context, palette)),
                Page::Projects => scroll(self.projects_section(context, palette)),
                Page::Automation => scroll(self.automation_section(context, palette)),
                Page::CloudMcp => scroll(self.cloud_section(context, palette)),
                Page::Advanced => scroll(self.advanced_section(context, palette)),
            }
        };
        // One explicit column width, so every section's groups line up
        // (content-sized until the window size is observed).
        let width = self.main_width().map(|width| {
            (width - SECTION_LIST_WIDTH - 2.0 * theme::spacing::LG)
                .clamp(320.0, theme::layout::READING_WIDTH)
        });
        // The section's tree is keyed by section, so its editors are new
        // controls (see `reprime_fields`).
        let content = Grid::new()
            .grid_column(1)
            .rows([GridLength::Auto, GridLength::STAR])
            .row_spacing(theme::spacing::LG)
            .width(width)
            .horizontal_alignment(HorizontalAlignment::Left)
            .margin(Thickness::uniform(theme::spacing::LG))
            .keyed_children(vec![
                KeyedView::new(
                    format!("title:{}", page.label()),
                    m3::title(page.label(), theme::type_scale::TITLE_LARGE),
                ),
                KeyedView::new(
                    format!("body:{}", page.label()),
                    Border::new().grid_row(1).content(body),
                ),
            ]);
        Grid::new()
            .columns([GridLength::Pixel(SECTION_LIST_WIDTH), GridLength::STAR])
            .children((nav, content))
    }

    fn read_error(&self, error: &str, context: &mut ViewContext<Self>, palette: Palette) -> View {
        StackPanel::new().spacing(theme::spacing::MD).children((
            m3::text(error.to_owned(), theme::type_scale::BODY_LARGE)
                .text_wrapping(TextWrapping::Wrap)
                .is_text_selection_enabled(true),
            m3::outlined(palette)
                .is_enabled(!self.settings.reading)
                .on_click(context.message(Message::Settings(SettingsMsg::Reload)))
                .content(TextBlock::new().text("Read again")),
        ))
    }

    /// Save / Validate / Discard and the section's last result, under every
    /// editable section.
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
        StackPanel::new().spacing(theme::spacing::SM).children((
            StackPanel::new()
                .orientation(Orientation::Horizontal)
                .spacing(theme::spacing::SM)
                .children((
                    m3::filled(palette)
                        .is_enabled(dirty && !busy)
                        .automation_name(format!("Save {}", page.label()))
                        .on_click(
                            context.message(Message::Settings(SettingsMsg::Save(page, false))),
                        )
                        .content(TextBlock::new().text(if saving { "Saving…" } else { "Save" })),
                    m3::outlined(palette)
                        .is_enabled(!busy)
                        .automation_name(format!("Validate {}", page.label()))
                        .on_click(context.message(Message::Settings(SettingsMsg::Save(page, true))))
                        .content(TextBlock::new().text("Validate")),
                    m3::text_button(palette)
                        .is_enabled(dirty && !busy)
                        .automation_name(format!("Discard changes to {}", page.label()))
                        .on_click(context.message(Message::Settings(SettingsMsg::Discard(page))))
                        .content(m3::label("Discard")),
                )),
            m3::muted(note, theme::type_scale::BODY_SMALL)
                .automation_name(format!("{} save result", page.label()))
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
                        .content(m3::segment_label(choice.label(), selected)),
                )
            })
            .collect();
        let reopen = self.prefs.startup == Startup::LastConversation;
        let switch = Grid::new().keyed_children(vec![KeyedView::new(
            format!("reopen:{reopen}"),
            ToggleSwitch::new()
                .is_on(reopen)
                .automation_name("Reopen the last conversation at start")
                .on_toggled(context.callback(|on: bool| {
                    Message::Shell(ShellMsg::StartupChosen(if on {
                        Startup::LastConversation
                    } else {
                        Startup::NewTask
                    }))
                })),
        )]);
        let note = self
            .settings
            .notes
            .get(&Page::General)
            .cloned()
            .unwrap_or_else(|| "Changes save as you choose them.".to_owned());
        StackPanel::new().spacing(theme::spacing::SM).children((
            m3::list_group(
                palette,
                vec![
                    (
                        "theme".to_owned(),
                        m3::list_item(
                            "Theme",
                            "System follows the Windows setting.",
                            m3::segmented(palette, segments),
                        ),
                    ),
                    (
                        "reopen".to_owned(),
                        m3::list_item(
                            "Reopen the last conversation",
                            "At start, open the most recently active task instead of a new one.",
                            switch,
                        ),
                    ),
                ],
            ),
            m3::muted(note, theme::type_scale::BODY_SMALL)
                .automation_name("General save result")
                .margin(Thickness::xy(theme::spacing::MD, 0.0))
                .text_wrapping(TextWrapping::Wrap),
        ))
    }

    fn models_section(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        let dirty = self.editor.draft != self.editor.base;
        Grid::new()
            .rows([GridLength::STAR, GridLength::Auto])
            .row_spacing(theme::spacing::MD)
            .children((
                self.profiles_view(context, palette),
                Border::new().grid_row(1).content(self.save_bar(
                    Page::Models,
                    dirty,
                    context,
                    palette,
                )),
            ))
    }

    fn routing_section(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        let busy = self.settings.busy();
        let mut groups: Vec<KeyedView> = vec![KeyedView::new(
            "intro",
            m3::muted(
                "When a task starts on Auto, Swarm tries the role's profiles in order and runs the first one that is installed, not on hold, not out of quota and within capacity. The choice and the skipped profiles are recorded on the task.",
                theme::type_scale::BODY_MEDIUM,
            )
            .text_wrapping(TextWrapping::Wrap)
            .margin(Thickness::xy(theme::spacing::MD, 0.0)),
        )];
        // What Auto picks right now, from the saved policy.
        if let Some(Ok(decision)) = &self.launch.preview {
            let (headline, skipped) = model::decision_lines(decision);
            let now = std::iter::once(format!("Now: {headline}."))
                .chain(skipped)
                .collect::<Vec<_>>()
                .join("\n");
            groups.push(KeyedView::new(
                format!("now:{now}"),
                m3::muted(now, theme::type_scale::BODY_SMALL)
                    .automation_name("What Auto picks now")
                    .text_wrapping(TextWrapping::Wrap)
                    .margin(Thickness::xy(theme::spacing::MD, 0.0)),
            ));
        }
        for role in model::routing_roles(&self.settings.routing) {
            let list = model::candidates(&self.settings.routing, &role);
            let mut items: Vec<(String, View)> = Vec::new();
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
            items.push((
                format!("{role}:head"),
                m3::list_item(headline, supporting, View::empty()),
            ));
            let count = list.len();
            for (index, name) in list.iter().enumerate() {
                let first = index == 0;
                let last = index + 1 == count;
                let buttons = StackPanel::new()
                    .orientation(Orientation::Horizontal)
                    .spacing(theme::spacing::XS)
                    .children((
                        m3::icon_button(palette)
                            .is_enabled(!busy && !first)
                            .automation_name(format!("Move {name} earlier in {role}"))
                            .on_click(context.message(Message::Settings(
                                SettingsMsg::RoutingMoved(role.clone(), index, true),
                            )))
                            .content(m3::icon_sized(m3::glyph::ARROW_UP, 14.0)),
                        m3::icon_button(palette)
                            .is_enabled(!busy && !last)
                            .automation_name(format!("Move {name} later in {role}"))
                            .on_click(context.message(Message::Settings(
                                SettingsMsg::RoutingMoved(role.clone(), index, false),
                            )))
                            .content(m3::icon_sized(m3::glyph::ARROW_DOWN, 14.0)),
                        m3::icon_button(palette)
                            .is_enabled(!busy)
                            .automation_name(format!("Remove {name} from {role}"))
                            .on_click(context.message(Message::Settings(
                                SettingsMsg::RoutingRemoved(role.clone(), index),
                            )))
                            .content(m3::icon_sized(m3::glyph::CLOSE, 14.0)),
                    ));
                items.push((
                    format!("{role}:{index}:{name}:{first}:{last}"),
                    candidate_row(index + 1, name, buttons),
                ));
            }
            let addable = self.addable_profiles(&role);
            if !addable.is_empty() {
                let key = format!("{role}:add:{}", addable.join("\u{1f}"));
                let callback_role = role.clone();
                items.push((
                    key.clone(),
                    Border::new()
                        .padding(Thickness::new(
                            theme::spacing::MD,
                            theme::spacing::XS,
                            theme::spacing::MD,
                            theme::spacing::SM,
                        ))
                        .content(Grid::new().keyed_children(vec![KeyedView::new(
                            key,
                            ComboBox::new()
                                .items_source(addable)
                                .selected_index(None)
                                .placeholder_text("Add a profile")
                                .is_enabled(!busy)
                                .automation_name(format!("Add a profile to {role}"))
                                .min_width(240.0)
                                .on_selection_changed(context.callback(move |index| {
                                    Message::Settings(SettingsMsg::RoutingAdded(
                                        callback_role.clone(),
                                        index,
                                    ))
                                })),
                        )])),
                ));
            }
            groups.push(KeyedView::new(
                format!("role:{role}"),
                m3::list_group(palette, items),
            ));
        }
        groups.push(KeyedView::new(
            "new-role",
            form_row(
                "Another role",
                m3::field()
                    .text(self.settings.new_role.clone())
                    .placeholder_text("Role name, for example review")
                    .automation_name("New role name")
                    .on_text_changed(
                        context
                            .callback(|text| Message::Settings(SettingsMsg::NewRoleChanged(text))),
                    )
                    .into(),
                Some(
                    m3::tonal(palette)
                        .is_enabled(!busy && !self.settings.new_role.trim().is_empty())
                        .automation_name("Add role")
                        .on_click(context.message(Message::Settings(SettingsMsg::RoleAdded)))
                        .content(TextBlock::new().text("Add role")),
                ),
            ),
        ));
        let dirty = self.settings.dirty(Page::Routing);
        groups.push(KeyedView::new(
            "save",
            self.save_bar(Page::Routing, dirty, context, palette),
        ));
        StackPanel::new()
            .spacing(theme::spacing::MD)
            .keyed_children(groups)
    }

    fn projects_section(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        let busy = self.settings.busy();
        let drafts = &self.settings.projects;
        let profiles = self.known_profiles();
        let mut items: Vec<(String, View)> = Vec::new();
        for id in model::project_ids(drafts) {
            let open = self.settings.project_open.as_deref() == Some(id.as_str());
            let path = model::project_path(drafts, &id);
            let allowed = model::project_profiles(drafts, &id);
            let summary = if allowed.is_empty() {
                "no profiles allowed".to_owned()
            } else {
                allowed.join(", ")
            };
            items.push((
                format!("project:{id}:{open}"),
                m3::nav_item(palette, open)
                    .automation_name(format!("Project {id}: {path}"))
                    .on_click(
                        context.message(Message::Settings(SettingsMsg::ProjectOpened(id.clone()))),
                    )
                    .content(m3::nav_content(
                        Grid::new()
                            .columns([GridLength::Auto, GridLength::STAR])
                            .column_spacing(12.0)
                            .children((
                                m3::icon(m3::glyph::FOLDER),
                                StackPanel::new()
                                    .grid_column(1)
                                    .vertical_alignment(VerticalAlignment::Center)
                                    .children((
                                        m3::text(id.clone(), theme::type_scale::BODY_LARGE),
                                        m3::muted(
                                            format!("{} · {summary}", nav::display_path(&path)),
                                            theme::type_scale::BODY_SMALL,
                                        )
                                        .text_trimming(TextTrimming::CharacterEllipsis),
                                    )),
                            )),
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
                            .automation_name(format!("Allow {profile} in {id}"))
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
            let editor = StackPanel::new()
                .spacing(12.0)
                .margin(Thickness::new(
                    theme::spacing::MD,
                    theme::spacing::SM,
                    theme::spacing::MD,
                    theme::spacing::MD,
                ))
                .children((
                    form_row(
                        "Checkout folder",
                        m3::field()
                            .text(path.clone())
                            .automation_name(format!("Checkout folder of {id}"))
                            .is_enabled(!busy)
                            .on_text_changed(context.callback(move |text| {
                                Message::Settings(SettingsMsg::ProjectPath(path_id.clone(), text))
                            }))
                            .into(),
                        None,
                    ),
                    form_row(
                        "Allowed profiles",
                        StackPanel::new()
                            .orientation(Orientation::Horizontal)
                            .spacing(theme::spacing::MD)
                            .keyed_children(checks),
                        None,
                    ),
                    form_row(
                        "Agent actions",
                        m3::field()
                            .text(model::project_actions(drafts, &id))
                            .placeholder_text("For example send, stop, git.fetch")
                            .automation_name(format!("Agent actions of {id}"))
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
                    m3::error_text_button(palette)
                        .is_enabled(!busy)
                        .automation_name(format!("Remove project {id}"))
                        .on_click(
                            context.message(Message::Settings(SettingsMsg::ProjectRemoved(
                                id.clone(),
                            ))),
                        )
                        .content(m3::text_icon_label(m3::glyph::DELETE, "Remove project")),
                ));
            items.push((format!("editor:{id}"), editor));
        }
        if items.is_empty() {
            items.push((
                "none".to_owned(),
                m3::list_item(
                    "No projects yet",
                    "Opening Swarm in a Git checkout registers it here.",
                    View::empty(),
                ),
            ));
        }
        let add = StackPanel::new()
            .spacing(12.0)
            .margin(Thickness::uniform(theme::spacing::MD))
            .children((
                m3::text("Add a project", theme::type_scale::TITLE_SMALL),
                form_row(
                    "Project id",
                    m3::field()
                        .text(self.settings.new_project_id.clone())
                        .placeholder_text("Letters, digits, - and _")
                        .automation_name("New project id")
                        .on_text_changed(
                            context.callback(|text| {
                                Message::Settings(SettingsMsg::NewProjectId(text))
                            }),
                        )
                        .into(),
                    None,
                ),
                form_row(
                    "Checkout folder",
                    m3::field()
                        .text(self.settings.new_project_path.clone())
                        .placeholder_text("C:\\path\\to\\checkout")
                        .automation_name("New project folder")
                        .on_text_changed(
                            context.callback(|text| {
                                Message::Settings(SettingsMsg::NewProjectPath(text))
                            }),
                        )
                        .into(),
                    Some(
                        m3::tonal(palette)
                            .is_enabled(!busy && !self.settings.new_project_id.trim().is_empty())
                            .automation_name("Add project")
                            .on_click(context.message(Message::Settings(SettingsMsg::ProjectAdded)))
                            .content(TextBlock::new().text("Add")),
                    ),
                ),
            ));
        let dirty = self.settings.dirty(Page::Projects);
        StackPanel::new().spacing(theme::spacing::MD).children((
            m3::list_group(palette, items),
            m3::list_group(palette, vec![("add".to_owned(), add)]),
            self.save_bar(Page::Projects, dirty, context, palette),
        ))
    }

    fn automation_section(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        StackPanel::new().spacing(theme::spacing::LG).children((
            self.assignments_view(context, palette),
            config_view::automation_screen(
                &self.automation.rules,
                &self.automation.status,
                context.message(Message::Automation(
                    AutomationMsg::AutomationRefreshRequested,
                )),
                palette,
            ),
            self.scripts_view(context, palette),
        ))
    }

    /// "Keep N busy from folder X": the `[automation.assignments.*]` entries.
    fn assignments_view(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        let busy = self.settings.busy();
        let automation = &self.settings.automation;
        let roles = model::routing_roles(&self.settings.routing);
        let mut groups: Vec<KeyedView> = vec![KeyedView::new(
            "title",
            StackPanel::new()
                .spacing(theme::spacing::XS)
                .margin(Thickness::xy(theme::spacing::MD, 0.0))
                .children((
                    m3::text("Assignments", theme::type_scale::TITLE_MEDIUM),
                    m3::muted(
                        "Keep a number of Agents busy with the briefs (.md files) of a folder, in filename order. Each brief starts one Agent through routing; a finished Agent is retired and the next brief starts.",
                        theme::type_scale::BODY_MEDIUM,
                    )
                    .text_wrapping(TextWrapping::Wrap),
                )),
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
                form_row(
                    label,
                    m3::field()
                        .text(model::assignment_text(automation, &key, field))
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
            let editor = StackPanel::new()
                .spacing(12.0)
                .margin(Thickness::uniform(theme::spacing::MD))
                .children((
                    Grid::new()
                        .columns([GridLength::STAR, GridLength::Auto])
                        .children((
                            m3::text(
                                format!(
                                    "Keep {} busy · {key}",
                                    busy_count.map_or("?".to_owned(), |count| count.to_string())
                                ),
                                theme::type_scale::TITLE_SMALL,
                            )
                            .vertical_alignment(VerticalAlignment::Center),
                            m3::icon_button(palette)
                                .grid_column(1)
                                .is_enabled(!busy)
                                .automation_name(format!("Remove assignment {key}"))
                                .on_click(context.message(Message::Settings(
                                    SettingsMsg::AssignmentRemoved(key.clone()),
                                )))
                                .content(m3::icon_sized(m3::glyph::DELETE, 14.0)),
                        )),
                    form_row(
                        "Agents kept busy",
                        NumberBox::new()
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
                    folder,
                    workspace,
                    form_row(
                        "Routing role",
                        Grid::new().keyed_children(vec![KeyedView::new(
                            format!("{}:{role}", role_items.join("\u{1f}")),
                            ComboBox::new()
                                .items_source(role_items)
                                .selected_index(role_index)
                                .is_enabled(!busy)
                                .automation_name(format!("Routing role of {key}"))
                                .min_width(200.0)
                                .on_selection_changed(context.callback(move |index| {
                                    Message::Settings(SettingsMsg::AssignmentRole(
                                        role_key.clone(),
                                        index,
                                    ))
                                })),
                        )]),
                        None,
                    ),
                    interval,
                ));
            groups.push(KeyedView::new(
                format!("assignment:{key}"),
                m3::list_group(palette, vec![(key.clone(), editor)]),
            ));
        }
        groups.push(KeyedView::new(
            "add",
            form_row(
                "New assignment",
                m3::field()
                    .text(self.settings.new_assignment.clone())
                    .placeholder_text("Name, for example backlog")
                    .automation_name("New assignment name")
                    .on_text_changed(
                        context
                            .callback(|text| Message::Settings(SettingsMsg::NewAssignment(text))),
                    )
                    .into(),
                Some(
                    m3::tonal(palette)
                        .is_enabled(!busy && !self.settings.new_assignment.trim().is_empty())
                        .automation_name("Add assignment")
                        .on_click(context.message(Message::Settings(SettingsMsg::AssignmentAdded)))
                        .content(TextBlock::new().text("Add")),
                ),
            ),
        ));
        let dirty = self.settings.dirty(Page::Automation);
        groups.push(KeyedView::new(
            "save",
            self.save_bar(Page::Automation, dirty, context, palette),
        ));
        StackPanel::new()
            .spacing(theme::spacing::MD)
            .keyed_children(groups)
    }

    fn cloud_section(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        let busy = self.settings.busy();
        let enabled = model::cloud_enabled(&self.settings.cloud);
        let switch = Grid::new().keyed_children(vec![KeyedView::new(
            format!("cloud:{enabled}"),
            ToggleSwitch::new()
                .is_on(enabled)
                .is_enabled(!busy)
                .automation_name("Cloud MCP configured")
                .on_toggled(
                    context.callback(|on: bool| Message::Settings(SettingsMsg::CloudEnabled(on))),
                ),
        )]);
        let mut items: Vec<(String, View)> = vec![(
            "enabled".to_owned(),
            m3::list_item(
                "Cloud MCP",
                "Serve Swarm's MCP endpoint through a Cloudflare tunnel behind Cloudflare Access. The daemon applies a change at its next start.",
                switch,
            ),
        )];
        if enabled {
            let fields: Vec<KeyedView> = model::CLOUD_FIELDS
                .iter()
                .enumerate()
                .map(|(index, (key, label))| {
                    KeyedView::new(
                        *key,
                        form_row(
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
                })
                .collect();
            items.push((
                "fields".to_owned(),
                StackPanel::new()
                    .spacing(12.0)
                    .margin(Thickness::uniform(theme::spacing::MD))
                    .keyed_children(fields),
            ));
        }
        let dirty = self.settings.dirty(Page::CloudMcp);
        StackPanel::new().spacing(theme::spacing::MD).children((
            m3::list_group(palette, items),
            self.save_bar(Page::CloudMcp, dirty, context, palette),
        ))
    }

    fn advanced_section(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        let data_dir = match &self.data_dir {
            Ok(path) => nav::display_path(&path.display().to_string()).to_owned(),
            Err(error) => format!("unavailable: {error}"),
        };
        let open = self.settings.history_open;
        let history: Vec<(String, View)> = self
            .settings
            .history
            .iter()
            .map(|entry| {
                let (headline, supporting) = model::history_lines(entry);
                (
                    format!("history:{}", entry.revision),
                    m3::list_item(&headline, &supporting, View::empty()),
                )
            })
            .collect();
        let history_view: View = if !open {
            View::empty()
        } else if history.is_empty() {
            m3::muted(
                "No settings changes recorded yet.",
                theme::type_scale::BODY_MEDIUM,
            )
            .margin(Thickness::xy(theme::spacing::MD, 0.0))
            .into()
        } else {
            m3::list_group(palette, history)
        };
        let restart = if self.settings.restart_required.is_empty() {
            "Every section applies without a restart.".to_owned()
        } else {
            format!(
                "Applied at the daemon's next start: {}.",
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
            .spacing(theme::spacing::MD)
            .children((
                m3::list_group(
                    palette,
                    vec![
                        (
                            "data".to_owned(),
                            m3::list_item(
                                "Daemon data",
                                &format!(
                                    "{data_dir}\nSettings live in its config.toml; every change goes through the daemon's checks."
                                ),
                                m3::outlined(palette)
                                    .automation_name("Reload everything")
                                    .on_click(
                                        context.message(Message::Shell(ShellMsg::RefreshRequested)),
                                    )
                                    .content(TextBlock::new().text("Reload")),
                            ),
                        ),
                        (
                            "history".to_owned(),
                            m3::list_item(
                                "History",
                                &format!(
                                    "Who changed which section, and when: {} recent change(s).",
                                    self.settings.history.len()
                                ),
                                m3::text_button(palette)
                                    .automation_name(if open {
                                        "Hide history"
                                    } else {
                                        "Show history"
                                    })
                                    .on_click(
                                        context.message(Message::Settings(
                                            SettingsMsg::HistoryToggled,
                                        )),
                                    )
                                    .content(m3::text_icon_label(
                                        m3::glyph::HISTORY,
                                        if open { "Hide" } else { "Show" },
                                    )),
                            ),
                        ),
                    ],
                ),
                history_view,
                m3::text("All settings", theme::type_scale::TITLE_MEDIUM).margin(Thickness::new(
                    theme::spacing::MD,
                    theme::spacing::SM,
                    0.0,
                    0.0,
                )),
                m3::muted(
                    format!(
                        "Every section of config.toml as JSON, including daemon, mail, accounts and proxies. {restart}"
                    ),
                    theme::type_scale::BODY_MEDIUM,
                )
                .text_wrapping(TextWrapping::Wrap)
                .margin(Thickness::xy(theme::spacing::MD, 0.0)),
                m3::field()
                    .accepts_return(true)
                    .text_wrapping(TextWrapping::NoWrap)
                    .min_height(240.0)
                    .max_height(420.0)
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

/// One routing candidate: its place in the order, its name and the
/// reorder / remove buttons.
fn candidate_row(place: usize, name: &str, buttons: View) -> View {
    Border::new()
        .padding(Thickness::new(
            theme::spacing::MD,
            2.0,
            theme::spacing::SM,
            2.0,
        ))
        .content(
            Grid::new()
                .columns([GridLength::Pixel(28.0), GridLength::STAR, GridLength::Auto])
                .children((
                    m3::muted(format!("{place}"), theme::type_scale::TITLE_SMALL)
                        .vertical_alignment(VerticalAlignment::Center),
                    m3::text(name.to_owned(), theme::type_scale::BODY_LARGE)
                        .grid_column(1)
                        .vertical_alignment(VerticalAlignment::Center)
                        .text_trimming(TextTrimming::CharacterEllipsis),
                    Border::new().grid_column(2).content(buttons),
                )),
        )
}

/// A labelled form row: the label column, the control, an optional action.
fn form_row(label: &str, control: View, action: Option<View>) -> View {
    Grid::new()
        .columns([
            GridLength::Pixel(LABEL_WIDTH),
            GridLength::STAR,
            GridLength::Auto,
        ])
        .column_spacing(theme::spacing::SM)
        .children((
            m3::text(label.to_owned(), theme::type_scale::BODY_LARGE)
                .vertical_alignment(VerticalAlignment::Center),
            Border::new().grid_column(1).content(control),
            Border::new()
                .grid_column(2)
                .content(action.unwrap_or_else(View::empty)),
        ))
}

/// A section body that scrolls.
fn scroll(content: View) -> View {
    ScrollViewer::new()
        .vertical_scroll_bar_visibility(ScrollBarVisibility::Auto)
        .content(content)
}
