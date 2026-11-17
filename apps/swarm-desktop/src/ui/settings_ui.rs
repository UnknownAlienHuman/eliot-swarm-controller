//! Settings, the Codex settings pages as the pattern (owner review
//! 2026-11-16): the sidebar becomes the settings navigation with a Search
//! field and groups (General: Appearance, Startup, Keyboard shortcuts;
//! Agents: Models & presets, Default access, Routing, Usage; Work: Projects,
//! Automations, Worktrees & cleanup; Integrations: Cloud MCP; Advanced:
//! the settings file and its history). Every page is one centred column: a
//! title, a one-line subtitle, then cards whose rows carry the name and
//! description on the left and the control on the right; destructive
//! actions are separate and red. Every section reads and writes the
//! daemon's unified settings (`settings.read` / `settings.save`, one store
//! with a revision and author history) through [`crate::config_client`].
//! Revisions and history stay in Advanced; the other pages say "Saved".

use std::collections::{BTreeMap, HashMap};

use super::*;
use crate::settings::{Access, Startup, ThemeChoice};
use crate::settings_model::{self as model, AssignmentField};
use crate::theme::winui::Palette;
use eliot_core::api::{
    HarnessQuotaResponse, SettingsClientSource, SettingsHistoryEntry, SettingsReadResponse,
    SettingsSaveRequest, SettingsSaveResponse, SettingsSection as Section,
};
use serde_json::{Value, json};

/// The Settings pages.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(super) enum Page {
    Appearance,
    Startup,
    Shortcuts,
    Models,
    Access,
    Routing,
    Usage,
    Projects,
    Automations,
    Worktrees,
    CloudMcp,
    Advanced,
}

/// The navigation's groups and their pages, in order.
const GROUPS: [(&str, &[Page]); 5] = [
    (
        "General",
        &[Page::Appearance, Page::Startup, Page::Shortcuts],
    ),
    (
        "Agents",
        &[Page::Models, Page::Access, Page::Routing, Page::Usage],
    ),
    (
        "Work",
        &[Page::Projects, Page::Automations, Page::Worktrees],
    ),
    ("Integrations", &[Page::CloudMcp]),
    ("Advanced", &[Page::Advanced]),
];

/// What the Search field finds: one entry per setting, `(page, name,
/// words it also answers to)`.
const INDEX: [(Page, &str, &str); 18] = [
    (
        Page::Appearance,
        "Theme",
        "appearance dark light system color colour",
    ),
    (
        Page::Startup,
        "Reopen the last task",
        "startup start conversation open",
    ),
    (Page::Shortcuts, "Keyboard shortcuts", "keys ctrl hotkey"),
    (
        Page::Models,
        "Presets",
        "model profile harness effort preset",
    ),
    (
        Page::Access,
        "Default access",
        "access permission approval full ask auto sandbox",
    ),
    (
        Page::Routing,
        "Routing order",
        "auto role candidate profile order",
    ),
    (Page::Usage, "Quota", "usage limit weekly 5-hour tokens"),
    (
        Page::Usage,
        "Running Agents and tokens",
        "usage swarm totals",
    ),
    (Page::Projects, "Projects", "checkout folder repository"),
    (Page::Projects, "Add a project", "new checkout"),
    (
        Page::Automations,
        "Assignments",
        "automation brief busy folder",
    ),
    (
        Page::Automations,
        "Rules and timers",
        "automation schedule rule timer",
    ),
    (Page::Automations, "Scripts", "automation script run"),
    (
        Page::Worktrees,
        "Managed worktrees",
        "worktree cleanup branch",
    ),
    (
        Page::Worktrees,
        "Clean up finished work",
        "cleanup remove delete disk space merged branch temp target archive",
    ),
    (
        Page::CloudMcp,
        "Cloud MCP",
        "cloudflare tunnel remote access mcp",
    ),
    (
        Page::Advanced,
        "Settings file (JSON)",
        "config toml json advanced",
    ),
    (Page::Advanced, "History", "revision changes author"),
];

impl Page {
    pub(super) fn label(self) -> &'static str {
        match self {
            Self::Appearance => "Appearance",
            Self::Startup => "Startup",
            Self::Shortcuts => "Keyboard shortcuts",
            Self::Models => "Models & presets",
            Self::Access => "Default access",
            Self::Routing => "Routing",
            Self::Usage => "Usage",
            Self::Projects => "Projects",
            Self::Automations => "Automations",
            Self::Worktrees => "Worktrees & cleanup",
            Self::CloudMcp => "Cloud MCP",
            Self::Advanced => "Settings file & history",
        }
    }

    /// The page's one-line subtitle.
    fn subtitle(self) -> &'static str {
        match self {
            Self::Appearance => "How Swarm looks on this computer.",
            Self::Startup => "What the window shows when it opens.",
            Self::Shortcuts => "Keys that work anywhere in the window.",
            Self::Models => {
                "Presets are saved harness, model and access combinations; a new task picks its model in the composer."
            }
            Self::Access => "What new tasks may do without asking you first.",
            Self::Routing => "How Auto chooses a preset when a task starts.",
            Self::Usage => "Remaining provider quota per harness, and what Swarm is running.",
            Self::Projects => "The checkouts tasks start in.",
            Self::Automations => "Assignments, rules, timers and scripts that start work for you.",
            Self::Worktrees => "Worktrees Swarm created for tasks, by project.",
            Self::CloudMcp => "Reach Swarm's MCP endpoint from the cloud.",
            Self::Advanced => "Every section of config.toml as JSON, and who changed what.",
        }
    }

    fn glyph(self) -> &'static str {
        match self {
            Self::Appearance => m3::glyph::APPEARANCE,
            Self::Startup => m3::glyph::STARTUP,
            Self::Shortcuts => m3::glyph::KEYBOARD,
            Self::Models => m3::glyph::MODEL,
            Self::Access => m3::glyph::SHIELD,
            Self::Routing => m3::glyph::ROUTING,
            Self::Usage => m3::glyph::USAGE,
            Self::Projects => m3::glyph::FOLDER,
            Self::Automations => m3::glyph::AUTOMATION,
            Self::Worktrees => m3::glyph::WORKTREE,
            Self::CloudMcp => m3::glyph::CLOUD,
            Self::Advanced => m3::glyph::CODE,
        }
    }

    /// Pages backed by config.toml sections ("Open config.toml").
    fn has_config(self) -> bool {
        matches!(
            self,
            Self::Models
                | Self::Access
                | Self::Routing
                | Self::Projects
                | Self::Automations
                | Self::CloudMcp
                | Self::Advanced
        )
    }
}

/// The index entries a Settings search finds (case-insensitive over the
/// name, the page and its other words).
fn search_settings(query: &str) -> Vec<(Page, &'static str)> {
    let query = query.trim().to_lowercase();
    if query.is_empty() {
        return Vec::new();
    }
    INDEX
        .iter()
        .filter(|(page, name, words)| {
            name.to_lowercase().contains(&query)
                || page.label().to_lowercase().contains(&query)
                || words.contains(&query)
        })
        .map(|(page, name, _)| (*page, *name))
        .collect()
}

#[derive(Clone, Debug)]
pub(super) enum SettingsMsg {
    /// Clean up finished work: read a plan (nothing is removed).
    CleanupPlan,
    CleanupPlanned(Fallible<eliot_core::api::CleanupPlan>),
    /// Check (true) or uncheck one group of the plan.
    CleanupGroupToggled(crate::worktrees::CleanupGroup, bool),
    /// Show or hide one group's kept items.
    CleanupKeptToggled(crate::worktrees::CleanupGroup),
    /// Ask for the one confirmation.
    CleanupApply,
    /// The confirmation's answer: true removes the selected items.
    CleanupConfirmed(bool),
    CleanupApplied(MutationResult<eliot_core::api::CleanupApplyResponse>),
    PageSelected(Page),
    Loaded(Fallible<SettingsReadResponse>),
    Reload,
    /// Validate (true) or save the page's section.
    Save(Page, bool),
    Saved(Page, bool, MutationResult<SettingsSaveResponse>),
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
    /// Default access: the index picked in its list.
    AccessDraft(Option<usize>),
    /// Open config.toml with its default app.
    OpenConfig,
    /// Open a folder in File Explorer.
    OpenFolder(String),
    /// Read every installed harness's quota again.
    UsageRefresh,
    QuotaLoaded(String, Box<Fallible<HarnessQuotaResponse>>),
    /// Read the managed worktrees again.
    WorktreesRefresh,
    /// Exact fleet totals and retention for Usage.
    FleetLoaded(Box<Fallible<config_client::FleetTotals>>),
    /// Every managed worktree (`workspace.list`), the total and whether
    /// the read reached the end.
    WorktreesLoaded(Box<Fallible<ManagedWorktrees>>),
}

/// `workspace.list` read to the end (or to the page bound).
pub(super) type ManagedWorktrees = (Vec<eliot_core::api::ManagedWorkspaceView>, usize, bool);

/// The read settings, the drafts of the page's sections and the last
/// save result of each section.
#[derive(Default)]
pub(super) struct SettingsState {
    /// The plan under review (nothing removed yet).
    cleanup_plan: Option<eliot_core::api::CleanupPlan>,
    cleanup_busy: bool,
    /// Why the last plan or apply read failed.
    cleanup_error: Option<String>,
    /// Unchecked groups (a fresh review starts with
    /// [`crate::worktrees::CleanupGroup::unchecked_by_default`]).
    cleanup_unchecked: std::collections::HashSet<crate::worktrees::CleanupGroup>,
    /// Groups whose kept items are shown (collapsed by default).
    cleanup_kept_open: std::collections::HashSet<crate::worktrees::CleanupGroup>,
    /// The one confirmation is shown.
    cleanup_confirm: bool,
    /// The ledger key of the apply in flight or uncertain (same id on retry).
    cleanup_key: Option<String>,
    /// The applied plan and its result.
    cleanup_result: Option<(
        eliot_core::api::CleanupPlan,
        eliot_core::api::CleanupApplyResponse,
    )>,
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
    /// The navigation's search text.
    pub(super) query: String,
    /// Default access as edited (None: as saved).
    access_draft: Option<Access>,
    /// Quota per harness, as read.
    pub(super) quotas: BTreeMap<String, Fallible<HarnessQuotaResponse>>,
    quotas_loading: usize,
    /// Usage's exact totals; `None` until read.
    fleet: Option<Fallible<config_client::FleetTotals>>,
    fleet_loading: bool,
    /// Worktrees' managed rows; `None` until read.
    worktrees: Option<Fallible<ManagedWorktrees>>,
    worktrees_loading: bool,
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
            Page::Automations => one(Section::Automation, self.automation.clone()),
            Page::CloudMcp => one(Section::CloudMcp, self.cloud.clone()),
            Page::Access => {
                let access = self
                    .access_draft
                    .ok_or_else(|| "No change to save.".to_owned())?;
                let mut desktop = self.base(Section::Desktop);
                if let Some(map) = desktop.as_object_mut() {
                    map.insert("last_access".to_owned(), json!(access));
                }
                one(Section::Desktop, desktop)
            }
            Page::Advanced => {
                let mut sections: BTreeMap<Section, Value> = serde_json::from_str(&self.all)
                    .map_err(|_| "The JSON does not parse; the draft is kept.".to_owned())?;
                sections.retain(|section, value| self.base.get(section) != Some(value));
                if sections.is_empty() {
                    return Err("No section changed.".to_owned());
                }
                Ok(sections)
            }
            Page::Appearance | Page::Startup => Err("This page saves as you choose.".to_owned()),
            Page::Shortcuts | Page::Usage | Page::Worktrees => {
                Err("Nothing to save on this page.".to_owned())
            }
        }
    }

    /// The saved default access level.
    fn saved_access(&self) -> Option<Access> {
        self.base(Section::Desktop)
            .get("last_access")
            .cloned()
            .and_then(|value| serde_json::from_value(value).ok())
    }

    fn dirty(&self, page: Page) -> bool {
        match page {
            Page::Routing => {
                model::routing_for_save(&self.routing)
                    != model::routing_for_save(&self.base(Section::Routing))
            }
            Page::Projects => self.projects != self.base(Section::Projects),
            Page::Automations => self.automation != self.base(Section::Automation),
            Page::CloudMcp => self.cloud != self.base(Section::CloudMcp),
            Page::Advanced => self.all != self.all_base,
            Page::Access => self
                .access_draft
                .is_some_and(|draft| Some(draft) != self.saved_access()),
            Page::Appearance
            | Page::Startup
            | Page::Shortcuts
            | Page::Models
            | Page::Usage
            | Page::Worktrees => false,
        }
    }
}

impl DesktopApp {
    pub(super) fn settings_update(&mut self, msg: SettingsMsg, context: &ComponentContext<Self>) {
        let s = &mut self.settings;
        match msg {
            SettingsMsg::CleanupPlan => {
                if s.cleanup_busy {
                    return;
                }
                s.cleanup_busy = true;
                s.cleanup_plan = None;
                s.cleanup_error = None;
                s.cleanup_confirm = false;
                s.cleanup_result = None;
                let Ok(data) = self.data_dir.clone() else {
                    s.cleanup_busy = false;
                    s.cleanup_error = Some("Daemon data directory unavailable".to_owned());
                    return;
                };
                let _task = context.spawn_background(move |_| {
                    let request = eliot_core::api::CleanupPlanRequest {
                        scope: eliot_core::api::CleanupScope::All,
                        project: None,
                        older_than: None,
                    };
                    Message::Settings(SettingsMsg::CleanupPlanned(
                        client::block_on(eliot_daemon::client_call_typed(
                            &data,
                            "cleanup.plan",
                            &request,
                        ))
                        .map_err(|error| error.to_string()),
                    ))
                });
            }
            SettingsMsg::CleanupPlanned(result) => {
                s.cleanup_busy = false;
                match result {
                    Ok(plan) => {
                        s.cleanup_unchecked =
                            crate::worktrees::CleanupGroup::unchecked_by_default();
                        s.cleanup_kept_open.clear();
                        s.cleanup_plan = Some(plan);
                    }
                    Err(e) => s.cleanup_error = Some(e),
                }
            }
            SettingsMsg::CleanupKeptToggled(group) => {
                if !s.cleanup_kept_open.remove(&group) {
                    s.cleanup_kept_open.insert(group);
                }
            }
            SettingsMsg::CleanupGroupToggled(group, on) => {
                if s.cleanup_busy {
                    return;
                }
                s.cleanup_confirm = false;
                if on {
                    s.cleanup_unchecked.remove(&group);
                } else {
                    s.cleanup_unchecked.insert(group);
                }
            }
            SettingsMsg::CleanupApply => {
                let selected = s
                    .cleanup_plan
                    .as_ref()
                    .map(|plan| crate::worktrees::selected_artifacts(plan, &s.cleanup_unchecked))
                    .unwrap_or_default();
                if !s.cleanup_busy && !selected.is_empty() {
                    s.cleanup_confirm = true;
                }
            }
            SettingsMsg::CleanupConfirmed(false) => s.cleanup_confirm = false,
            SettingsMsg::CleanupConfirmed(true) => {
                s.cleanup_confirm = false;
                if s.cleanup_busy {
                    return;
                }
                let Some(plan) = s.cleanup_plan.clone() else {
                    return;
                };
                let selected = crate::worktrees::selected_artifacts(&plan, &s.cleanup_unchecked);
                if selected.is_empty() {
                    return;
                }
                let Ok(data) = self.data_dir.clone() else {
                    s.cleanup_error = Some("Daemon data directory unavailable".to_owned());
                    return;
                };
                // One plan and selection keep one mutation id until a
                // definite reply; an uncertain one retries under the same id.
                let key = format!("cleanup.apply:{}:{}", plan.plan_id, selected.join(","));
                let id = self.ledger.id_for(&key);
                let s = &mut self.settings;
                s.cleanup_key = Some(key);
                s.cleanup_busy = true;
                s.cleanup_error = None;
                let _task = context.spawn_background(move |_| {
                    let request = eliot_core::api::CleanupApplyRequest {
                        id,
                        plan_id: plan.plan_id,
                        // The desktop applies local-only deletions; remote
                        // branch deletion stays an explicit Owner CLI flag.
                        delete_remote: false,
                        artifacts: Some(selected),
                    };
                    Message::Settings(SettingsMsg::CleanupApplied(client::block_on(
                        eliot_daemon::client_call_typed(&data, "cleanup.apply", &request),
                    )))
                });
            }
            SettingsMsg::CleanupApplied(result) => {
                s.cleanup_busy = false;
                let uncertain = result.as_ref().err().is_some_and(crate::state::uncertain);
                let key = s.cleanup_key.take();
                match result {
                    Ok(reply) => {
                        if let Some(plan) = s.cleanup_plan.take() {
                            s.cleanup_result = Some((plan, reply));
                        }
                    }
                    Err(error) if uncertain => {
                        s.cleanup_key = key.clone();
                        s.cleanup_error = Some(format!(
                            "Cleanup not confirmed; Remove again retries the same request: {}",
                            error.user_message()
                        ));
                    }
                    Err(error) => {
                        s.cleanup_error = Some(format!("Cleanup refused: {}", error.user_message()))
                    }
                }
                if !uncertain && let Some(key) = key {
                    self.ledger.complete(&key);
                }
                // The worktree list follows what was removed.
                self.settings_update(SettingsMsg::WorktreesRefresh, context);
            }
            SettingsMsg::PageSelected(page) => {
                // A search result opens its page; the groups come back.
                self.settings.query.clear();
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
                    // The new default applies to the next new task now.
                    if page == Page::Access
                        && let Some(access) = self.settings.access_draft.take()
                    {
                        self.prefs.last_access = Some(access);
                        self.launch.access = Some(access);
                    }
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
                    Page::Automations => s.automation = s.base(Section::Automation),
                    Page::CloudMcp => s.cloud = s.base(Section::CloudMcp),
                    Page::Advanced => s.all = s.all_base.clone(),
                    Page::Access => s.access_draft = None,
                    Page::Appearance
                    | Page::Startup
                    | Page::Shortcuts
                    | Page::Usage
                    | Page::Worktrees => {}
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
                        s.notes.insert(Page::Automations, error);
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
            SettingsMsg::AccessDraft(index) => {
                if let Some(access) = index.and_then(|index| Access::ALL.get(index)) {
                    s.access_draft = Some(*access);
                }
            }
            SettingsMsg::OpenConfig => {
                if let Ok(data_dir) = self.data_dir.clone() {
                    let config = data_dir.join("config.toml");
                    open_with_shell(&config.to_string_lossy());
                }
            }
            SettingsMsg::OpenFolder(path) => open_with_shell(&path),
            SettingsMsg::UsageRefresh => {
                self.load_quotas(context);
                self.load_fleet(context);
            }
            SettingsMsg::QuotaLoaded(harness, reply) => {
                self.settings.quotas_loading = self.settings.quotas_loading.saturating_sub(1);
                self.settings.quotas.insert(harness, *reply);
            }
            SettingsMsg::WorktreesRefresh => self.load_worktrees(context),
            SettingsMsg::FleetLoaded(reply) => {
                self.settings.fleet_loading = false;
                self.settings.fleet = Some(*reply);
            }
            SettingsMsg::WorktreesLoaded(reply) => {
                self.settings.worktrees_loading = false;
                self.settings.worktrees = Some(*reply);
            }
        }
    }

    /// Read Usage's exact fleet totals and the retention view.
    pub(super) fn load_fleet(&mut self, context: &ComponentContext<Self>) {
        let Ok(data_dir) = self.data_dir.clone() else {
            return;
        };
        if self.settings.fleet_loading {
            return;
        }
        self.settings.fleet_loading = true;
        let _task = context.spawn_background(move |_| {
            let reply = client::block_on(config_client::fleet_totals(&data_dir))
                .map_err(|error| error.to_string());
            Message::Settings(SettingsMsg::FleetLoaded(Box::new(reply)))
        });
    }

    /// Read every managed worktree from the daemon.
    pub(super) fn load_worktrees(&mut self, context: &ComponentContext<Self>) {
        let Ok(data_dir) = self.data_dir.clone() else {
            return;
        };
        if self.settings.worktrees_loading {
            return;
        }
        self.settings.worktrees_loading = true;
        let _task = context.spawn_background(move |_| {
            let reply = client::block_on(config_client::managed_workspaces(&data_dir))
                .map_err(|error| error.to_string());
            Message::Settings(SettingsMsg::WorktreesLoaded(Box::new(reply)))
        });
    }

    /// Read the quota of every installed harness (one worker each) and the
    /// catalogue's harness list when it is not read yet.
    pub(super) fn load_quotas(&mut self, context: &ComponentContext<Self>) {
        let Ok(data_dir) = self.data_dir.clone() else {
            return;
        };
        if self.settings.quotas_loading > 0 {
            return;
        }
        if self.launch.catalog.is_none() {
            self.load_catalog(context);
        }
        for harness in crate::picker::HARNESSES {
            self.settings.quotas_loading += 1;
            let data_dir = data_dir.clone();
            let _task = context.spawn_background(move |_| {
                let reply = client::block_on(config_client::harness_quota(&data_dir, harness))
                    .map_err(|error| error.to_string());
                Message::Settings(SettingsMsg::QuotaLoaded(
                    harness.to_owned(),
                    Box::new(reply),
                ))
            });
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
                client::block_on(config_client::save_settings(&data_dir, &request)),
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
            Page::Automations => {
                self.load_rules(context);
                if !self.scripts.listed {
                    self.load_scripts(context);
                }
            }
            Page::Usage => {
                self.load_quotas(context);
                self.load_fleet(context);
            }
            Page::Worktrees => self.load_worktrees(context),
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

    /// The Settings navigation's rows (the sidebar's place, see
    /// `settings_nav`): the pages in their groups, or, while searching,
    /// the settings that match with the page each is on.
    pub(super) fn settings_rows(
        &self,
        context: &mut ViewContext<Self>,
        palette: Palette,
    ) -> Vec<KeyedView> {
        let mut rows = Vec::new();
        if !self.settings.query.trim().is_empty() {
            let found = search_settings(&self.settings.query);
            rows.push(KeyedView::new("found", nav_label("Results")));
            if found.is_empty() {
                rows.push(KeyedView::new(
                    "none",
                    m3::secondary("No setting matches. Try another word.")
                        .text_wrapping(TextWrapping::Wrap)
                        .margin(Thickness::new(tokens::spacing::INSET, 0.0, 0.0, 0.0)),
                ));
            }
            for (page, name) in found {
                rows.push(KeyedView::new(
                    format!("found:{name}"),
                    m3::row(palette, false)
                        .automation_name(format!("{name}, in {}", page.label()))
                        .on_click(
                            context.message(Message::Settings(SettingsMsg::PageSelected(page))),
                        )
                        .content(m3::row_content(
                            Some(page.glyph()),
                            name,
                            // The page, unless the setting carries its name.
                            if name == page.label() {
                                ""
                            } else {
                                page.label()
                            },
                            View::empty(),
                        )),
                ));
            }
            return rows;
        }
        for (group, pages) in GROUPS {
            rows.push(KeyedView::new(format!("group:{group}"), nav_label(group)));
            for page in pages {
                let selected = *page == self.settings_page;
                rows.push(KeyedView::new(
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
                        .content(m3::icon_label(page.glyph(), page.label())),
                ));
            }
        }
        rows
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
                Page::Appearance => self.appearance_page(context, palette),
                Page::Startup => self.startup_page(context, palette),
                Page::Shortcuts => shortcuts_page(palette),
                Page::Models => self.models_section(context, palette),
                Page::Access => self.access_page(context, palette),
                Page::Routing => self.routing_section(context, palette),
                Page::Usage => self.usage_page(context, palette),
                Page::Projects => self.projects_section(context, palette),
                Page::Automations => self.automation_section(context, palette),
                Page::Worktrees => self.worktrees_page(context, palette),
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
                            m3::page_header(
                                page.label(),
                                page.subtitle(),
                                page.has_config().then(|| {
                                    m3::text_button(palette)
                                        .automation_name("Open config.toml")
                                        .on_click(
                                            context.message(Message::Settings(
                                                SettingsMsg::OpenConfig,
                                            )),
                                        )
                                        .content(m3::link_content("Open config.toml"))
                                }),
                            ),
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
            } else if page == Page::Advanced {
                // Revisions belong to Advanced only.
                format!(
                    "Saved settings, revision {}.",
                    self.settings
                        .revision
                        .as_deref()
                        .map_or("none yet", |revision| &revision[..revision.len().min(12)])
                )
            } else {
                "All changes saved.".to_owned()
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

    fn appearance_page(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
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
        StackPanel::new().spacing(tokens::spacing::SM).children((
            m3::settings_card(
                palette,
                vec![KeyedView::new(
                    "theme",
                    m3::setting_row(
                        "Theme",
                        "System follows the Windows setting.",
                        m3::segmented(palette, segments),
                    ),
                )],
            ),
            self.page_note(Page::Appearance, "Changes save as you choose them."),
        ))
    }

    fn startup_page(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        let reopen = self.prefs.startup == Startup::LastConversation;
        // A check box rather than a switch: its shape stays on the scale (a
        // switch is a pill with a round thumb).
        let check = Grid::new().keyed_children(vec![KeyedView::new(
            format!("reopen:{reopen}"),
            CheckBox::new()
                .is_checked(reopen)
                .automation_name("Reopen the last task at start")
                .on_is_checked_changed(context.callback(|on: bool| {
                    Message::Shell(ShellMsg::StartupChosen(if on {
                        Startup::LastConversation
                    } else {
                        Startup::NewTask
                    }))
                })),
        )]);
        StackPanel::new().spacing(tokens::spacing::SM).children((
            m3::settings_card(
                palette,
                vec![KeyedView::new(
                    "reopen",
                    m3::setting_row(
                        "Reopen the last task",
                        "At start, open the most recently active task instead of a new one.",
                        check,
                    ),
                )],
            ),
            self.page_note(Page::Startup, "Changes save as you choose them."),
        ))
    }

    /// The page's last result, or `idle` when nothing happened yet.
    fn page_note(&self, page: Page, idle: &str) -> View {
        m3::secondary(
            self.settings
                .notes
                .get(&page)
                .cloned()
                .unwrap_or_else(|| idle.to_owned()),
        )
        .text_wrapping(TextWrapping::Wrap)
        .into()
    }

    /// Default access: the level a new task starts with (the composer's
    /// access chip and `[desktop] last_access`), saved with this page.
    fn access_page(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        let shown = self
            .settings
            .access_draft
            .or_else(|| self.settings.saved_access())
            .unwrap_or(Access::Ask);
        let index = Access::ALL.iter().position(|access| *access == shown);
        let labels: Vec<String> = Access::ALL
            .iter()
            .map(|access| access.label().to_owned())
            .collect();
        let busy = self.settings.busy();
        let picker = Grid::new().keyed_children(vec![KeyedView::new(
            format!("access:{}", shown.wire()),
            m3::enum_width(ComboBox::new())
                .items_source(labels)
                .selected_index(index)
                .is_enabled(!busy)
                .automation_name("Default access for new tasks")
                .on_selection_changed(
                    context.callback(|index| Message::Settings(SettingsMsg::AccessDraft(index))),
                ),
        )]);
        let levels: Vec<KeyedView> = Access::ALL
            .iter()
            .map(|access| {
                let title = m3::body(access.label());
                let title = if *access == Access::Full {
                    title.foreground(palette.warning())
                } else {
                    title
                };
                KeyedView::new(
                    access.wire(),
                    Grid::new()
                        .min_height(tokens::height::FORM_ROW)
                        .margin(Thickness::xy(0.0, tokens::spacing::SM))
                        .columns([
                            GridLength::Pixel(tokens::layout::LABEL_WIDTH),
                            GridLength::STAR,
                        ])
                        .column_spacing(tokens::spacing::MD)
                        .children((
                            title.vertical_alignment(VerticalAlignment::Center),
                            m3::secondary(access.description())
                                .grid_column(1)
                                .vertical_alignment(VerticalAlignment::Center)
                                .text_wrapping(TextWrapping::Wrap),
                        )),
                )
            })
            .collect();
        let dirty = self.settings.dirty(Page::Access);
        StackPanel::new().spacing(tokens::spacing::MD).children((
            m3::settings_card(
                palette,
                vec![KeyedView::new(
                    "default",
                    m3::setting_row(
                        "Default access for new tasks",
                        "The composer starts with this level; choosing another there also changes it.",
                        picker,
                    ),
                )],
            ),
            StackPanel::new().spacing(tokens::spacing::SM).children((
                m3::card_title("What each level allows"),
                m3::settings_card(palette, levels),
                m3::secondary(
                    "Presets and Auto keep their own permissions. OpenCode has no Full access mode.",
                )
                .text_wrapping(TextWrapping::Wrap),
            )),
            self.save_bar(Page::Access, dirty, context, palette),
        ))
    }

    /// Models & Usage (Antigravity's page as the pattern): each installed
    /// harness's remaining provider quota as rings (weekly and 5-hour where
    /// the provider reports them), and what Swarm is running.
    fn usage_page(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        let today = today_start_ms();
        let loaded = self.nav.agents.len();
        let (tokens_today, reporting) = self
            .nav
            .agents
            .iter()
            .filter(|agent| agent.updated_at >= today)
            .fold((0i64, 0usize), |(sum, count), agent| match agent.tokens {
                Some(tokens) => (sum + tokens, count + 1),
                None => (sum, count),
            });
        let fleet = self.settings.fleet.as_ref();
        let total = fleet.and_then(|fleet| fleet.as_ref().ok().map(|fleet| fleet.agents));
        let (running_value, running_note) = match fleet {
            None => ("…".to_owned(), "Counting…".to_owned()),
            Some(Ok(fleet)) => (
                fleet.running.to_string(),
                format!(
                    "Working now or starting, of {} Agent(s) in Swarm (exact counts).",
                    fleet.agents
                ),
            ),
            Some(Err(error)) => (
                "not counted".to_owned(),
                format!("Could not count: {}", crate::state::plain_error(error)),
            ),
        };
        let coverage = match total {
            Some(total) if total > loaded => format!(
                "Partial: from the {loaded} most recent Agents loaded here, of {total} in Swarm; {reporting} active today report tokens (their totals; Swarm has no per-day usage read)."
            ),
            _ => format!(
                "From all {loaded} Agent(s); {reporting} active today report tokens (their totals; Swarm has no per-day usage read)."
            ),
        };
        let (retention_value, retention_note) = match fleet {
            None => ("…".to_owned(), "Reading…".to_owned()),
            Some(Err(_)) => (
                "not read".to_owned(),
                "daemon.status could not be read.".to_owned(),
            ),
            Some(Ok(fleet)) => retention_lines(fleet.retention.as_ref()),
        };
        let totals = m3::settings_card(
            palette,
            vec![
                KeyedView::new(
                    format!("running:{running_value}:{running_note}"),
                    m3::setting_row(
                        "Running Agents",
                        &running_note,
                        m3::body(running_value).into(),
                    ),
                ),
                KeyedView::new(
                    format!("tokens:{tokens_today}:{coverage}"),
                    m3::setting_row(
                        "Tokens today",
                        &coverage,
                        m3::body(group_digits(tokens_today)).into(),
                    ),
                ),
                KeyedView::new(
                    format!("retention:{retention_value}:{retention_note}"),
                    m3::setting_row(
                        "History retention",
                        &retention_note,
                        m3::body(retention_value).into(),
                    ),
                ),
            ],
        );
        let installed: Vec<(String, Option<String>)> = match &self.launch.catalog {
            Some(Ok(reply)) => reply
                .harnesses
                .iter()
                .filter(|entry| entry.installed)
                .map(|entry| (entry.harness.clone(), entry.version.clone()))
                .collect(),
            _ => crate::picker::HARNESSES
                .iter()
                .map(|harness| ((*harness).to_owned(), None))
                .collect(),
        };
        let mut cards: Vec<KeyedView> = Vec::new();
        for (harness, version) in installed {
            let label = crate::picker::harness_label(&harness);
            let provider =
                crate::picker::provider_label(&eliot_core::api::catalog_provider(&harness, ""));
            let title = if provider.eq_ignore_ascii_case(label) || provider.is_empty() {
                label.to_owned()
            } else {
                format!("{label} · {provider}")
            };
            let rows: Vec<KeyedView> = match self.settings.quotas.get(&harness) {
                None => vec![KeyedView::new(
                    "reading",
                    m3::setting_row("Quota", "Reading…", View::empty()),
                )],
                Some(Err(error)) => vec![KeyedView::new(
                    "error",
                    m3::setting_row(
                        &format!("{label}: quota not read"),
                        &crate::state::plain_error(error),
                        View::empty(),
                    ),
                )],
                Some(Ok(reply)) => {
                    let windows = crate::quota::windows(reply);
                    if windows.is_empty() {
                        vec![KeyedView::new(
                            "status",
                            m3::setting_row(
                                &crate::quota::status_line(label, reply),
                                "",
                                View::empty(),
                            ),
                        )]
                    } else {
                        let now = crate::handoff::now_ms();
                        let observed = crate::quota::observed_note(reply);
                        windows
                            .iter()
                            .enumerate()
                            .map(|(index, window)| {
                                let mut line = window.line(now);
                                if let Some(observed) = &observed {
                                    line = format!("{line} · {observed}");
                                }
                                KeyedView::new(
                                    format!("{index}:{line}"),
                                    m3::setting_row(&line, "", m3::ring(window.remaining as f64)),
                                )
                            })
                            .collect()
                    }
                }
            };
            cards.push(KeyedView::new(
                format!("harness:{harness}"),
                StackPanel::new().spacing(tokens::spacing::SM).children((
                    StackPanel::new()
                        .orientation(Orientation::Horizontal)
                        .spacing(tokens::spacing::SM)
                        .margin(Thickness::new(0.0, tokens::spacing::SM, 0.0, 0.0))
                        .children((
                            m3::section_title(title),
                            m3::secondary(version.unwrap_or_default())
                                .vertical_alignment(VerticalAlignment::Center),
                        )),
                    m3::settings_card(palette, rows),
                )),
            ));
        }
        StackPanel::new().spacing(tokens::spacing::MD).children((
            StackPanel::new().spacing(tokens::spacing::SM).children((
                m3::section_heading(
                    "Swarm",
                    "",
                    Some(
                        m3::text_button(palette)
                            .is_enabled(
                                self.settings.quotas_loading == 0 && !self.settings.fleet_loading,
                            )
                            .automation_name("Read quota again")
                            .on_click(context.message(Message::Settings(SettingsMsg::UsageRefresh)))
                            .content(m3::label("Refresh")),
                    ),
                ),
                totals,
            )),
            StackPanel::new()
                .spacing(tokens::spacing::SM)
                .keyed_children(cards),
        ))
    }

    /// The cleanup review under its row: the plan by kind (a checkbox per
    /// group that has something to remove, each item with its size and its
    /// keep or remove reason), Remove with one inline confirmation, then the
    /// result: removed, kept with why, and unknown.
    fn cleanup_review(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        let state = &self.settings;
        // A task title when Swarm knows one (else the item keeps its id).
        let title = |agent: &str| {
            let title = self.agent_title(agent);
            (title != agent).then_some(title)
        };
        let mut parts: Vec<KeyedView> = Vec::new();
        if state.cleanup_busy {
            parts.push(KeyedView::new(
                "busy",
                m3::secondary(if state.cleanup_plan.is_some() {
                    "Removing the selected items…"
                } else {
                    "Reviewing what can be removed…"
                }),
            ));
        }
        if let Some(error) = &state.cleanup_error {
            parts.push(KeyedView::new(
                format!("error:{error}"),
                m3::body(error.clone())
                    .foreground(palette.error())
                    .text_wrapping(TextWrapping::Wrap)
                    .is_text_selection_enabled(true),
            ));
        }
        if let Some((plan, reply)) = &state.cleanup_result {
            let lines = crate::worktrees::result_lines(plan, reply, &title);
            let rows: Vec<KeyedView> = lines
                .iter()
                .enumerate()
                .map(|(index, line)| {
                    let text = if index == 0 {
                        m3::text(line.clone(), tokens::text::SECTION_TITLE)
                    } else {
                        m3::body(line.clone())
                    };
                    KeyedView::new(
                        format!("result:{index}"),
                        text.text_wrapping(TextWrapping::Wrap)
                            .is_text_selection_enabled(true),
                    )
                })
                .collect();
            parts.push(KeyedView::new(
                "result",
                m3::card_high(palette)
                    .automation_name("Cleanup result")
                    .content(
                        StackPanel::new()
                            .spacing(tokens::spacing::XS)
                            .keyed_children(rows),
                    ),
            ));
        }
        if let Some(plan) = &state.cleanup_plan {
            let groups = crate::worktrees::cleanup_groups(plan);
            if groups.is_empty() {
                parts.push(KeyedView::new(
                    "empty",
                    m3::secondary("Nothing to clean up: Swarm holds no finished work.")
                        .text_wrapping(TextWrapping::Wrap),
                ));
            }
            for view in &groups {
                let group = view.group;
                let checked = !state.cleanup_unchecked.contains(&group);
                let kept = view.items.len() - view.removable;
                let size = if crate::worktrees::sized(group) {
                    format!(" · {}", crate::worktrees::size_words(view.bytes))
                } else {
                    String::new()
                };
                let summary = match (view.removable, kept) {
                    (0, kept) => format!("{kept} kept"),
                    (removable, 0) => format!("{removable} to remove{size}"),
                    (removable, kept) => format!("{removable} to remove{size} · {kept} kept"),
                };
                let header: View = if view.removable > 0 {
                    CheckBox::new()
                        .is_checked(checked)
                        .is_enabled(!state.cleanup_busy)
                        .automation_name(format!("Remove {}: {summary}", group.title()))
                        .on_is_checked_changed(context.callback(move |on: bool| {
                            Message::Settings(SettingsMsg::CleanupGroupToggled(group, on))
                        }))
                        .content(m3::text(
                            format!("{} · {summary}", group.title()),
                            tokens::text::SECTION_TITLE,
                        ))
                } else {
                    m3::text(
                        format!("{} · {summary}", group.title()),
                        tokens::text::SECTION_TITLE,
                    )
                    .into()
                };
                let mut rows: Vec<KeyedView> =
                    vec![KeyedView::new(format!("header:{checked}"), header)];
                let item_row = |item: &eliot_core::api::CleanupCandidate| {
                    let (name, line) =
                        crate::worktrees::item_line(item, title(&item.artifact.agent).as_deref());
                    KeyedView::new(
                        item.artifact.id.clone(),
                        StackPanel::new().children((
                            m3::body(name).text_trimming(TextTrimming::CharacterEllipsis),
                            m3::secondary(line)
                                .text_wrapping(TextWrapping::Wrap)
                                .is_text_selection_enabled(true),
                        )),
                    )
                };
                // What Remove takes comes first; the kept items fold under
                // one "N kept" row.
                let (removable, kept_items) = view.split();
                rows.extend(removable.iter().map(|item| item_row(item)));
                if !kept_items.is_empty() {
                    let open = state.cleanup_kept_open.contains(&group);
                    let label = format!("{} kept", kept_items.len());
                    rows.push(KeyedView::new(
                        format!("kept:{open}"),
                        m3::sub_row(palette, false)
                            .automation_name(format!(
                                "{label} in {} ({})",
                                group.title(),
                                if open { "shown" } else { "hidden" }
                            ))
                            .on_click(
                                context.message(Message::Settings(
                                    SettingsMsg::CleanupKeptToggled(group),
                                )),
                            )
                            .content(m3::row_content(
                                Some(if open {
                                    m3::glyph::CHEVRON_DOWN
                                } else {
                                    m3::glyph::CHEVRON_RIGHT
                                }),
                                &label,
                                "",
                                View::empty(),
                            )),
                    ));
                    if open {
                        rows.extend(kept_items.iter().map(|item| item_row(item)));
                    }
                }
                parts.push(KeyedView::new(
                    format!("group:{group:?}"),
                    m3::card_high(palette).content(
                        StackPanel::new()
                            .spacing(tokens::spacing::SM)
                            .keyed_children(rows),
                    ),
                ));
            }
            if plan.truncated {
                parts.push(KeyedView::new(
                    "truncated",
                    m3::secondary(
                        "The review stopped at its bound; review again after this cleanup for the rest.",
                    )
                    .text_wrapping(TextWrapping::Wrap),
                ));
            }
            let selected = crate::worktrees::selected_artifacts(plan, &state.cleanup_unchecked);
            let bytes: u64 = plan
                .items
                .iter()
                .filter(|item| selected.contains(&item.artifact.id))
                .filter_map(|item| item.bytes)
                .sum();
            let what = format!(
                "{} item(s), {}",
                selected.len(),
                crate::worktrees::size_words(bytes)
            );
            if state.cleanup_confirm {
                parts.push(KeyedView::new(
                    "confirm",
                    m3::card_high(palette)
                        .automation_name("Confirm cleanup")
                        .content(
                            StackPanel::new().spacing(tokens::spacing::SM).children((
                                m3::body(format!(
                                    "Remove {what}? This cannot be undone. Kept items stay; Swarm checks each item again before removing it."
                                ))
                                .text_wrapping(TextWrapping::Wrap),
                                StackPanel::new()
                                    .orientation(Orientation::Horizontal)
                                    .spacing(tokens::spacing::SM)
                                    .children((
                                        m3::destructive(palette)
                                            .automation_name(format!("Remove: {what}"))
                                            .on_click(context.message(Message::Settings(
                                                SettingsMsg::CleanupConfirmed(true),
                                            )))
                                            .content(m3::label("Remove")),
                                        m3::text_button(palette)
                                            .automation_name("Cancel cleanup")
                                            .on_click(context.message(Message::Settings(
                                                SettingsMsg::CleanupConfirmed(false),
                                            )))
                                            .content(m3::label("Cancel")),
                                    )),
                            )),
                        ),
                ));
            } else if !groups.is_empty() {
                parts.push(KeyedView::new(
                    "apply",
                    m3::destructive(palette)
                        .automation_name(format!("Remove selected: {what}"))
                        .is_enabled(!state.cleanup_busy && !selected.is_empty())
                        .on_click(context.message(Message::Settings(SettingsMsg::CleanupApply)))
                        .content(m3::label(&if selected.is_empty() {
                            "Nothing selected".to_owned()
                        } else {
                            format!("Remove {what}")
                        })),
                ));
            }
        }
        StackPanel::new()
            .spacing(tokens::spacing::SM)
            .keyed_children(parts)
    }

    /// Worktrees & cleanup (Codex's Worktrees page as the pattern): where
    /// Swarm creates worktrees, Clean up finished work (`cleanup.plan`
    /// reviewed by kind, then `cleanup.apply` of the checked groups after
    /// one confirmation), and the worktrees it manages, by project (the
    /// daemon's `workspace.list`, every page).
    fn worktrees_page(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        let root = self
            .data_dir
            .as_ref()
            .map(|dir| dir.join("worktrees").to_string_lossy().into_owned())
            .unwrap_or_default();
        let settings = m3::settings_card(
            palette,
            vec![
                KeyedView::new(
                    "root",
                    m3::setting_row(
                        "Worktree root",
                        nav::display_path(&root),
                        m3::outlined(palette)
                            .automation_name("Open the worktree root")
                            .is_enabled(!root.is_empty())
                            .on_click(
                                context.message(Message::Settings(SettingsMsg::OpenFolder(
                                    root.clone(),
                                ))),
                            )
                            .content(m3::icon_label(m3::glyph::OPEN_FOLDER, "Open")),
                    ),
                ),
                KeyedView::new(
                    "cleanup",
                    m3::setting_row(
                        "Clean up finished work",
                        "Review what Swarm can remove for finished tasks, by kind, with sizes and reasons. Nothing is removed until you confirm.",
                        m3::outlined(palette)
                            .automation_name("Review cleanup")
                            .is_enabled(!self.settings.cleanup_busy)
                            .on_click(context.message(Message::Settings(SettingsMsg::CleanupPlan)))
                            .content(m3::label(if self.settings.cleanup_plan.is_some() {
                                "Review again"
                            } else {
                                "Review…"
                            })),
                    ),
                ),
            ],
        );
        let cleanup = self.cleanup_review(context, palette);
        let mut groups: Vec<KeyedView> = Vec::new();
        let empty: &[eliot_core::api::ManagedWorkspaceView] = &[];
        let (rows, total, complete) = match &self.settings.worktrees {
            Some(Ok((rows, total, complete))) => (&rows[..], *total, *complete),
            Some(Err(error)) => {
                groups.push(KeyedView::new(
                    format!("error:{error}"),
                    m3::body(format!(
                        "Managed worktrees could not be read: {}",
                        crate::state::plain_error(error)
                    ))
                    .foreground(palette.error())
                    .text_wrapping(TextWrapping::Wrap)
                    .is_text_selection_enabled(true),
                ));
                (empty, 0, true)
            }
            None => (empty, 0, true),
        };
        let managed = crate::worktrees::grouped(rows, &self.registered_projects());
        let heading = match &self.settings.worktrees {
            None => "Reading…".to_owned(),
            Some(Err(_)) => String::new(),
            Some(Ok(_)) if !complete => format!(
                "{} of {total} shown (the read stopped at its page bound)",
                rows.len()
            ),
            Some(Ok(_)) => format!("{total} managed worktree(s)"),
        };
        if matches!(self.settings.worktrees, Some(Ok(_))) && managed.is_empty() {
            groups.push(KeyedView::new(
                "none",
                m3::secondary(
                    "No managed worktrees yet. A task started with swarm launch --worktree BRANCH gets one here.",
                )
                .text_wrapping(TextWrapping::Wrap),
            ));
        }
        for (project, worktrees) in managed {
            let rows: Vec<KeyedView> = worktrees
                .iter()
                .map(|worktree| {
                    let name = std::path::Path::new(&worktree.path)
                        .file_name()
                        .map_or_else(
                            || worktree.path.clone(),
                            |name| name.to_string_lossy().into_owned(),
                        );
                    let detail = format!(
                        "{} · {} · {} · {}",
                        worktree.branch, worktree.agent, worktree.phase, worktree.path
                    );
                    KeyedView::new(
                        format!("{}:{detail}", worktree.path),
                        m3::setting_row(
                            &name,
                            &detail,
                            m3::icon_button(palette)
                                .automation_name(format!("Open {name} in File Explorer"))
                                .on_click(context.message(Message::Settings(
                                    SettingsMsg::OpenFolder(worktree.path.clone()),
                                )))
                                .content(m3::icon(m3::glyph::OPEN_FOLDER)),
                        ),
                    )
                })
                .collect();
            groups.push(KeyedView::new(
                format!("project:{project}"),
                StackPanel::new().spacing(tokens::spacing::SM).children((
                    m3::section_heading(
                        &project,
                        &format!("{} worktree(s)", worktrees.len()),
                        None,
                    ),
                    m3::settings_card(palette, rows),
                )),
            ));
        }
        StackPanel::new().spacing(tokens::spacing::MD).children((
            StackPanel::new().spacing(tokens::spacing::SM).children((
                m3::card_title("Creation and cleanup"),
                settings,
                cleanup,
            )),
            m3::section_heading(
                "Managed worktrees",
                &heading,
                Some(
                    m3::text_button(palette)
                        .is_enabled(!self.settings.worktrees_loading)
                        .automation_name("Read managed worktrees again")
                        .on_click(context.message(Message::Settings(SettingsMsg::WorktreesRefresh)))
                        .content(m3::label("Refresh")),
                ),
            ),
            StackPanel::new()
                .spacing(tokens::spacing::MD)
                .keyed_children(groups),
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
            m3::card_title("Scripts"),
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
        let dirty = self.settings.dirty(Page::Automations);
        groups.push(KeyedView::new(
            "save",
            self.save_bar(Page::Automations, dirty, context, palette),
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

/// Keyboard shortcuts: read-only (the window's keyboard hook owns them).
fn shortcuts_page(palette: Palette) -> View {
    let rows: Vec<KeyedView> = [
        ("New task", "Ctrl+N"),
        ("Search tasks", "Ctrl+K"),
        ("Choose the model", "Ctrl+M"),
        ("Settings", "Ctrl+,"),
        ("Model picker: move", "↑ ↓"),
        ("Model picker: effort", "← →"),
        ("Model picker: favorite", "Ctrl+F"),
        ("Choose / close a menu", "Enter / Esc"),
    ]
    .into_iter()
    .map(|(name, keys)| KeyedView::new(name, m3::setting_row(name, "", m3::badge(palette, keys))))
    .collect();
    m3::settings_card(palette, rows)
}

/// A settings navigation group label: secondary text at the rows' inset.
fn nav_label(text: &str) -> TextBlock {
    m3::section_label(text).margin(Thickness::new(
        tokens::spacing::INSET,
        tokens::spacing::MD,
        0.0,
        tokens::spacing::XS,
    ))
}

/// Open a file or folder the way File Explorer does (explicit argv; the
/// shell picks the default app). Nothing is waited for.
fn open_with_shell(path: &str) {
    if path.trim().is_empty() {
        return;
    }
    let _ = std::process::Command::new("explorer.exe").arg(path).spawn();
}

/// `1234567` as `1,234,567`.
fn group_digits(value: i64) -> String {
    let digits = value.unsigned_abs().to_string();
    let mut out = String::new();
    for (index, digit) in digits.chars().enumerate() {
        if index > 0 && (digits.len() - index).is_multiple_of(3) {
            out.push(',');
        }
        out.push(digit);
    }
    if value < 0 { format!("-{out}") } else { out }
}

/// The retention row: the configured windows, or why they are unknown.
/// An unreadable configuration is never shown as "unbounded".
fn retention_lines(view: Option<&eliot_core::api::RetentionView>) -> (String, String) {
    let Some(view) = view else {
        return (
            "not reported".to_owned(),
            "This daemon reports no retention view.".to_owned(),
        );
    };
    let store = format!(
        "Store {} (database and log).",
        crate::monitor::bytes_label(Some(view.db_bytes.saturating_add(view.wal_bytes)))
    );
    if let Some(error) = &view.config_error {
        return (
            "unknown".to_owned(),
            format!("config.toml could not be read ({error}); the windows are unknown. {store}"),
        );
    }
    if view.unbounded {
        let advisory = view
            .size_advisory
            .as_deref()
            .map(|advisory| format!(" {advisory}."))
            .unwrap_or_default();
        return (
            "unbounded".to_owned(),
            format!("No daemon.retention window is set: nothing is pruned. {store}{advisory}"),
        );
    }
    let window =
        |days: Option<i64>| days.map_or_else(|| "kept".to_owned(), |days| format!("{days} d"));
    (
        "bounded".to_owned(),
        format!(
            "Events {}, commands {}, mail {}. {store}",
            window(view.events_days),
            window(view.commands_days),
            window(view.mail_days)
        ),
    )
}

/// The start of the local day, in ms since the epoch.
fn today_start_ms() -> i64 {
    use chrono::{Local, TimeZone};
    Local::now()
        .date_naive()
        .and_hms_opt(0, 0, 0)
        .and_then(|midnight| Local.from_local_datetime(&midnight).earliest())
        .map_or(0, |start| start.timestamp_millis())
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

    #[test]
    fn retention_row_never_shows_unbounded_for_an_unreadable_config() {
        let view = eliot_core::api::RetentionView {
            events_days: None,
            commands_days: None,
            mail_days: None,
            pruned_through: 0,
            db_bytes: 1024,
            wal_bytes: 0,
            unbounded: false,
            size_advisory: None,
            config_error: Some("profile_error: config.toml: bad".to_owned()),
        };
        let (value, note) = retention_lines(Some(&view));
        assert_eq!(value, "unknown");
        assert!(note.contains("config.toml could not be read (profile_error: config.toml: bad)"));
        assert!(!note.contains("nothing is pruned"));
        let unset = eliot_core::api::RetentionView {
            unbounded: true,
            config_error: None,
            ..view.clone()
        };
        assert_eq!(retention_lines(Some(&unset)).0, "unbounded");
        let bounded = eliot_core::api::RetentionView {
            events_days: Some(30),
            config_error: None,
            ..view
        };
        let (value, note) = retention_lines(Some(&bounded));
        assert_eq!(value, "bounded");
        assert!(note.starts_with("Events 30 d, commands kept, mail kept."));
    }

    fn read_with(revision: &str, routing: Value, desktop: Value) -> SettingsReadResponse {
        SettingsReadResponse {
            revision: Some(revision.to_owned()),
            sections: BTreeMap::from([(Section::Routing, routing), (Section::Desktop, desktop)]),
            restart_required: Vec::new(),
            history: Vec::new(),
        }
    }

    #[test]
    fn settings_search_finds_rows_across_pages() {
        let found = search_settings("ACCESS");
        assert_eq!(found[0], (Page::Access, "Default access"));
        assert!(search_settings("").is_empty());
        assert!(
            search_settings("quota")
                .iter()
                .all(|(page, _)| *page == Page::Usage)
        );
        assert_eq!(group_digits(1_234_567), "1,234,567");
        assert_eq!(group_digits(12), "12");
        // Every page is in exactly one group.
        let pages: Vec<Page> = GROUPS
            .iter()
            .flat_map(|(_, pages)| pages.iter().copied())
            .collect();
        assert_eq!(pages.len(), 12);
    }

    #[test]
    fn default_access_saves_into_the_desktop_section() {
        let mut state = SettingsState::default();
        state.take(read_with(
            "rev1",
            json!({"default": []}),
            json!({"theme": "dark", "last_access": "ask"}),
        ));
        assert!(!state.dirty(Page::Access));
        state.access_draft = Some(Access::Full);
        assert!(state.dirty(Page::Access));
        let sections = state.request_sections(Page::Access, "").unwrap();
        assert_eq!(
            sections[&Section::Desktop],
            json!({"theme": "dark", "last_access": "full"})
        );
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
