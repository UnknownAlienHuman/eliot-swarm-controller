//! Settings, the Codex settings pages as the pattern (owner review
//! 2026-11-16, k128): the sidebar becomes the settings navigation with a
//! Search field and groups (General: General, Keyboard shortcuts; Agents:
//! Models & presets, Default access, Usage; Work: Projects, Automations,
//! Worktrees & cleanup; Integrations: Cloud MCP; Advanced: Routing and the
//! settings file with its history). Every page is one centred column: a
//! title, a one-line subtitle, then cards whose rows carry the name and
//! description on the left and the control on the right; destructive
//! actions are separate and red. Every section reads and writes the
//! daemon's unified settings (`settings.read` / `settings.save`, one store
//! with a revision and author history) through [`crate::config_client`].
//! A control change saves at once (a typed one when typing on its page
//! pauses), one revision-checked save at a time, with "Saved" or the error
//! in the page's status line; a stale or refused save follows the rule in
//! `docs/spec/configuration.md` (unified settings). Only the raw editors
//! (the presets' TOML and every section's JSON) keep Save, Validate and
//! Discard. Revisions and history stay in Advanced.

use std::collections::{BTreeMap, HashMap};

mod access;
mod advanced;
mod automations;
mod cloud_mcp;
mod folder;
mod general;
mod models;
mod projects;
mod routing;
mod shortcuts;
mod usage;
mod worktrees;

use super::*;
use crate::settings::{Access, Startup, ThemeChoice};
use crate::settings_model::{self as model, AssignmentField};
use crate::theme::winui::Palette;
use eliot_core::api::{
    HarnessQuotaResponse, SettingsClientSource, SettingsHistoryEntry, SettingsReadResponse,
    SettingsSaveRequest, SettingsSaveResponse, SettingsSection as Section,
};
use serde_json::{Value, json};
use shortcuts::shortcuts_page;

#[cfg(test)]
use usage::{group_digits, retention_lines};

/// The Settings pages.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(super) enum Page {
    /// Appearance and startup on one page.
    General,
    Shortcuts,
    Models,
    Access,
    Usage,
    Projects,
    Automations,
    Worktrees,
    CloudMcp,
    Routing,
    Advanced,
}

/// The navigation's groups and their pages, in order: the everyday pages
/// first, Routing and the settings file under Advanced.
const GROUPS: [(&str, &[Page]); 5] = [
    ("General", &[Page::General, Page::Shortcuts]),
    ("Agents", &[Page::Models, Page::Access, Page::Usage]),
    (
        "Work",
        &[Page::Projects, Page::Automations, Page::Worktrees],
    ),
    ("Integrations", &[Page::CloudMcp]),
    ("Advanced", &[Page::Routing, Page::Advanced]),
];

/// What the Search field finds: one entry per setting, `(page, name,
/// words it also answers to)`.
const INDEX: [(Page, &str, &str); 18] = [
    (
        Page::General,
        "Theme",
        "appearance dark light system color colour",
    ),
    (
        Page::General,
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
        "Automations",
        "assignment brief busy folder schedule rule timer pause resume run now",
    ),
    (
        Page::Automations,
        "New automation",
        "add create assignment queue briefs",
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
            Self::General => "General",
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
            Self::General => "How Swarm looks and what it opens with on this computer.",
            Self::Shortcuts => "Keys that work anywhere in the window.",
            Self::Models => {
                "Saved model and access combinations; a new task picks its model in the composer."
            }
            Self::Access => "What new tasks may do without asking you first.",
            Self::Routing => "How Auto chooses a preset when a task starts.",
            Self::Usage => "Remaining quota of each agent app, and what Swarm is running.",
            Self::Projects => "The checkouts tasks start in.",
            Self::Automations => "Work Swarm starts for you: briefs from a folder, on a schedule.",
            Self::Worktrees => "Worktrees Swarm created for tasks, by project.",
            Self::CloudMcp => "Reach Swarm's MCP endpoint from the cloud.",
            Self::Advanced => "Every section of config.toml as JSON, and who changed what.",
        }
    }

    fn glyph(self) -> &'static str {
        match self {
            Self::General => m3::glyph::SETTINGS,
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
    CleanupPlanned(Fallible<(eliot_core::api::CleanupPlan, HashMap<String, String>)>),
    /// Check (true) or uncheck one group of the plan.
    CleanupGroupToggled(crate::worktrees::CleanupGroup, bool),
    /// Check every group that has something safe to remove (opt-in
    /// groups included).
    CleanupSelectAll,
    /// Show or hide one group's kept items.
    CleanupKeptToggled(crate::worktrees::CleanupGroup),
    CleanupExpanded(crate::worktrees::CleanupGroup),
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
    /// A typed change rests: save the page if no later keystroke came on
    /// that page (the edit sequence it was scheduled at).
    AutoSave(Page, u64),
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
    /// Check (true) or uncheck one project for a bulk removal.
    ProjectSelected(String, bool),
    /// Check every project (true) or none.
    ProjectsSelectAll(bool),
    ProjectBrowse,
    /// Index into [`model::CLOUD_FIELDS`] and its text.
    CloudField(usize, String),
    /// Configure (true) or remove Cloud MCP.
    CloudEnabled(bool),
    /// The Advanced JSON draft of every section.
    AllDraft(String),
    HistoryToggled,
    /// Default access: the index picked in its list (saved at once).
    AccessChosen(Option<usize>),
    /// Open config.toml with its default app.
    OpenConfig,
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

/// How long typing must rest before a typed change saves.
const TYPING_PAUSE_MS: u64 = 800;

/// A change refused for an invalid value or a section changed elsewhere:
/// the form shows the stored values again.
const SHOWS_SAVED: &str = "The page shows the saved values.";

/// An edit never applied because another writer changed its section
/// meanwhile (the stale-revision rule in `docs/spec/configuration.md`).
const CHANGED_ELSEWHERE: &str = "Not saved: these settings were changed somewhere else meanwhile. The page shows the saved values.";

/// Whether a section as read is a value this desktop's own save sent (sure,
/// of unknown outcome, or still running), as the store keeps it. Having
/// sent the section is not enough: another writer may have saved over it
/// before the read, and that value is never this desktop's.
fn saved_here<'a>(
    section: Section,
    read: &Value,
    saves: impl IntoIterator<Item = &'a BTreeMap<Section, Value>>,
) -> bool {
    saves
        .into_iter()
        .filter_map(|sent| sent.get(&section))
        .any(|sent| eliot_daemon::stored_settings_section(section, sent).as_ref() == Some(read))
}

/// Whether a section's draft differs from a stored value as a save would
/// send it (Routing drops its empty roles).
fn differs(section: Section, draft: &Value, stored: &Value) -> bool {
    match section {
        Section::Routing => model::routing_for_save(draft) != model::routing_for_save(stored),
        _ => draft != stored,
    }
}

/// A page's last save result in words; an error is drawn in the error
/// colour and stays until the next change.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Note {
    pub(super) text: String,
    pub(super) error: bool,
}

impl Note {
    pub(super) fn info(text: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            error: false,
        }
    }

    pub(super) fn error(text: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            error: true,
        }
    }

    /// A save or validation result.
    pub(super) fn of(dry_run: bool, result: &MutationResult<SettingsSaveResponse>) -> Self {
        Self {
            text: model::save_note(dry_run, result),
            error: result.is_err(),
        }
    }
}

/// The read settings, the drafts of the page's sections and the last
/// save result of each section.
#[derive(Default)]
pub(super) struct SettingsState {
    /// The plan under review (nothing removed yet).
    cleanup_plan: Option<eliot_core::api::CleanupPlan>,
    cleanup_busy: bool,
    cleanup_titles: HashMap<String, String>,
    /// Why the last plan or apply read failed.
    cleanup_error: Option<String>,
    /// Unchecked groups (a fresh review starts with
    /// [`crate::worktrees::CleanupGroup::unchecked_by_default`]).
    cleanup_unchecked: std::collections::HashSet<crate::worktrees::CleanupGroup>,
    /// Groups whose kept items are shown (collapsed by default).
    cleanup_kept_open: std::collections::HashSet<crate::worktrees::CleanupGroup>,
    cleanup_open: std::collections::HashSet<crate::worktrees::CleanupGroup>,
    /// The one confirmation is shown.
    pub(super) cleanup_confirm: bool,
    /// The ledger key of the apply in flight or uncertain (same id on retry).
    cleanup_key: Option<String>,
    /// The applied plan and its result.
    cleanup_result: Option<(
        eliot_core::api::CleanupPlan,
        eliot_core::api::CleanupApplyResponse,
    )>,
    pub(super) loaded: bool,
    read_at_ms: Option<i64>,
    pub(super) reading: bool,
    /// A read was asked for while one ran (after a save): read again.
    reread: bool,
    /// [`DesktopApp::prefs_rev`] when the running read was issued.
    read_prefs_rev: u64,
    /// The section whose save is running (one save at a time).
    saving: Option<Page>,
    /// The running save's sections as sent.
    sending: Option<BTreeMap<Section, Value>>,
    /// Sections the last saves wrote, until a read shows them: a draft
    /// equal to one follows the read even when the daemon normalised it.
    sent: BTreeMap<Section, Value>,
    /// Sections a save of unknown outcome sent, as sent, until the next
    /// read: if it landed, that read shows this desktop's own value, not
    /// another writer's.
    unsure: BTreeMap<Section, Value>,
    /// Pages whose control changes wait for the running save or read.
    queued: Vec<Page>,
    /// Pages whose save was refused for a stale revision: the next read
    /// decides whether each goes again (see [`SettingsState::take`]).
    retry: Vec<Page>,
    /// Numbers every typed change.
    edit_seq: u64,
    /// Each page's latest typed change still waiting for its pause: typing
    /// on one page never cancels another page's save.
    typing: HashMap<Page, u64>,
    revision: Option<String>,
    base: BTreeMap<Section, Value>,
    restart_required: Vec<Section>,
    history: Vec<SettingsHistoryEntry>,
    error: Option<String>,
    pub(super) notes: HashMap<Page, Note>,
    routing: Value,
    projects: Value,
    automation: Value,
    cloud: Value,
    /// Advanced: every section as pretty JSON, and its read form.
    all: String,
    all_base: String,
    /// The read sections the JSON draft was made from: its edits are the
    /// sections that differ from them, and only those are sent.
    all_from: BTreeMap<Section, Value>,
    new_role: String,
    project_open: Option<String>,
    /// Projects checked for Remove selected (registration IDs).
    pub(super) project_selected: std::collections::BTreeSet<String>,
    history_open: bool,
    /// The navigation's search text.
    pub(super) query: String,
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

    /// A typed change on `page`: the sequence its pause carries.
    fn typed(&mut self, page: Page) -> u64 {
        self.edit_seq += 1;
        self.typing.insert(page, self.edit_seq);
        self.edit_seq
    }

    /// Whether the pause scheduled at `seq` is the page's latest typed
    /// change (then it saves).
    fn rested(&mut self, page: Page, seq: u64) -> bool {
        let latest = self.typing.get(&page) == Some(&seq);
        if latest {
            self.typing.remove(&page);
        }
        latest
    }

    /// A definite refusal (an invalid value): the stored revision did not
    /// move. A form shows the stored values again under the error; the
    /// settings file's JSON, a raw editor, keeps its draft for correction.
    fn refused(&mut self, page: Page, mut note: Note) {
        // A refused Cloud MCP draft stays for fixing: discarding it would
        // wipe a first setup back to the empty section.
        let keep = page == Page::CloudMcp;
        if page != Page::Advanced && !keep {
            self.discard(page);
            note.text = format!("{}. {SHOWS_SAVED}", note.text.trim_end_matches('.'));
        }
        self.notes.insert(page, note);
    }

    /// Take a fresh read. A draft without unsaved edits follows it. An
    /// edited draft stays while its section is unchanged in the read (or
    /// holds exactly what this desktop's own save stored) and saves on the
    /// new revision; when another writer changed that section, the edit is
    /// never applied: the stored values show with the refusal.
    fn take(&mut self, read: SettingsReadResponse) {
        let old = std::mem::take(&mut self.base);
        let sent = std::mem::take(&mut self.sent);
        let unsure = std::mem::take(&mut self.unsure);
        // A save still running may already be in this read.
        let running = self.sending.as_ref();
        let fresh = !self.loaded;
        let mut refused = Vec::new();
        let mut follow = |draft: &mut Value, section: Section, page: Page| {
            let before = old.get(&section).cloned().unwrap_or_else(|| json!({}));
            let new = read
                .sections
                .get(&section)
                .cloned()
                .unwrap_or_else(|| json!({}));
            let edited =
                !fresh && *draft != before && *draft != new && sent.get(&section) != Some(draft);
            if edited
                && (new == before
                    || saved_here(section, &new, [&sent, &unsure].into_iter().chain(running)))
            {
                return;
            }
            if edited && differs(section, draft, &before) {
                refused.push(page);
            }
            *draft = new;
        };
        follow(&mut self.routing, Section::Routing, Page::Routing);
        follow(&mut self.projects, Section::Projects, Page::Projects);
        follow(&mut self.automation, Section::Automation, Page::Automations);
        follow(&mut self.cloud, Section::CloudMcp, Page::CloudMcp);
        for page in refused {
            self.typing.remove(&page);
            self.notes.insert(page, Note::error(CHANGED_ELSEWHERE));
        }
        // The JSON draft follows the read unless it holds edits not saved
        // yet; its edited sections are judged when it is sent
        // ([`Self::refuse_if_changed`]).
        let all = serde_json::to_string_pretty(&read.sections).unwrap_or_default();
        if fresh
            || self.all == self.all_base
            || self.json_edits().is_ok_and(|edits| {
                edits
                    .iter()
                    .all(|(section, value)| sent.get(section) == Some(value))
            })
        {
            self.all = all.clone();
            self.all_from = read.sections.clone();
        }
        self.all_base = all;
        self.base = read.sections;
        self.revision = read.revision;
        self.restart_required = read.restart_required;
        self.history = read.history;
        self.loaded = true;
        self.read_at_ms = Some(crate::handoff::now_ms());
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
            Page::CloudMcp => {
                let cloud = self.cloud.clone();
                // A fresh template is a local draft, never a save: its
                // required fields are empty and the daemon would refuse
                // it, discarding the new page (#678).
                if model::cloud_enabled(&cloud)
                    && let Some(missing) = model::cloud_missing(&cloud)
                {
                    return Err(format!("Fill in {missing} before Cloud MCP is saved."));
                }
                one(Section::CloudMcp, cloud)
            }
            Page::Advanced => {
                let sections = self.json_edits()?;
                if sections.is_empty() {
                    return Err("No section changed.".to_owned());
                }
                Ok(sections)
            }
            Page::General | Page::Access => Err("This page saves as you choose.".to_owned()),
            Page::Shortcuts | Page::Usage | Page::Worktrees => {
                Err("Nothing to save on this page.".to_owned())
            }
        }
    }

    /// The sections the owner edited in the settings file's JSON: those
    /// that differ from the read the draft was made from. A section another
    /// writer changed since is not one of them, so it is never sent back.
    fn json_edits(&self) -> Result<BTreeMap<Section, Value>, String> {
        let mut sections: BTreeMap<Section, Value> = serde_json::from_str(&self.all)
            .map_err(|_| "The JSON does not parse; the draft is kept.".to_owned())?;
        sections.retain(|section, value| self.all_from.get(section) != Some(value));
        Ok(sections)
    }

    /// The JSON draft is sent only when every section it edits still holds
    /// the value it was edited from; otherwise it is never applied and the
    /// stored values show with the refusal. True when refused. (A form
    /// follows the same rule as each read arrives, in [`Self::take`].)
    fn refuse_if_changed(&mut self, page: Page) -> bool {
        let changed = page == Page::Advanced
            && self.json_edits().is_ok_and(|edits| {
                edits
                    .keys()
                    .any(|section| self.all_from.get(section) != self.base.get(section))
            });
        if changed {
            self.discard(page);
            self.notes.insert(page, Note::error(CHANGED_ELSEWHERE));
        }
        changed
    }

    fn dirty(&self, page: Page) -> bool {
        match page {
            Page::Routing => differs(
                Section::Routing,
                &self.routing,
                &self.base(Section::Routing),
            ),
            Page::Projects => self.projects != self.base(Section::Projects),
            Page::Automations => self.automation != self.base(Section::Automation),
            Page::CloudMcp => self.cloud != self.base(Section::CloudMcp),
            Page::Advanced => self.all != self.all_base,
            Page::General
            | Page::Access
            | Page::Shortcuts
            | Page::Models
            | Page::Usage
            | Page::Worktrees => false,
        }
    }

    /// Put a page's draft back to the saved values (a typed change still
    /// waiting for its pause is dropped with it).
    fn discard(&mut self, page: Page) {
        self.typing.remove(&page);
        match page {
            Page::Routing => self.routing = self.base(Section::Routing),
            Page::Projects => self.projects = self.base(Section::Projects),
            Page::Automations => self.automation = self.base(Section::Automation),
            Page::CloudMcp => self.cloud = self.base(Section::CloudMcp),
            Page::Advanced => {
                self.all = self.all_base.clone();
                self.all_from = self.base.clone();
            }
            Page::General
            | Page::Access
            | Page::Models
            | Page::Shortcuts
            | Page::Usage
            | Page::Worktrees => {}
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
                        continuation: None,
                    };
                    let result = client::block_on(async {
                        let plan: eliot_core::api::CleanupPlan =
                            eliot_daemon::client_call_typed(&data, "cleanup.plan", &request)
                                .await?;
                        let wanted: HashSet<_> = plan
                            .items
                            .iter()
                            .map(|item| item.artifact.agent.clone())
                            .collect();
                        let mut titles = HashMap::new();
                        let mut after = None;
                        // A bounded retained-summary read includes archived tasks,
                        // unlike the sidebar's current page. No native-history I/O.
                        for _ in 0..8 {
                            let read = client::list_agents(
                                &data,
                                &AgentListRequest {
                                    archived: eliot_core::api::ArchivedFilter::Include,
                                    after,
                                    limit: 500,
                                    ..Default::default()
                                },
                            )
                            .await;
                            let Ok(page) = read else {
                                break;
                            };
                            for row in &page.agents {
                                let agent = &row.summary;
                                if wanted.contains(&agent.id) {
                                    titles.insert(agent.id.clone(), nav::summary_title(agent));
                                }
                            }
                            if titles.len() == wanted.len() {
                                break;
                            }
                            after = page.next;
                            if after.is_none() {
                                break;
                            }
                        }
                        Ok::<_, eliot_core::Error>((plan, titles))
                    })
                    .map_err(|error| error.to_string());
                    Message::Settings(SettingsMsg::CleanupPlanned(result))
                });
            }
            SettingsMsg::CleanupPlanned(result) => {
                s.cleanup_busy = false;
                match result {
                    Ok((plan, titles)) => {
                        s.cleanup_titles = titles;
                        s.cleanup_unchecked =
                            crate::worktrees::CleanupGroup::unchecked_by_default();
                        s.cleanup_kept_open.clear();
                        s.cleanup_open.clear();
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
            SettingsMsg::CleanupExpanded(group) => {
                if !s.cleanup_open.remove(&group) {
                    s.cleanup_open.insert(group);
                }
            }
            SettingsMsg::CleanupSelectAll => {
                s.cleanup_unchecked.clear();
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
                    self.settings_entered(false, context);
                }
            }
            SettingsMsg::Loaded(result) => {
                self.settings.reading = false;
                if std::mem::take(&mut self.settings.reread) {
                    // A save or choice landed after this read was issued:
                    // it is stale, so read again before taking anything.
                    self.load_settings(context);
                    return;
                }
                match result {
                    Ok(read) => {
                        // A choice made after this read was issued (or still
                        // being written) wins over the read.
                        if !self.prefs_saving && self.prefs_rev == self.settings.read_prefs_rev {
                            self.prefs = crate::settings::Preferences::from_reply(&read);
                        }
                        self.take_profiles(&read);
                        let automation_changed = self.settings.base.get(&Section::Automation)
                            != read.sections.get(&Section::Automation);
                        self.settings.take(read);
                        if automation_changed {
                            self.automation.global_rules_current = false;
                            self.load_rules(context);
                        }
                        if let Some(form) = self.automation.form.as_mut().filter(|form| form.saving)
                        {
                            if let Some(note) = self.settings.notes.get(&Page::Automations)
                                && note.text == CHANGED_ELSEWHERE
                            {
                                form.saving = false;
                                form.error = Some(note.text.clone());
                            } else if form.error.is_some() {
                                // An uncertain reply was followed by a fresh settings read.
                                if self.settings.dirty(Page::Automations) {
                                    form.saving = false;
                                    self.settings.discard(Page::Automations);
                                } else {
                                    self.automation.form = None;
                                }
                            } else if !self.settings.dirty(Page::Automations)
                                && self.settings.saving != Some(Page::Automations)
                            {
                                self.automation.form = None;
                            }
                        }
                        // Saves refused as stale go again first, on this
                        // revision, unless this read showed their edited
                        // section changed (then their drafts are the stored
                        // values now and nothing is sent).
                        let s = &mut self.settings;
                        for page in std::mem::take(&mut s.retry).into_iter().rev() {
                            s.queued.retain(|queued| *queued != page);
                            s.queued.insert(0, page);
                        }
                        // Only the Settings page has primed editors to refill;
                        // the composer keeps its draft untouched.
                        if self.main == Main::Settings {
                            self.reprime_fields(context);
                        }
                    }
                    Err(error) => {
                        self.settings.error = Some(format!("Settings not read: {error}"));
                        if let Some(form) = self.automation.form.as_mut().filter(|form| form.saving)
                            && (form.error.is_some()
                                || self.settings.retry.contains(&Page::Automations))
                        {
                            form.saving = false;
                            form.error = self.settings.error.clone();
                            self.settings
                                .retry
                                .retain(|page| *page != Page::Automations);
                            self.settings
                                .queued
                                .retain(|page| *page != Page::Automations);
                            self.settings.discard(Page::Automations);
                        }
                    }
                }
                self.drain_saves(context);
            }
            SettingsMsg::Reload => {
                self.settings.notes.clear();
                self.load_settings(context);
            }
            SettingsMsg::Save(page, dry_run) => self.save_page(page, dry_run, context),
            SettingsMsg::AutoSave(page, seq) => {
                if self.settings.rested(page, seq) {
                    self.autosave(page, context);
                }
            }
            SettingsMsg::Saved(page, dry_run, result) => {
                self.settings.saving = None;
                let sent = self.settings.sending.take().unwrap_or_default();
                let sent_draft = self.editor.sending.take();
                let sent_edit = self.editor.sending_edit.take();
                self.settings.notes.insert(page, Note::of(dry_run, &result));
                if dry_run {
                    self.drain_saves(context);
                    return;
                }
                if page == Page::Automations
                    && let Some(form) = self.automation.form.as_mut().filter(|form| form.saving)
                {
                    match &result {
                        Ok(_) => self.automation.form = None,
                        Err(error) if error.code == "conflict" => {}
                        Err(error) => {
                            form.error = Some(Note::of(false, &result).text);
                            form.saving = crate::state::uncertain(error);
                        }
                    }
                }
                match &result {
                    Ok(reply) => {
                        if reply.saved {
                            // The read that follows shows these values, maybe
                            // normalised; the drafts that sent them follow it.
                            self.settings.sent.extend(sent);
                            if page == Page::Models {
                                self.editor.saved(sent_draft, sent_edit);
                            }
                        }
                        if page == Page::Automations
                            || reply.changed_sections.contains(&Section::Automation)
                        {
                            self.automation.global_rules_current = false;
                            self.load_rules(context);
                            self.reload_rules_soon(context);
                        }
                        self.after_settings_saved(context);
                    }
                    // Another writer moved the revision and nothing was
                    // written: the next read decides (`SettingsState::take`).
                    Err(error) if error.code == "conflict" => {
                        let s = &mut self.settings;
                        s.queued.retain(|queued| *queued != page);
                        if !s.retry.contains(&page) {
                            s.retry.push(page);
                        }
                        self.load_settings(context);
                    }
                    // The outcome is not known: never send it again on our
                    // own; a read shows whether it landed (and if it did, the
                    // value there is this desktop's own).
                    Err(error) if crate::state::uncertain(error) => {
                        self.settings.unsure.extend(sent);
                        self.load_settings(context);
                    }
                    // Refused (an invalid value), nothing changed: a form
                    // shows the stored values under the error, a raw editor
                    // (the JSON, typed presets TOML) and the Cloud MCP form
                    // keep their drafts, and the other pages' changes still
                    // save.
                    Err(_) => {
                        if !(page == Page::Models && self.editor.raw_dirty) {
                            if page == Page::Models {
                                self.discard_profiles();
                            }
                            self.settings.refused(page, Note::of(false, &result));
                            self.reprime_fields(context);
                        }
                        self.drain_saves(context);
                    }
                }
            }
            SettingsMsg::Discard(page) => {
                if page == Page::Models {
                    self.discard_profiles();
                }
                self.settings.discard(page);
                self.settings.notes.remove(&page);
                self.reprime_fields(context);
            }
            SettingsMsg::RoutingMoved(role, index, up) => {
                model::routing_move(&mut s.routing, &role, index, up);
                self.autosave(Page::Routing, context);
            }
            SettingsMsg::RoutingRemoved(role, index) => {
                model::routing_remove(&mut s.routing, &role, index);
                self.autosave(Page::Routing, context);
            }
            SettingsMsg::RoutingAdded(role, index) => {
                // A ComboBox reports None when its items are replaced.
                if let Some(profile) =
                    index.and_then(|index| self.addable_profiles(&role).into_iter().nth(index))
                {
                    model::routing_add(&mut self.settings.routing, &role, &profile);
                    self.autosave(Page::Routing, context);
                }
            }
            SettingsMsg::NewRoleChanged(text) => s.new_role = text,
            SettingsMsg::RoleAdded => {
                let role = s.new_role.trim().to_owned();
                match model::routing_add_role(&mut s.routing, &role) {
                    Ok(()) => {
                        s.new_role.clear();
                        self.autosave(Page::Routing, context);
                    }
                    Err(error) => {
                        s.notes.insert(Page::Routing, Note::error(error));
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
            // Every control reports its own value when it is created or
            // re-rendered; only a real change edits the draft (and saves).
            // The folder field shows the path without its verbatim prefix.
            SettingsMsg::ProjectPath(id, path) => {
                if nav::display_path(&model::project_path(&s.projects, &id)) != path {
                    model::set_project_path(&mut s.projects, &id, &path);
                    self.autosave_later(Page::Projects, context);
                }
            }
            SettingsMsg::ProjectProfile(id, profile, on) => {
                if model::project_profiles(&s.projects, &id).contains(&profile) != on {
                    model::set_project_profile(&mut s.projects, &id, &profile, on);
                    self.autosave(Page::Projects, context);
                }
            }
            SettingsMsg::ProjectActions(id, text) => {
                if model::project_actions(&s.projects, &id) != text {
                    model::set_project_actions(&mut s.projects, &id, &text);
                    self.autosave_later(Page::Projects, context);
                }
            }
            SettingsMsg::ProjectRemoved(id) => {
                model::remove_entry(&mut s.projects, &id);
                s.project_open = None;
                s.project_selected.remove(&id);
                self.autosave(Page::Projects, context);
            }
            SettingsMsg::ProjectSelected(id, on) => {
                // A check box may report the value it already shows.
                if on {
                    s.project_selected.insert(id);
                } else {
                    s.project_selected.remove(&id);
                }
            }
            SettingsMsg::ProjectsSelectAll(on) => {
                s.project_selected = if on {
                    model::project_ids(&s.projects).into_iter().collect()
                } else {
                    Default::default()
                };
            }
            SettingsMsg::ProjectBrowse => match folder::pick() {
                Ok(Some(path)) => {
                    let profiles = self.known_profiles();
                    let s = &mut self.settings;
                    let id = model::project_id_for_folder(&s.projects, &path);
                    match model::add_project(&mut s.projects, &id, &path, &profiles) {
                        Ok(()) => {
                            s.project_open = Some(id);
                            self.autosave(Page::Projects, context);
                        }
                        Err(error) => {
                            s.notes.insert(Page::Projects, Note::error(error));
                        }
                    }
                }
                Ok(None) => {}
                Err(error) => {
                    self.settings
                        .notes
                        .insert(Page::Projects, Note::error(error));
                }
            },
            SettingsMsg::CloudField(index, text) => {
                if let Some((key, _)) = model::CLOUD_FIELDS.get(index)
                    && model::cloud_field(&s.cloud, key) != text
                {
                    model::set_cloud_field(&mut s.cloud, key, &text);
                    self.autosave_later(Page::CloudMcp, context);
                }
            }
            // Never replace a configured section with the template because
            // the box reported the state it already shows.
            SettingsMsg::CloudEnabled(on) => {
                if on != model::cloud_enabled(&s.cloud) {
                    s.cloud = if on {
                        model::cloud_template()
                    } else {
                        json!({})
                    };
                    if on {
                        // The template stays a local draft until every
                        // field is filled; each typed field saves when it
                        // rests, and the gate above holds back the
                        // incomplete draft instead of sending it to a
                        // refusal that would discard the new page (#678).
                        s.notes.insert(
                            Page::CloudMcp,
                            Note::info("Fill in every field; Cloud MCP saves when complete."),
                        );
                    } else {
                        self.autosave(Page::CloudMcp, context);
                    }
                }
            }
            SettingsMsg::AllDraft(text) => {
                if !s.busy() {
                    s.all = profiles::normalize_newlines(&text);
                }
            }
            SettingsMsg::HistoryToggled => s.history_open = !s.history_open,
            SettingsMsg::AccessChosen(index) => {
                // The composer's writer: `[desktop] last_access`, at once;
                // the level the list already shows (Ask while unset) is no
                // change.
                let shown = self.prefs.last_access.unwrap_or(Access::Ask);
                if let Some(access) = index.and_then(|index| Access::ALL.get(index).copied())
                    && access != shown
                {
                    self.prefs.last_access = Some(access);
                    self.launch.access = Some(access);
                    self.prefs_rev += 1;
                    self.settings
                        .notes
                        .insert(Page::Access, Note::info("Saving…"));
                    self.save_preferences(context);
                }
            }
            SettingsMsg::OpenConfig => {
                if let Ok(data_dir) = self.data_dir.clone() {
                    let config = data_dir.join("config.toml");
                    let _ = super::reveal::open_with_default_app(&config);
                }
            }
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

    /// The profile editor follows a read unless it has unsaved edits; those
    /// follow the rule of [`SettingsState::take`]: they stay while the
    /// presets are unchanged in the read (or hold exactly what this
    /// desktop's own save stored), and when another writer changed them
    /// they are never applied. Runs before [`SettingsState::take`] clears
    /// the saves it compares with.
    fn take_profiles(&mut self, read: &SettingsReadResponse) {
        let section = read
            .sections
            .get(&Section::Profiles)
            .cloned()
            .unwrap_or_else(|| json!({}));
        let editor = &mut self.editor;
        editor.loaded = true;
        editor.revision = read.revision.clone();
        let sent = editor.sent.take();
        match model::profiles_toml(&section) {
            Ok(text) => {
                // A draft a save wrote follows the read's formatting.
                // A draft edited after the last submission stays, even
                // when the edit undid back to the old text.
                let edited = editor.keep_draft(&text, sent.as_deref());
                let saves = &self.settings;
                let ours = saved_here(
                    Section::Profiles,
                    &section,
                    [&saves.sent, &saves.unsure]
                        .into_iter()
                        .chain(saves.sending.as_ref()),
                );
                let elsewhere = edited && text != editor.base && !ours;
                if !edited || elsewhere {
                    editor.draft = text.clone();
                    editor.view = text.clone();
                    editor.raw_dirty = false;
                }
                if elsewhere {
                    self.settings
                        .notes
                        .insert(Page::Models, Note::error(CHANGED_ELSEWHERE));
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
        editor.select_existing();
    }

    /// Validate or save one section from its draft. A save asked for
    /// while another save, a read or a preference write runs waits for it
    /// (the next read then saves against the new revision).
    pub(super) fn save_page(
        &mut self,
        page: Page,
        dry_run: bool,
        context: &ComponentContext<Self>,
    ) {
        if !self.settings.loaded {
            return;
        }
        if self.settings.busy() || self.prefs_saving {
            if !dry_run && !self.settings.queued.contains(&page) {
                self.settings.queued.push(page);
            }
            return;
        }
        let Ok(data_dir) = self.data_dir.clone() else {
            return;
        };
        if !dry_run && self.settings.refuse_if_changed(page) {
            self.reprime_fields(context);
            return;
        }
        let sections = match self.settings.request_sections(page, &self.editor.draft) {
            Ok(sections) => sections,
            Err(note) => {
                self.settings.notes.insert(page, Note::error(note));
                return;
            }
        };
        self.settings.sending = Some(sections.clone());
        self.editor.sending = (page == Page::Models).then(|| self.editor.draft.clone());
        self.editor.sending_edit = (page == Page::Models).then_some(self.editor.edit);
        let request = SettingsSaveRequest {
            expected_revision: self.settings.revision.clone(),
            sections,
            dry_run,
            client: SettingsClientSource::Desktop,
        };
        self.settings.saving = Some(page);
        self.settings.notes.insert(
            page,
            Note::info(if dry_run {
                "Validating…"
            } else {
                "Saving…"
            }),
        );
        let _task = context.spawn_background(move |_| {
            Message::Settings(SettingsMsg::Saved(
                page,
                dry_run,
                client::block_on(config_client::save_settings(&data_dir, &request)),
            ))
        });
    }

    /// Save a control change now, or once the running save or read is
    /// done. Nothing is sent when the page has no change.
    pub(super) fn autosave(&mut self, page: Page, context: &ComponentContext<Self>) {
        if self.page_dirty(page) {
            self.save_page(page, false, context);
        } else if self
            .settings
            .notes
            .get(&page)
            .is_some_and(|note| note.error)
        {
            // Back to the saved values: an earlier refusal no longer applies.
            self.settings.notes.remove(&page);
        }
    }

    /// Save a typed change once typing rests for [`TYPING_PAUSE_MS`].
    fn autosave_later(&mut self, page: Page, context: &ComponentContext<Self>) {
        let seq = self.settings.typed(page);
        let _task = context.spawn_background(move |cancel| {
            let cancelled = || cancel.is_cancelled();
            let _ = feed::backoff(TYPING_PAUSE_MS, &cancelled);
            Message::Settings(SettingsMsg::AutoSave(page, seq))
        });
    }

    /// Whether a page's draft differs from the saved settings.
    fn page_dirty(&self, page: Page) -> bool {
        match page {
            Page::Models => self.editor.draft != self.editor.base,
            _ => self.settings.dirty(page),
        }
    }

    /// Put the presets' draft back to the saved presets.
    fn discard_profiles(&mut self) {
        self.editor.draft = self.editor.base.clone();
        self.editor.view = self.editor.base.clone();
        self.editor.raw_dirty = false;
        self.editor.select_existing();
    }

    /// After a read, a refused save or a preference write: start the next
    /// waiting save (one at a time). A page with nothing to send now (no
    /// change left, or refused before sending) lets the next one go.
    pub(super) fn drain_saves(&mut self, context: &ComponentContext<Self>) {
        if self.settings.busy() || self.prefs_saving || !self.settings.loaded {
            return;
        }
        while !self.settings.queued.is_empty() {
            let page = self.settings.queued.remove(0);
            if self.page_dirty(page) {
                self.save_page(page, false, context);
                if self.settings.busy() {
                    return;
                }
            }
        }
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

    /// The connection already read the document to load preferences.
    pub(super) fn seed_settings(
        &mut self,
        read: SettingsReadResponse,
        context: &ComponentContext<Self>,
    ) {
        if !self.settings.loaded && !self.settings.reading {
            self.settings_update(SettingsMsg::Loaded(Ok(read)), context);
        }
    }

    /// Reopen with the saved document, revalidating it once in the
    /// background. Changing sections reuses the same document.
    pub(super) fn settings_entered(&mut self, reopened: bool, context: &ComponentContext<Self>) {
        if (reopened || !self.settings.loaded) && !self.settings.reading {
            self.load_settings(context);
        }
        match self.settings_page {
            Page::Models => {
                self.load_profile_list(context);
                // Model names for the presets, not their ids.
                if self.launch.catalog.is_none() {
                    self.load_catalog(context);
                }
            }
            Page::Routing | Page::Projects => self.load_profile_list(context),
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
            // Storage and history (the store's size, retention).
            Page::Advanced => self.load_fleet(context),
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
        let body: View = if self.settings.loaded {
            match page {
                Page::General => self.general_page(context, palette),
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
        } else if self.settings.error.is_none() {
            m3::secondary("Reading settings…").into()
        } else {
            View::empty()
        };
        let freshness = self
            .settings
            .read_at_ms
            .and_then(crate::monitor::last_read_label);
        let status = self.settings.error.as_deref().map_or_else(
            || {
                // The same text during a background read, so the page never
                // rewraps or shifts while it refreshes.
                freshness.map_or_else(View::empty, |label| m3::secondary(label).into())
            },
            |error| self.read_error(error, context, palette),
        );
        let width = self.settings_column_width();
        // The page's tree is keyed by page, so its editors are new controls
        // (see `reprime_fields`).
        let scroll =
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
                                            .on_click(context.message(Message::Settings(
                                                SettingsMsg::OpenConfig,
                                            )))
                                            .content(m3::link_content("Open config.toml"))
                                    }),
                                ),
                            ),
                            KeyedView::new("settings-read-status", status),
                            KeyedView::new(format!("body:{}", page.label()), body),
                        ]),
                );
        // The save status stays in view below the page, however far the
        // page is scrolled.
        Grid::new()
            .rows([GridLength::STAR, GridLength::Auto, GridLength::Auto])
            .children((
                scroll,
                Border::new()
                    .grid_row(1)
                    .content(if page == Page::Worktrees {
                        self.cleanup_actions(context, palette)
                    } else {
                        View::empty()
                    }),
                Border::new()
                    .grid_row(2)
                    .content(self.save_status(page, width, palette)),
            ))
    }

    /// The settings column: one explicit width, so every page's groups
    /// line up (content-sized until the window size is observed).
    pub(super) fn settings_column_width(&self) -> Option<f64> {
        self.main_width().map(|width| {
            (width - 2.0 * tokens::spacing::LG)
                .clamp(tokens::layout::MIN_COLUMN, tokens::layout::SETTINGS_WIDTH)
        })
    }

    /// Whether the page's changes save as they are made (no Save button):
    /// every editable page but the raw editors.
    fn autosaves(&self, page: Page) -> bool {
        match page {
            Page::Advanced => false,
            Page::Models => !self.editor.source_open && !self.editor.raw_dirty,
            _ => true,
        }
    }

    /// The page's save status in one line under the page: "Saving…",
    /// "Saved" or why not (in the error colour, selectable). Empty until a
    /// change is made, and on the raw editors (their save bar says it).
    fn save_status(&self, page: Page, width: Option<f64>, palette: Palette) -> View {
        let Some(note) = self
            .settings
            .notes
            .get(&page)
            .filter(|_| self.autosaves(page))
        else {
            return View::empty();
        };
        let text = if note.error {
            m3::text(note.text.clone(), tokens::text::SECONDARY).foreground(palette.error())
        } else {
            m3::secondary(note.text.clone())
        }
        .text_wrapping(TextWrapping::Wrap)
        .is_text_selection_enabled(true)
        .automation_name(format!("{} status: {}", page.label(), note.text))
        .vertical_alignment(VerticalAlignment::Center);
        Border::new()
            .grid_row(1)
            .width(width)
            .horizontal_alignment(HorizontalAlignment::Center)
            .padding(Thickness::new(
                0.0,
                tokens::spacing::SM,
                0.0,
                tokens::spacing::MD,
            ))
            .content(text)
    }

    fn read_error(&self, error: &str, context: &mut ViewContext<Self>, palette: Palette) -> View {
        StackPanel::new().spacing(tokens::spacing::SM).children((
            m3::body(error.to_owned())
                .foreground(palette.error())
                .text_wrapping(TextWrapping::Wrap)
                .is_text_selection_enabled(true),
            self.settings
                .read_at_ms
                .and_then(crate::monitor::last_read_label)
                .map_or_else(View::empty, |label| m3::secondary(label).into()),
            m3::outlined(palette)
                .horizontal_alignment(HorizontalAlignment::Left)
                .is_enabled(!self.settings.reading)
                .on_click(context.message(Message::Settings(SettingsMsg::Reload)))
                .content(m3::label("Read again")),
        ))
    }

    /// Save / Validate / Discard and the last result, under the raw
    /// editors only (every section's JSON, the presets' TOML): the page's
    /// one filled action, then outlined, then text.
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
            Note::info(if dirty {
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
            })
        });
        let note_text = m3::secondary(note.text)
            .text_wrapping(TextWrapping::Wrap)
            .is_text_selection_enabled(true);
        let note_text = if note.error {
            note_text.foreground(palette.error())
        } else {
            note_text
        };
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
            note_text,
        ))
    }
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
        let size = crate::monitor::bytes_label(Some(1024));
        let (value, note) = retention_lines(Some(&view));
        assert_eq!(value, size);
        assert!(note.contains("config.toml failed to load (profile_error: config.toml: bad)"));
        assert!(!note.contains("All history is kept"));
        let unset = eliot_core::api::RetentionView {
            unbounded: true,
            config_error: None,
            ..view.clone()
        };
        assert!(
            retention_lines(Some(&unset))
                .1
                .starts_with("All history is kept")
        );
        let bounded = eliot_core::api::RetentionView {
            events_days: Some(30),
            config_error: None,
            ..view
        };
        let (value, note) = retention_lines(Some(&bounded));
        assert_eq!(value, size);
        assert!(note.starts_with("History kept: events 30 days, commands kept, mail kept."));
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
        assert_eq!(pages.len(), 11);
        let unique: std::collections::HashSet<Page> = pages.iter().copied().collect();
        assert_eq!(unique.len(), pages.len());
        // Appearance and startup are one General page; Routing is Advanced.
        assert_eq!(search_settings("theme")[0].0, Page::General);
        assert_eq!(search_settings("reopen")[0].0, Page::General);
        assert!(GROUPS.iter().any(|(group, pages)| *group == "Advanced"
            && pages.contains(&Page::Routing)
            && pages.contains(&Page::Advanced)));
    }

    #[test]
    fn a_saved_draft_follows_the_normalised_read_and_is_clean() {
        let mut state = SettingsState::default();
        state.take(read_with("rev1", json!({"default": ["a"]}), json!({})));
        // A control change saves at once: the draft as sent.
        state.routing = json!({"default": ["a", "b"]});
        assert!(state.dirty(Page::Routing));
        let sent = state.request_sections(Page::Routing, "").unwrap();
        state.sent.extend(sent);
        // The daemon stored it with a field the desktop did not send.
        state.take(read_with(
            "rev2",
            json!({"default": ["a", "b"], "fallback": true}),
            json!({}),
        ));
        assert_eq!(
            state.routing,
            json!({"default": ["a", "b"], "fallback": true})
        );
        assert!(!state.dirty(Page::Routing));
        assert!(state.sent.is_empty());
        // Access and General save through the preferences writer, never a
        // page save.
        assert!(state.request_sections(Page::Access, "").is_err());
        assert!(state.request_sections(Page::General, "").is_err());
    }

    #[test]
    fn a_refused_draft_can_be_put_back() {
        let mut state = SettingsState::default();
        state.take(read_with("rev1", json!({"default": ["a"]}), json!({})));
        state.routing = json!({"default": []});
        assert!(state.dirty(Page::Routing));
        state.discard(Page::Routing);
        assert!(!state.dirty(Page::Routing));
        assert_eq!(state.routing, json!({"default": ["a"]}));
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

    fn read(revision: &str, sections: &[(Section, Value)]) -> SettingsReadResponse {
        SettingsReadResponse {
            revision: Some(revision.to_owned()),
            sections: sections.iter().cloned().collect(),
            restart_required: Vec::new(),
            history: Vec::new(),
        }
    }

    /// The settings file's JSON draft with one section replaced.
    fn edit_all(state: &mut SettingsState, section: &str, value: Value) {
        let mut all: Value = serde_json::from_str(&state.all).unwrap();
        all[section] = value;
        state.all = serde_json::to_string_pretty(&all).unwrap();
    }

    #[test]
    fn typed_changes_on_two_pages_both_save() {
        let mut state = SettingsState::default();
        // A Projects path, then a Cloud field within the pause.
        let projects = state.typed(Page::Projects);
        let cloud = state.typed(Page::CloudMcp);
        assert!(state.rested(Page::Projects, projects));
        assert!(state.rested(Page::CloudMcp, cloud));
        // On one page only the latest keystroke's pause saves.
        let first = state.typed(Page::Projects);
        let second = state.typed(Page::Projects);
        assert!(!state.rested(Page::Projects, first));
        assert!(state.rested(Page::Projects, second));
    }

    #[test]
    fn a_refused_value_shows_the_stored_one_and_a_raw_draft_stays() {
        let stored = json!({"atlas": {"path": "C:/atlas"}});
        let mut state = SettingsState::default();
        state.take(read("rev1", &[(Section::Projects, stored.clone())]));
        state.projects = json!({"atlas": {"path": "C:/missing"}});
        state.refused(Page::Projects, Note::error("Not saved: no such folder"));
        assert_eq!(state.projects, stored);
        assert!(!state.dirty(Page::Projects));
        let note = &state.notes[&Page::Projects];
        assert!(note.error);
        assert_eq!(
            note.text,
            "Not saved: no such folder. The page shows the saved values."
        );
        // The settings file's JSON keeps its draft for correction.
        edit_all(
            &mut state,
            "projects",
            json!({"atlas": {"path": "C:/missing"}}),
        );
        let draft = state.all.clone();
        state.refused(Page::Advanced, Note::error("Not saved: no such folder"));
        assert_eq!(state.all, draft);
        assert!(state.notes[&Page::Advanced].error);
    }

    #[test]
    fn a_stale_save_reapplies_only_the_sections_the_owner_edited() {
        let routing = json!({"default": ["a"]});
        let mut state = SettingsState::default();
        state.take(read(
            "rev1",
            &[
                (Section::Routing, routing.clone()),
                (Section::Projects, json!({"atlas": {}})),
            ],
        ));
        edit_all(&mut state, "routing", json!({"default": ["a", "b"]}));
        let edited = BTreeMap::from([(Section::Routing, json!({"default": ["a", "b"]}))]);
        assert_eq!(
            state.request_sections(Page::Advanced, ""),
            Ok(edited.clone())
        );
        // Refused as stale: another writer added a project meanwhile. Routing
        // is unchanged, so the change goes again, and only routing: the
        // other writer's project stays.
        state.take(read(
            "rev2",
            &[
                (Section::Routing, routing.clone()),
                (Section::Projects, json!({"atlas": {}, "beta": {}})),
            ],
        ));
        assert!(!state.refuse_if_changed(Page::Advanced));
        assert_eq!(state.request_sections(Page::Advanced, ""), Ok(edited));
        // Another writer changed routing itself: never applied; the stored
        // values show with the refusal.
        state.take(read(
            "rev3",
            &[
                (Section::Routing, json!({"default": ["c"]})),
                (Section::Projects, json!({"atlas": {}, "beta": {}})),
            ],
        ));
        assert!(state.refuse_if_changed(Page::Advanced));
        assert!(!state.dirty(Page::Advanced));
        assert!(state.notes[&Page::Advanced].error);
        assert!(state.request_sections(Page::Advanced, "").is_err());
    }

    #[test]
    fn a_form_change_follows_the_rule_on_the_next_read() {
        let mut state = SettingsState::default();
        state.take(read(
            "rev1",
            &[
                (Section::Routing, json!({"default": ["a"]})),
                (Section::Projects, json!({})),
            ],
        ));
        // Edited (unsent, or refused as stale); another writer changes a
        // different section: the change stays and saves on the new revision.
        state.routing = json!({"default": ["a", "b"]});
        state.take(read(
            "rev2",
            &[
                (Section::Routing, json!({"default": ["a"]})),
                (Section::Projects, json!({"beta": {}})),
            ],
        ));
        assert_eq!(state.routing, json!({"default": ["a", "b"]}));
        assert!(state.dirty(Page::Routing));
        assert!(!state.notes.contains_key(&Page::Routing));
        // Another writer changes routing itself: the stored values show
        // with the refusal, and nothing is sent.
        state.take(read(
            "rev3",
            &[
                (Section::Routing, json!({"default": ["c"]})),
                (Section::Projects, json!({"beta": {}})),
            ],
        ));
        assert_eq!(state.routing, json!({"default": ["c"]}));
        assert!(!state.dirty(Page::Routing));
        assert!(state.notes[&Page::Routing].error);
        // This desktop's own save (sure or not) is not someone else's: a
        // later edit on top of it stays.
        state.routing = json!({"default": ["c", "d", "e"]});
        state
            .sent
            .insert(Section::Routing, json!({"default": ["c", "d"]}));
        state.take(read(
            "rev4",
            &[
                (Section::Routing, json!({"default": ["c", "d"]})),
                (Section::Projects, json!({"beta": {}})),
            ],
        ));
        assert_eq!(state.routing, json!({"default": ["c", "d", "e"]}));
        state
            .unsure
            .insert(Section::Routing, json!({"default": ["c", "d", "f"]}));
        state.take(read(
            "rev5",
            &[
                (Section::Routing, json!({"default": ["c", "d", "f"]})),
                (Section::Projects, json!({"beta": {}})),
            ],
        ));
        assert_eq!(state.routing, json!({"default": ["c", "d", "e"]}));
        // A save still running may already be in the read.
        state.sending = Some(BTreeMap::from([(
            Section::Routing,
            json!({"default": ["c", "d", "e"]}),
        )]));
        state.routing = json!({"default": ["c", "d", "e", "g"]});
        state.take(read(
            "rev6",
            &[
                (Section::Routing, json!({"default": ["c", "d", "e"]})),
                (Section::Projects, json!({"beta": {}})),
            ],
        ));
        assert_eq!(state.routing, json!({"default": ["c", "d", "e", "g"]}));
    }

    #[test]
    fn a_queued_edit_never_overwrites_another_writers_save() {
        let b = json!({"default": ["z"]});
        let mut state = SettingsState::default();
        state.take(read(
            "rev1",
            &[(Section::Routing, json!({"default": ["a"]}))],
        ));
        // A1 saves; A2 is queued while the reread runs; another writer
        // saves B to routing before that read.
        state.routing = json!({"default": ["a", "b"]});
        let a1 = state.request_sections(Page::Routing, "").unwrap();
        state.sent.extend(a1);
        state.routing = json!({"default": ["a", "b", "c"]});
        state.take(read("rev3", &[(Section::Routing, b.clone())]));
        // A2 is refused and B shows: nothing is left to send over it.
        assert_eq!(state.routing, b);
        assert!(!state.dirty(Page::Routing));
        assert_eq!(state.notes[&Page::Routing], Note::error(CHANGED_ELSEWHERE));
        // The same when A1's outcome was unknown.
        state.notes.clear();
        state.routing = json!({"default": ["z", "y", "x"]});
        state
            .unsure
            .insert(Section::Routing, json!({"default": ["z", "y"]}));
        state.take(read(
            "rev5",
            &[(Section::Routing, json!({"default": ["w"]}))],
        ));
        assert_eq!(state.routing, json!({"default": ["w"]}));
        assert!(state.notes[&Page::Routing].error);
        // A read holding A1 as stored is this desktop's own save: A2 stays
        // and saves on that revision.
        let mut state = SettingsState::default();
        state.take(read("rev1", &[(Section::Projects, json!({}))]));
        state.projects = json!({"atlas": {"path": "C:/atlas"}});
        state
            .sent
            .extend(state.request_sections(Page::Projects, "").unwrap());
        let a2 = json!({"atlas": {"path": "C:/atlas"}, "beta": {"path": "C:/beta"}});
        state.projects = a2.clone();
        state.take(read(
            "rev2",
            &[(Section::Projects, json!({"atlas": {"path": "C:/atlas"}}))],
        ));
        assert_eq!(state.projects, a2);
        assert!(state.dirty(Page::Projects));
        assert!(!state.notes.contains_key(&Page::Projects));
        // The presets editor ([`DesktopApp::take_profiles`]) asks the same:
        // the presets a save sent, as stored, against the read.
        let sent = model::profiles_from_toml(
            "[profiles.dev]
harness = \"codex\"
model = \"gpt-6\"
",
        )
        .unwrap();
        let saves = BTreeMap::from([(Section::Profiles, sent.clone())]);
        assert!(saved_here(Section::Profiles, &sent, [&saves]));
        let other = json!({"dev": {"harness": "codex", "model": "gpt-7"}});
        assert!(!saved_here(Section::Profiles, &other, [&saves]));
    }
    #[test]
    fn cloud_template_is_held_back_until_every_field_is_filled() {
        // #678: a fresh template must never be sent (the daemon would
        // refuse it and discard the new page); only a complete draft
        // and the empty (off) section go out.
        let mut state = SettingsState {
            cloud: model::cloud_template(),
            ..SettingsState::default()
        };
        let refused = state
            .request_sections(Page::CloudMcp, "")
            .expect_err("bare template is not sent");
        assert!(refused.contains("Public hostname"), "{refused}");
        for (key, _) in model::CLOUD_FIELDS {
            model::set_cloud_field(&mut state.cloud, key, "x");
        }
        assert!(state.request_sections(Page::CloudMcp, "").is_ok());
        let off = SettingsState {
            cloud: serde_json::json!({}),
            ..SettingsState::default()
        };
        assert!(off.request_sections(Page::CloudMcp, "").is_ok());
    }
    #[test]
    fn a_refused_cloud_draft_stays_for_fixing() {
        // Finding 6: a daemon validation error keeps the Cloud MCP draft
        // and shows the error; other forms still reset to stored values.
        let mut state = SettingsState::default();
        let draft = serde_json::json!({"hostname": "example.com"});
        state.cloud = draft.clone();
        state.refused(
            Page::CloudMcp,
            Note::error("Cloud MCP configuration is malformed."),
        );
        assert_eq!(state.cloud, draft);
        let note = &state.notes[&Page::CloudMcp];
        assert!(note.error);
        assert!(!note.text.contains(SHOWS_SAVED));
        state.projects = serde_json::json!({"beta": {}});
        state.refused(Page::Projects, Note::error("bad value."));
        assert_eq!(state.projects, serde_json::json!({}));
        let note = &state.notes[&Page::Projects];
        assert!(note.text.contains(SHOWS_SAVED));
    }
}
