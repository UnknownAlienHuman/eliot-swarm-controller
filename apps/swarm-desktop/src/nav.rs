//! The Agent navigation tree: listed pages grouped by project, children
//! indented under their manager, and the branches fetched for expanded
//! parents (`agent.list` with `parent`). Pure data, so the grouping is
//! tested without a window.
//!
//! The main list pages every Agent in scope by id (the daemon's order). An
//! Agent whose manager is on a listed page appears under that manager, not
//! at the top level; an Agent whose manager is not listed (another page or
//! another project) stays at the top level. Expanding a manager shows its
//! listed children at once and its fetched branch, the authoritative
//! direct-children page, when that read returns.
//!
//! The sidebar's projects are the daemon's `chat.organization` groups
//! (#657 G): a registered project with its manager, the manager's standard
//! worktree branch, competing roots and the exact server totals, then the
//! no-project bucket (unattached chats and Agents whose execution label is
//! not a registration, such as legacy `wt-*` folders). Every listed row
//! carries the group the daemon placed it in (`AgentSummary::organization`),
//! so nothing is grouped here by path, repository or name ([`sections`]).
//! A registered checkout is named by its repository ([`repository_name`]:
//! the `origin` remote's name, else its main checkout's folder) and, in a
//! linked worktree, its branch in words ([`branch_words`]:
//! `feat/k141-monitor-charts` is "k141 monitor charts"). Running tasks stay
//! on top, then the rest newest first; stopped tasks last changed before
//! today fold into one "Show N older" row (the Codex sidebar's pattern).
//! Titles come with each listed summary ([`summary_title`]), so every page
//! carries its own.

use std::collections::{HashMap, HashSet};

use eliot_core::api::{
    AgentSummary, LaunchIntentState, LaunchQueueView, LaunchWaitReason, OrgGroupView,
    ProjectAssignmentState, ProjectManagerIdentity, ProjectOverviewEntry,
};
use pulldown_cmark::{Event, Options, Parser, Tag, TagEnd};

/// UI copy of one Agent row: identity, generation, liveness and placement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UiAgent {
    pub id: String,
    pub generation: i64,
    pub harness: String,
    pub state: eliot_core::AgentState,
    /// The recorded registration ID (the daemon's project label), independently
    /// of the execution path. Move queries the authorization scope, never
    /// this label; Handoff narrows listed rows by the scope.
    pub project_id: String,
    /// The authorization scope the server compares for same-project moves
    /// and handoffs, when it differs from [`Self::project_id`]. Absent on
    /// old payloads and synthetic rows, which fall back to the project ID.
    pub project_scope: Option<String>,
    pub repo_common_dir: Option<String>,
    pub workspace: Option<String>,
    /// The current manager, if any.
    pub parent: Option<String>,
    /// Last change of the Agent record (ms since the epoch), for Recents.
    pub updated_at: i64,
    /// What a person reads for the Agent ([`summary_title`]).
    pub title: String,
    /// A native observed child: observe-only, outside Swarm's control.
    pub native_child: bool,
    /// Tokens the Agent's summary reports ([`AgentSummary::fleet_usage`]).
    pub tokens: Option<i64>,
    /// A chat without a project (`launch {chat: true}`, k92): listed under
    /// Chats, not under a project.
    pub chat: bool,
    /// Archived (`agent.archive`, k92): listed only with Show archived.
    pub archived: bool,
    /// An optimistic row for a just-launched Agent until the list reads it
    /// back: its project ID and workspace are the submitted ones, but its
    /// authorization scope is still unknown. Pickers never treat the ID as
    /// the scope and never offer this row as a candidate meanwhile.
    pub pending: bool,
    /// The `chat.organization` group the daemon reads this row in (its
    /// registered project, or a chat's placement); `None` for the
    /// no-project bucket. Placement only, never authority.
    pub organization: Option<String>,
    /// Organization tags (`chat.tag`).
    pub tags: Vec<String>,
}

impl UiAgent {
    pub fn from_summary(summary: &AgentSummary) -> Self {
        Self {
            id: summary.id.clone(),
            generation: summary.generation,
            harness: summary.harness.clone(),
            state: summary.state,
            project_id: summary.project.clone(),
            project_scope: summary.project_scope.clone(),
            repo_common_dir: summary.repo_common_dir.clone(),
            workspace: summary.workspace.clone(),
            parent: summary.parent.clone(),
            updated_at: summary.updated_at,
            title: summary_title(summary),
            // The daemon reports child activity only for native observed
            // children.
            native_child: summary.activity.is_some(),
            tokens: summary.fleet_usage().map(|(tokens, _)| tokens),
            chat: false,
            archived: false,
            pending: false,
            organization: summary.organization.clone(),
            tags: summary.tags.clone(),
        }
    }

    /// The authorization scope the server compares for same-project moves
    /// and handoffs: the retained scope when it differs, else the project
    /// ID. None while the row is pending: the daemon row has not arrived,
    /// so the ID must not stand in for the scope. The daemon stays the
    /// judge; the pickers only narrow by this.
    pub fn scope(&self) -> Option<&str> {
        if self.pending {
            return None;
        }
        Some(self.project_scope.as_deref().unwrap_or(&self.project_id))
    }

    /// Working now or on its way.
    pub fn running(&self) -> bool {
        matches!(
            self.state,
            eliot_core::AgentState::Busy
                | eliot_core::AgentState::Created
                | eliot_core::AgentState::Starting
        )
    }

    /// Its process may still exist: neither stopped, failed nor lost.
    pub fn live(&self) -> bool {
        !matches!(
            self.state,
            eliot_core::AgentState::Stopped
                | eliot_core::AgentState::Failed
                | eliot_core::AgentState::Lost
        )
    }

    /// The Store deletes only a stopped or failed tree (`docs/spec/core.md`,
    /// explicit Agent tree removal); a lost one is resumed or archived.
    pub fn deletable(&self) -> bool {
        matches!(
            self.state,
            eliot_core::AgentState::Stopped | eliot_core::AgentState::Failed
        )
    }

    /// The owner has something to do: answer a pending request
    /// (`waiting`), decide about a failed or lost task (Resume, why, or
    /// archive it), or settle an Agent Swarm controls whose state it cannot
    /// prove (`unknown`). An observe-only native child is never counted:
    /// its state is only observed and detach is all it offers.
    pub fn needs_attention(&self) -> bool {
        !self.native_child
            && matches!(
                self.state,
                eliot_core::AgentState::Waiting
                    | eliot_core::AgentState::Failed
                    | eliot_core::AgentState::Lost
                    | eliot_core::AgentState::Unknown
            )
    }

    /// Ready for the next message: idle and controlled.
    pub fn idle(&self) -> bool {
        self.state == eliot_core::AgentState::Idle && !self.native_child
    }

    /// Ended: stopped, failed or lost.
    fn ended(&self) -> bool {
        matches!(
            self.state,
            eliot_core::AgentState::Stopped
                | eliot_core::AgentState::Failed
                | eliot_core::AgentState::Lost
        )
    }
}

/// The direct-children read of one expanded manager.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Branch {
    Loading,
    Loaded { children: Vec<UiAgent>, more: bool },
    Failed(String),
}

/// One visible row of the navigation list.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Row {
    pub agent: UiAgent,
    /// Nesting below the top level (0 for a top-level Agent).
    pub depth: usize,
    /// Children known now: the fetched branch, or the listed pages.
    pub children: usize,
    pub expanded: bool,
    /// The branch read state when expanded (loading, failed, more exist).
    pub note: Option<String>,
}

impl Row {
    pub fn expandable(&self) -> bool {
        self.children > 0 || self.note.is_some()
    }
}

/// A checkout from the daemon's project registration or Git readback.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Checkout {
    pub path: String,
    pub registration_ids: Vec<String>,
    /// The Git common directory of its repository.
    pub common_dir: String,
    /// The verified checkout's branch; `None` for a
    /// detached HEAD or when it was not read.
    pub branch: Option<String>,
    /// Explicit Git identity, absent when only the registration remains.
    pub main: Option<bool>,
    /// The repository's name from its `origin` remote (`project.list`'s
    /// read-only `origin`), on any checkout of the repository.
    pub origin: Option<String>,
    /// A registered checkout whose folder is gone.
    pub missing: bool,
}

impl Checkout {
    pub fn new(path: impl Into<String>, common_dir: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            common_dir: common_dir.into(),
            ..Self::default()
        }
    }
}

/// The last component of a path (both separators: labels are Windows
/// paths on Windows, POSIX elsewhere).
pub fn folder_name(path: &str) -> &str {
    path.rsplit(['\\', '/'])
        .find(|part| !part.is_empty())
        .unwrap_or(path)
}

/// Branch prefixes that say what kind of change it is, not what it is
/// about (conventional commit types and common topic folders).
const BRANCH_KINDS: [&str; 18] = [
    "feat", "feature", "features", "fix", "bugfix", "hotfix", "chore", "refactor", "docs", "doc",
    "test", "tests", "perf", "build", "ci", "style", "wip", "topic",
];

/// A branch as a person reads it, by the substance of its task: without a
/// leading change kind (`feat/`, `fix/`, …), separators as spaces:
/// `feat/k141-monitor-charts` reads "k141 monitor charts", `release/1.2`
/// "release 1.2", `main` "main".
pub fn branch_words(branch: &str) -> String {
    let segments: Vec<&str> = branch.split('/').filter(|part| !part.is_empty()).collect();
    let topic = segments
        .iter()
        .position(|part| !BRANCH_KINDS.contains(&part.to_ascii_lowercase().as_str()))
        .map_or(&segments[segments.len().saturating_sub(1)..], |start| {
            &segments[start..]
        });
    let words = topic
        .join(" ")
        .replace(['-', '_'], " ")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    if words.is_empty() {
        branch.trim().to_owned()
    } else {
        words
    }
}

/// A repository's logical name: its `origin` remote's repository name,
/// else its main checkout's folder ([`repository`]), never a registration
/// ID or a linked worktree's folder.
pub fn repository_name(common_dir: &str, origin: Option<&str>) -> String {
    match origin.map(str::trim).filter(|origin| !origin.is_empty()) {
        Some(origin) => origin.to_owned(),
        None => repository(common_dir).0,
    }
}

/// The origin name any checkout of the repository `key` carries.
fn origin_of<'a>(key: &str, checkouts: &'a [Checkout]) -> Option<&'a str> {
    checkouts
        .iter()
        .filter(|checkout| path_key(&checkout.common_dir) == key)
        .find_map(|checkout| checkout.origin.as_deref())
}

/// A checkout's subproject name, or `None` for its repository's main
/// checkout (the project itself): its branch in words, else (detached)
/// its folder without the repository's name in front.
pub fn subproject_name(checkout: &Checkout) -> Option<String> {
    let (folder, main) = repository(&checkout.common_dir);
    if checkout
        .main
        .unwrap_or_else(|| main.as_deref().map(path_key) == Some(path_key(&checkout.path)))
    {
        return None;
    }
    Some(match checkout.branch.as_deref() {
        Some(branch) => branch_words(branch),
        None => place_name(folder_name(&checkout.path), &folder),
    })
}

/// The display name of a registration or checkout, with its worktree name
/// when applicable. An unmatched path uses its folder name.
pub fn place_of(label: &str, checkouts: &[Checkout]) -> (String, Option<String>) {
    match checkout_of(label, checkouts) {
        Some(checkout) => (
            checkout_repository_name(checkout, checkouts),
            subproject_name(checkout),
        ),
        None => (folder_name(label).to_owned(), None),
    }
}

fn main_checkout(common_dir: &str, checkouts: &[Checkout]) -> Option<String> {
    let common = path_key(common_dir);
    checkouts
        .iter()
        .find(|checkout| checkout.main == Some(true) && path_key(&checkout.common_dir) == common)
        .map(|checkout| checkout.path.clone())
        .or_else(|| repository(common_dir).1)
}

fn checkout_repository_name(checkout: &Checkout, checkouts: &[Checkout]) -> String {
    let origin = origin_of(&path_key(&checkout.common_dir), checkouts);
    if origin.is_none()
        && let Some(main) = main_checkout(&checkout.common_dir, checkouts)
    {
        return folder_name(&main).to_owned();
    }
    repository_name(&checkout.common_dir, origin)
}

/// [`place_of`] in one line: "atlas-web › search filters", or the
/// project's name alone.
pub fn place_label(label: &str, checkouts: &[Checkout]) -> String {
    match place_of(label, checkouts) {
        (project, Some(subproject)) => format!("{project} › {subproject}"),
        (project, None) => project,
    }
}

/// A path as a person reads it: without the `\\?\` verbatim prefix the
/// daemon keeps on canonical Windows paths (a verbatim UNC path is left as
/// it is).
pub fn display_path(path: &str) -> &str {
    if path.starts_with(r"\\?\UNC\") {
        return path;
    }
    path.strip_prefix(r"\\?\").unwrap_or(path)
}

/// A path as the daemon's Git identity compares it: no verbatim prefix,
/// one separator, no trailing separator, case-insensitive on Windows.
pub fn path_key(path: &str) -> String {
    let unified = display_path(path).replace('\\', "/");
    let trimmed = unified.trim_end_matches('/');
    if cfg!(windows) {
        trimmed.to_lowercase()
    } else {
        trimmed.to_owned()
    }
}

/// The repository of a Git common directory: its name and its main
/// checkout. `<folder>/.git` is the repository `folder`, checked out
/// there; any other common directory (a bare `name.git`) names itself
/// without `.git` and has no main checkout.
pub fn repository(common_dir: &str) -> (String, Option<String>) {
    let shown = display_path(common_dir).trim_end_matches(['\\', '/']);
    let mut parts = shown.rsplitn(2, ['\\', '/']);
    let last = parts.next().unwrap_or(shown);
    if last.eq_ignore_ascii_case(".git")
        && let Some(main) = parts.next().filter(|main| !main.is_empty())
    {
        return (folder_name(main).to_owned(), Some(main.to_owned()));
    }
    let name = last
        .strip_suffix(".git")
        .filter(|name| !name.is_empty())
        .unwrap_or(last);
    (name.to_owned(), None)
}

/// Match a registration ID or an exact checkout path.
fn checkout_of<'a>(label: &str, checkouts: &'a [Checkout]) -> Option<&'a Checkout> {
    let path = path_key(label);
    checkouts
        .iter()
        .find(|checkout| checkout.registration_ids.iter().any(|id| id == label))
        .or_else(|| {
            checkouts
                .iter()
                .find(|checkout| path_key(&checkout.path) == path)
        })
}

/// The no-project bucket's sidebar key ([`group_key`] of no group).
pub const OTHER_LOCATIONS: &str = "other-locations";
const NO_PROJECT_NAME: &str = "No project";

/// Resolve the Agent's repository and, independently, its verified workspace.
fn agent_checkout<'a>(
    agent: &UiAgent,
    checkouts: &'a [Checkout],
) -> Option<(&'a Checkout, Option<&'a Checkout>)> {
    let project = agent.project_id.as_str();
    let workspace = path_key(agent.workspace.as_deref().unwrap_or(project));
    let registered = checkouts
        .iter()
        .find(|checkout| checkout.registration_ids.iter().any(|id| id == project));
    let repository = registered.or_else(|| {
        if let Some(common_dir) = &agent.repo_common_dir {
            let common_dir = path_key(common_dir);
            return checkouts
                .iter()
                .find(|checkout| path_key(&checkout.common_dir) == common_dir);
        }
        if !project.contains(['/', '\\']) {
            return None;
        }
        let mut verified = checkouts.iter().filter(|checkout| {
            !checkout.missing && checkout.main.is_some() && path_key(&checkout.path) == workspace
        });
        let checkout = verified.next()?;
        verified.next().is_none().then_some(checkout)
    })?;
    let common = path_key(&repository.common_dir);
    let checkout = checkouts.iter().find(|checkout| {
        !checkout.missing
            && checkout.main.is_some()
            && path_key(&checkout.common_dir) == common
            && path_key(&checkout.path) == workspace
    });
    Some((repository, checkout))
}

fn checkout_subproject_key(checkout: &Checkout) -> Option<String> {
    subproject_name(checkout).map(|_| format!("sub:{}", path_key(&checkout.path)))
}

pub fn agent_place_label(agent: &UiAgent, checkouts: &[Checkout]) -> String {
    if agent.chat {
        return "Chats".to_owned();
    }
    match agent_checkout(agent, checkouts) {
        Some((repository, workspace)) => {
            let name = checkout_repository_name(repository, checkouts);
            match workspace.and_then(subproject_name) {
                Some(sub) => format!("{name} › {sub}"),
                None => name,
            }
        }
        None => folder_name(agent.workspace.as_deref().unwrap_or(&agent.project_id)).to_owned(),
    }
}

/// A worktree folder as a task row names it: without its repository's
/// name in front (`atlas-wt-tiles` in `atlas` reads `wt-tiles`).
fn place_name(folder: &str, repository: &str) -> String {
    folder
        .strip_prefix(repository)
        .and_then(|rest| rest.strip_prefix(['-', '_', '.']))
        .filter(|rest| !rest.is_empty())
        .unwrap_or(folder)
        .to_owned()
}

/// The project key of a label (a sidebar section's key): its
/// repository's, or its own folder's.
pub fn project_key(label: &str, checkouts: &[Checkout]) -> String {
    match checkout_of(label, checkouts) {
        Some(checkout) => format!("repo:{}", path_key(&checkout.common_dir)),
        None => format!("folder:{}", path_key(label)),
    }
}

/// The project name of a label ([`place_of`]): its repository's logical
/// name, or its folder's. The sidebar, the picker, Settings › Projects and
/// the Monitor all name a project with it.
pub fn project_of(label: &str, checkouts: &[Checkout]) -> String {
    place_of(label, checkouts).0
}

/// The subproject key of a label in a linked worktree (`sub:<path>`), the
/// key a subproject collapses and is renamed by; `None` in a main
/// checkout or an unregistered folder.
pub fn subproject_key(label: &str, checkouts: &[Checkout]) -> Option<String> {
    checkout_of(label, checkouts).and_then(checkout_subproject_key)
}

/// The organization readbacks the sidebar shows, as the daemon returned
/// them: groups from `chat.organization`, exact team totals from
/// `project.overview`, and waits from `launch.queue`. A failed read keeps
/// its error line; nothing here is regrouped or recounted.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Organization {
    pub groups: Vec<OrgGroupView>,
    pub overview: Vec<ProjectOverviewEntry>,
    pub queue: Option<LaunchQueueView>,
    /// The current state of the intents this window queued
    /// (`launch.intent.get`), so an admission or refusal is reported.
    pub intents: Vec<eliot_core::api::LaunchIntentView>,
    pub errors: Vec<String>,
}

impl Organization {
    fn overview_of(&self, project: &str) -> Option<&ProjectOverviewEntry> {
        self.overview.iter().find(|entry| entry.project == project)
    }

    /// The retained registered projects, in the daemon's order: where a
    /// chat can be attached.
    pub fn registered(&self) -> impl Iterator<Item = &OrgGroupView> {
        self.groups
            .iter()
            .filter(|group| group.registration.is_some() && group.project.is_some())
    }
}

/// The sidebar key of an organization group: `org:<project>`, or the
/// no-project bucket's [`OTHER_LOCATIONS`].
pub fn group_key(organization: Option<&str>) -> String {
    organization.map_or_else(
        || OTHER_LOCATIONS.to_owned(),
        |project| format!("org:{project}"),
    )
}

/// One project of the sidebar: one `chat.organization` group with the
/// listed rows the daemon placed in it (each row's `organization`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Section {
    /// `org:<project>`, or the no-project bucket's [`OTHER_LOCATIONS`].
    pub key: String,
    /// The group's project; `None` for the no-project bucket.
    pub project: Option<String>,
    /// The registered checkout's place name ([`place_label`]), else the
    /// project label; "No project" for the bucket.
    pub name: String,
    /// Where a new task in it starts: the registered checkout.
    pub checkout: String,
    pub checkouts: Vec<String>,
    /// The header's lines: the team (manager, its branch, assignment and
    /// transfer state, or "Needs assignment"), competing roots, and the
    /// exact server totals ([`group_detail`]).
    pub detail: Vec<String>,
    /// Queued launches and automation waits with their reasons ([`waits`]).
    pub waits: Vec<String>,
    /// Visible task rows, newest work first.
    pub rows: Vec<Row>,
    /// Working now by the server's team totals (busy and starting); 0
    /// without a team readback.
    pub running: usize,
    /// Whether the project is collapsed (it then has no rows).
    pub collapsed: bool,
    /// Older stopped tasks, folded into one row.
    pub older: usize,
    /// Whether those are unfolded (the row then hides them again).
    pub older_shown: bool,
    /// The registered checkout's folder is gone.
    pub missing: bool,
}

/// Newest tasks a project keeps visible when every task of it is older.
pub const RECENT_WHEN_ALL_OLDER: usize = 3;

/// How the sidebar shows the tree.
#[derive(Clone, Copy)]
pub struct Shown<'a> {
    pub branches: &'a HashMap<String, Branch>,
    pub expanded: &'a HashSet<String>,
    /// Section keys of collapsed projects.
    pub collapsed: &'a HashSet<String>,
    /// Section keys whose older tasks are unfolded.
    pub unfolded: &'a HashSet<String>,
    /// The start of the local day (ms since the epoch): stopped tasks last
    /// changed before it fold.
    pub today_start: i64,
    /// Only the listed Agents this keeps (a search, the attention filter),
    /// flat in their projects: no tree, no folding, no collapsing, and no
    /// project without a match.
    pub only: Option<&'a dyn Fn(&UiAgent) -> bool>,
    /// The open Agent: it renders explicitly above the list, so sections
    /// skip its rows (no duplicate, no folding, no "Show N older" count).
    pub pinned: Option<&'a str>,
}

struct Group<'a> {
    section: Section,
    /// The bucket or an unregistered group shows only with members.
    members: usize,
    top: Vec<&'a UiAgent>,
}

/// The sidebar's order of tasks: running first, then live, then ended
/// (stopping a task is a change too, but not activity), each newest first;
/// ties in id order (`a9` < `a10`).
fn task_order(a: &UiAgent, b: &UiAgent) -> std::cmp::Ordering {
    b.running()
        .cmp(&a.running())
        .then(a.ended().cmp(&b.ended()))
        .then(b.updated_at.cmp(&a.updated_at))
        .then_with(|| (a.id.len(), &a.id).cmp(&(b.id.len(), &b.id)))
}

/// The tasks among `top` that fold under "Show N older": ended before
/// today. A list whose tasks would all fold keeps its newest few (they
/// sort first) when `keep_newest`, never "No tasks yet" above "Show N
/// older".
fn folding<'a>(top: &[&'a UiAgent], today_start: i64, keep_newest: bool) -> HashSet<&'a str> {
    let mut folding: HashSet<&str> = top
        .iter()
        .filter(|agent| agent.ended() && agent.updated_at < today_start)
        .map(|agent| agent.id.as_str())
        .collect();
    if keep_newest && !top.is_empty() && folding.len() == top.len() {
        for agent in top.iter().take(RECENT_WHEN_ALL_OLDER) {
            folding.remove(agent.id.as_str());
        }
    }
    folding
}

/// A group's name, start checkout and missing flag: the registered
/// checkout's place name ([`place_label`]), else the label's.
fn group_place(
    project: Option<&str>,
    group: Option<&OrgGroupView>,
    checkouts: &[Checkout],
) -> (String, String, bool) {
    let Some(project) = project else {
        return (NO_PROJECT_NAME.to_owned(), String::new(), false);
    };
    match group.and_then(|group| group.registration.as_ref()) {
        Some(registration) => {
            let path = path_key(&registration.checkout);
            let missing = checkouts
                .iter()
                .any(|checkout| checkout.missing && path_key(&checkout.path) == path);
            (
                place_label(&registration.checkout, checkouts),
                registration.checkout.clone(),
                missing,
            )
        }
        None => (
            place_label(project, checkouts),
            checkout_of(project, checkouts)
                .map(|checkout| checkout.path.clone())
                .unwrap_or_default(),
            false,
        ),
    }
}

/// Most competing roots a header names before "+N".
const ROOTS_NAMED: usize = 3;

/// A header's lines, from the server alone: the team line (the assigned
/// manager with its state and standard worktree branch, a remote manager,
/// "Needs assignment" or "No manager", plus a starting or blocked
/// assignment and an open transfer), the competing roots, and the exact
/// totals (`member_total` from `chat.organization`; live, working,
/// waiting, queued, unknown and the project slot use from
/// `project.overview`).
pub fn group_detail(
    group: &OrgGroupView,
    overview: Option<&ProjectOverviewEntry>,
    checkouts: &[Checkout],
) -> Vec<String> {
    let mut lines = Vec::new();
    if group.registration.is_some() {
        let assignment = group.assignment.as_ref();
        let mut team = match (&group.manager, assignment.and_then(|a| a.manager.as_ref())) {
            (Some(manager), _) => {
                let mut line = format!(
                    "Manager {} · {}",
                    manager.agent,
                    eliot_core::api::agent_state_name(manager.state)
                );
                let workspace = manager
                    .workspace
                    .as_deref()
                    .or_else(|| assignment.and_then(|a| a.workspace.as_deref()));
                if let Some(place) =
                    workspace.map(|path| worktree_branch(path, overview, checkouts))
                {
                    line.push_str(" · ");
                    line.push_str(&place);
                }
                line
            }
            (None, Some(ProjectManagerIdentity::Remote { instance, .. })) => {
                format!("Remote manager {instance}")
            }
            _ if group.needs_assignment => "Needs assignment".to_owned(),
            _ => NO_MANAGER.to_owned(),
        };
        match assignment.map(|assignment| assignment.state) {
            Some(ProjectAssignmentState::Starting) => team.push_str(" · starting"),
            Some(ProjectAssignmentState::Blocked) => team.push_str(" · blocked"),
            _ => {}
        }
        if group.transfer.is_some() {
            team.push_str(" · transfer in progress");
        }
        lines.push(team);
        if !group.conflicting_roots.is_empty() {
            let named: Vec<&str> = group
                .conflicting_roots
                .iter()
                .take(ROOTS_NAMED)
                .map(String::as_str)
                .collect();
            let rest = group.conflicting_roots.len() - named.len();
            let mut line = format!(
                "{}: {}",
                if group.conflicting_roots.len() == 1 {
                    "Competing root"
                } else {
                    "Competing roots"
                },
                named.join(", ")
            );
            if rest > 0 {
                line.push_str(&format!(" +{rest}"));
            }
            lines.push(line);
        }
    }
    let mut totals = vec![eliot_core::count_words(
        group.member_total as u64,
        "task",
        "tasks",
    )];
    if let Some(entry) = overview {
        let lifecycle = &entry.totals.lifecycle;
        for (count, words) in [
            (entry.totals.live, "live"),
            (lifecycle.busy + lifecycle.starting, "working"),
            (lifecycle.waiting, "waiting"),
            (entry.totals.queued, "queued"),
            (entry.totals.retained_unknown, "unknown"),
        ] {
            if count > 0 {
                totals.push(format!("{count} {words}"));
            }
        }
        if let Some(limit) = entry.capacity.first().and_then(|scope| scope.limit) {
            totals.push(format!("{}/{limit} slots", entry.capacity[0].usage));
        }
    }
    let totals = totals.join(" · ");
    // A vacant project with no roots says so on one line.
    match lines.as_slice() {
        [team] if team == NO_MANAGER => lines = vec![format!("{NO_MANAGER} · {totals}")],
        _ => lines.push(totals),
    }
    lines
}

/// The team line of a registered project without a manager or roots.
const NO_MANAGER: &str = "No manager";

/// A manager workspace as the header names it: the branch of its managed
/// worktree (`project.overview` workspaces), else of its verified checkout
/// (`project.list`), else its folder.
fn worktree_branch(
    workspace: &str,
    overview: Option<&ProjectOverviewEntry>,
    checkouts: &[Checkout],
) -> String {
    let path = path_key(workspace);
    overview
        .and_then(|entry| {
            entry
                .workspaces
                .iter()
                .find(|row| path_key(&row.worktree_path) == path)
                .map(|row| row.branch.clone())
        })
        .or_else(|| {
            checkouts
                .iter()
                .find(|checkout| path_key(&checkout.path) == path)
                .and_then(|checkout| checkout.branch.clone())
        })
        .unwrap_or_else(|| folder_name(workspace).to_owned())
}

/// The waits of one group, from `launch.queue`: queued interactive launches
/// and automation assignment waits with the wait reason and who can change
/// it. A project group shows its own; the no-project bucket shows chats and
/// launches of unregistered folders.
pub fn waits(
    queue: Option<&LaunchQueueView>,
    project: Option<&str>,
    registered: &HashSet<&str>,
) -> Vec<String> {
    let Some(queue) = queue else {
        return Vec::new();
    };
    let here = |candidate: &str| match project {
        Some(project) => candidate == project,
        None => !registered.contains(candidate),
    };
    let mut lines: Vec<String> = queue
        .interactive
        .iter()
        .filter(|intent| intent.state == LaunchIntentState::Queued && here(&intent.project))
        .map(|intent| match &intent.reason {
            Some(reason) => format!("Queued launch: {}", wait_reason_words(reason)),
            None => "Queued launch: waiting for its turn".to_owned(),
        })
        .collect();
    lines.extend(
        queue
            .assignments
            .iter()
            .filter(|wait| here(&wait.project))
            .map(|wait| {
                format!(
                    "Automation {} waits: {}",
                    wait.rule,
                    wait_reason_words(&wait.reason)
                )
            }),
    );
    lines
}

/// A wait reason in plain words: what holds the launch (the daemon's code
/// and scope) and who can change it (`changed_by`).
pub fn wait_reason_words(reason: &LaunchWaitReason) -> String {
    let capacity = reason.code == "capacity_unavailable";
    let what = match reason.code.as_str() {
        "capacity_unavailable" => match reason.scope.split_once(':') {
            Some(("global", _)) => "Swarm capacity is full".to_owned(),
            Some(("project", _)) => "the project budget is full".to_owned(),
            Some(("manager_assignment", _)) => "the manager budget is full".to_owned(),
            Some((kind, name)) => format!("{kind} {name} is at its limit"),
            None => "launch capacity is full".to_owned(),
        },
        "provider_held" => format!("{} is on hold at the provider", reason.scope),
        "transfer_in_progress" => "a project transfer is in progress".to_owned(),
        "workspace_unavailable" => "the workspace is unavailable".to_owned(),
        code => eliot_core::reason_words(code),
    };
    let what_changes = if capacity { "the limit" } else { "this" };
    let who = match reason.changed_by.as_str() {
        "owner" => format!("the Owner can change {what_changes}"),
        "owner_or_delegated_manager" => {
            format!("the Owner or the project manager can change {what_changes}")
        }
        "caller" => "change the request and queue it again".to_owned(),
        other => format!("{} can change {what_changes}", other.replace('_', " ")),
    };
    format!("{what}; {who}")
}

/// The sidebar's projects: the `chat.organization` groups in the daemon's
/// order (registered projects, even with no row yet; an unregistered label
/// or the no-project bucket only with members), each with the listed rows
/// the daemon placed in it. A row whose group this read did not return
/// lists under that group anyway, before the bucket. Nothing is grouped by
/// path, repository or name here.
pub fn sections(
    listed: &[UiAgent],
    organization: &Organization,
    checkouts: &[Checkout],
    shown: &Shown,
) -> Vec<Section> {
    let registered: HashSet<&str> = organization
        .registered()
        .filter_map(|group| group.project.as_deref())
        .collect();
    let section = |project: Option<&str>, group: Option<&OrgGroupView>| {
        let (name, checkout, missing) = group_place(project, group, checkouts);
        let overview = project.and_then(|project| organization.overview_of(project));
        let running = overview.map_or(0, |entry| {
            entry.totals.lifecycle.busy + entry.totals.lifecycle.starting
        });
        Section {
            key: group_key(project),
            project: project.map(str::to_owned),
            name,
            checkouts: if checkout.is_empty() {
                Vec::new()
            } else {
                vec![checkout.clone()]
            },
            checkout,
            detail: group.map_or_else(Vec::new, |group| group_detail(group, overview, checkouts)),
            waits: waits(organization.queue.as_ref(), project, &registered),
            rows: Vec::new(),
            running,
            collapsed: false,
            older: 0,
            older_shown: false,
            missing,
        }
    };
    let mut groups: Vec<Group> = Vec::new();
    for group in &organization.groups {
        groups.push(Group {
            section: section(group.project.as_deref(), Some(group)),
            members: if group.registration.is_some() {
                usize::MAX
            } else {
                group.member_total
            },
            top: Vec::new(),
        });
    }
    let placements: HashMap<&str, String> = listed
        .iter()
        .map(|agent| (agent.id.as_str(), group_key(agent.organization.as_deref())))
        .collect();
    for agent in listed.iter().filter(|agent| !agent.native_child) {
        let key = &placements[agent.id.as_str()];
        if !groups.iter().any(|group| group.section.key == *key) {
            groups.push(Group {
                section: section(agent.organization.as_deref(), None),
                members: 0,
                top: Vec::new(),
            });
        }
    }
    // The daemon leaves an empty bucket out; its queued launches still show.
    if !groups.iter().any(|group| group.section.project.is_none()) {
        let bucket = section(None, None);
        if !bucket.waits.is_empty() {
            groups.push(Group {
                section: bucket,
                members: 0,
                top: Vec::new(),
            });
        }
    }
    // The no-project bucket closes the list, as the daemon orders it.
    groups.sort_by_key(|group| group.section.project.is_none());
    for agent in listed {
        let key = &placements[agent.id.as_str()];
        let Some(group) = groups.iter_mut().find(|group| group.section.key == *key) else {
            continue;
        };
        // A native observed child shows only nested under its parent
        // conversation (collapsed), never as a task of its own. The open
        // Agent renders explicitly above the list.
        let pinned = shown.pinned == Some(agent.id.as_str());
        let shows = match shown.only {
            Some(keep) => keep(agent) && !agent.native_child,
            None => {
                !agent.native_child
                    && agent
                        .parent
                        .as_deref()
                        .is_none_or(|parent| placements.get(parent) != Some(key))
            }
        };
        if shows && !pinned {
            group.top.push(agent);
        }
    }

    let filtered = shown.only.is_some();
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for mut group in groups {
        if filtered && group.top.is_empty() {
            continue;
        }
        // An unregistered label's group is always empty (its Agents read in
        // the bucket), and an empty bucket says nothing.
        if group.members == 0 && group.top.is_empty() && group.section.waits.is_empty() {
            continue;
        }
        group.section.collapsed = !filtered && shown.collapsed.contains(&group.section.key);
        if group.section.collapsed {
            out.push(group.section);
            continue;
        }
        group.top.sort_by(|a, b| task_order(a, b));
        let unfolded = shown.unfolded.contains(&group.section.key);
        let folds = if filtered {
            HashSet::new()
        } else {
            folding(&group.top, shown.today_start, true)
        };
        for agent in &group.top {
            if folds.contains(agent.id.as_str()) && !unfolded {
                continue;
            }
            push(
                agent,
                0,
                listed,
                shown,
                !filtered,
                &mut seen,
                &mut group.section.rows,
            );
        }
        group.section.older = folds.len();
        group.section.older_shown = unfolded && !folds.is_empty();
        out.push(group.section);
    }
    out
}

/// Tasks using a project registration: `(live, all)`, top-level or not.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Use {
    pub live: usize,
    pub tasks: usize,
}

/// Count usages of the recorded registration. For legacy Agents, use the
/// verified checkout's registration, or the repository's preferred registration
/// when the checkout is unregistered. Count ambiguous legacy usage against every
/// matching registration so a live task keeps them in use.
pub fn registration_use(
    registrations: &[(String, String)],
    agents: &[UiAgent],
    checkouts: &[Checkout],
) -> HashMap<String, Use> {
    let mut usage: HashMap<String, Use> = registrations
        .iter()
        .map(|(id, _)| (id.clone(), Use::default()))
        .collect();
    let mut seen = HashSet::new();
    for agent in agents
        .iter()
        .filter(|agent| !agent.chat && seen.insert(agent.id.as_str()))
    {
        let recorded = agent.project_id.as_str();
        if let Some(entry) = usage.get_mut(recorded) {
            entry.tasks += 1;
            entry.live += usize::from(agent.live());
            continue;
        }
        let Some((repository, workspace)) = agent_checkout(agent, checkouts) else {
            continue;
        };
        let checkout = workspace.unwrap_or(repository);
        let path = path_key(&checkout.path);
        let mut users: Vec<&String> = registrations
            .iter()
            .filter(|(_, registered)| path_key(registered) == path)
            .map(|(id, _)| id)
            .collect();
        if users.is_empty() {
            let repository = path_key(&checkout.common_dir);
            let main = main_checkout(&checkout.common_dir, checkouts)
                .as_deref()
                .map(path_key);
            let same: Vec<&(String, String)> = registrations
                .iter()
                .filter(|(_, registered)| {
                    checkouts.iter().any(|known| {
                        path_key(&known.path) == path_key(registered)
                            && path_key(&known.common_dir) == repository
                    })
                })
                .collect();
            users = match same
                .iter()
                .find(|(_, registered)| Some(path_key(registered)) == main)
            {
                Some((id, _)) => vec![id],
                None => same.iter().map(|(id, _)| id).collect(),
            };
        }
        for id in users {
            let entry = usage.entry(id.clone()).or_default();
            entry.tasks += 1;
            entry.live += usize::from(agent.live());
        }
    }
    usage
}

/// Whether `agent` matches a search: every word of `query` appears,
/// ignoring case, in its title, its project name or its harness.
pub fn matches(agent: &UiAgent, project: &str, query: &str) -> bool {
    let haystack = format!("{} {project} {}", agent.title, agent.harness).to_lowercase();
    query
        .split_whitespace()
        .all(|word| haystack.contains(&word.to_lowercase()))
}

/// Most suggestions a search offers.
pub const SEARCH_RESULTS: usize = 12;

/// The tasks matching a search, running first then newest, as
/// `(label, id)`: the label is the title, project and harness (unique: a
/// repeated label also names its id). Empty for a blank query.
pub fn search(listed: &[UiAgent], checkouts: &[Checkout], query: &str) -> Vec<(String, String)> {
    if query.trim().is_empty() {
        return Vec::new();
    }
    let mut seen = HashSet::new();
    let mut found: Vec<(&UiAgent, String)> = listed
        .iter()
        .filter(|agent| seen.insert(agent.id.as_str()))
        .map(|agent| (agent, agent_place_label(agent, checkouts)))
        .filter(|(agent, project)| matches(agent, project, query))
        .collect();
    found.sort_by(|(a, _), (b, _)| {
        b.running()
            .cmp(&a.running())
            .then(b.updated_at.cmp(&a.updated_at))
    });
    found.truncate(SEARCH_RESULTS);
    let labels: Vec<String> = found
        .iter()
        .map(|(agent, project)| format!("{} · {project} · {}", agent.title, agent.harness))
        .collect();
    labels
        .iter()
        .zip(&found)
        .map(|(label, (agent, _))| {
            let label = if labels.iter().filter(|other| *other == label).count() > 1 {
                format!("{label} · {}", agent.id)
            } else {
                label.clone()
            };
            (label, agent.id.clone())
        })
        .collect()
}

/// The most recently changed tasks `keep` admits, newest first (ties by
/// id), at most `limit`: top-level tasks only, never a worker or a native
/// subagent (each lives under its parent), nothing archived, and one row
/// per title (a prompt run again and again shows its newest run once).
pub fn recents<'a>(
    agents: &'a [UiAgent],
    limit: usize,
    keep: &dyn Fn(&UiAgent) -> bool,
) -> Vec<&'a UiAgent> {
    let mut recent: Vec<&UiAgent> = agents
        .iter()
        .filter(|agent| agent.parent.is_none() && !agent.archived && keep(agent))
        .collect();
    recent.sort_by(|a, b| b.updated_at.cmp(&a.updated_at).then(a.id.cmp(&b.id)));
    // An id read on two pages shows once, at its newest time; so does a
    // repeated title.
    let mut seen = HashSet::new();
    let mut titles = HashSet::new();
    recent.retain(|agent| {
        seen.insert(agent.id.as_str()) && titles.insert(agent.title.trim().to_lowercase())
    });
    recent.truncate(limit);
    recent
}

/// The sidebar's Recents: the newest tasks of collapsed projects (their
/// group keys in `collapsed`), the ones the list above hides. Tasks folded
/// under "Show N older" stay there, so Recents never repeats the list or
/// brings back old runs.
pub fn sidebar_recents<'a>(
    agents: &'a [UiAgent],
    collapsed: &HashSet<&str>,
    limit: usize,
) -> Vec<&'a UiAgent> {
    recents(agents, limit, &|agent: &UiAgent| {
        collapsed.contains(group_key(agent.organization.as_deref()).as_str())
    })
}

/// Tags typed as one comma-separated line, trimmed, empty ones dropped;
/// the daemon validates the rest (`chat.tag`).
pub fn parse_tags(text: &str) -> Vec<String> {
    text.split(',')
        .map(str::trim)
        .filter(|tag| !tag.is_empty())
        .map(str::to_owned)
        .collect()
}

/// Archived Agents the default list leaves out: those still live (not
/// stopped, failed or lost), those running, and every archived record.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Archived {
    pub live: usize,
    pub running: usize,
    pub total: usize,
}

impl Archived {
    /// Counts over one `agent.list {archived: only}` page and its total.
    pub fn of(agents: &[UiAgent], total: usize) -> Self {
        let mut seen = HashSet::new();
        let mut archived = Self {
            total,
            ..Self::default()
        };
        for agent in agents {
            if !seen.insert(agent.id.as_str()) {
                continue;
            }
            archived.live += usize::from(agent.live());
            archived.running += usize::from(agent.running());
        }
        archived
    }
}

/// The fleet line's counts over the listed Agents.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Fleet {
    pub running: usize,
    /// Agents that need the owner ([`UiAgent::needs_attention`]).
    pub attention: usize,
    /// Controlled Agents ready for the next message.
    pub idle: usize,
}

impl Fleet {
    pub fn of(agents: &[UiAgent]) -> Self {
        let mut seen = HashSet::new();
        let mut fleet = Self::default();
        for agent in agents {
            if !seen.insert(agent.id.as_str()) {
                continue;
            }
            fleet.running += usize::from(agent.running());
            fleet.attention += usize::from(agent.needs_attention());
            fleet.idle += usize::from(agent.idle());
        }
        fleet
    }

    /// The line itself: what is live ("2 working · 1 needs attention · 3
    /// idle"), never the stopped ones. A live archived Agent still counts
    /// (k98: "No Agents yet" while one was live). "All quiet" when nothing
    /// is live; "No Agents yet" when there is none at all.
    pub fn summary(&self, total: usize, archived: Archived) -> String {
        let (working, idle) = self.live(archived);
        if total == 0 && archived.total == 0 {
            return "No Agents yet".to_owned();
        }
        crate::monitor::live_words(working, self.attention, idle)
    }

    /// The sidebar's fleet line, short enough never to be cut off: at most
    /// two counts, the most urgent first ("2 working · 1 needs attention",
    /// "12 idle"); the full [`Self::summary`] is its accessible name and
    /// the Monitor's line.
    pub fn compact(&self, total: usize, archived: Archived) -> String {
        let (working, idle) = self.live(archived);
        if total == 0 && archived.total == 0 {
            return "No Agents yet".to_owned();
        }
        // Idle is the least urgent: it gives way when both others show.
        let idle = if working > 0 && self.attention > 0 {
            0
        } else {
            idle
        };
        let attention = self.attention;
        crate::monitor::live_words(working, attention, idle)
    }

    /// Working and idle, with the live archived Agents (an archived live
    /// Agent that is not running is idle: archive accepts only idle or
    /// ended trees).
    fn live(&self, archived: Archived) -> (usize, usize) {
        (
            self.running + archived.running,
            self.idle + archived.live.saturating_sub(archived.running),
        )
    }
}

/// Most characters of a title; the row trims what its width cannot show.
const TITLE_CHARS: usize = 60;

/// The title of a listed Agent from Swarm's retained data: the first line
/// of its first input ([`title`] of `preview`), else its name, else
/// `<harness> · <profile>`; a native observed child reads `Codex subagent`
/// (it inherits its parent's internal profile name, which says nothing).
/// The id appears only when the daemon reported no profile.
pub fn summary_title(summary: &AgentSummary) -> String {
    if let Some(title) = summary.preview.as_deref().and_then(title) {
        return title;
    }
    if let Some(name) = summary.name.as_deref().and_then(title) {
        return name;
    }
    if summary.activity.is_some() {
        return format!(
            "{} subagent",
            crate::picker::harness_label(&summary.harness)
        );
    }
    format!(
        "{} · {}",
        summary.harness,
        summary.profile.as_deref().unwrap_or(&summary.id)
    )
}

/// A title from a first message, like a chat title: the first line that
/// has words (of a heading, paragraph or list item; code blocks, HTML and
/// front matter skipped) as plain text without Markdown marks, absolute
/// paths shortened to their file name, whitespace collapsed, cut at a word
/// near [`TITLE_CHARS`] with an ellipsis. `None` when no line has words.
pub fn title(text: &str) -> Option<String> {
    let options = Options::ENABLE_YAML_STYLE_METADATA_BLOCKS
        | Options::ENABLE_PLUSES_DELIMITED_METADATA_BLOCKS
        | Options::ENABLE_STRIKETHROUGH
        | Options::ENABLE_TASKLISTS
        | Options::ENABLE_TABLES;
    let mut line = String::new();
    let mut skipped = 0_usize;
    for event in Parser::new_ext(text, options) {
        match event {
            Event::Start(Tag::CodeBlock(_) | Tag::HtmlBlock | Tag::MetadataBlock(_)) => {
                skipped += 1;
            }
            Event::End(TagEnd::CodeBlock | TagEnd::HtmlBlock | TagEnd::MetadataBlock(_)) => {
                skipped = skipped.saturating_sub(1);
            }
            _ if skipped > 0 => {}
            Event::Text(part) | Event::Code(part) => line.push_str(&part),
            Event::SoftBreak
            | Event::HardBreak
            | Event::Start(
                Tag::Paragraph
                | Tag::Heading { .. }
                | Tag::List(_)
                | Tag::Item
                | Tag::BlockQuote(_)
                | Tag::TableCell,
            )
            | Event::End(
                TagEnd::Paragraph
                | TagEnd::Heading(_)
                | TagEnd::Item
                | TagEnd::BlockQuote(_)
                | TagEnd::TableCell,
            ) => {
                if line.split_whitespace().next().is_some() {
                    break;
                }
                line.clear();
            }
            _ => {}
        }
    }
    let words: Vec<String> = line.split_whitespace().map(short_path).collect();
    if words.is_empty() {
        return None;
    }
    Some(cut(&words.join(" ")))
}

/// `text` cut at a word near [`TITLE_CHARS`] with an ellipsis (a single
/// longer word is cut inside).
fn cut(text: &str) -> String {
    if text.chars().count() <= TITLE_CHARS {
        return text.to_owned();
    }
    let mut kept = String::new();
    for word in text.split(' ') {
        let next = if kept.is_empty() {
            word.to_owned()
        } else {
            format!("{kept} {word}")
        };
        if next.chars().count() > TITLE_CHARS {
            break;
        }
        kept = next;
    }
    if kept.is_empty() {
        kept = text.chars().take(TITLE_CHARS).collect();
    }
    format!("{kept}…")
}

/// An absolute path as its last component (`C:\briefs\k75.md` reads
/// `k75.md`); any other word unchanged. Quotes, brackets and trailing
/// punctuation around the path stay.
fn short_path(word: &str) -> String {
    let start = word
        .find(|c: char| c.is_alphanumeric() || c == '\\' || c == '/')
        .unwrap_or(word.len());
    let (lead, rest) = word.split_at(start);
    let end = rest
        .trim_end_matches(|c: char| ".,;:!?)]}\"'`".contains(c))
        .len();
    let (path, tail) = rest.split_at(end);
    let bytes = path.as_bytes();
    let absolute = (bytes.len() > 3
        && bytes[0].is_ascii_alphabetic()
        && bytes[1] == b':'
        && matches!(bytes[2], b'\\' | b'/'))
        || path.starts_with(r"\\")
        || path
            .strip_prefix('/')
            .is_some_and(|rest| rest.contains('/'));
    if !absolute {
        return word.to_owned();
    }
    format!("{lead}{}{tail}", folder_name(path))
}

/// Stable child order: numeric Agent-ID order (`a9` before `a10`), whatever
/// the fetch order. Both the branch read and the listed fallback arrive in
/// fetch order; the sidebar renders children by ID.
pub fn child_order(a: &UiAgent, b: &UiAgent) -> std::cmp::Ordering {
    (a.id.len(), &a.id).cmp(&(b.id.len(), &b.id))
}

/// Children of `id` to show now: the fetched branch, else the listed ones,
/// both in [`child_order`].
pub(crate) fn children_of<'a>(
    id: &str,
    listed: &'a [UiAgent],
    branches: &'a HashMap<String, Branch>,
) -> Vec<&'a UiAgent> {
    let mut children: Vec<&UiAgent> = match branches.get(id) {
        Some(Branch::Loaded { children, .. }) => children.iter().collect(),
        _ => listed
            .iter()
            .filter(|agent| agent.parent.as_deref() == Some(id))
            .collect(),
    };
    children.sort_by(|a, b| child_order(a, b));
    children
}

/// Push `agent`'s row and, when it is expanded and `tree` is on, its
/// children's rows beneath it.
fn push(
    agent: &UiAgent,
    depth: usize,
    listed: &[UiAgent],
    shown: &Shown,
    tree: bool,
    seen: &mut HashSet<String>,
    out: &mut Vec<Row>,
) {
    // Parent links form a tree in the daemon; a repeated id (an Agent moved
    // between reads) is still shown once.
    if !seen.insert(agent.id.clone()) {
        return;
    }
    let children = if tree {
        children_of(&agent.id, listed, shown.branches)
            .into_iter()
            .filter(|child| child.native_child || child.organization == agent.organization)
            .collect()
    } else {
        Vec::new()
    };
    let is_expanded = tree && shown.expanded.contains(&agent.id);
    let note = if is_expanded {
        match shown.branches.get(&agent.id) {
            Some(Branch::Loading) => Some("loading children…".to_owned()),
            Some(Branch::Failed(error)) => Some(format!("children not read: {error}")),
            Some(Branch::Loaded { more: true, .. }) => Some("more children exist".to_owned()),
            _ => None,
        }
    } else {
        None
    };
    out.push(Row {
        agent: agent.clone(),
        depth,
        children: children.len(),
        expanded: is_expanded,
        note,
    });
    if is_expanded {
        for child in children {
            // The open Agent renders explicitly above the list.
            if shown.pinned == Some(child.id.as_str()) {
                continue;
            }
            push(child, depth + 1, listed, shown, tree, seen, out);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use eliot_core::AgentState;

    #[test]
    fn a_project_is_named_by_its_origin_then_its_main_checkout_never_a_worktree() {
        let git = r"\\?\C:\src\atlas\.git";
        // The origin's repository name comes first.
        assert_eq!(repository_name(git, Some("atlas-web")), "atlas-web");
        // Without one, the main checkout's folder (the common dir's parent).
        assert_eq!(repository_name(git, None), "atlas");
        assert_eq!(repository_name(git, Some("  ")), "atlas");
        assert_eq!(repository_name("/srv/git/tools.git", None), "tools");
        let mut linked = Checkout::new(r"\\?\C:\wt\atlas-wt-search", git);
        let main = Checkout::new(r"\\?\C:\src\atlas", git);
        // A linked worktree's folder never names the project.
        assert_eq!(
            place_of(
                r"\\?\C:\wt\atlas-wt-search",
                &[main.clone(), linked.clone()]
            ),
            ("atlas".to_owned(), Some("wt-search".to_owned()))
        );
        linked.branch = Some("feat/search-filters".to_owned());
        let mut named = main.clone();
        named.origin = Some("atlas-web".to_owned());
        // Any checkout's origin names the whole repository.
        let checkouts = [named, linked];
        assert_eq!(
            place_label(r"\\?\C:\wt\atlas-wt-search", &checkouts),
            "atlas-web › search filters"
        );
        assert_eq!(place_label(r"\\?\C:\src\atlas", &checkouts), "atlas-web");
        assert_eq!(
            project_of(r"\\?\C:\wt\atlas-wt-search", &checkouts),
            "atlas-web"
        );
        // A folder no checkout holds is its own project.
        assert_eq!(place_label(r"C:\scratch\notes", &checkouts), "notes");
        assert_eq!(subproject_key(r"\\?\C:\src\atlas", &checkouts), None);
        assert_eq!(
            subproject_key(r"\\?\C:\wt\atlas-wt-search", &checkouts),
            Some(format!("sub:{}", path_key(r"C:\wt\atlas-wt-search")))
        );
    }

    fn agent(id: &str, project: &str, parent: Option<&str>) -> UiAgent {
        UiAgent {
            id: id.to_owned(),
            generation: 1,
            harness: "muse".to_owned(),
            state: AgentState::Idle,
            project_id: project.to_owned(),
            project_scope: None,
            repo_common_dir: None,
            workspace: None,
            parent: parent.map(str::to_owned),
            updated_at: 0,
            title: id.to_owned(),
            native_child: false,
            tokens: None,
            chat: false,
            archived: false,
            pending: false,
            organization: None,
            tags: Vec::new(),
        }
    }

    fn with(mut agent: UiAgent, state: &str, updated_at: i64) -> UiAgent {
        agent.state = match state {
            "created" => AgentState::Created,
            "starting" => AgentState::Starting,
            "idle" => AgentState::Idle,
            "busy" | "running" => AgentState::Busy,
            "waiting" => AgentState::Waiting,
            "stopped" => AgentState::Stopped,
            "failed" => AgentState::Failed,
            "lost" => AgentState::Lost,
            "unknown" => AgentState::Unknown,
            _ => unreachable!("unknown test state {state}"),
        };
        agent.updated_at = updated_at;
        agent
    }

    #[derive(Default)]
    struct View {
        branches: HashMap<String, Branch>,
        expanded: HashSet<String>,
        collapsed: HashSet<String>,
        unfolded: HashSet<String>,
        today_start: i64,
        organization: Organization,
    }

    impl View {
        fn sections(&self, listed: &[UiAgent], checkouts: &[Checkout]) -> Vec<Section> {
            self.filtered(listed, checkouts, None)
        }

        fn filtered(
            &self,
            listed: &[UiAgent],
            checkouts: &[Checkout],
            only: Option<&dyn Fn(&UiAgent) -> bool>,
        ) -> Vec<Section> {
            sections(
                listed,
                &self.organization,
                checkouts,
                &Shown {
                    branches: &self.branches,
                    expanded: &self.expanded,
                    collapsed: &self.collapsed,
                    unfolded: &self.unfolded,
                    today_start: self.today_start,
                    only,
                    pinned: None,
                },
            )
        }
    }

    fn ids(section: &Section) -> Vec<(&str, usize)> {
        section
            .rows
            .iter()
            .map(|row| (row.agent.id.as_str(), row.depth))
            .collect()
    }

    fn checkout(path: &str, common_dir: &str) -> Checkout {
        let mut checkout = Checkout::new(path, common_dir);
        checkout.main =
            Some(repository(common_dir).1.as_deref().map(path_key) == Some(path_key(path)));
        checkout
    }

    #[test]
    fn children_hide_under_a_listed_manager_until_expanded() {
        let listed = vec![
            agent("a1", "/p", None),
            agent("a2", "/p", Some("a1")),
            agent("a3", "/p", None),
        ];
        let mut view = View::default();
        let collapsed = view.sections(&listed, &[]);
        assert_eq!(ids(&collapsed[0]), [("a1", 0), ("a3", 0)]);
        assert_eq!(collapsed[0].rows[0].children, 1);
        assert!(collapsed[0].rows[0].expandable());
        assert!(!collapsed[0].rows[1].expandable());

        view.expanded.insert("a1".to_owned());
        let open = view.sections(&listed, &[]);
        assert_eq!(ids(&open[0]), [("a1", 0), ("a2", 1), ("a3", 0)]);
        assert!(open[0].rows[0].expanded);
    }

    #[test]
    fn a_fetched_branch_replaces_the_listed_children_and_nests() {
        let listed = vec![agent("a1", "/p", None), agent("a2", "/p", Some("a1"))];
        let view = View {
            branches: HashMap::from([
                (
                    "a1".to_owned(),
                    Branch::Loaded {
                        children: vec![
                            agent("a2", "/p", Some("a1")),
                            agent("a9", "/p", Some("a1")),
                        ],
                        more: true,
                    },
                ),
                (
                    "a9".to_owned(),
                    Branch::Loaded {
                        children: vec![agent("a10", "/p", Some("a9"))],
                        more: false,
                    },
                ),
            ]),
            expanded: HashSet::from(["a1".to_owned(), "a9".to_owned()]),
            ..View::default()
        };
        let section = &view.sections(&listed, &[])[0];
        assert_eq!(ids(section), [("a1", 0), ("a2", 1), ("a9", 1), ("a10", 2)]);
        assert_eq!(section.rows[0].children, 2);
        assert_eq!(section.rows[0].note.as_deref(), Some("more children exist"));
        assert_eq!(section.rows[2].note, None);
    }

    #[test]
    fn an_unlisted_manager_leaves_its_child_at_the_top_level() {
        let listed = vec![agent("a5", "/p", Some("a1")), agent("a6", "/p", None)];
        let section = &View::default().sections(&listed, &[])[0];
        assert_eq!(ids(section), [("a5", 0), ("a6", 0)]);
    }

    #[test]
    fn children_render_in_child_id_order_whatever_the_fetch_order() {
        // The listed fallback arrives newest-first; the fetched branch
        // arrives in its own order. Both render by child ID.
        let listed = vec![
            agent("a1", "/p", None),
            agent("a10", "/p", Some("a1")),
            agent("a2", "/p", Some("a1")),
            agent("a9", "/p", Some("a1")),
        ];
        let mut view = View::default();
        view.expanded.insert("a1".to_owned());
        let section = &view.sections(&listed, &[])[0];
        assert_eq!(ids(section), [("a1", 0), ("a2", 1), ("a9", 1), ("a10", 1)]);
        view.branches = HashMap::from([(
            "a1".to_owned(),
            Branch::Loaded {
                children: vec![
                    agent("a10", "/p", Some("a1")),
                    agent("a2", "/p", Some("a1")),
                    agent("a9", "/p", Some("a1")),
                ],
                more: false,
            },
        )]);
        let section = &view.sections(&listed, &[])[0];
        assert_eq!(ids(section), [("a1", 0), ("a2", 1), ("a9", 1), ("a10", 1)]);
    }

    #[test]
    fn the_pinned_open_agent_leaves_the_sections() {
        // An old stopped task would fold under "Show N older"; pinned, it
        // leaves the section rows and the older count.
        fn shown<'x>(
            branches: &'x HashMap<String, Branch>,
            expanded: &'x HashSet<String>,
            collapsed: &'x HashSet<String>,
            unfolded: &'x HashSet<String>,
            pinned: Option<&'x str>,
        ) -> Shown<'x> {
            Shown {
                branches,
                expanded,
                collapsed,
                unfolded,
                today_start: 1_000_000,
                only: None,
                pinned,
            }
        }
        let branches = HashMap::new();
        let expanded = HashSet::new();
        let collapsed = HashSet::new();
        let unfolded = HashSet::new();
        let listed = vec![
            with(agent("a2", "/p", None), "busy", 3),
            with(agent("a1", "/p", None), "stopped", 2),
        ];
        let organization = Organization::default();
        let plain = sections(
            &listed,
            &organization,
            &[],
            &shown(&branches, &expanded, &collapsed, &unfolded, None),
        );
        assert_eq!(plain[0].older, 1);
        let pinned = sections(
            &listed,
            &organization,
            &[],
            &shown(&branches, &expanded, &collapsed, &unfolded, Some("a1")),
        );
        assert_eq!(pinned[0].older, 0);
        assert_eq!(
            pinned[0]
                .rows
                .iter()
                .map(|row| row.agent.id.as_str())
                .collect::<Vec<_>>(),
            ["a2"]
        );
    }

    #[test]
    fn expanded_branch_states_show_as_notes() {
        let listed = vec![agent("a1", "/p", None)];
        let mut view = View::default();
        view.expanded.insert("a1".to_owned());
        for (branch, note) in [
            (Branch::Loading, "loading children…"),
            (Branch::Failed("bad".to_owned()), "children not read: bad"),
        ] {
            view.branches = HashMap::from([("a1".to_owned(), branch)]);
            let row = &view.sections(&listed, &[])[0].rows[0];
            assert_eq!(row.note.as_deref(), Some(note));
            assert!(row.expandable());
        }
    }

    #[test]
    fn a_repeated_id_is_shown_once() {
        let listed = vec![agent("a1", "/p", None)];
        let view = View {
            branches: HashMap::from([(
                "a1".to_owned(),
                Branch::Loaded {
                    children: vec![agent("a1", "/p", Some("a1"))],
                    more: false,
                },
            )]),
            expanded: HashSet::from(["a1".to_owned()]),
            ..View::default()
        };
        assert_eq!(ids(&view.sections(&listed, &[])[0]), [("a1", 0)]);
    }

    #[test]
    fn repositories_are_named_by_their_common_directory() {
        assert_eq!(
            repository(r"\\?\C:\src\eliot-swarm-controller\.git"),
            (
                "eliot-swarm-controller".to_owned(),
                Some(r"C:\src\eliot-swarm-controller".to_owned())
            )
        );
        assert_eq!(
            repository("/srv/git/tools.git/"),
            ("tools".to_owned(), None)
        );
        assert_eq!(repository("/srv/git/plain"), ("plain".to_owned(), None));
    }

    #[test]
    fn registrations_count_the_tasks_that_use_them() {
        let git = r"C:\src\app\.git";
        let checkouts = vec![
            checkout(r"C:\src\app", git),
            checkout(r"C:\wt\legacy", git),
            // An unregistered worktree the desktop resolved.
            checkout(r"C:\wt\fresh", git),
            checkout(r"C:\src\web", r"C:\src\web\.git"),
        ];
        let registrations = vec![
            ("app".to_owned(), r"C:\src\app".to_owned()),
            ("legacy".to_owned(), r"C:\wt\legacy".to_owned()),
            ("web".to_owned(), r"C:\src\web".to_owned()),
            ("gone".to_owned(), r"C:\wt\gone".to_owned()),
        ];
        let mut chat = agent("a6", r"C:\src\web", None);
        chat.chat = true;
        let agents = vec![
            with(agent("a1", r"C:\src\app", None), "stopped", 0),
            with(agent("a2", r"C:\wt\legacy", None), "busy", 0),
            // The daemon resolves an unregistered worktree to the main
            // checkout's registration.
            with(agent("a3", r"C:\wt\fresh", None), "idle", 0),
            with(agent("a4", r"C:\scratch", None), "idle", 0),
            with(agent("a1", r"C:\src\app", None), "stopped", 0),
            chat,
        ];
        let usage = registration_use(&registrations, &agents, &checkouts);
        assert_eq!(usage["app"], Use { live: 1, tasks: 2 });
        assert_eq!(usage["legacy"], Use { live: 1, tasks: 1 });
        assert_eq!(usage["web"], Use::default());
        assert_eq!(usage["gone"], Use::default());

        // Without a main checkout registration, an unregistered worktree's
        // task keeps every registration of its repository in use.
        let registrations = vec![("legacy".to_owned(), r"C:\wt\legacy".to_owned())];
        let usage = registration_use(&registrations, &agents[2..3], &checkouts);
        assert_eq!(usage["legacy"], Use { live: 1, tasks: 1 });
    }

    #[test]
    fn a_search_label_names_the_worktree_and_lists_matches_in_their_group() {
        let git = r"C:\src\app\.git";
        let mut worktree = checkout(r"C:\wt\w1", git);
        worktree.branch = Some("feat/k9-search".to_owned());
        let checkouts = vec![checkout(r"C:\src\app", git), worktree];
        let mut found = agent("a1", r"C:\wt\w1", None);
        found.title = "needle".to_owned();
        found.organization = Some("app".to_owned());
        let listed = vec![found, agent("a2", r"C:\src\app", None)];
        let needle = |agent: &UiAgent| agent.title == "needle";
        let sections = View::default().filtered(&listed, &checkouts, Some(&needle));
        assert_eq!(sections.len(), 1);
        assert_eq!(sections[0].key, "org:app");
        assert_eq!(ids(&sections[0]), [("a1", 0)]);
        assert_eq!(
            search(&listed, &checkouts, "needle")[0].0,
            "needle · app › k9 search · muse"
        );
    }

    #[test]
    fn running_tasks_lead_then_newest_and_older_stopped_tasks_fold() {
        let listed = vec![
            with(agent("a1", "/p", None), "stopped", 10),
            with(agent("a2", "/p", None), "busy", 5),
            with(agent("a3", "/p", None), "idle", 300),
            with(agent("a4", "/p", None), "failed", 20),
            with(agent("a5", "/p", None), "stopped", 200),
            // An old idle task is not stopped: it stays.
            with(agent("a6", "/p", None), "idle", 30),
        ];
        let mut view = View {
            today_start: 100,
            ..View::default()
        };
        let folded = &view.sections(&listed, &[])[0];
        // Live before ended: idle a6 (older) precedes stopped a5 (newer).
        assert_eq!(ids(folded), [("a2", 0), ("a3", 0), ("a6", 0), ("a5", 0)]);
        assert_eq!((folded.older, folded.older_shown), (2, false));

        view.unfolded.insert(folded.key.clone());
        let unfolded = &view.sections(&listed, &[])[0];
        assert_eq!(
            ids(unfolded),
            [
                ("a2", 0),
                ("a3", 0),
                ("a6", 0),
                ("a5", 0),
                ("a4", 0),
                ("a1", 0)
            ]
        );
        assert_eq!((unfolded.older, unfolded.older_shown), (2, true));
    }

    #[test]
    fn a_project_with_only_older_tasks_shows_its_newest_ones() {
        let listed: Vec<UiAgent> = (1..=5)
            .map(|index| {
                with(
                    agent(&format!("a{index}"), "/p", None),
                    "stopped",
                    index * 10,
                )
            })
            .collect();
        let view = View {
            today_start: 100,
            ..View::default()
        };
        let section = &view.sections(&listed, &[])[0];
        // Never empty above "Show N older": the newest three stay.
        assert_eq!(ids(section), [("a5", 0), ("a4", 0), ("a3", 0)]);
        assert_eq!((section.older, section.older_shown), (2, false));
        let few = &view.sections(&listed[..2], &[])[0];
        assert_eq!(ids(few), [("a2", 0), ("a1", 0)]);
        assert_eq!(few.older, 0);
    }

    #[test]
    fn a_collapsed_project_keeps_only_the_servers_running_count() {
        let mut listed = vec![
            with(agent("a1", "/p", None), "busy", 0),
            with(agent("a2", "/p", Some("a1")), "starting", 0),
            with(agent("a3", "/p", None), "idle", 0),
        ];
        for agent in &mut listed {
            agent.organization = Some("app".to_owned());
        }
        let mut view = View {
            organization: Organization {
                groups: vec![group("app", 3)],
                overview: vec![overview("app", 2, 1)],
                ..Organization::default()
            },
            ..View::default()
        };
        let key = view.sections(&listed, &[])[0].key.clone();
        assert_eq!(key, "org:app");
        view.collapsed.insert(key);
        let section = &view.sections(&listed, &[])[0];
        assert!(section.collapsed);
        assert!(section.rows.is_empty());
        // busy + starting from `project.overview`, not the listed rows.
        assert_eq!(section.running, 3);
    }

    #[test]
    fn a_filter_lists_matches_flat_and_drops_empty_projects() {
        let mut child = with(agent("a3", "/p", Some("a1")), "unknown", 0);
        child.native_child = true;
        let listed = vec![
            with(agent("a1", "/p", None), "busy", 0),
            with(agent("a2", "/p", Some("a1")), "waiting", 0),
            child,
            with(agent("a4", "/q", None), "unknown", 0),
            with(agent("a5", "/q", None), "lost", 0),
            with(agent("a6", "/r", None), "idle", 0),
        ];
        let mut listed = listed;
        for agent in &mut listed {
            agent.organization = Some(agent.project_id.trim_start_matches('/').to_owned());
        }
        let mut view = View::default();
        // Collapsing never hides a match.
        view.collapsed.insert("org:p".to_owned());
        let attention = |agent: &UiAgent| agent.needs_attention();
        let sections = view.filtered(&listed, &[], Some(&attention));
        let flat: Vec<Vec<(&str, usize)>> = sections.iter().map(ids).collect();
        // A lost task needs a decision too (Resume, why or archive).
        assert_eq!(flat, [vec![("a2", 0)], vec![("a4", 0), ("a5", 0)]]);
        assert_eq!(
            Fleet::of(&listed),
            Fleet {
                running: 1,
                attention: 3,
                idle: 1,
            }
        );
    }

    #[test]
    fn search_matches_every_word_in_title_project_or_harness() {
        let git = r"\\?\C:\src\app\.git";
        let checkouts = vec![checkout(r"\\?\C:\wt\wt-k1", git)];
        let mut first = with(agent("a1", r"\\?\C:\wt\wt-k1", None), "idle", 10);
        first.title = "k75: sidebar titles".to_owned();
        first.harness = "codex".to_owned();
        let mut second = with(agent("a2", "/x", None), "busy", 5);
        second.title = "k75: sidebar titles".to_owned();
        let mut third = with(agent("a3", "/x", None), "idle", 50);
        third.title = "unrelated".to_owned();
        let listed = vec![first, second, third];
        assert_eq!(
            search(&listed, &checkouts, "k75 app"),
            [(
                "k75: sidebar titles · app › wt-k1 · codex".to_owned(),
                "a1".to_owned()
            )]
        );
        let both: Vec<String> = search(&listed, &checkouts, "SIDEBAR")
            .into_iter()
            .map(|(_, id)| id)
            .collect();
        // Running first.
        assert_eq!(both, ["a2", "a1"]);
        assert!(search(&listed, &checkouts, "muse")[0].0.ends_with("muse"));
        assert!(search(&listed, &checkouts, "  ").is_empty());
    }

    #[test]
    fn repeated_search_labels_name_their_ids() {
        let listed = vec![agent("a1", "/x", None), agent("a2", "/x", None)];
        let mut same = listed.clone();
        for agent in &mut same {
            agent.title = "same".to_owned();
        }
        let labels: Vec<String> = search(&same, &[], "same")
            .into_iter()
            .map(|(label, _)| label)
            .collect();
        assert_eq!(labels, ["same · x · muse · a1", "same · x · muse · a2"]);
    }

    #[test]
    fn the_fleet_line_says_what_is_live() {
        let quiet = Fleet::default();
        let none = Archived::default();
        assert_eq!(quiet.summary(0, none), "No Agents yet");
        // Stopped Agents are not live: nothing to count.
        assert_eq!(quiet.summary(35, none), "All quiet");
        // k98: an archived Agent that is still live is counted.
        let idle = with(agent("a1", "/p", None), "idle", 1);
        let stopped = with(agent("a2", "/p", None), "stopped", 1);
        let archived = Archived::of(&[idle.clone(), stopped], 2);
        assert_eq!(
            archived,
            Archived {
                live: 1,
                running: 0,
                total: 2
            }
        );
        assert_eq!(quiet.summary(0, archived), "1 idle");
        assert_eq!(quiet.summary(0, Archived::of(&[], 2)), "All quiet");
        let busy_archived = Archived::of(&[with(idle, "busy", 1)], 1);
        assert_eq!(quiet.summary(0, busy_archived), "1 working");
        let busy = Fleet {
            running: 2,
            attention: 1,
            idle: 3,
        };
        assert_eq!(
            busy.summary(40, none),
            "2 working · 1 needs attention · 3 idle"
        );
        // The sidebar line keeps the two most urgent counts.
        assert_eq!(busy.compact(40, none), "2 working · 1 needs attention");
        let calm = Fleet {
            running: 2,
            attention: 0,
            idle: 12,
        };
        assert_eq!(calm.compact(40, none), "2 working · 12 idle");
        let waiting = Fleet {
            running: 0,
            attention: 3,
            idle: 12,
        };
        assert_eq!(waiting.compact(40, none), "3 need attention · 12 idle");
        assert_eq!(quiet.compact(0, none), "No Agents yet");
        assert_eq!(quiet.compact(35, none), "All quiet");
        // Failed and lost need a decision; a native child never counts.
        let fleet = Fleet::of(&[
            with(agent("a3", "/p", None), "lost", 1),
            with(agent("a4", "/p", None), "failed", 1),
            with(agent("a5", "/p", None), "idle", 1),
            {
                let mut child = with(agent("a6", "/p", Some("a5")), "unknown", 1);
                child.native_child = true;
                child
            },
        ]);
        assert_eq!((fleet.attention, fleet.idle, fleet.running), (2, 1, 0));
    }

    #[test]
    fn recents_are_newest_first_and_bounded() {
        let mut listed = vec![
            agent("a1", "p", None),
            agent("a2", "p", None),
            agent("a3", "p", None),
        ];
        listed[0].updated_at = 10;
        listed[1].updated_at = 30;
        listed[2].updated_at = 20;
        let all = |_: &UiAgent| true;
        let ids: Vec<&str> = recents(&listed, 2, &all)
            .into_iter()
            .map(|agent| agent.id.as_str())
            .collect();
        assert_eq!(ids, ["a2", "a3"]);
        let mut repeated = listed.clone();
        repeated.push(UiAgent {
            updated_at: 5,
            ..listed[1].clone()
        });
        let ids: Vec<&str> = recents(&repeated, 9, &all)
            .into_iter()
            .map(|agent| agent.id.as_str())
            .collect();
        assert_eq!(ids, ["a2", "a3", "a1"]);
        // A prompt run three times shows once, its newest run.
        let mut runs = listed.clone();
        for (index, run) in runs.iter_mut().enumerate() {
            run.title = if index == 0 {
                "Fix the build".to_owned()
            } else {
                "Reply with exactly OK".to_owned()
            };
        }
        let ids: Vec<&str> = recents(&runs, 9, &all)
            .into_iter()
            .map(|agent| agent.id.as_str())
            .collect();
        assert_eq!(ids, ["a2", "a1"]);
    }

    #[test]
    fn sidebar_recents_hold_only_collapsed_projects_tasks() {
        let mut listed = vec![
            with(agent("a1", "C:\\w\\atlas", None), "stopped", 10),
            with(agent("a2", "C:\\w\\atlas-wt-1", None), "idle", 30),
            with(agent("a3", "C:\\w\\other", None), "stopped", 20),
            with(agent("a4", "C:\\chats\\a4", None), "idle", 40),
        ];
        listed[0].organization = Some("atlas".to_owned());
        listed[1].organization = Some("atlas".to_owned());
        listed[3].chat = true;
        // Nothing collapsed: the list shows every task, Recents is empty
        // (older tasks stay folded under their project).
        assert!(sidebar_recents(&listed, &HashSet::new(), 8).is_empty());
        let key = group_key(Some("atlas"));
        let collapsed: HashSet<&str> = [key.as_str()].into_iter().collect();
        let ids: Vec<&str> = sidebar_recents(&listed, &collapsed, 8)
            .into_iter()
            .map(|agent| agent.id.as_str())
            .collect();
        // Every task the daemon reads in the collapsed project; the bucket
        // (a legacy label and the chat) stays where it is.
        assert_eq!(ids, ["a2", "a1"]);
    }

    #[test]
    fn native_children_nest_only_and_stay_out_of_recents_and_chats_read_in_their_group() {
        let parent = with(agent("a1", "/w", None), "idle", 10);
        let mut child = with(agent("a2", "/w", Some("a1")), "unknown", 50);
        child.native_child = true;
        child.title = "codex subagent · adhoc-codex".to_owned();
        let mut orphan = with(agent("a3", "/w", Some("a9")), "running", 40);
        orphan.native_child = true;
        let mut chat = with(agent("a4", "/home/Eliot/chats/a4", None), "idle", 30);
        chat.chat = true;
        // Swarm workers stay out of Recents too, newest or not, whether
        // their manager is listed (a1) or not (a9).
        let worker = with(agent("a5", "/w", Some("a1")), "idle", 60);
        let far_worker = with(agent("a6", "/w", Some("a9")), "idle", 70);
        let listed = vec![parent, child, orphan, chat, worker, far_worker];
        let recent: Vec<&str> = recents(&listed, 8, &|_| true)
            .iter()
            .map(|a| a.id.as_str())
            .collect();
        assert_eq!(recent, ["a4", "a1"]);
        let sections = View::default().sections(&listed, &[]);
        // Everything here reads in the no-project bucket, the unattached
        // chat included; a worker whose manager is not listed stays a
        // top-level row.
        assert_eq!(sections.len(), 1);
        assert_eq!(sections[0].key, OTHER_LOCATIONS);
        assert_eq!(ids(&sections[0]), [("a6", 0), ("a4", 0), ("a1", 0)]);
        assert_eq!(
            sections[0].rows[2].children, 2,
            "the native child and the worker nest under a1"
        );
    }

    fn registration(id: &str) -> eliot_core::api::ProjectRegistrationView {
        eliot_core::api::ProjectRegistrationView {
            id: id.to_owned(),
            checkout: format!("/src/{id}"),
            repo_common_dir: format!("/src/{id}/.git"),
            profiles: Vec::new(),
            actions: Vec::new(),
            interpreters: Default::default(),
            run_env: Vec::new(),
            git: None,
            revision: 1,
            origin: None,
            checkouts: Vec::new(),
        }
    }

    fn group(project: &str, members: usize) -> OrgGroupView {
        OrgGroupView {
            project: Some(project.to_owned()),
            registration: Some(registration(project)),
            assignment: Some(eliot_core::api::ProjectAssignment {
                project: project.to_owned(),
                manager: None,
                epoch: 0,
                revision: 1,
                state: ProjectAssignmentState::Vacant,
                workspace: None,
                start_command_id: None,
            }),
            needs_assignment: false,
            conflicting_roots: Vec::new(),
            manager: None,
            transfer: None,
            members: Vec::new(),
            member_total: members,
            members_truncated: false,
        }
    }

    fn bucket(members: usize) -> OrgGroupView {
        OrgGroupView {
            project: None,
            registration: None,
            assignment: None,
            needs_assignment: false,
            conflicting_roots: Vec::new(),
            manager: None,
            transfer: None,
            members: Vec::new(),
            member_total: members,
            members_truncated: false,
        }
    }

    fn overview(project: &str, busy: usize, starting: usize) -> ProjectOverviewEntry {
        ProjectOverviewEntry {
            project: project.to_owned(),
            detail: true,
            manager: None,
            assignment: None,
            native_child_total: 0,
            workers: Vec::new(),
            worker_total: 0,
            workers_truncated: false,
            workspaces: Vec::new(),
            workspace_total: 0,
            workspaces_truncated: false,
            queue: 0,
            totals: eliot_core::api::ProjectTotals {
                live: busy,
                lifecycle: eliot_core::api::LifecycleTotals {
                    busy,
                    starting,
                    ..Default::default()
                },
                ..Default::default()
            },
            capacity: Vec::new(),
        }
    }

    #[test]
    fn sections_are_the_daemons_groups_with_the_rows_it_placed() {
        // Registered projects in the daemon's order (an empty one too), an
        // unregistered label's empty group skipped, the bucket last.
        let mut label = bucket(0);
        label.project = Some("/legacy/wt-k1".to_owned());
        let organization = Organization {
            groups: vec![label, group("alpha", 2), group("beta", 0), bucket(2)],
            ..Organization::default()
        };
        let mut attached = with(agent("a1", "", None), "idle", 30);
        attached.chat = true;
        attached.organization = Some("alpha".to_owned());
        let mut placed = with(agent("a2", "alpha", None), "idle", 20);
        placed.organization = Some("alpha".to_owned());
        // A legacy `wt-*` label the daemon does not link stays in the
        // bucket, whatever its path says.
        let legacy = with(agent("a3", "/legacy/wt-k1", None), "idle", 10);
        let mut loose = with(agent("a4", "", None), "idle", 5);
        loose.chat = true;
        // A row of a group this read did not return keeps its own group.
        let mut stale = with(agent("a5", "gamma", None), "idle", 1);
        stale.organization = Some("gamma".to_owned());
        let listed = vec![attached, placed, legacy, loose, stale];
        let view = View {
            organization,
            ..View::default()
        };
        let sections = view.sections(&listed, &[]);
        let keys: Vec<&str> = sections.iter().map(|s| s.key.as_str()).collect();
        assert_eq!(
            keys,
            ["org:alpha", "org:beta", "org:gamma", OTHER_LOCATIONS]
        );
        assert_eq!(ids(&sections[0]), [("a1", 0), ("a2", 0)]);
        assert!(sections[1].rows.is_empty());
        assert_eq!(ids(&sections[2]), [("a5", 0)]);
        assert_eq!(ids(&sections[3]), [("a3", 0), ("a4", 0)]);
        assert_eq!(sections[3].name, "No project");
        // Exact server totals, not the listed rows.
        assert_eq!(sections[0].detail, ["No manager · 2 tasks"]);
        assert_eq!(sections[3].detail, ["2 tasks"]);
    }

    #[test]
    fn group_detail_names_manager_branch_assignment_roots_and_totals() {
        let mut team = group("app", 7);
        team.assignment.as_mut().unwrap().manager = Some(ProjectManagerIdentity::Local {
            agent: "a10".to_owned(),
            generation: 1,
        });
        team.assignment.as_mut().unwrap().state = ProjectAssignmentState::Active;
        team.manager = Some(eliot_core::api::OverviewManagerView {
            agent: "a10".to_owned(),
            generation: 1,
            state: AgentState::Busy,
            assignment_revision: 2,
            workspace: Some("/wt/app-k1".to_owned()),
        });
        team.transfer = Some("01JTRANSFER".to_owned());
        let mut entry = overview("app", 2, 0);
        entry.totals.queued = 1;
        entry.workspaces = vec![eliot_core::api::ManagedWorkspaceView {
            agent: "a10".to_owned(),
            project: "app".to_owned(),
            repo_path: "/src/app".to_owned(),
            branch: "feat/k1-standard".to_owned(),
            worktree_path: "/wt/app-k1".to_owned(),
            phase: eliot_core::WorkspacePhase::Ready,
            updated_at: 0,
        }];
        entry.capacity = vec![
            eliot_core::api::ScopeUsage {
                scope: "project:app".to_owned(),
                usage: 2,
                limit: Some(4),
            },
            eliot_core::api::ScopeUsage {
                scope: "manager_assignment:app".to_owned(),
                usage: 2,
                limit: None,
            },
        ];
        assert_eq!(
            group_detail(&team, Some(&entry), &[]),
            [
                "Manager a10 · busy · feat/k1-standard · transfer in progress",
                "7 tasks · 2 live · 2 working · 1 queued · 2/4 slots",
            ]
        );
        // No manager and several roots: shown plainly, never auto-assigned.
        let mut vacant = group("app", 3);
        vacant.needs_assignment = true;
        vacant.conflicting_roots = ["a2", "a3", "a4", "a5"].map(str::to_owned).to_vec();
        assert_eq!(
            group_detail(&vacant, None, &[]),
            [
                "Needs assignment",
                "Competing roots: a2, a3, a4 +1",
                "3 tasks"
            ]
        );
        vacant.assignment.as_mut().unwrap().state = ProjectAssignmentState::Blocked;
        vacant.conflicting_roots.truncate(1);
        assert_eq!(
            group_detail(&vacant, None, &[])[..2],
            ["Needs assignment · blocked", "Competing root: a2"]
        );
    }

    #[test]
    fn waits_show_the_reason_and_who_can_change_it_in_their_group() {
        let reason = |code: &str, scope: &str, by: &str| LaunchWaitReason {
            code: code.to_owned(),
            scope: scope.to_owned(),
            changed_by: by.to_owned(),
        };
        let intent = |id: &str, project: &str, state, reason| eliot_core::api::LaunchIntentView {
            id: id.to_owned(),
            project: project.to_owned(),
            state,
            command_id: None,
            reason,
        };
        let queue = LaunchQueueView {
            interactive: vec![
                intent(
                    "i1",
                    "app",
                    LaunchIntentState::Queued,
                    Some(reason("capacity_unavailable", "global:swarm", "owner")),
                ),
                intent("i2", "", LaunchIntentState::Queued, None),
                intent("i3", "app", LaunchIntentState::Admitted, None),
            ],
            assignments: vec![eliot_core::api::AssignmentWaitView {
                rule: "nightly".to_owned(),
                project: "app".to_owned(),
                due_at: 0,
                reason: reason(
                    "capacity_unavailable",
                    "manager_assignment:app",
                    "owner_or_delegated_manager",
                ),
            }],
            next_after: None,
        };
        let registered: HashSet<&str> = ["app"].into_iter().collect();
        assert_eq!(
            waits(Some(&queue), Some("app"), &registered),
            [
                "Queued launch: Swarm capacity is full; the Owner can change the limit",
                "Automation nightly waits: the manager budget is full; the Owner or the \
                 project manager can change the limit",
            ]
        );
        assert_eq!(
            waits(Some(&queue), None, &registered),
            ["Queued launch: waiting for its turn"]
        );
        // The daemon omits an empty bucket; its queued launch still lists.
        let view = View {
            organization: Organization {
                groups: vec![group("app", 0)],
                queue: Some(queue.clone()),
                ..Organization::default()
            },
            ..View::default()
        };
        let sections = view.sections(&[], &[]);
        assert_eq!(sections.len(), 2);
        assert_eq!(sections[1].key, OTHER_LOCATIONS);
        assert_eq!(sections[1].waits, ["Queued launch: waiting for its turn"]);
        assert_eq!(
            wait_reason_words(&reason("workspace_unavailable", "workspace", "owner")),
            "the workspace is unavailable; the Owner can change this"
        );
    }

    #[test]
    fn tags_are_trimmed_and_empty_ones_dropped() {
        assert_eq!(parse_tags(" inbox, k175 ,, "), ["inbox", "k175"]);
        assert!(parse_tags(" , ").is_empty());
    }

    #[test]
    fn titles_take_the_first_line_and_cut_at_a_word() {
        assert_eq!(
            title("\n  Fix   the build \nmore"),
            Some("Fix the build".to_owned())
        );
        assert_eq!(title("   \n "), None);
        let long = "Read docs/spec/api.md completely and list every method that the desktop does not call yet";
        let cut = title(long).unwrap();
        assert!(cut.ends_with('…'), "{cut}");
        assert!(cut.chars().count() <= 61, "{cut}");
        assert!(long.starts_with(cut.trim_end_matches('…')));
        let unbroken = "x".repeat(80);
        assert_eq!(title(&unbroken).unwrap().chars().count(), 61);
    }

    #[test]
    fn titles_drop_markdown_marks_and_shorten_absolute_paths() {
        for (text, expected) in [
            ("# k58: desktop window size", "k58: desktop window size"),
            ("## **Bold** and `code` title ##", "Bold and code title"),
            ("- [ ] first *task*\n- second", "first task"),
            ("1. step one\n2. step two", "step one"),
            ("> quoted _line_\n\nnext", "quoted line"),
            (
                "---\nbrief: k1\n---\n\n# After front matter",
                "After front matter",
            ),
            ("```\ncode only\n```\n\nThen prose", "Then prose"),
            ("<!-- note -->\n\nAfter HTML", "After HTML"),
            (
                "See [the spec](https://example.com/spec) now",
                "See the spec now",
            ),
            (
                r"Read and follow exactly C:\briefs\20261116\k75-titles.md . Do it.",
                "Read and follow exactly k75-titles.md . Do it.",
            ),
            (
                "Edit (/home/me/app/src/main.rs), not src/lib.rs",
                "Edit (main.rs), not src/lib.rs",
            ),
        ] {
            assert_eq!(title(text).as_deref(), Some(expected), "{text:?}");
        }
        assert_eq!(title("```\nonly code\n```"), None);
        assert_eq!(title("# \n\n---\n"), None);
    }

    fn summary(preview: Option<&str>, name: Option<&str>, profile: Option<&str>) -> AgentSummary {
        AgentSummary {
            archived: false,
            chat: false,

            id: "a7".to_owned(),
            generation: 1,
            project: "/p".to_owned(),
            repo_common_dir: None,
            workspace: None,
            harness: "codex".to_owned(),
            name: name.map(str::to_owned),
            profile: profile.map(str::to_owned),
            preview: preview.map(str::to_owned),
            state: AgentState::Idle,
            parent: None,
            root: "a7".to_owned(),
            active_turn: None,
            last_progress_at: None,
            progress_age_ms: None,
            last_turn_id: None,
            last_outcome: None,
            last_resolution_source: None,
            activity: None,
            usage: None,
            usage_scope: None,
            updated_at: 0,
            project_scope: None,
            heavy: None,
            session_growth: None,
            organization: None,
            tags: Vec::new(),
        }
    }

    #[test]
    fn summary_titles_prefer_the_first_input_then_the_name_then_the_profile() {
        let full = summary(
            Some("# k75: titles\n\nbody"),
            Some("planner"),
            Some("codex-lite"),
        );
        assert_eq!(summary_title(&full), "k75: titles");
        let named = summary(Some("```\n```"), Some("planner"), Some("codex-lite"));
        assert_eq!(summary_title(&named), "planner");
        let bare = summary(None, None, Some("codex-lite"));
        assert_eq!(summary_title(&bare), "codex · codex-lite");
        let unknown = summary(None, None, None);
        assert_eq!(summary_title(&unknown), "codex · a7");
        let agent = UiAgent::from_summary(&full);
        assert_eq!(
            (agent.title.as_str(), agent.state, agent.native_child),
            ("k75: titles", AgentState::Idle, false)
        );
    }

    #[test]
    fn agent_lifecycle_is_carried_as_the_typed_state() {
        let ready = with(agent("a1", "/p", None), "idle", 0);
        assert_eq!(ready.state, AgentState::Idle);
        assert!(ready.idle());
        assert!(ready.live());
        assert!(!ready.ended());

        let failed = with(agent("a2", "/p", None), "failed", 0);
        assert!(failed.needs_attention());
        assert!(failed.ended());
        assert!(!failed.live());
    }

    #[test]
    fn display_paths_drop_only_the_verbatim_drive_prefix() {
        assert_eq!(display_path(r"\\?\C:\src\app"), r"C:\src\app");
        assert_eq!(display_path(r"C:\src\app"), r"C:\src\app");
        assert_eq!(display_path(r"\\?\UNC\host\share"), r"\\?\UNC\host\share");
        assert_eq!(display_path("/home/me/app"), "/home/me/app");
    }

    #[cfg(windows)]
    #[test]
    fn path_keys_compare_like_the_daemon_on_windows() {
        assert_eq!(path_key(r"\\?\C:\Src\App\"), path_key("c:/src/app"));
        assert_ne!(path_key(r"C:\src\app"), path_key(r"C:\src\apple"));
    }

    #[test]
    fn from_summary_keeps_project_id_separate_from_workspace() {
        let mut full = summary(None, None, Some("codex-lite"));
        full.project = "p".to_owned();
        full.workspace = Some("/repo".to_owned());
        full.project_scope = Some("/repo".to_owned());
        let agent = UiAgent::from_summary(&full);
        assert_eq!(agent.project_id, "p");
        assert_eq!(agent.workspace.as_deref(), Some("/repo"));
        // The server compares the scope, not the project ID: same ID with
        // a different scope is a different authorization namespace.
        assert_eq!(agent.scope(), Some("/repo"));
        let mut legacy = summary(None, None, Some("codex-lite"));
        legacy.project = "p".to_owned();
        let fallback = UiAgent::from_summary(&legacy);
        assert_eq!(fallback.scope(), Some("p"));
        // A pending row keeps its submitted ID but its scope is unknown:
        // the pickers must not treat the ID as the scope.
        let mut pending = fallback.clone();
        pending.pending = true;
        assert_eq!(pending.scope(), None);
    }
}
