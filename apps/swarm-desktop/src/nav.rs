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
//! (#657 G): each registered project with its tasks, then the
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
    AgentSummary, LaunchIntentState, LaunchQueueView, LaunchWaitReason, OrgGroupView, OrgRowChange,
};
use pulldown_cmark::{Event, Options, Parser, Tag, TagEnd};

/// UI copy of one Agent row: identity, generation, liveness and placement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UiAgent {
    pub id: String,
    pub generation: i64,
    pub harness: String,
    pub state: eliot_core::AgentState,
    pub status_line: Option<String>,
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
    /// The overlay link revision the row's placement and tags were read
    /// with (`None` for no overlay row); a menu write carries it back.
    pub organization_revision: Option<i64>,
    /// The model the harness last reported ([`AgentSummary::model`]);
    /// `None` until it reports one.
    pub model: Option<String>,
    /// The Git-aware work summary ([`AgentSummary::work`]): the running
    /// row's work line. `None` on an older daemon and for synthetic rows.
    /// Boxed: rows travel inside messages.
    pub work: Option<Box<eliot_core::api::WorkSummaryView>>,
}

impl UiAgent {
    pub fn from_summary(summary: &AgentSummary) -> Self {
        Self {
            id: summary.id.clone(),
            generation: summary.generation,
            harness: summary.harness.clone(),
            state: summary.state,
            status_line: eliot_core::api::agent_status_line(
                summary.active_turn.as_deref(),
                summary.provider_hold.as_ref(),
                summary.native_notice.as_ref(),
                crate::handoff::now_ms(),
            ),
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
            organization_revision: summary.organization_revision,
            model: summary.model.clone(),
            work: summary.work.clone().map(Box::new),
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
        None => (folder_name(&display_path(label)).to_owned(), None),
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

pub use eliot_core::display_path;

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
    let shown = display_path(common_dir);
    let shown = shown.trim_end_matches(['\\', '/']);
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
pub const NO_PROJECT_NAME: &str = "No project";

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
        None => {
            let fallback = display_path(agent.workspace.as_deref().unwrap_or(&agent.project_id));
            folder_name(&fallback).to_owned()
        }
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
/// them: groups with their headers and exact totals from
/// `chat.organization`, and waits from `launch.queue`. A failed read keeps
/// the last server grouping with a stale marker; nothing here is regrouped
/// or recounted.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Organization {
    pub groups: Vec<OrgGroupView>,
    pub queue: Option<LaunchQueueView>,
    /// The current state of the intents this window queued
    /// (`launch.intent.get`), so an admission or refusal is reported.
    pub intents: Vec<eliot_core::api::LaunchIntentView>,
    /// Watched intents the daemon does not know (`not_found`): an
    /// unconfirmed Queue it that never committed.
    pub missing_intents: Vec<String>,
    /// The grouping read's error lines (the stale marker among them).
    pub errors: Vec<String>,
    /// The queue read's error lines, replaced on every queue read.
    pub queue_errors: Vec<String>,
    /// The grouping read failed: headers show the last server grouping
    /// with a stale indicator, never a synthesized one.
    pub stale: bool,
    /// The exclusive `organization.changed` cursor: rows committed after
    /// it refresh the loaded Agents by id. Starts at the first projection
    /// read's event watermark.
    pub changes_after: Option<i64>,
    /// The `organization.changed` rows the last read returned, applied to
    /// the loaded Agents once ([`apply_changes`]).
    pub changes: Vec<OrgRowChange>,
}

impl Organization {
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
    /// The registered checkout's project name ([`place_of`]), else the
    /// project label's; "No project" for the bucket.
    pub name: String,
    /// A linked worktree's subproject name ([`place_of`]): the header
    /// shows it beside the name, so sibling worktrees of one repository
    /// never trim to the same text.
    pub subproject: Option<String>,
    /// Where a new task in it starts: the registered checkout.
    pub checkout: String,
    pub checkouts: Vec<String>,
    /// Queued launches and automation waits with their reasons ([`waits`]).
    pub waits: Vec<String>,
    /// Visible task rows, newest work first.
    pub rows: Vec<Row>,
    /// Its tasks working now, counted over the listed rows' live states
    /// (so the count moves with each state event, not with a team read).
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
    /// The open Agent: it stays in view inside its own project (never
    /// folded under Show older, never hoisted out).
    pub open: Option<&'a str>,
}

struct Group<'a> {
    section: Section,
    /// The bucket or an unregistered group shows only with members.
    members: usize,
    top: Vec<&'a UiAgent>,
}

/// The sidebar's order of tasks: running first, then live, then ended
/// (stopping a task is a change too, but not activity), each newest first;
/// ties newest id first (`a10` before `a9`), the server's `recent` order,
/// so a reconciliation that stamps one `updated_at` on many Agents still
/// lists the newest first.
fn task_order(a: &UiAgent, b: &UiAgent) -> std::cmp::Ordering {
    b.running()
        .cmp(&a.running())
        .then(a.ended().cmp(&b.ended()))
        .then(b.updated_at.cmp(&a.updated_at))
        .then_with(|| (b.id.len(), &b.id).cmp(&(a.id.len(), &a.id)))
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

/// A group's place, start checkout and missing flag.
struct GroupPlace {
    name: String,
    subproject: Option<String>,
    checkout: String,
    missing: bool,
}

/// A group's place: the registered checkout's project and subproject names
/// ([`place_of`]), else the label's.
fn group_place(
    project: Option<&str>,
    group: Option<&OrgGroupView>,
    checkouts: &[Checkout],
) -> GroupPlace {
    let Some(project) = project else {
        return GroupPlace {
            name: NO_PROJECT_NAME.to_owned(),
            subproject: None,
            checkout: String::new(),
            missing: false,
        };
    };
    match group.and_then(|group| group.registration.as_ref()) {
        Some(registration) => {
            let path = path_key(&registration.checkout);
            let (name, subproject) = place_of(&registration.checkout, checkouts);
            GroupPlace {
                name,
                subproject,
                checkout: registration.checkout.clone(),
                missing: checkouts
                    .iter()
                    .any(|checkout| checkout.missing && path_key(&checkout.path) == path),
            }
        }
        None => {
            let (name, subproject) = place_of(project, checkouts);
            GroupPlace {
                name,
                subproject,
                checkout: checkout_of(project, checkouts)
                    .map(|checkout| checkout.path.clone())
                    .unwrap_or_default(),
                missing: false,
            }
        }
    }
}

/// The waits of one group, from `launch.queue` plus this window's watched
/// intents: queued interactive launches and automation assignment waits
/// with the wait reason and who can change it. A watched intent's reason
/// renders even beyond the first queue page. A project group shows its
/// own; the no-project bucket shows chats and launches of unregistered
/// folders.
pub fn waits(
    queue: Option<&LaunchQueueView>,
    intents: &[eliot_core::api::LaunchIntentView],
    project: Option<&str>,
    registered: &HashSet<&str>,
) -> Vec<String> {
    let here = |candidate: &str| match project {
        Some(project) => candidate == project,
        None => !registered.contains(candidate),
    };
    let mut lines: Vec<String> = queue
        .map(|queue| {
            queue
                .interactive
                .iter()
                .filter(|intent| intent.state == LaunchIntentState::Queued && here(&intent.project))
                .map(|intent| match &intent.reason {
                    Some(reason) => format!("Queued launch: {}", wait_reason_words(reason)),
                    None => "Queued launch: waiting for its turn".to_owned(),
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let seen: HashSet<&str> = queue
        .map(|queue| {
            queue
                .interactive
                .iter()
                .map(|intent| intent.id.as_str())
                .collect::<HashSet<_>>()
        })
        .unwrap_or_default();
    // Watched intents this window queued: their wait reason shows even
    // when the queue page carrying them was never read.
    lines.extend(
        intents
            .iter()
            .filter(|intent| {
                intent.state == LaunchIntentState::Queued
                    && here(&intent.project)
                    && !seen.contains(intent.id.as_str())
            })
            .map(|intent| match &intent.reason {
                Some(reason) => {
                    format!("Queued launch {}: {}", intent.id, wait_reason_words(reason))
                }
                None => format!("Queued launch {}: waiting for its turn", intent.id),
            }),
    );
    lines.extend(
        queue
            .into_iter()
            .flat_map(|queue| &queue.assignments)
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
/// the daemon placed in it. Empty linked worktree registrations sort below
/// the populated projects and stay selectable. A row whose group a fresh
/// read did not return lists under that group anyway, before the bucket;
/// on a stale read nothing is synthesized and such rows list in the
/// bucket. Nothing is grouped by path, repository or name here.
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
        let GroupPlace {
            name,
            subproject,
            checkout,
            missing,
        } = group_place(project, group, checkouts);
        Section {
            key: group_key(project),
            project: project.map(str::to_owned),
            name,
            subproject,
            checkouts: if checkout.is_empty() {
                Vec::new()
            } else {
                vec![checkout.clone()]
            },
            checkout,
            waits: waits(
                organization.queue.as_ref(),
                &organization.intents,
                project,
                &registered,
            ),
            rows: Vec::new(),
            running: 0,
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
    let mut placements: HashMap<&str, String> = listed
        .iter()
        .map(|agent| (agent.id.as_str(), group_key(agent.organization.as_deref())))
        .collect();
    // On a stale read nothing is synthesized: rows whose group the last
    // server grouping did not return list in the bucket instead.
    let stale = organization.stale;
    if stale {
        if !groups.iter().any(|group| group.section.project.is_none()) {
            groups.push(Group {
                section: section(None, None),
                members: 0,
                top: Vec::new(),
            });
        }
        let bucket = group_key(None);
        for key in placements.values_mut() {
            if !groups.iter().any(|group| group.section.key == *key) {
                *key = bucket.clone();
            }
        }
    } else {
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
    // Empty linked worktree registrations sort below the populated
    // projects (and any group with listed rows); the no-project bucket
    // closes the list, as the daemon orders it.
    let empty_registration = |key: &str, project: Option<&str>| {
        project.is_some_and(|_| {
            organization
                .groups
                .iter()
                .find(|group| group_key(group.project.as_deref()) == key)
                .is_some_and(|group| group.registration.is_some() && group.member_total == 0)
        })
    };
    groups.sort_by_key(|group| {
        (
            group.section.project.is_none(),
            empty_registration(&group.section.key, group.section.project.as_deref()),
        )
    });
    for agent in listed {
        let key = &placements[agent.id.as_str()];
        let Some(group) = groups.iter_mut().find(|group| group.section.key == *key) else {
            continue;
        };
        // A native observed child shows only nested under its parent
        // conversation (collapsed), never as a task of its own.
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
        if shows {
            group.top.push(agent);
        }
    }

    // Working now, per group, from the listed rows' live states.
    let mut running: HashMap<&str, HashSet<&str>> = HashMap::new();
    for agent in listed.iter().filter(|agent| agent.running()) {
        running
            .entry(placements[agent.id.as_str()].as_str())
            .or_default()
            .insert(agent.id.as_str());
    }
    let filtered = shown.only.is_some();
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for mut group in groups {
        group.section.running = running
            .get(group.section.key.as_str())
            .map_or(0, HashSet::len);
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
        let mut folds = if filtered {
            HashSet::new()
        } else {
            folding(&group.top, shown.today_start, true)
        };
        // The open task stays in view, so it is neither folded nor counted
        // in "Show N older".
        if let Some(open) = shown.open {
            folds.remove(open);
        }
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

/// A short age for a quiet task's row: "now", "5m", "2h", "3d".
pub fn short_age(now_ms: i64, at_ms: i64) -> String {
    let seconds = (now_ms - at_ms).max(0) / 1000;
    match seconds {
        0..60 => "now".to_owned(),
        60..3_600 => format!("{}m", seconds / 60),
        3_600..86_400 => format!("{}h", seconds / 3_600),
        _ => format!("{}d", seconds / 86_400),
    }
}

/// Apply `organization.changed` rows to the loaded Agents (the list and
/// expanded branches) by id: a retag, attach or detach refreshes a loaded
/// row even when it sits beyond the recent and attention pages. A row
/// already read at the same or a newer link revision stays as it is.
/// Returns how many rows changed.
pub fn apply_changes(
    agents: &mut [UiAgent],
    branches: &mut HashMap<String, Branch>,
    changes: &[OrgRowChange],
) -> usize {
    let mut changed = 0;
    let mut merge = |agent: &mut UiAgent| {
        for change in changes.iter().filter(|change| change.agent == agent.id) {
            if agent.organization_revision.unwrap_or(0) < change.revision {
                agent.organization = change.organization.clone();
                agent.tags = change.tags.clone();
                agent.organization_revision = Some(change.revision);
                changed += 1;
            }
        }
    };
    for agent in agents.iter_mut() {
        merge(agent);
    }
    for branch in branches.values_mut() {
        if let Branch::Loaded { children, .. } = branch {
            for agent in children.iter_mut() {
                merge(agent);
            }
        }
    }
    changed
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

/// Whether `agent` matches a search locally: every word of `query`
/// appears, ignoring case, in its id, title, project name or harness (the
/// server's `agent.list` `query` also reads the first input, the project
/// label and the folder).
pub fn matches(agent: &UiAgent, project: &str, query: &str) -> bool {
    let haystack =
        format!("{} {} {project} {}", agent.id, agent.title, agent.harness).to_lowercase();
    query
        .split_whitespace()
        .all(|word| haystack.contains(&word.to_lowercase()))
}

/// Most suggestions a search offers.
pub const SEARCH_RESULTS: usize = 12;

/// A search's suggestions among the tasks it found, running first then
/// newest, as `(label, id)`: the label is the title, project and harness
/// (unique: a repeated label also names its id).
pub fn search(found: &[UiAgent], checkouts: &[Checkout]) -> Vec<(String, String)> {
    let mut seen = HashSet::new();
    let mut found: Vec<(&UiAgent, String)> = found
        .iter()
        .filter(|agent| seen.insert(agent.id.as_str()))
        .map(|agent| (agent, agent_place_label(agent, checkouts)))
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

/// The tag editor's fields as the tag set, one tag per field: each field
/// trimmed and blank ones dropped, nothing split, so an existing tag such
/// as `ops, EU` saves back unchanged. The daemon validates the rest
/// (`chat.tag`).
pub fn field_tags(fields: &[String]) -> Vec<String> {
    fields
        .iter()
        .map(|field| field.trim())
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
    /// The counts of `agents`; one with an open attention report
    /// (`reported`, the Monitor's) needs attention, as in the Monitor.
    pub fn of(agents: &[UiAgent], reported: impl Fn(&UiAgent) -> bool) -> Self {
        let mut seen = HashSet::new();
        let mut fleet = Self::default();
        for agent in agents {
            if !seen.insert(agent.id.as_str()) {
                continue;
            }
            if !agent.needs_attention() && reported(agent) {
                fleet.attention += 1;
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
            push(child, depth + 1, listed, shown, tree, seen, out);
        }
    }
}
