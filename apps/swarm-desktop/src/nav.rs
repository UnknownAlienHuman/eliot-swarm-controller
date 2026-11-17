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
//! A project is a repository: every registered checkout carries the Git
//! common directory the daemon registered with it, so the worktrees of one
//! repository form one group named after the repository, and a task row
//! names its worktree only where the group spans several. Inside a group,
//! running tasks stay on top, then the rest newest first; stopped tasks
//! last changed before today fold into one "Show N older" row (the Codex
//! sidebar's pattern). Titles come with each listed summary
//! ([`summary_title`]), so every page carries its own.

use std::collections::{HashMap, HashSet};

use eliot_core::api::AgentSummary;
use pulldown_cmark::{Event, Options, Parser, Tag, TagEnd};

/// UI copy of one Agent row: identity, generation, liveness and placement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UiAgent {
    pub id: String,
    pub generation: i64,
    pub harness: String,
    pub state: String,
    /// The Agent's project label: its workspace path, as listed.
    pub project: String,
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
}

impl UiAgent {
    pub fn from_summary(summary: &AgentSummary) -> Self {
        Self {
            id: summary.id.clone(),
            generation: summary.generation,
            harness: summary.harness.clone(),
            state: format!("{:?}", summary.state).to_lowercase(),
            project: summary.project.clone(),
            parent: summary.parent.clone(),
            updated_at: summary.updated_at,
            title: summary_title(summary),
            // The daemon reports child activity only for native observed
            // children.
            native_child: summary.activity.is_some(),
            tokens: summary.fleet_usage().map(|(tokens, _)| tokens),
        }
    }

    /// Working now or on its way.
    pub fn running(&self) -> bool {
        matches!(
            self.state.as_str(),
            "busy" | "running" | "created" | "starting"
        )
    }

    /// The owner has something to do: answer a pending request
    /// (`waiting`), or settle an Agent Swarm controls whose state it cannot
    /// prove (`unknown`). An observe-only native child is never counted:
    /// its state is only observed and detach is all it offers.
    pub fn needs_attention(&self) -> bool {
        self.state == "waiting" || (self.state == "unknown" && !self.native_child)
    }

    /// Ended: stopped, failed or lost.
    fn ended(&self) -> bool {
        matches!(self.state.as_str(), "stopped" | "failed" | "lost")
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
    /// The worktree the task runs in, named only in a project whose tasks
    /// run in more than one checkout, and only outside its main checkout.
    pub place: Option<String>,
}

impl Row {
    pub fn expandable(&self) -> bool {
        self.children > 0 || self.note.is_some()
    }
}

/// A registered checkout and the Git common directory of its repository,
/// as the daemon registered them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Checkout {
    pub path: String,
    pub common_dir: String,
}

/// A readable project name: the registered id for a registered checkout,
/// otherwise the last component of the label.
pub fn project_name(label: &str, registered: &[(String, String)]) -> String {
    if let Some((_, id)) = registered.iter().find(|(checkout, _)| checkout == label) {
        return id.clone();
    }
    folder_name(label).to_owned()
}

/// The last component of a path (both separators: labels are Windows
/// paths on Windows, POSIX elsewhere).
fn folder_name(path: &str) -> &str {
    path.rsplit(['\\', '/'])
        .find(|part| !part.is_empty())
        .unwrap_or(path)
}

/// Ids the daemon's project discovery mints: `project-` and a SHA-256.
fn is_discovered_id(id: &str) -> bool {
    id.strip_prefix("project-")
        .is_some_and(|hash| hash.len() == 64 && hash.bytes().all(|byte| byte.is_ascii_hexdigit()))
}

/// The name a person reads for a project: its id, or for an id minted by
/// discovery, its checkout's folder name.
pub fn project_display(id: &str, checkout: &str) -> String {
    if is_discovered_id(id) {
        project_name(checkout, &[])
    } else {
        id.to_owned()
    }
}

/// Display names of `(id, checkout)` registrations, unique: a folder name
/// shared by two discovered projects falls back to their ids.
pub fn project_names(projects: &[(String, String)]) -> Vec<String> {
    let names: Vec<String> = projects
        .iter()
        .map(|(id, checkout)| project_display(id, checkout))
        .collect();
    names
        .iter()
        .zip(projects)
        .map(|(name, (id, _))| {
            if names.iter().filter(|other| *other == name).count() > 1 {
                id.clone()
            } else {
                name.clone()
            }
        })
        .collect()
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

/// The registered checkout holding `label` (an Agent's workspace path):
/// the longest registered path equal to it or containing it.
fn checkout_of<'a>(label: &str, checkouts: &'a [Checkout]) -> Option<&'a Checkout> {
    let label = path_key(label);
    checkouts
        .iter()
        .filter(|checkout| {
            let path = path_key(&checkout.path);
            label == path
                || label
                    .strip_prefix(path.as_str())
                    .is_some_and(|rest| rest.starts_with('/'))
        })
        .max_by_key(|checkout| path_key(&checkout.path).len())
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

/// The project key of a label: its repository's, or its own folder's.
fn project_key(label: &str, checkouts: &[Checkout]) -> String {
    match checkout_of(label, checkouts) {
        Some(checkout) => format!("repo:{}", path_key(&checkout.common_dir)),
        None => format!("folder:{}", path_key(label)),
    }
}

/// The project name of a label: its repository's, or its folder's.
pub fn project_of(label: &str, checkouts: &[Checkout]) -> String {
    match checkout_of(label, checkouts) {
        Some(checkout) => repository(&checkout.common_dir).0,
        None => folder_name(label).to_owned(),
    }
}

/// One project of the sidebar: a repository with every registered
/// checkout of it, or an unregistered folder.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Section {
    /// The repository's identity (its common directory), or the folder's.
    pub key: String,
    pub name: String,
    /// Where a new task in it starts: the main checkout when it is
    /// registered, else the first registered checkout (or the folder).
    pub checkout: String,
    /// Every checkout of the project.
    pub checkouts: Vec<String>,
    pub rows: Vec<Row>,
    /// Listed Agents of the project working now (the collapsed badge).
    pub running: usize,
    /// Whether the project is collapsed (it then has no rows).
    pub collapsed: bool,
    /// Stopped tasks from before today, folded into one row.
    pub older: usize,
    /// Whether those tasks are unfolded (the row then hides them again).
    pub older_shown: bool,
}

impl Section {
    /// Whether `checkout` is one of the project's checkouts.
    pub fn holds(&self, checkout: &str) -> bool {
        let wanted = path_key(checkout);
        self.checkouts.iter().any(|path| path_key(path) == wanted)
    }
}

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
}

struct Group<'a> {
    section: Section,
    /// The main checkout's key, for a repository that has one.
    main: Option<String>,
    top: Vec<&'a UiAgent>,
}

/// The sidebar's projects: registered repositories in registration order
/// (with no rows when they have no Agent yet), then every other project
/// label in order of first appearance, each with its visible rows.
pub fn sections(listed: &[UiAgent], checkouts: &[Checkout], shown: &Shown) -> Vec<Section> {
    let empty = |key: String, name: String, checkout: &str| Section {
        key,
        name,
        checkout: checkout.to_owned(),
        checkouts: vec![checkout.to_owned()],
        rows: Vec::new(),
        running: 0,
        collapsed: false,
        older: 0,
        older_shown: false,
    };
    let mut groups: Vec<Group> = Vec::new();
    for checkout in checkouts {
        let key = format!("repo:{}", path_key(&checkout.common_dir));
        if let Some(group) = groups.iter_mut().find(|group| group.section.key == key) {
            group.section.checkouts.push(checkout.path.clone());
            continue;
        }
        let (name, main) = repository(&checkout.common_dir);
        groups.push(Group {
            section: empty(key, name, &checkout.path),
            main: main.as_deref().map(path_key),
            top: Vec::new(),
        });
    }
    for group in &mut groups {
        if let Some(main) = &group.main
            && let Some(path) = group
                .section
                .checkouts
                .iter()
                .find(|path| path_key(path) == *main)
        {
            group.section.checkout = path.clone();
        }
    }

    let listed_ids: HashSet<&str> = listed.iter().map(|agent| agent.id.as_str()).collect();
    let mut counted = HashSet::new();
    for agent in listed {
        let key = project_key(&agent.project, checkouts);
        let index = match groups.iter().position(|group| group.section.key == key) {
            Some(index) => index,
            None => {
                let name = folder_name(&agent.project).to_owned();
                groups.push(Group {
                    section: empty(key, name, &agent.project),
                    main: None,
                    top: Vec::new(),
                });
                groups.len() - 1
            }
        };
        let group = &mut groups[index];
        if counted.insert(agent.id.as_str()) && agent.running() {
            group.section.running += 1;
        }
        let shows = match shown.only {
            Some(keep) => keep(agent),
            None => agent
                .parent
                .as_deref()
                .is_none_or(|parent| !listed_ids.contains(parent)),
        };
        if shows {
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
        group.section.collapsed = !filtered && shown.collapsed.contains(&group.section.key);
        if group.section.collapsed {
            out.push(group.section);
            continue;
        }
        // Running first, then live, then ended (stopping a task is a
        // change too, but not activity), each newest first; ties in id
        // order (`a9` < `a10`).
        group.top.sort_by(|a, b| {
            b.running()
                .cmp(&a.running())
                .then(a.ended().cmp(&b.ended()))
                .then(b.updated_at.cmp(&a.updated_at))
                .then_with(|| (a.id.len(), &a.id).cmp(&(b.id.len(), &b.id)))
        });
        let folds =
            |agent: &UiAgent| !filtered && agent.ended() && agent.updated_at < shown.today_start;
        let place_key = |agent: &UiAgent| match checkout_of(&agent.project, checkouts) {
            Some(checkout) => path_key(&checkout.path),
            None => path_key(&agent.project),
        };
        let places: HashSet<String> = group.top.iter().map(|agent| place_key(agent)).collect();
        let unfolded = shown.unfolded.contains(&group.section.key);
        group.section.older = group.top.iter().filter(|agent| folds(agent)).count();
        group.section.older_shown = unfolded && group.section.older > 0;
        for agent in &group.top {
            if folds(agent) && !unfolded {
                continue;
            }
            let place = (places.len() > 1 && group.main.as_ref() != Some(&place_key(agent)))
                .then(|| {
                    checkout_of(&agent.project, checkouts)
                        .map_or(agent.project.as_str(), |checkout| checkout.path.as_str())
                })
                .map(|path| place_name(folder_name(path), &group.section.name));
            push(
                agent,
                0,
                place,
                listed,
                shown,
                !filtered,
                &mut seen,
                &mut group.section.rows,
            );
        }
        out.push(group.section);
    }
    out
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
        .map(|agent| (agent, project_of(&agent.project, checkouts)))
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

/// The most recently changed Agents, newest first (ties by id), at most
/// `limit`.
pub fn recents(agents: &[UiAgent], limit: usize) -> Vec<&UiAgent> {
    let mut recent: Vec<&UiAgent> = agents.iter().collect();
    recent.sort_by(|a, b| b.updated_at.cmp(&a.updated_at).then(a.id.cmp(&b.id)));
    // An id read on two pages shows once, at its newest time.
    let mut seen = HashSet::new();
    recent.retain(|agent| seen.insert(agent.id.as_str()));
    recent.truncate(limit);
    recent
}

/// The fleet line's counts over the listed Agents.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Fleet {
    pub running: usize,
    /// Agents that need the owner ([`UiAgent::needs_attention`]).
    pub attention: usize,
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
        }
        fleet
    }

    /// The line itself: live work, or how quiet the fleet is.
    pub fn summary(&self, total: usize) -> String {
        if self.running > 0 {
            format!("{} running", self.running)
        } else if total == 0 {
            "No Agents yet".to_owned()
        } else {
            format!(
                "All quiet · {total} {}",
                if total == 1 { "Agent" } else { "Agents" }
            )
        }
    }

    /// The attention row's label ("1 needs attention", "3 need attention").
    pub fn attention_label(&self) -> String {
        if self.attention == 1 {
            "1 needs attention".to_owned()
        } else {
            format!("{} need attention", self.attention)
        }
    }
}

/// Most characters of a title; the row trims what its width cannot show.
const TITLE_CHARS: usize = 60;

/// The title of a listed Agent from Swarm's retained data: the first line
/// of its first input ([`title`] of `preview`), else its name, else
/// `<harness> · <profile>` (a native child reads `<harness> subagent`).
/// The id appears only when the daemon reported no profile.
pub fn summary_title(summary: &AgentSummary) -> String {
    if let Some(title) = summary.preview.as_deref().and_then(title) {
        return title;
    }
    if let Some(name) = summary.name.as_deref().and_then(title) {
        return name;
    }
    let kind = if summary.activity.is_some() {
        format!("{} subagent", summary.harness)
    } else {
        summary.harness.clone()
    };
    format!(
        "{kind} · {}",
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

/// Children of `id` to show now: the fetched branch, else the listed ones.
fn children_of<'a>(
    id: &str,
    listed: &'a [UiAgent],
    branches: &'a HashMap<String, Branch>,
) -> Vec<&'a UiAgent> {
    match branches.get(id) {
        Some(Branch::Loaded { children, .. }) => children.iter().collect(),
        _ => listed
            .iter()
            .filter(|agent| agent.parent.as_deref() == Some(id))
            .collect(),
    }
}

/// Push `agent`'s row and, when it is expanded and `tree` is on, its
/// children's rows beneath it.
#[allow(clippy::too_many_arguments)]
fn push(
    agent: &UiAgent,
    depth: usize,
    place: Option<String>,
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
        place,
    });
    if is_expanded {
        for child in children {
            push(child, depth + 1, None, listed, shown, tree, seen, out);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use eliot_core::AgentState;

    #[test]
    fn discovered_projects_read_as_their_folder() {
        let hash = "ab".repeat(32);
        let discovered = format!("project-{hash}");
        assert_eq!(project_display(&discovered, "C:\\work\\app"), "app");
        assert_eq!(project_display("app-main", "C:\\work\\app"), "app-main");
        assert_eq!(
            project_display("project-short", "C:\\work\\app"),
            "project-short"
        );
        let other = format!("project-{}", "cd".repeat(32));
        let names = project_names(&[
            (discovered.clone(), "C:\\a\\app".to_owned()),
            (other.clone(), "C:\\b\\app".to_owned()),
            ("web".to_owned(), "C:\\web".to_owned()),
        ]);
        assert_eq!(names, [discovered, other, "web".to_owned()]);
    }

    fn agent(id: &str, project: &str, parent: Option<&str>) -> UiAgent {
        UiAgent {
            id: id.to_owned(),
            generation: 1,
            harness: "muse".to_owned(),
            state: "idle".to_owned(),
            project: project.to_owned(),
            parent: parent.map(str::to_owned),
            updated_at: 0,
            title: id.to_owned(),
            native_child: false,
            tokens: None,
        }
    }

    fn with(mut agent: UiAgent, state: &str, updated_at: i64) -> UiAgent {
        agent.state = state.to_owned();
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
                checkouts,
                &Shown {
                    branches: &self.branches,
                    expanded: &self.expanded,
                    collapsed: &self.collapsed,
                    unfolded: &self.unfolded,
                    today_start: self.today_start,
                    only,
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
        Checkout {
            path: path.to_owned(),
            common_dir: common_dir.to_owned(),
        }
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
    fn project_names_prefer_the_registered_id() {
        let registered = vec![(r"\\?\C:\src\app".to_owned(), "app".to_owned())];
        assert_eq!(project_name(r"\\?\C:\src\app", &registered), "app");
        assert_eq!(project_name(r"\\?\C:\work\other", &registered), "other");
        assert_eq!(project_name("plain", &registered), "plain");
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
    fn worktrees_of_one_repository_form_one_project_named_after_it() {
        let git = r"\\?\C:\src\app\.git";
        let checkouts = vec![
            checkout(r"\\?\C:\wt\wt-k1", git),
            checkout(r"\\?\C:\src\app", git),
            checkout(r"\\?\C:\wt\wt-k2", git),
            checkout(r"\\?\C:\src\web", r"\\?\C:\src\web\.git"),
            checkout(r"\\?\C:\src\idle", r"\\?\C:\src\idle\.git"),
        ];
        let listed = vec![
            agent("a1", r"\\?\C:\wt\wt-k1", None),
            agent("a2", r"\\?\C:\src\app", None),
            // A workspace below a registered checkout belongs to it.
            agent("a3", r"\\?\C:\wt\wt-k2\crates", None),
            agent("a4", r"\\?\C:\src\web", None),
            agent("a5", r"\\?\C:\scratch\notes", None),
        ];
        let sections = View::default().sections(&listed, &checkouts);
        let shape: Vec<(&str, &str, Vec<&str>)> = sections
            .iter()
            .map(|section| {
                (
                    section.name.as_str(),
                    section.checkout.as_str(),
                    section
                        .rows
                        .iter()
                        .map(|row| row.agent.id.as_str())
                        .collect(),
                )
            })
            .collect();
        assert_eq!(
            shape,
            [
                ("app", r"\\?\C:\src\app", vec!["a1", "a2", "a3"]),
                ("web", r"\\?\C:\src\web", vec!["a4"]),
                ("idle", r"\\?\C:\src\idle", vec![]),
                ("notes", r"\\?\C:\scratch\notes", vec!["a5"]),
            ]
        );
        // The worktree shows where the project spans several checkouts,
        // never for the main checkout or a single-checkout project.
        let places: Vec<Option<&str>> = sections[0]
            .rows
            .iter()
            .map(|row| row.place.as_deref())
            .collect();
        assert_eq!(places, [Some("wt-k1"), None, Some("wt-k2")]);
        assert_eq!(place_name("app-wt-tiles", "app"), "wt-tiles");
        assert_eq!(place_name("application", "app"), "application");
        assert_eq!(place_name("app", "app"), "app");
        assert_eq!(sections[1].rows[0].place, None);
        assert!(sections[0].holds(r"C:\wt\wt-k2"));
        assert!(!sections[0].holds(r"C:\src\web"));
        assert_eq!(project_of(r"\\?\C:\wt\wt-k1", &checkouts), "app");
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
    fn a_collapsed_project_keeps_only_its_running_count() {
        let listed = vec![
            with(agent("a1", "/p", None), "busy", 0),
            with(agent("a2", "/p", Some("a1")), "starting", 0),
            with(agent("a3", "/p", None), "idle", 0),
        ];
        let mut view = View::default();
        let key = view.sections(&listed, &[])[0].key.clone();
        view.collapsed.insert(key);
        let section = &view.sections(&listed, &[])[0];
        assert!(section.collapsed);
        assert!(section.rows.is_empty());
        assert_eq!(section.running, 2);
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
        let mut view = View::default();
        // Collapsing never hides a match.
        view.collapsed.insert("folder:/p".to_owned());
        let attention = |agent: &UiAgent| agent.needs_attention();
        let sections = view.filtered(&listed, &[], Some(&attention));
        let flat: Vec<Vec<(&str, usize)>> = sections.iter().map(ids).collect();
        assert_eq!(flat, [vec![("a2", 0)], vec![("a4", 0)]]);
        assert_eq!(
            Fleet::of(&listed),
            Fleet {
                running: 1,
                attention: 2
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
                "k75: sidebar titles · app · codex".to_owned(),
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
    fn the_fleet_line_reports_work_calmly() {
        let quiet = Fleet::default();
        assert_eq!(quiet.summary(0), "No Agents yet");
        assert_eq!(quiet.summary(1), "All quiet · 1 Agent");
        let busy = Fleet {
            running: 6,
            attention: 1,
        };
        assert_eq!(busy.summary(40), "6 running");
        assert_eq!(busy.attention_label(), "1 needs attention");
        assert_eq!(
            Fleet {
                running: 0,
                attention: 3
            }
            .attention_label(),
            "3 need attention"
        );
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
        let ids: Vec<&str> = recents(&listed, 2)
            .into_iter()
            .map(|agent| agent.id.as_str())
            .collect();
        assert_eq!(ids, ["a2", "a3"]);
        let mut repeated = listed.clone();
        repeated.push(UiAgent {
            updated_at: 5,
            ..listed[1].clone()
        });
        let ids: Vec<&str> = recents(&repeated, 9)
            .into_iter()
            .map(|agent| agent.id.as_str())
            .collect();
        assert_eq!(ids, ["a2", "a3", "a1"]);
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
            activity: None,
            usage: None,
            usage_scope: None,
            updated_at: 0,
            heavy: None,
            session_growth: None,
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
            (
                agent.title.as_str(),
                agent.state.as_str(),
                agent.native_child
            ),
            ("k75: titles", "idle", false)
        );
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
}
