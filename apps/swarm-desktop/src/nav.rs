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

use std::collections::{HashMap, HashSet};

/// UI copy of one Agent row: identity, generation, liveness and placement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UiAgent {
    pub id: String,
    pub generation: i64,
    pub harness: String,
    pub state: String,
    /// The Agent's project label (a registered checkout path, as listed).
    pub project: String,
    /// The current manager, if any.
    pub parent: Option<String>,
    /// Last change of the Agent record (ms since the epoch), for Recents.
    pub updated_at: i64,
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
    /// The project caption when this row opens a project group.
    pub group: Option<String>,
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

/// A readable project name: the registered id for a registered checkout,
/// otherwise the last component of the label.
pub fn project_name(label: &str, registered: &[(String, String)]) -> String {
    if let Some((_, id)) = registered.iter().find(|(checkout, _)| checkout == label) {
        return id.clone();
    }
    // Both separators: labels are Windows paths on Windows, POSIX elsewhere.
    label
        .rsplit(['\\', '/'])
        .find(|part| !part.is_empty())
        .unwrap_or(label)
        .to_owned()
}

/// One project of the sidebar: its name, its checkout (where a new task in
/// it starts) and its visible rows.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Section {
    pub name: String,
    pub checkout: String,
    pub rows: Vec<Row>,
}

/// The sidebar's projects: registered ones in registration order (with no
/// rows when they have no Agent yet), then every other project label in
/// order of first appearance. `rows` come from [`rows`] grouped by project;
/// `registered` pairs each checkout with its project id.
pub fn sections(rows: Vec<Row>, registered: &[(String, String)]) -> Vec<Section> {
    let mut grouped: Vec<Section> = Vec::new();
    for row in rows {
        match (&row.group, grouped.last_mut()) {
            (None, Some(section)) => section.rows.push(row),
            _ => grouped.push(Section {
                name: project_name(&row.agent.project, registered),
                checkout: row.agent.project.clone(),
                rows: vec![row],
            }),
        }
    }
    let mut out: Vec<Section> = registered
        .iter()
        .map(|(checkout, id)| {
            let index = grouped
                .iter()
                .position(|section| &section.checkout == checkout);
            match index {
                Some(index) => grouped.remove(index),
                None => Section {
                    name: id.clone(),
                    checkout: checkout.clone(),
                    rows: Vec::new(),
                },
            }
        })
        .collect();
    out.extend(grouped);
    out
}

/// The most recently changed Agents, newest first (ties by id), at most
/// `limit`.
pub fn recents(agents: &[UiAgent], limit: usize) -> Vec<&UiAgent> {
    let mut recent: Vec<&UiAgent> = agents.iter().collect();
    recent.sort_by(|a, b| b.updated_at.cmp(&a.updated_at).then(a.id.cmp(&b.id)));
    recent.dedup_by(|a, b| a.id == b.id);
    recent.truncate(limit);
    recent
}

/// Most characters of a title derived from a first message.
const TITLE_CHARS: usize = 60;

/// A conversation title from its first message, like a chat title: the
/// first non-blank line, whitespace collapsed, cut at a word near
/// [`TITLE_CHARS`] with an ellipsis. `None` for a blank message.
pub fn title(text: &str) -> Option<String> {
    let line = text.lines().map(str::trim).find(|line| !line.is_empty())?;
    let words: Vec<&str> = line.split_whitespace().collect();
    let joined = words.join(" ");
    if joined.chars().count() <= TITLE_CHARS {
        return Some(joined);
    }
    let mut cut = String::new();
    for word in words {
        let next = if cut.is_empty() {
            word.to_owned()
        } else {
            format!("{cut} {word}")
        };
        if next.chars().count() > TITLE_CHARS {
            break;
        }
        cut = next;
    }
    if cut.is_empty() {
        cut = joined.chars().take(TITLE_CHARS).collect();
    }
    Some(format!("{cut}…"))
}

/// The visible rows for `listed` (main pages, in daemon order), the fetched
/// `branches` and the `expanded` managers. With `group_by_project`, top-level
/// rows are grouped by project in order of first appearance and the first
/// row of each group carries its caption from `name`.
pub fn rows(
    listed: &[UiAgent],
    branches: &HashMap<String, Branch>,
    expanded: &HashSet<String>,
    group_by_project: bool,
    name: &dyn Fn(&str) -> String,
) -> Vec<Row> {
    let listed_ids: HashSet<&str> = listed.iter().map(|agent| agent.id.as_str()).collect();
    let mut top: Vec<&UiAgent> = listed
        .iter()
        .filter(|agent| {
            agent
                .parent
                .as_deref()
                .is_none_or(|parent| !listed_ids.contains(parent))
        })
        .collect();
    if group_by_project {
        let mut order: Vec<&str> = Vec::new();
        for agent in &top {
            if !order.contains(&agent.project.as_str()) {
                order.push(&agent.project);
            }
        }
        // Stable: daemon order inside each project.
        top.sort_by_key(|agent| order.iter().position(|project| *project == agent.project));
    }
    let mut out = Vec::new();
    let mut seen = HashSet::new();
    let mut last_project: Option<&str> = None;
    for agent in top {
        let group = (group_by_project && last_project != Some(agent.project.as_str()))
            .then(|| name(&agent.project));
        last_project = Some(&agent.project);
        push(
            agent, 0, group, listed, branches, expanded, &mut seen, &mut out,
        );
    }
    out
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

#[allow(clippy::too_many_arguments)]
fn push(
    agent: &UiAgent,
    depth: usize,
    group: Option<String>,
    listed: &[UiAgent],
    branches: &HashMap<String, Branch>,
    expanded: &HashSet<String>,
    seen: &mut HashSet<String>,
    out: &mut Vec<Row>,
) {
    // Parent links form a tree in the daemon; a repeated id (an Agent moved
    // between reads) is still shown once.
    if !seen.insert(agent.id.clone()) {
        return;
    }
    let children = children_of(&agent.id, listed, branches);
    let is_expanded = expanded.contains(&agent.id);
    let note = if is_expanded {
        match branches.get(&agent.id) {
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
        group,
        children: children.len(),
        expanded: is_expanded,
        note,
    });
    if is_expanded {
        for child in children {
            push(
                child,
                depth + 1,
                None,
                listed,
                branches,
                expanded,
                seen,
                out,
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn agent(id: &str, project: &str, parent: Option<&str>) -> UiAgent {
        UiAgent {
            id: id.to_owned(),
            generation: 1,
            harness: "muse".to_owned(),
            state: "idle".to_owned(),
            project: project.to_owned(),
            parent: parent.map(str::to_owned),
            updated_at: 0,
        }
    }

    fn name(label: &str) -> String {
        label.to_uppercase()
    }

    fn ids(rows: &[Row]) -> Vec<(&str, usize)> {
        rows.iter()
            .map(|row| (row.agent.id.as_str(), row.depth))
            .collect()
    }

    #[test]
    fn children_hide_under_a_listed_manager_until_expanded() {
        let listed = vec![
            agent("a1", "p", None),
            agent("a2", "p", Some("a1")),
            agent("a3", "p", None),
        ];
        let collapsed = rows(&listed, &HashMap::new(), &HashSet::new(), false, &name);
        assert_eq!(ids(&collapsed), [("a1", 0), ("a3", 0)]);
        assert_eq!(collapsed[0].children, 1);
        assert!(collapsed[0].expandable());
        assert!(!collapsed[1].expandable());

        let expanded = HashSet::from(["a1".to_owned()]);
        let open = rows(&listed, &HashMap::new(), &expanded, false, &name);
        assert_eq!(ids(&open), [("a1", 0), ("a2", 1), ("a3", 0)]);
        assert!(open[0].expanded);
    }

    #[test]
    fn a_fetched_branch_replaces_the_listed_children_and_nests() {
        let listed = vec![agent("a1", "p", None), agent("a2", "p", Some("a1"))];
        let branches = HashMap::from([
            (
                "a1".to_owned(),
                Branch::Loaded {
                    children: vec![agent("a2", "p", Some("a1")), agent("a9", "p", Some("a1"))],
                    more: true,
                },
            ),
            (
                "a9".to_owned(),
                Branch::Loaded {
                    children: vec![agent("a10", "p", Some("a9"))],
                    more: false,
                },
            ),
        ]);
        let expanded = HashSet::from(["a1".to_owned(), "a9".to_owned()]);
        let rows = rows(&listed, &branches, &expanded, false, &name);
        assert_eq!(ids(&rows), [("a1", 0), ("a2", 1), ("a9", 1), ("a10", 2)]);
        assert_eq!(rows[0].children, 2);
        assert_eq!(rows[0].note.as_deref(), Some("more children exist"));
        assert_eq!(rows[2].note, None);
    }

    #[test]
    fn an_unlisted_manager_leaves_its_child_at_the_top_level() {
        let listed = vec![agent("a5", "p", Some("a1")), agent("a6", "p", None)];
        let rows = rows(&listed, &HashMap::new(), &HashSet::new(), false, &name);
        assert_eq!(ids(&rows), [("a5", 0), ("a6", 0)]);
    }

    #[test]
    fn projects_group_in_first_appearance_order_with_one_caption_each() {
        let listed = vec![
            agent("a1", "p", None),
            agent("a2", "q", None),
            agent("a3", "p", None),
        ];
        let rows = rows(&listed, &HashMap::new(), &HashSet::new(), true, &name);
        assert_eq!(ids(&rows), [("a1", 0), ("a3", 0), ("a2", 0)]);
        let groups: Vec<Option<&str>> = rows.iter().map(|row| row.group.as_deref()).collect();
        assert_eq!(groups, [Some("P"), None, Some("Q")]);
    }

    #[test]
    fn expanded_branch_states_show_as_notes() {
        let listed = vec![agent("a1", "p", None)];
        let expanded = HashSet::from(["a1".to_owned()]);
        for (branch, note) in [
            (Branch::Loading, "loading children…"),
            (Branch::Failed("bad".to_owned()), "children not read: bad"),
        ] {
            let branches = HashMap::from([("a1".to_owned(), branch)]);
            let rows = rows(&listed, &branches, &expanded, false, &name);
            assert_eq!(rows[0].note.as_deref(), Some(note));
            assert!(rows[0].expandable());
        }
    }

    #[test]
    fn a_repeated_id_is_shown_once() {
        let listed = vec![agent("a1", "p", None)];
        let branches = HashMap::from([(
            "a1".to_owned(),
            Branch::Loaded {
                children: vec![agent("a1", "p", Some("a1"))],
                more: false,
            },
        )]);
        let expanded = HashSet::from(["a1".to_owned()]);
        assert_eq!(
            ids(&rows(&listed, &branches, &expanded, false, &name)),
            [("a1", 0)]
        );
    }

    #[test]
    fn project_names_prefer_the_registered_id() {
        let registered = vec![(r"\\?\C:\src\app".to_owned(), "app".to_owned())];
        assert_eq!(project_name(r"\\?\C:\src\app", &registered), "app");
        assert_eq!(project_name(r"\\?\C:\work\other", &registered), "other");
        assert_eq!(project_name("plain", &registered), "plain");
    }

    #[test]
    fn sections_list_registered_projects_first_even_without_agents() {
        let listed = vec![
            agent("a1", "/x", None),
            agent("a2", "/p", None),
            agent("a3", "/p", Some("a2")),
        ];
        let registered = vec![
            ("/p".to_owned(), "pee".to_owned()),
            ("/q".to_owned(), "queue".to_owned()),
        ];
        let all = rows(&listed, &HashMap::new(), &HashSet::new(), true, &|label| {
            project_name(label, &registered)
        });
        let sections = sections(all, &registered);
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
                ("pee", "/p", vec!["a2"]),
                ("queue", "/q", vec![]),
                ("x", "/x", vec!["a1"]),
            ]
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
}
