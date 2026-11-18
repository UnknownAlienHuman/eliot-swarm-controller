//! "Move under another task…" (`swarm.reparent`, `docs/spec/api.md`
//! Handoff methods): the task's menus open a picker of the tasks it can
//! work under; one confirmation moves it with its own workers.
//!
//! The picker reads its own candidates ([`search_managers`]): every page of
//! `agent.list {project_scope, state}` for each state the daemon accepts as
//! a destination, up to [`SEARCH_BOUND`] rows, and says how many it left
//! unread. The sidebar's single page is not the search set. The scope
//! filter is the effective authorization scope
//! (`COALESCE(project_scope, project)`), so a legacy row and a registered
//! row with the same scope reach the same candidate set, exactly the
//! namespace the daemon compares. A chat's search (no scope) reads every
//! project and says so ([`Scope`]).
//!
//! The binding is captured when the owner confirms: the exact source and
//! destination generations go into the request, so a click acts on exactly
//! what was displayed. One ULID per action key, reused only after an
//! uncertain reply. The picker only leaves out what the daemon would
//! refuse anyway (another authorization scope, an ended or observed task,
//! the task itself or its own workers, a row whose scope is still unknown);
//! the daemon still owns every check (scope, cycle, fanout, observe-only,
//! generation, destination state) and its refusal is shown in words.

use eliot_core::AgentState;
use eliot_core::api::{AgentListRequest, ReparentRequest};

use crate::nav::UiAgent;

/// The states `swarm.reparent` accepts for the new parent: controlled,
/// nonterminal and not unknown (`docs/spec/coordination.md`).
pub const MANAGER_STATES: [AgentState; 5] = [
    AgentState::Created,
    AgentState::Starting,
    AgentState::Idle,
    AgentState::Busy,
    AgentState::Waiting,
];

/// Rows per `agent.list` page of the picker's search.
pub const SEARCH_PAGE: usize = 200;

/// Most rows one picker search reads, across states and pages, including
/// the reads of each state's total.
pub const SEARCH_BOUND: usize = 1_000;

/// One `agent.list` page as the search reads it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Page {
    pub rows: Vec<UiAgent>,
    pub next: Option<String>,
    /// Matching rows for the filters, ignoring the cursor.
    pub total: usize,
}

/// What one search read: the project's tasks in [`MANAGER_STATES`] (ID
/// order per state, as listed) and how many matching rows it left unread.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Managers {
    pub agents: Vec<UiAgent>,
    /// Matching rows past the bound; 0 when the search read them all.
    pub unread: usize,
}

/// The `agent.list` scope filter for `source`'s picker: its effective
/// authorization scope, never its workspace path or its recorded project
/// ID. Empty (a chat) or unknown (a pending row) reads every project;
/// [`eligible_parents`] then keeps only same-scope rows.
pub fn search_scope(source: &UiAgent) -> Option<&str> {
    source.scope().filter(|scope| !scope.is_empty())
}

/// Page through `agent.list {project_scope, state}` for every state in
/// [`MANAGER_STATES`], `page` rows at a time and at most `bound` rows in
/// all. `fetch` is one validated page read (at most its `limit` rows).
/// The scope filter comes from the source itself ([`search_scope`]), so
/// the search cannot drift from what the picker offers: a chat (empty
/// scope) or a pending row (unknown scope) reads every project's rows
/// ([`Scope::AllProjects`]), and [`eligible_parents`] keeps the ones
/// without a project (never the pending rows).
///
/// A state's total arrives only with a read of at least one row, so one
/// row of the bound stays reserved for each state not yet read: every
/// total is counted inside the bound, and those rows are candidates too.
/// A bound below one row per state is raised to that.
pub async fn search_managers<F, Fut, E>(
    source: &UiAgent,
    page: usize,
    bound: usize,
    mut fetch: F,
) -> Result<Managers, E>
where
    F: FnMut(AgentListRequest) -> Fut,
    Fut: std::future::Future<Output = Result<Page, E>>,
{
    let bound = bound.max(MANAGER_STATES.len());
    let scope = search_scope(source).map(str::to_owned);
    let mut managers = Managers::default();
    for (index, state) in MANAGER_STATES.into_iter().enumerate() {
        let reserved = MANAGER_STATES.len() - index - 1;
        let room = |read: usize| bound.saturating_sub(read + reserved);
        let mut after = None;
        let mut read = 0;
        loop {
            // At least one row: the previous state stopped one row short.
            let limit = page.min(room(managers.agents.len())).max(1);
            let reply = fetch(AgentListRequest {
                project: None,
                project_scope: scope.clone(),
                state: Some(state),
                limit,
                after: after.take(),
                ..AgentListRequest::default()
            })
            .await?;
            read += reply.rows.len();
            let empty = reply.rows.is_empty();
            managers.agents.extend(reply.rows);
            let left = reply.total.saturating_sub(read);
            match reply.next {
                Some(next) if !empty && left > 0 && room(managers.agents.len()) > 0 => {
                    after = Some(next)
                }
                _ => {
                    managers.unread += left;
                    break;
                }
            }
        }
    }
    Ok(managers)
}

/// What a picker search covers. The search filters by effective scope, so
/// a task's search reads running or ready tasks of its own scope; a chat
/// (empty scope) or a pending row (unknown scope) reads every project's
/// rows and keeps the ones without a project (never the pending rows); its
/// counts cover every project.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Scope {
    Project,
    AllProjects,
}

impl Scope {
    pub fn of(scope: Option<&str>) -> Self {
        match scope {
            Some(scope) if !scope.is_empty() => Self::Project,
            _ => Self::AllProjects,
        }
    }

    /// The rows the search reads, in words.
    pub fn read(self) -> &'static str {
        match self {
            Self::Project => "of this project",
            Self::AllProjects => "of every project",
        }
    }

    /// The tasks the picker offers, in words.
    pub fn offered(self) -> &'static str {
        match self {
            Self::Project => "task of this project",
            Self::AllProjects => "task without a project",
        }
    }
}

/// What a bounded search left unread, in words, over its [`Scope`]; None
/// when it read every matching row.
pub fn unread_words(scope: Option<&str>, read: usize, unread: usize) -> Option<String> {
    (unread > 0).then(|| {
        format!(
            "Searched {read} running or ready tasks {}; {unread} more were not read, so the one you want may be missing.",
            Scope::of(scope).read()
        )
    })
}

/// A move of one Agent (with its subtree) under a new parent, bound to the
/// generations shown at click time.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MoveBinding {
    pub source: String,
    pub source_generation: i64,
    pub parent: String,
    pub parent_generation: i64,
}

impl MoveBinding {
    /// Ledger key: the same move (both Agents and generations) reuses its
    /// mutation id; any difference is a new action.
    pub fn key(&self) -> String {
        format!(
            "reparent:{}:{}:{}:{}",
            self.source, self.source_generation, self.parent, self.parent_generation
        )
    }

    pub fn request(&self, id: String) -> ReparentRequest {
        ReparentRequest {
            id,
            agent: Some(self.source.clone()),
            children_of: None,
            expected_generation: self.source_generation,
            parent: self.parent.clone(),
            parent_generation: self.parent_generation,
        }
    }
}

/// The `candidates` `source` can be moved under, newest first: the same
/// authorization scope (the daemon compares `COALESCE(project_scope,
/// project)` exactly), controlled and not ended (`swarm.reparent` needs a
/// live manager), neither `source` itself, its current parent, nor one of
/// its own workers (a cycle), each once. Rows with an unknown scope (a
/// pending launch whose daemon row has not arrived) are never offered, and
/// a pending source is offered nothing. `links` (the sidebar's rows) only
/// add parent links for the cycle check; the candidates' own rows are
/// fresher and win.
pub fn eligible_parents<'a>(
    candidates: &'a [UiAgent],
    links: &[UiAgent],
    source: &UiAgent,
) -> Vec<&'a UiAgent> {
    let parents: std::collections::HashMap<&str, Option<&str>> = links
        .iter()
        .chain(candidates)
        .map(|agent| (agent.id.as_str(), agent.parent.as_deref()))
        .collect();
    let source_parent = match parents.get(source.id.as_str()) {
        Some(parent) => *parent,
        None => source.parent.as_deref(),
    };
    // Whether `id` sits anywhere under `source` (bounded: links form a
    // tree, but rows read on different pages may disagree).
    let under_source = |id: &str| {
        let mut current = parents.get(id).copied().flatten();
        for _ in 0..parents.len() {
            match current {
                Some(parent) if parent == source.id => return true,
                Some(parent) => current = parents.get(parent).copied().flatten(),
                None => return false,
            }
        }
        false
    };
    let mut seen = std::collections::HashSet::new();
    let scoped = |agent: &UiAgent| {
        matches!(
            (source.scope(), agent.scope()),
            (Some(want), Some(have)) if want == have
        )
    };
    let mut eligible: Vec<&UiAgent> = candidates
        .iter()
        .filter(|agent| agent.id != source.id && seen.insert(agent.id.as_str()))
        .filter(|agent| scoped(agent) && !agent.native_child)
        .filter(|agent| {
            !matches!(
                agent.state,
                AgentState::Stopped | AgentState::Failed | AgentState::Lost | AgentState::Unknown
            )
        })
        .filter(|agent| source_parent != Some(agent.id.as_str()))
        .filter(|agent| !under_source(&agent.id))
        .collect();
    eligible.sort_by(|a, b| b.updated_at.cmp(&a.updated_at).then(a.id.cmp(&b.id)));
    eligible
}

#[cfg(test)]
mod tests {
    use super::*;

    fn agent(id: &str, parent: Option<&str>, state: AgentState, updated_at: i64) -> UiAgent {
        UiAgent {
            id: id.to_owned(),
            generation: 1,
            harness: "claude".to_owned(),
            state,
            project_id: "/w".to_owned(),
            project_scope: None,
            repo_common_dir: None,
            workspace: None,
            parent: parent.map(str::to_owned),
            updated_at,
            title: id.to_owned(),
            native_child: false,
            tokens: None,
            chat: false,
            archived: false,
            pending: false,
        }
    }

    #[test]
    fn request_moves_the_agent_with_both_generations() {
        let binding = MoveBinding {
            source: "a2".to_owned(),
            source_generation: 1,
            parent: "a5".to_owned(),
            parent_generation: 3,
        };
        let request = binding.request("01ID".to_owned());
        assert_eq!(request.agent.as_deref(), Some("a2"));
        assert!(request.children_of.is_none());
        assert_eq!(
            (request.expected_generation, request.parent_generation),
            (1, 3)
        );
        request.validate().unwrap();
        let mut later = binding.clone();
        later.parent_generation = 4;
        assert_ne!(binding.key(), later.key());
    }

    #[test]
    fn only_live_controlled_tasks_of_the_same_project_outside_the_subtree_qualify() {
        let source = agent("a2", Some("a1"), AgentState::Idle, 5);
        let mut other_project = agent("a9", None, AgentState::Idle, 9);
        other_project.project_id = "/elsewhere".to_owned();
        let mut native = agent("a8", None, AgentState::Busy, 8);
        native.native_child = true;
        let agents = vec![
            agent("a1", None, AgentState::Busy, 1),
            source.clone(),
            agent("a3", Some("a2"), AgentState::Idle, 3),
            agent("a4", Some("a3"), AgentState::Busy, 4),
            agent("a5", None, AgentState::Idle, 6),
            agent("a6", None, AgentState::Lost, 7),
            agent("a7", None, AgentState::Waiting, 10),
            agent("a5", None, AgentState::Idle, 6),
            native,
            other_project,
        ];
        let ids: Vec<&str> = eligible_parents(&agents, &[], &source)
            .iter()
            .map(|agent| agent.id.as_str())
            .collect();
        // Not itself, its current parent a1, its workers a3/a4, the lost
        // a6, the native a8 or the other project's a9; newest first, once.
        assert_eq!(ids, ["a7", "a5"]);
    }

    /// `agent.list` over `rows` (listing order): the request's scope and
    /// state filters, `limit` rows after the cursor, and the exact total.
    /// Mirrors the Store: `project_scope` matches the effective scope
    /// (`COALESCE(project_scope, project)`), `project` the recorded label.
    fn list(rows: &[UiAgent], request: &AgentListRequest) -> Page {
        let matching: Vec<&UiAgent> = rows
            .iter()
            .filter(|row| {
                request
                    .project
                    .as_deref()
                    .is_none_or(|p| row.project_id == p)
            })
            .filter(|row| {
                request
                    .project_scope
                    .as_deref()
                    .is_none_or(|scope| row.scope() == Some(scope))
            })
            .filter(|row| request.state.is_none_or(|state| row.state == state))
            .collect();
        let start = request.after.as_deref().map_or(0, |after| {
            matching
                .iter()
                .position(|row| row.id == after)
                .map_or(matching.len(), |index| index + 1)
        });
        let rows: Vec<UiAgent> = matching
            .iter()
            .skip(start)
            .take(request.limit)
            .map(|row| (*row).clone())
            .collect();
        Page {
            next: rows.last().map(|row| row.id.clone()),
            rows,
            total: matching.len(),
        }
    }

    #[test]
    fn the_search_finds_a_manager_past_the_first_page_and_counts_what_it_left_unread() {
        let source = agent("a1", None, AgentState::Idle, 1);
        let mut rows = vec![source.clone()];
        // Six of its own workers fill the first pages (a cycle, never offered).
        for n in 2..=7 {
            rows.push(agent(&format!("a{n}"), Some("a1"), AgentState::Idle, n));
        }
        // The one valid manager: the fourth page at two rows a page.
        rows.push(agent("a8", None, AgentState::Idle, 8));
        let mut elsewhere = agent("a9", None, AgentState::Busy, 9);
        elsewhere.project_id = "/elsewhere".to_owned();
        rows.push(elsewhere);
        rows.push(agent("a10", None, AgentState::Busy, 10));
        // a12 works under a stopped worker a11 of a1: only the sidebar's
        // rows know that link.
        rows.push(agent("a12", Some("a11"), AgentState::Waiting, 12));
        let links = [agent("a11", Some("a1"), AgentState::Stopped, 11)];
        let mut requests = Vec::new();
        let managers = crate::client::block_on(search_managers(&source, 2, 100, |request| {
            requests.push(request.clone());
            let page = list(&rows, &request);
            async move { Ok::<_, ()>(page) }
        }))
        .unwrap();
        assert_eq!(managers.unread, 0);
        assert!(requests.iter().all(|request| {
            request.project.is_none()
                && request.project_scope.as_deref() == Some("/w")
                && request.limit <= 2
                && request
                    .state
                    .is_some_and(|state| MANAGER_STATES.contains(&state))
        }));
        let ids: Vec<&str> = eligible_parents(&managers.agents, &links, &source)
            .iter()
            .map(|agent| agent.id.as_str())
            .collect();
        assert_eq!(ids, ["a10", "a8"]);

        // Bounded: five rows read in all, and the other five of the
        // scope's ten are counted, never dropped silently.
        let bounded = crate::client::block_on(search_managers(&source, 2, 5, |request| {
            let page = list(&rows, &request);
            async move { Ok::<_, ()>(page) }
        }))
        .unwrap();
        assert_eq!((bounded.agents.len(), bounded.unread), (5, 5));
    }

    #[test]
    fn the_search_reads_at_most_its_bound_including_every_states_total() {
        // More Idle tasks than the bound, then one Busy and one Waiting task:
        // their totals are read inside the bound, never one row past it.
        let mut rows: Vec<UiAgent> = (1..=SEARCH_BOUND + 3)
            .map(|n| agent(&format!("a{n}"), None, AgentState::Idle, n as i64))
            .collect();
        rows.push(agent("a2000", None, AgentState::Busy, 2000));
        rows.push(agent("a2001", None, AgentState::Waiting, 2001));
        let source = agent("a0", None, AgentState::Idle, 0);
        let mut fetched = 0;
        let managers = crate::client::block_on(search_managers(
            &source,
            SEARCH_PAGE,
            SEARCH_BOUND,
            |request| {
                let page = list(&rows, &request);
                fetched += page.rows.len();
                async move { Ok::<_, ()>(page) }
            },
        ))
        .unwrap();
        assert!(fetched <= SEARCH_BOUND, "read {fetched} rows");
        assert_eq!(managers.agents.len(), fetched);
        assert_eq!(managers.agents.len() + managers.unread, rows.len());
    }

    #[test]
    fn a_chats_search_says_it_read_every_project() {
        // A scope-less search reads every project, and its counts say so:
        // a chat has an empty scope, a pending row an unknown one.
        let mut chat = agent("a1", None, AgentState::Idle, 1);
        chat.project_id = String::new();
        assert_eq!(search_scope(&chat), None);
        assert_eq!(Scope::of(chat.scope()), Scope::AllProjects);
        let mut requests = Vec::new();
        let rows = [chat.clone()];
        crate::client::block_on(search_managers(&chat, 2, 10, |request| {
            requests.push(request.clone());
            let page = list(&rows, &request);
            async move { Ok::<_, ()>(page) }
        }))
        .unwrap();
        assert!(
            requests
                .iter()
                .all(|request| request.project.is_none() && request.project_scope.is_none())
        );
        let every = unread_words(chat.scope(), 1_000, 4).unwrap();
        assert!(every.contains("of every project"), "{every}");
        let task = unread_words(Some("/w"), 1_000, 4).unwrap();
        assert!(task.contains("of this project"), "{task}");
        assert_eq!(unread_words(chat.scope(), 3, 0), None);
        assert_eq!(unread_words(None, 3, 0), None);
    }

    #[test]
    fn same_scope_qualifies_when_the_project_id_differs_from_the_workspace() {
        // Same ID with another scope is another authorization namespace: the
        // daemon refuses the move, so the picker never offers it.
        fn placed(
            id: &str,
            project_id: &str,
            scope: Option<&str>,
            workspace: &str,
            state: AgentState,
        ) -> UiAgent {
            let mut row = agent(id, None, state, 1);
            row.project_id = project_id.to_owned();
            row.project_scope = scope.map(str::to_owned);
            row.workspace = Some(workspace.to_owned());
            row
        }
        let source = placed("a1", "p", Some("/repo"), "/repo", AgentState::Idle);
        let same_scope = placed("a2", "p", Some("/repo"), "/repo", AgentState::Idle);
        // Same project ID and workspace, but another tree's scope.
        let other_scope = placed("a3", "p", Some("/elsewhere"), "/repo", AgentState::Idle);
        let other_project = placed("a4", "q", None, "/repo", AgentState::Idle);
        let candidates = vec![
            same_scope.clone(),
            other_scope.clone(),
            other_project.clone(),
            source.clone(),
        ];
        let ids: Vec<&str> = eligible_parents(&candidates, &[], &source)
            .iter()
            .map(|row| row.id.as_str())
            .collect();
        assert_eq!(ids, ["a2"]);
        // A pending row with the same label and workspace is not offered
        // while its scope is unknown, and a pending source is offered
        // nothing: the picker never treats the ID as the scope.
        let mut pending = placed("a5", "p", Some("/repo"), "/repo", AgentState::Idle);
        pending.pending = true;
        let candidates_with_pending = [same_scope.clone(), pending.clone()];
        let ids: Vec<&str> = eligible_parents(&candidates_with_pending, &[], &source)
            .iter()
            .map(|row| row.id.as_str())
            .collect();
        assert_eq!(ids, ["a2"]);
        let none: Vec<&str> = eligible_parents(&candidates, &[], &pending)
            .iter()
            .map(|row| row.id.as_str())
            .collect();
        assert!(none.is_empty(), "{none:?}");
    }

    #[test]
    fn the_move_search_filters_by_effective_scope_never_the_workspace_path() {
        // The search derives its filter from the source row itself
        // ([`search_managers`] reads [`search_scope`]), exactly as the
        // picker does: there is no caller-supplied filter to drift. The
        // source's retained scope differs from its recorded project ID, and
        // a legacy destination shares the scope under another ID: querying
        // by ID would miss it, querying by workspace path would read
        // nothing.
        fn placed(
            id: &str,
            project_id: &str,
            scope: Option<&str>,
            workspace: &str,
            state: AgentState,
        ) -> UiAgent {
            let mut row = agent(id, None, state, 1);
            row.project_id = project_id.to_owned();
            row.project_scope = scope.map(str::to_owned);
            row.workspace = Some(workspace.to_owned());
            row
        }
        let source = placed("a1", "p", Some("/repo"), "/repo/linked", AgentState::Idle);
        let legacy_same_scope = placed("a2", "legacy", Some("/repo"), "/repo", AgentState::Idle);
        let same_id_other_scope = placed(
            "a3",
            "p",
            Some("/elsewhere"),
            "/repo/linked",
            AgentState::Idle,
        );
        // The filter is the effective scope, never the ID or workspace path.
        assert_eq!(search_scope(&source), Some("/repo"));
        let rows = vec![
            source.clone(),
            legacy_same_scope.clone(),
            same_id_other_scope.clone(),
        ];
        let mut requests = Vec::new();
        let managers = crate::client::block_on(search_managers(&source, 10, 100, |request| {
            requests.push(request.clone());
            let page = list(&rows, &request);
            async move { Ok::<_, ()>(page) }
        }))
        .unwrap();
        assert!(requests.iter().all(|request| request.project.is_none()
            && request.project_scope.as_deref() == Some("/repo")));
        let ids: Vec<&str> = eligible_parents(&managers.agents, &[], &source)
            .iter()
            .map(|row| row.id.as_str())
            .collect();
        assert!(ids.contains(&"a2"), "same-scope agent missing: {ids:?}");
        assert!(!ids.contains(&"a3"));
        // By recorded ID the legacy destination is invisible: an
        // ID-filtered read returns only the source's own label.
        let by_id = list(
            &rows,
            &AgentListRequest {
                project: Some("p".to_owned()),
                project_scope: None,
                state: None,
                limit: 10,
                after: None,
                ..AgentListRequest::default()
            },
        );
        assert!(
            !by_id.rows.iter().any(|row| row.id == "a2"),
            "ID query must miss the legacy destination: {:?}",
            by_id.rows.iter().map(|row| &row.id).collect::<Vec<_>>()
        );
    }
}
