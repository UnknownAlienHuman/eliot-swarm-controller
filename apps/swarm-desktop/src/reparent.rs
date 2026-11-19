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
