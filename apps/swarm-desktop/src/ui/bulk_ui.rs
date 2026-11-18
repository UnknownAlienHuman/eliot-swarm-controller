//! Bulk actions (owner request 2026-11-17, k142): remove many project
//! registrations at once, archive every finished task and delete every
//! stopped task tree, for one project, one subproject or everything.
//!
//! Each reads exactly what it would touch first (every `agent.list` page,
//! `project.list`), then asks once, naming the count and what stays and
//! why, then shows progress and a result. Projects leave through one
//! revision-checked settings save (`config_client::remove_projects`, the
//! settings writer); one with a live task is kept with its reason, never
//! forced, and the daemon rechecks usage under the Store lock. Missing
//! registrations go through the stale-project cleanup plan and apply. Tasks go
//! through `agent.archive` / `agent.delete` one tree at a time, each with
//! its own ledger-held mutation id, so a refusal (an `unknown` turn cannot
//! be archived, a tree still stopping) is reported per task and an
//! uncertain outcome keeps its id for the next run. Delete removes records
//! only: folders, files, branches and native store files stay.

use super::*;
use crate::nav::Checkout;
use crate::theme::winui::Palette;
use eliot_core::api::{
    ArtifactKind, CleanupApplyRequest, CleanupApplyResponse, CleanupPlan, CleanupPlanRequest,
    CleanupScope,
};

/// What a bulk action does.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum BulkOp {
    RemoveProjects,
    ArchiveFinished,
    DeleteStopped,
}

impl BulkOp {
    fn verb(self) -> &'static str {
        match self {
            Self::RemoveProjects => "Remove",
            Self::ArchiveFinished => "Archive",
            Self::DeleteStopped => "Delete",
        }
    }

    fn doing(self) -> &'static str {
        match self {
            Self::RemoveProjects => "Removing",
            Self::ArchiveFinished => "Archiving",
            Self::DeleteStopped => "Deleting",
        }
    }

    fn done(self) -> &'static str {
        match self {
            Self::RemoveProjects => "Removed",
            Self::ArchiveFinished => "Archived",
            Self::DeleteStopped => "Deleted",
        }
    }

    fn noun(self, count: usize) -> String {
        match self {
            Self::RemoveProjects => eliot_core::count_words(count as u64, "project", "projects"),
            Self::ArchiveFinished => {
                eliot_core::count_words(count as u64, "finished task", "finished tasks")
            }
            Self::DeleteStopped => {
                eliot_core::count_words(count as u64, "stopped tree", "stopped trees")
            }
        }
    }
}

/// Which registrations or tasks a bulk action reads.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Pick {
    /// Settings › Projects' checked registrations (their IDs).
    Selected(Vec<String>),
    /// Registrations nothing uses: no live task, and the folder is gone or
    /// no task ever ran there.
    Unused,
    /// One missing registration's one-click remove (no confirmation).
    Missing(String),
    /// The tasks of one project or subproject (its sidebar key), or of
    /// everything (`None`).
    Tasks(Option<String>),
}

/// One thing a bulk action acts on: a registration or a task tree's root.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct BulkItem {
    pub(super) id: String,
    pub(super) name: String,
}

/// What a bulk action would do: the items, and what stays and why.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(super) struct BulkPlan {
    pub(super) items: Vec<BulkItem>,
    pub(super) kept: Vec<String>,
    affected_tasks: usize,
    missing_cleanup: Option<MissingCleanup>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct MissingCleanup {
    plan_id: String,
    artifact_id: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Phase {
    Reading,
    Confirm,
    /// Acting; `stopping` once Stop was pressed (the running step ends).
    Running {
        stopping: bool,
    },
    Done,
}

/// The open bulk dialog. `seq` is the dialog's immutable epoch: every
/// read and step carries the epoch of the dialog that started it, so a
/// late reply can never land on a cancelled or replaced dialog (#682).
pub(super) struct BulkDialog {
    pub(super) seq: u64,
    pub(super) op: BulkOp,
    pub(super) pick: Pick,
    /// What it covers, in words ("all projects", "atlas-web").
    pub(super) scope: String,
    pub(super) phase: Phase,
    pub(super) plan: BulkPlan,
    /// A read or the save failed (definite or not).
    pub(super) error: Option<String>,
    /// Per item, in order: its name and how it went.
    pub(super) outcomes: Vec<(String, Outcome)>,
}

/// Done, or why not and whether that is uncertain.
pub(super) type Outcome = Result<(), (bool, String)>;

#[derive(Clone, Debug)]
pub(super) enum BulkMsg {
    /// The Projects caption's "…" menu: the label chosen (Reactor 0.100.0
    /// reports a native menu item by its label, not its key).
    Menu(String),
    /// Read what an action would touch (the action, what, its scope).
    Start(BulkOp, Pick, String),
    /// The plan read for one dialog epoch, with the operation and
    /// selection it was read for.
    Planned(u64, BulkOp, Pick, String, Result<BulkPlan, String>),
    /// The confirmation's answer.
    Confirmed(bool),
    /// One task tree settled (its dialog epoch, its index).
    Stepped(u64, usize, MutationResult<usize>),
    /// The registrations' save settled (its dialog epoch): removed names
    /// and what stayed, or the save's error (whether it is uncertain, its
    /// words).
    Removed(u64, Result<(Vec<String>, Vec<String>), (bool, String)>),
    /// Stop after the running step.
    Stop,
    Closed,
}

/// The Projects caption's menu items: (id, label).
pub(super) const ALL_MENU: [(&str, &str); 3] = [
    ("archive", "Archive all finished tasks…"),
    ("delete", "Delete all stopped tasks…"),
    ("unused", "Remove unused projects…"),
];

/// Most names a confirmation lists before "and N more".
const LISTED: usize = 8;
/// `agent.list` pages a bulk read takes at most (1,000 rows each).
const PAGES: usize = 20;

/// Every listed Agent, page by page, under the `archived` filter.
fn read_agents(data_dir: &Path, archived: Option<&str>) -> Result<Vec<UiAgent>, String> {
    let mut agents = Vec::new();
    let mut after = None;
    for _ in 0..PAGES {
        let request = AgentListRequest {
            after: after.clone(),
            limit: 1_000,
            ..Default::default()
        };
        let page = load_agents_blocking(data_dir, &request, archived)?;
        let done = page.rows.is_empty() || page.next.is_none();
        after = page.next;
        agents.extend(page.rows);
        if done {
            return Ok(agents);
        }
    }
    Err("there are more tasks than one bulk action reads; try a single project".to_owned())
}

/// Scoped mutations use the retained execution directory and an exact
/// daemon-verified checkout. Folder-only rows have no repository scope.
fn in_scope(agent: &UiAgent, scope: Option<&str>, checkouts: &[Checkout]) -> bool {
    let Some(key) = scope else {
        return true;
    };
    let Some(workspace) = agent.workspace.as_deref().filter(|_| !agent.chat) else {
        return false;
    };
    let workspace = nav::path_key(workspace);
    let mut verified = checkouts
        .iter()
        .filter(|checkout| checkout.main.is_some() && nav::path_key(&checkout.path) == workspace);
    let Some(checkout) = verified.next() else {
        return false;
    };
    verified.next().is_none()
        && (key == format!("repo:{}", nav::path_key(&checkout.common_dir))
            || (checkout.main == Some(false) && key == format!("sub:{workspace}")))
}

/// A kept-count line ("3 live tasks stay: …"), or nothing for zero.
fn kept_line(count: usize, one: &str, many: &str, why: &str) -> Option<String> {
    (count > 0).then(|| {
        format!(
            "{} stay{}: {why}",
            eliot_core::count_words(count as u64, one, many),
            if count == 1 { "s" } else { "" }
        )
    })
}

/// Whether a planned read belongs to the open dialog: its epoch,
/// still reading, and the same operation and selection (#682).
fn planned_current(
    dialog: Option<&BulkDialog>,
    seq: u64,
    op: BulkOp,
    pick: &Pick,
    scope: &str,
) -> bool {
    dialog.is_some_and(|dialog| {
        dialog.seq == seq
            && dialog.phase == Phase::Reading
            && dialog.op == op
            && dialog.pick == *pick
            && dialog.scope == scope
    })
}

/// The one-click remove's target: the picked registration, but only when
/// the dialog's plan is bound to exactly that registration and its own
/// cleanup identity. A late plan from a cancelled Selected removal has no
/// cleanup identity and must never auto-apply here (#682).
fn missing_bound(dialog: &BulkDialog) -> Option<(String, MissingCleanup)> {
    match &dialog.pick {
        Pick::Missing(project)
            if dialog.plan.items.len() == 1 && dialog.plan.items[0].id == *project =>
        {
            dialog
                .plan
                .missing_cleanup
                .clone()
                .map(|cleanup| (project.clone(), cleanup))
        }
        _ => None,
    }
}

/// What archiving or deleting would touch: the roots of top-level task
/// trees in scope (a tree goes with its root; the daemon refuses one whose
/// worker still runs).
fn plan_tasks(
    data_dir: &Path,
    op: BulkOp,
    scope: Option<&str>,
    show_archived: bool,
) -> Result<BulkPlan, String> {
    let agents = read_agents(data_dir, archived_filter(op, show_archived))?;
    let checkouts = if scope.is_some() {
        project_checkouts(&load_projects_blocking(data_dir)?)
    } else {
        Vec::new()
    };
    Ok(task_plan(&agents, op, scope, &checkouts))
}

fn archived_filter(op: BulkOp, show_archived: bool) -> Option<&'static str> {
    (op == BulkOp::DeleteStopped || show_archived).then_some("include")
}

fn task_plan(
    agents: &[UiAgent],
    op: BulkOp,
    scope: Option<&str>,
    checkouts: &[Checkout],
) -> BulkPlan {
    let mut seen = HashSet::new();
    let roots: Vec<&UiAgent> = agents
        .iter()
        .filter(|agent| {
            agent.parent.is_none()
                && !agent.native_child
                && seen.insert(agent.id.as_str())
                && in_scope(agent, scope, checkouts)
        })
        .collect();
    let mut plan = BulkPlan::default();
    let count = |keep: &dyn Fn(&UiAgent) -> bool| roots.iter().filter(|agent| keep(agent)).count();
    let item = |agent: &&UiAgent| BulkItem {
        id: agent.id.clone(),
        name: agent.title.clone(),
    };
    match op {
        BulkOp::ArchiveFinished => {
            plan.items = roots
                .iter()
                .filter(|agent| !agent.live() && !agent.archived)
                .map(item)
                .collect();
            plan.kept.extend(kept_line(
                count(&|agent| agent.live()),
                "live task",
                "live tasks",
                "still working, idle or waiting for you",
            ));
        }
        BulkOp::DeleteStopped => {
            use eliot_core::AgentState;
            plan.items = roots
                .iter()
                .filter(|agent| agent.state == AgentState::Stopped)
                .map(item)
                .collect();
            plan.affected_tasks = affected_tasks(agents, &plan.items);
            plan.kept.extend(kept_line(
                count(&|agent| agent.state == AgentState::Failed),
                "failed task",
                "failed tasks",
                "left for you to decide (Resume, Why or Archive)",
            ));
            plan.kept.extend(kept_line(
                count(&|agent| agent.state == AgentState::Lost),
                "lost task",
                "lost tasks",
                "left for Resume or Archive",
            ));
            plan.kept.extend(kept_line(
                count(&|agent| agent.live()),
                "live task",
                "live tasks",
                "still working, idle or waiting; a task is deleted once stopped",
            ));
        }
        BulkOp::RemoveProjects => {}
    }
    plan
}

fn affected_tasks(agents: &[UiAgent], roots: &[BulkItem]) -> usize {
    let mut children: HashMap<&str, Vec<&str>> = HashMap::new();
    for agent in agents {
        if let Some(parent) = agent.parent.as_deref() {
            children.entry(parent).or_default().push(&agent.id);
        }
    }
    let mut pending: Vec<_> = roots.iter().map(|item| item.id.as_str()).collect();
    let mut affected = HashSet::new();
    while let Some(id) = pending.pop() {
        if affected.insert(id)
            && let Some(children) = children.get(id)
        {
            pending.extend(children);
        }
    }
    affected.len()
}

/// Each registration's usage, its name and whether its folder is gone.
struct Registrations {
    rows: Vec<(String, String, nav::Use, bool)>,
}

fn read_registrations(data_dir: &Path) -> Result<Registrations, String> {
    let registrations = load_projects_blocking(data_dir)?;
    let agents = read_agents(data_dir, Some("include"))?;
    let checkouts = project_checkouts(&registrations);
    let pairs: Vec<(String, String)> = registrations
        .iter()
        .map(|registration| (registration.id.clone(), registration.workspace.clone()))
        .collect();
    let usage = nav::registration_use(&pairs, &agents, &checkouts);
    Ok(Registrations {
        rows: registrations
            .iter()
            .map(|registration| {
                (
                    registration.id.clone(),
                    nav::place_label(&registration.workspace, &checkouts),
                    usage.get(&registration.id).copied().unwrap_or_default(),
                    registration.missing,
                )
            })
            .collect(),
    })
}

/// A registration's use in words, for what stays.
fn use_words(usage: nav::Use) -> String {
    if usage.live > 0 {
        format!(
            "{} working, idle or waiting",
            eliot_core::count_words(usage.live as u64, "task is", "tasks are")
        )
    } else {
        format!(
            "{} ran there",
            eliot_core::count_words(usage.tasks as u64, "task", "tasks")
        )
    }
}

fn plan_projects(data_dir: &Path, pick: &Pick) -> Result<BulkPlan, String> {
    let registrations = read_registrations(data_dir)?;
    let mut plan = BulkPlan::default();
    for (id, name, usage, missing) in registrations.rows {
        let wanted = match pick {
            Pick::Selected(ids) => ids.contains(&id),
            Pick::Unused => true,
            Pick::Missing(_) | Pick::Tasks(_) => false,
        };
        if !wanted {
            continue;
        }
        let removable = usage.live == 0
            && match pick {
                Pick::Unused => missing || usage.tasks == 0,
                _ => true,
            };
        if removable {
            plan.items.push(BulkItem { id, name });
        } else {
            plan.kept.push(format!("{name}: {}", use_words(usage)));
        }
    }
    Ok(plan)
}

fn plan_missing_project(
    data_dir: &Path,
    project: &str,
    name: String,
    retained: Option<MissingCleanup>,
) -> Result<BulkPlan, String> {
    let mut plan = BulkPlan::default();
    let cleanup = if let Some(retained) = retained {
        retained
    } else {
        let request = CleanupPlanRequest {
            scope: CleanupScope::Project,
            project: Some(project.to_owned()),
            older_than: None,
            continuation: None,
        };
        let reply: CleanupPlan = client::block_on(eliot_daemon::client_call_typed(
            data_dir,
            "cleanup.plan",
            &request,
        ))
        .map_err(|error| error.user_message())?;
        let Some(item) = reply.items.iter().find(|item| {
            item.artifact.kind == ArtifactKind::ProjectRegistration
                && item.artifact.project == project
        }) else {
            return Err(
                "The daemon did not return this project registration in the cleanup plan."
                    .to_owned(),
            );
        };
        if item.verdict != "safe" {
            plan.kept.push(format!("{name}: {}", item.reason));
            return Ok(plan);
        }
        MissingCleanup {
            plan_id: reply.plan_id,
            artifact_id: item.artifact.id.clone(),
        }
    };
    plan.items.push(BulkItem {
        id: project.to_owned(),
        name,
    });
    plan.missing_cleanup = Some(cleanup);
    Ok(plan)
}

fn apply_missing_project(
    data_dir: &Path,
    name: &str,
    cleanup: MissingCleanup,
    id: String,
) -> Result<(Vec<String>, Vec<String>), (bool, String)> {
    let request = CleanupApplyRequest {
        id,
        plan_id: cleanup.plan_id,
        delete_remote: false,
        artifacts: Some(vec![cleanup.artifact_id.clone()]),
    };
    let reply: CleanupApplyResponse = client::block_on(eliot_daemon::client_call_typed(
        data_dir,
        "cleanup.apply",
        &request,
    ))
    .map_err(|error| (crate::state::uncertain(&error), error.user_message()))?;
    let mut matches = reply
        .items
        .iter()
        .filter(|item| item.artifact_id == cleanup.artifact_id);
    let outcome = matches
        .next()
        .filter(|_| reply.plan_id == request.plan_id && matches.next().is_none())
        .ok_or_else(|| {
            (
                true,
                "The cleanup reply did not identify the selected registration.".to_owned(),
            )
        })?;
    let reason = outcome
        .reason
        .as_deref()
        .unwrap_or("cleanup outcome unavailable");
    match outcome.outcome.as_str() {
        "removed" => Ok((vec![name.to_owned()], Vec::new())),
        "skipped" => Ok((Vec::new(), vec![format!("{name}: {reason}")])),
        _ => Err((true, reason.to_owned())),
    }
}

/// Remove the registrations in one settings save, after checking again
/// that none got a live task meanwhile; read back what is gone.
fn remove_checked(
    data_dir: &Path,
    items: &[BulkItem],
) -> Result<(Vec<String>, Vec<String>), (bool, String)> {
    let definite = |error: String| (false, error);
    let now = read_registrations(data_dir).map_err(definite)?;
    let mut kept = Vec::new();
    let mut ids = Vec::new();
    for item in items {
        match now.rows.iter().find(|(id, ..)| *id == item.id) {
            Some((_, _, usage, _)) if usage.live > 0 => kept.push(format!(
                "{}: {} since the review",
                item.name,
                use_words(*usage)
            )),
            Some(_) => ids.push(item.id.clone()),
            None => kept.push(format!("{}: already removed", item.name)),
        }
    }
    if ids.is_empty() {
        return Ok((Vec::new(), kept));
    }
    client::block_on(config_client::remove_projects(data_dir, &ids))
        .map_err(|error| (crate::state::uncertain(&error), error.user_message()))?;
    let after = client::block_on(config_client::list_projects(data_dir)).map_err(|error| {
        (
            true,
            format!("saved, but the project list was not read back: {error}"),
        )
    })?;
    let mut removed = Vec::new();
    for item in items.iter().filter(|item| ids.contains(&item.id)) {
        if after.iter().any(|registration| registration.id == item.id) {
            kept.push(format!("{}: still registered after the save", item.name));
        } else {
            removed.push(item.name.clone());
        }
    }
    Ok((removed, kept))
}

impl DesktopApp {
    pub(super) fn bulk_update(&mut self, msg: BulkMsg, context: &ComponentContext<Self>) {
        match msg {
            BulkMsg::Menu(label) => match ALL_MENU
                .iter()
                .find(|(_, item)| *item == label)
                .map_or("", |(id, _)| *id)
            {
                "archive" => self.start_bulk(
                    BulkOp::ArchiveFinished,
                    Pick::Tasks(None),
                    "all projects".to_owned(),
                    context,
                ),
                "delete" => self.start_bulk(
                    BulkOp::DeleteStopped,
                    Pick::Tasks(None),
                    "all projects".to_owned(),
                    context,
                ),
                "unused" => self.start_bulk(
                    BulkOp::RemoveProjects,
                    Pick::Unused,
                    "all projects".to_owned(),
                    context,
                ),
                _ => {}
            },
            BulkMsg::Start(op, pick, scope) => self.start_bulk(op, pick, scope, context),
            BulkMsg::Planned(seq, op, pick, scope, result) => {
                // A late plan from a cancelled or replaced dialog never
                // lands on whatever is open now (#682).
                if !planned_current(self.bulk.as_ref(), seq, op, &pick, &scope) {
                    return;
                }
                // The owner's own names, where they gave one.
                let result = result.map(|mut plan| {
                    for item in &mut plan.items {
                        let name = if op == BulkOp::RemoveProjects {
                            self.nav
                                .projects
                                .iter()
                                .find(|project| project.id == item.id)
                                .map(|project| self.project_label(project))
                        } else {
                            self.prefs
                                .display_name(&format!("agent:{}", item.id))
                                .map(str::to_owned)
                        };
                        if let Some(name) = name {
                            item.name = name;
                        }
                    }
                    plan
                });
                let Some(dialog) = self.bulk.as_mut() else {
                    return;
                };
                match result {
                    Ok(plan) => {
                        dialog.plan = plan;
                        dialog.phase = Phase::Confirm;
                        // The missing folder's one click needs no question.
                        if matches!(dialog.pick, Pick::Missing(_)) && !dialog.plan.items.is_empty()
                        {
                            self.bulk_update(BulkMsg::Confirmed(true), context);
                        }
                    }
                    Err(error) => {
                        dialog.error = Some(error);
                        dialog.phase = Phase::Done;
                    }
                }
            }
            BulkMsg::Confirmed(false) | BulkMsg::Closed => {
                let acted = self
                    .bulk
                    .take()
                    .is_some_and(|dialog| matches!(dialog.phase, Phase::Done));
                if acted {
                    self.after_bulk(context);
                }
            }
            BulkMsg::Confirmed(true) => {
                let Some(dialog) = self.bulk.as_mut() else {
                    return;
                };
                if dialog.phase != Phase::Confirm || dialog.plan.items.is_empty() {
                    return;
                }
                dialog.phase = Phase::Running { stopping: false };
                let seq = dialog.seq;
                if dialog.op == BulkOp::RemoveProjects {
                    let Ok(data_dir) = self.data_dir.clone() else {
                        return;
                    };
                    let missing = matches!(dialog.pick, Pick::Missing(_));
                    if missing {
                        // The one-click remove runs only for the plan bound
                        // to its exact registration and cleanup identity;
                        // a late Selected plan (no cleanup identity) or a
                        // mismatched one never removes here (#682).
                        let bound = missing_bound(dialog);
                        let Some((project, cleanup)) = bound else {
                            dialog.error = Some(
                                "The removal plan no longer matches this project; review again."
                                    .to_owned(),
                            );
                            dialog.phase = Phase::Done;
                            return;
                        };
                        let name = dialog
                            .plan
                            .items
                            .first()
                            .map(|item| item.name.clone())
                            .unwrap_or_default();
                        self.missing_cleanup
                            .insert(project.clone(), cleanup.clone());
                        let id = self.ledger.id_for(&missing_cleanup_key(&project));
                        let _task = context.spawn_background(move |_| {
                            Message::Bulk(BulkMsg::Removed(
                                seq,
                                apply_missing_project(&data_dir, &name, cleanup, id),
                            ))
                        });
                    } else {
                        let items = dialog.plan.items.clone();
                        let _task = context.spawn_background(move |_| {
                            Message::Bulk(BulkMsg::Removed(seq, remove_checked(&data_dir, &items)))
                        });
                    }
                } else {
                    self.bulk_step(0, context);
                }
            }
            BulkMsg::Stop => {
                if let Some(dialog) = self.bulk.as_mut()
                    && let Phase::Running { stopping } = &mut dialog.phase
                {
                    *stopping = true;
                }
            }
            BulkMsg::Stepped(seq, index, outcome) => {
                if self.bulk.as_ref().is_none_or(|dialog| dialog.seq != seq) {
                    return;
                }
                self.bulk_stepped(index, outcome, context)
            }
            BulkMsg::Removed(seq, result) => {
                let Some(dialog) = self.bulk.as_mut() else {
                    return;
                };
                if dialog.seq != seq {
                    return;
                }
                if let Pick::Missing(project) = &dialog.pick
                    && !matches!(&result, Err((true, _)))
                {
                    self.ledger.complete(&missing_cleanup_key(project));
                    self.missing_cleanup.remove(project);
                }
                match result {
                    Ok((removed, kept)) => {
                        dialog
                            .outcomes
                            .extend(removed.into_iter().map(|name| (name, Ok(()))));
                        dialog.plan.kept.extend(kept);
                    }
                    Err(error) => dialog.error = Some(save_words(error)),
                }
                dialog.phase = Phase::Done;
                let quiet = matches!(dialog.pick, Pick::Missing(_))
                    && dialog.error.is_none()
                    && dialog.plan.kept.is_empty();
                self.after_bulk(context);
                if quiet {
                    let name = self
                        .bulk
                        .take()
                        .and_then(|dialog| dialog.outcomes.first().map(|(name, _)| name.clone()))
                        .unwrap_or_default();
                    self.set_status(format!("Removed {name}: its folder was gone."));
                }
            }
        }
    }

    /// Open the dialog and read what the action would touch.
    pub(super) fn start_bulk(
        &mut self,
        op: BulkOp,
        pick: Pick,
        scope: String,
        context: &ComponentContext<Self>,
    ) {
        if self.bulk.is_some() {
            return;
        }
        let Ok(data_dir) = self.data_dir.clone() else {
            return;
        };
        self.close_menu();
        self.bulk_seq += 1;
        let seq = self.bulk_seq;
        self.bulk = Some(BulkDialog {
            seq,
            op,
            pick: pick.clone(),
            scope: scope.clone(),
            phase: Phase::Reading,
            plan: BulkPlan::default(),
            error: None,
            outcomes: Vec::new(),
        });
        let show_archived = self.nav.show_archived;
        let retained = match &pick {
            Pick::Missing(project) => self.missing_cleanup.get(project).cloned(),
            _ => None,
        };
        let _task = context.spawn_background(move |_| {
            let plan = match &pick {
                Pick::Tasks(scope) => plan_tasks(&data_dir, op, scope.as_deref(), show_archived),
                Pick::Missing(project) => {
                    plan_missing_project(&data_dir, project, scope.clone(), retained)
                }
                _ => plan_projects(&data_dir, &pick),
            };
            Message::Bulk(BulkMsg::Planned(seq, op, pick, scope, plan))
        });
    }

    /// Archive or delete the plan's task at `index`.
    fn bulk_step(&mut self, index: usize, context: &ComponentContext<Self>) {
        let Some(dialog) = self.bulk.as_ref() else {
            return;
        };
        let Some(item) = dialog.plan.items.get(index) else {
            return;
        };
        let Ok(data_dir) = self.data_dir.clone() else {
            return;
        };
        let op = dialog.op;
        let seq = dialog.seq;
        let agent = item.id.clone();
        let mutation = self.ledger.id_for(&step_key(op, &agent));
        let _task = context.spawn_background(move |_| {
            let outcome = match op {
                BulkOp::ArchiveFinished => {
                    client::block_on(client::archive_agent(&data_dir, mutation, &agent, true))
                }
                _ => client::block_on(client::delete_agent(&data_dir, mutation, &agent)),
            };
            Message::Bulk(BulkMsg::Stepped(seq, index, outcome))
        });
    }

    fn bulk_stepped(
        &mut self,
        index: usize,
        outcome: MutationResult<usize>,
        context: &ComponentContext<Self>,
    ) {
        let Some(dialog) = self.bulk.as_mut() else {
            return;
        };
        let Some(item) = dialog.plan.items.get(index).cloned() else {
            return;
        };
        let key = step_key(dialog.op, &item.id);
        let result = match outcome {
            Ok(_) => Ok(()),
            Err(error) => Err((crate::state::uncertain(&error), error.user_message())),
        };
        // A definite outcome releases the id; an uncertain one keeps it,
        // so the next run asks the daemon about the same request.
        if !matches!(result, Err((true, _))) {
            self.ledger.complete(&key);
        }
        let Some(dialog) = self.bulk.as_mut() else {
            return;
        };
        dialog.outcomes.push((item.name, result));
        let stopping = matches!(dialog.phase, Phase::Running { stopping: true });
        if !stopping && index + 1 < dialog.plan.items.len() {
            self.bulk_step(index + 1, context);
        } else {
            dialog.phase = Phase::Done;
            self.after_bulk(context);
        }
    }

    /// Read what a bulk action changed: projects, settings and the list.
    fn after_bulk(&mut self, context: &ComponentContext<Self>) {
        self.after_settings_saved(context);
        self.shell_update(ShellMsg::RefreshRequested, context);
    }

    /// The bulk dialog: the question, the progress or the result.
    pub(super) fn bulk_dialog(
        &self,
        dialog: &BulkDialog,
        context: &mut ViewContext<Self>,
        palette: Palette,
    ) -> View {
        let count = dialog.plan.items.len();
        // Projects are named in the list; tasks say where they are.
        let place = if dialog.op == BulkOp::RemoveProjects {
            String::new()
        } else {
            format!(" in {}", dialog.scope)
        };
        let title = match dialog.phase {
            Phase::Reading => format!("{}: reading {}…", dialog.op.verb(), dialog.scope),
            Phase::Confirm if count == 0 => {
                format!("Nothing to {}{place}", dialog.op.verb().to_lowercase())
            }
            Phase::Confirm if dialog.op == BulkOp::DeleteStopped => format!(
                "Delete {}{place} ({} affected)?",
                dialog.op.noun(count),
                eliot_core::count_words(dialog.plan.affected_tasks as u64, "task", "tasks"),
            ),
            Phase::Confirm => format!("{} {}{place}?", dialog.op.verb(), dialog.op.noun(count)),
            Phase::Running { .. } => format!(
                "{} {} of {}…",
                dialog.op.doing(),
                (dialog.outcomes.len() + 1).min(count),
                count
            ),
            Phase::Done => format!("Done{place}"),
        };
        let mut lines: Vec<KeyedView> = Vec::new();
        let mut line = |key: String, view: View| lines.push(KeyedView::new(key, view));
        match dialog.phase {
            Phase::Reading => line(
                "reading".to_owned(),
                StackPanel::new()
                    .orientation(Orientation::Horizontal)
                    .spacing(tokens::spacing::SM)
                    .children((
                        ProgressRing::new()
                            .is_indeterminate(true)
                            .width(16.0)
                            .height(16.0),
                        m3::secondary("Reading every task and project…"),
                    )),
            ),
            Phase::Confirm => {
                for (index, item) in dialog.plan.items.iter().take(LISTED).enumerate() {
                    line(
                        format!("item:{index}"),
                        m3::body(format!("• {}", item.name))
                            .text_trimming(TextTrimming::CharacterEllipsis)
                            .into(),
                    );
                }
                if count > LISTED {
                    line(
                        "more".to_owned(),
                        m3::secondary(format!("and {} more", count - LISTED)).into(),
                    );
                }
                if count > 0 {
                    line(
                    "what".to_owned(),
                    m3::secondary(match dialog.op {
                        BulkOp::RemoveProjects => {
                            "Swarm forgets these projects in one save. Their folders, files and tasks stay; you can add a project again in Settings › Projects."
                        }
                        BulkOp::ArchiveFinished => {
                            "Archived tasks leave the list; Show archived brings them back, and Unarchive restores one."
                        }
                        BulkOp::DeleteStopped => {
                            "Their conversations and records leave Swarm for good. Folders, files, branches and the harnesses' own session files stay."
                        }
                    })
                    .text_wrapping(TextWrapping::Wrap)
                    .into(),
                );
                }
            }
            Phase::Running { stopping } => line(
                "running".to_owned(),
                StackPanel::new()
                    .orientation(Orientation::Horizontal)
                    .spacing(tokens::spacing::SM)
                    .children((
                        ProgressRing::new()
                            .is_indeterminate(true)
                            .width(16.0)
                            .height(16.0),
                        m3::secondary(if stopping {
                            "Stopping after this one…"
                        } else {
                            "Each one is checked by Swarm as it goes."
                        }),
                    )),
            ),
            Phase::Done => {
                let done = dialog
                    .outcomes
                    .iter()
                    .filter(|(_, result)| result.is_ok())
                    .count();
                line(
                    "summary".to_owned(),
                    m3::text(
                        format!("{} {}.", dialog.op.done(), dialog.op.noun(done)),
                        tokens::text::SECTION_TITLE,
                    )
                    .into(),
                );
                let names: Vec<&String> = dialog
                    .outcomes
                    .iter()
                    .filter(|(_, result)| result.is_ok())
                    .map(|(name, _)| name)
                    .collect();
                for (index, name) in names.iter().take(LISTED).enumerate() {
                    line(
                        format!("done:{index}"),
                        m3::body(format!("• {name}"))
                            .text_trimming(TextTrimming::CharacterEllipsis)
                            .into(),
                    );
                }
                if names.len() > LISTED {
                    line(
                        "done-more".to_owned(),
                        m3::secondary(format!("and {} more", names.len() - LISTED)).into(),
                    );
                }
                let left = count.saturating_sub(dialog.outcomes.len());
                if left > 0 && dialog.error.is_none() && dialog.op != BulkOp::RemoveProjects {
                    line(
                        "left".to_owned(),
                        m3::secondary(format!(
                            "Stopped before the other {}.",
                            eliot_core::count_words(left as u64, "one", "ones")
                        ))
                        .into(),
                    );
                }
                for (index, (name, result)) in dialog.outcomes.iter().enumerate() {
                    if let Err((uncertain, why)) = result {
                        let words = if *uncertain {
                            format!(
                                "{name}: not confirmed ({why}); running it again asks about the same request"
                            )
                        } else {
                            format!("{name}: not {} ({why})", dialog.op.done().to_lowercase())
                        };
                        line(
                            format!("refused:{index}"),
                            m3::body(words)
                                .foreground(palette.error())
                                .text_wrapping(TextWrapping::Wrap)
                                .is_text_selection_enabled(true)
                                .into(),
                        );
                    }
                }
            }
        }
        if let Some(error) = &dialog.error {
            line(
                "error".to_owned(),
                m3::body(error.clone())
                    .foreground(palette.error())
                    .text_wrapping(TextWrapping::Wrap)
                    .is_text_selection_enabled(true)
                    .into(),
            );
        }
        if !dialog.plan.kept.is_empty() && dialog.phase != Phase::Reading {
            line(
                "kept-title".to_owned(),
                m3::text("Kept", tokens::text::SECTION_TITLE).into(),
            );
            for (index, kept) in dialog.plan.kept.iter().enumerate() {
                line(
                    format!("kept:{index}"),
                    m3::secondary(kept.clone())
                        .text_wrapping(TextWrapping::Wrap)
                        .into(),
                );
            }
        }
        let close_label = match dialog.phase {
            Phase::Done => "Close",
            Phase::Running { .. } => "Stop",
            _ => "Cancel",
        };
        let close_message = match dialog.phase {
            Phase::Running { .. } => BulkMsg::Stop,
            Phase::Done => BulkMsg::Closed,
            _ => BulkMsg::Confirmed(false),
        };
        let close = m3::text_button(palette)
            .element_ref(&self.dialog.close)
            .is_enabled(
                !matches!(dialog.phase, Phase::Running { stopping: true })
                    && !(dialog.op == BulkOp::RemoveProjects
                        && matches!(dialog.phase, Phase::Running { .. })),
            )
            .automation_name(format!("{close_label} {}", dialog.op.verb().to_lowercase()))
            .on_click(context.message(Message::Bulk(close_message)))
            .content(m3::label(close_label));
        let primary: View = if dialog.phase == Phase::Confirm && count > 0 {
            let label = format!("{} {}", dialog.op.verb(), dialog.op.noun(count));
            m3::destructive(palette)
                .element_ref(&self.dialog.primary)
                .automation_name(format!("Confirm: {label}"))
                .on_click(context.message(Message::Bulk(BulkMsg::Confirmed(true))))
                .content(m3::label(&label))
        } else {
            View::empty()
        };
        super::dialog_ui::surface(
            context,
            self.dialog.epoch,
            palette,
            title,
            StackPanel::new()
                .spacing(tokens::spacing::XS)
                .keyed_children(lines),
            super::dialog_ui::actions(close, primary),
            460.0,
        )
    }
}

/// The ledger key of one bulk step.
fn missing_cleanup_key(project: &str) -> String {
    format!("cleanup.project:{project}")
}

fn step_key(op: BulkOp, agent: &str) -> String {
    match op {
        BulkOp::ArchiveFinished => format!("archive:{agent}:true"),
        _ => format!("delete:{agent}"),
    }
}

/// The settings save's error in words.
fn save_words((uncertain, error): (bool, String)) -> String {
    if uncertain {
        format!("The save was not confirmed: {error}. Review again before removing more.")
    } else {
        format!("Nothing was removed: {error}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use eliot_core::AgentState;

    fn stopped(id: &str, parent: Option<&str>, workspace: &str) -> UiAgent {
        UiAgent {
            id: id.to_owned(),
            generation: 1,
            harness: "codex".to_owned(),
            state: AgentState::Stopped,
            project_id: workspace.to_owned(),
            project_scope: None,
            repo_common_dir: None,
            workspace: Some(workspace.to_owned()),
            parent: parent.map(str::to_owned),
            updated_at: 0,
            title: id.to_owned(),
            native_child: false,
            tokens: None,
            chat: false,
            archived: true,
            pending: false,
        }
    }

    #[test]
    fn delete_all_includes_archived_roots_and_counts_descendants() {
        assert_eq!(
            archived_filter(BulkOp::DeleteStopped, false),
            Some("include")
        );
        let agents = [
            stopped("a1", None, "/checkout"),
            stopped("a2", Some("a1"), "/checkout"),
            stopped("a3", Some("a2"), "/checkout"),
        ];
        let plan = task_plan(&agents, BulkOp::DeleteStopped, None, &[]);
        assert_eq!(plan.items.len(), 1);
        assert_eq!(plan.items[0].id, "a1");
        assert_eq!(plan.affected_tasks, 3);
    }

    #[test]
    fn scope_requires_exact_verified_workspace_even_when_labels_match() {
        let mut verified = Checkout::new("/repo/linked", "/repo/.git");
        verified.main = Some(false);
        let mut shadow = stopped("a1", None, "/repo/linked/other-repo");
        shadow.project_id = verified.path.clone();
        let linked = stopped("a2", None, &verified.path);
        let stale = Checkout::new("/unverified", "/repo/.git");
        let unverified = stopped("a3", None, &stale.path);
        let plan = task_plan(
            &[shadow, linked, unverified],
            BulkOp::DeleteStopped,
            Some("repo:/repo/.git"),
            &[verified, stale],
        );
        assert_eq!(
            plan.items
                .iter()
                .map(|item| item.id.as_str())
                .collect::<Vec<_>>(),
            ["a2"]
        );
    }
    fn dialog(seq: u64, pick: Pick, plan: BulkPlan, phase: Phase) -> BulkDialog {
        BulkDialog {
            seq,
            op: BulkOp::RemoveProjects,
            pick,
            scope: "all projects".to_owned(),
            phase,
            plan,
            error: None,
            outcomes: Vec::new(),
        }
    }

    fn missing_plan(id: &str, cleanup: bool) -> BulkPlan {
        BulkPlan {
            items: vec![BulkItem {
                id: id.to_owned(),
                name: id.to_owned(),
            }],
            missing_cleanup: cleanup.then(|| MissingCleanup {
                plan_id: "plan".to_owned(),
                artifact_id: "artifact".to_owned(),
            }),
            ..BulkPlan::default()
        }
    }

    #[test]
    fn late_or_foreign_plans_never_land_on_the_open_dialog() {
        // #682: a cancelled Selected removal's late plan must not install
        // onto the Missing dialog that replaced it.
        let missing = Pick::Missing("p2".to_owned());
        let open = dialog(7, missing.clone(), BulkPlan::default(), Phase::Reading);
        assert!(planned_current(
            Some(&open),
            7,
            BulkOp::RemoveProjects,
            &missing,
            "all projects"
        ));
        // An older epoch, another operation, selection or scope: dropped.
        assert!(!planned_current(
            Some(&open),
            6,
            BulkOp::RemoveProjects,
            &missing,
            "all projects"
        ));
        assert!(!planned_current(
            Some(&open),
            7,
            BulkOp::ArchiveFinished,
            &missing,
            "all projects"
        ));
        assert!(!planned_current(
            Some(&open),
            7,
            BulkOp::RemoveProjects,
            &Pick::Selected(vec!["p1".to_owned()]),
            "all projects"
        ));
        assert!(!planned_current(
            Some(&open),
            7,
            BulkOp::RemoveProjects,
            &missing,
            "other"
        ));
        // No dialog (cancelled), or no longer reading: dropped.
        assert!(!planned_current(
            None,
            7,
            BulkOp::RemoveProjects,
            &missing,
            "all projects"
        ));
        let confirming = dialog(7, missing.clone(), BulkPlan::default(), Phase::Confirm);
        assert!(!planned_current(
            Some(&confirming),
            7,
            BulkOp::RemoveProjects,
            &missing,
            "all projects"
        ));
    }

    #[test]
    fn missing_auto_apply_needs_its_exact_plan() {
        // #682: only the plan bound to the picked registration and its own
        // cleanup identity may auto-apply; never a generic Selected plan.
        let missing = Pick::Missing("p2".to_owned());
        let bound = dialog(7, missing.clone(), missing_plan("p2", true), Phase::Confirm);
        assert_eq!(
            missing_bound(&bound).map(|(project, _)| project),
            Some("p2".to_owned())
        );
        // A late Selected plan for another project carries no cleanup.
        let foreign = dialog(
            7,
            missing.clone(),
            missing_plan("p1", false),
            Phase::Confirm,
        );
        assert!(missing_bound(&foreign).is_none());
        // Right shape, wrong registration.
        let mismatched = dialog(7, missing.clone(), missing_plan("p9", true), Phase::Confirm);
        assert!(missing_bound(&mismatched).is_none());
        // A Selected dialog never auto-applies through the Missing path.
        let selected = dialog(
            7,
            Pick::Selected(vec!["p1".to_owned()]),
            missing_plan("p1", false),
            Phase::Confirm,
        );
        assert!(missing_bound(&selected).is_none());
    }
}
