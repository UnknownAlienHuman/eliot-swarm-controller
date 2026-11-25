//! The project page (`Project.dc.html`): its header (path, Open folder,
//! New task, the project's menu), then two columns. Left: the manager
//! (`project.assignment` as the sidebar's `chat.organization` read carries
//! it, with a hand-over in progress and competing top-level tasks), the
//! tasks by state read from the daemon (`agent.list` with `project`,
//! paged, so a project is never limited to the sidebar's loaded rows) with
//! queued launches, and the project check. Right: who works where (`who`,
//! by Git), branches and worktrees, capacity (read only) and the project's
//! automations. A project's name opens it; Back returns.

use super::menu_ui::{MenuMsg, Target as MenuTarget};
use super::*;
use eliot_core::AgentState;

/// Task rows per page of one state group.
const PAGE: usize = 25;
/// The most rows one refresh reads back for a group.
const MAX_REFRESH: usize = 1000;

/// The state groups the page lists, in order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Group {
    Working,
    Waiting,
    Ready,
    Finished,
    /// Tasks working in a checkout on a detached HEAD (Git's word), from
    /// the groups above.
    Detached,
}

impl Group {
    const ALL: [Self; 5] = [
        Self::Working,
        Self::Waiting,
        Self::Ready,
        Self::Finished,
        Self::Detached,
    ];
    /// The groups read from the daemon (one list each).
    const LISTED: [Self; 4] = [Self::Working, Self::Waiting, Self::Ready, Self::Finished];

    fn title(self) -> &'static str {
        match self {
            Self::Working => "Working",
            Self::Waiting => "Waiting for you",
            Self::Ready => "Ready",
            Self::Finished => "Finished",
            Self::Detached => "Detached",
        }
    }

    fn states(self) -> Vec<AgentState> {
        match self {
            Self::Working => vec![AgentState::Busy, AgentState::Starting, AgentState::Created],
            Self::Waiting => vec![
                AgentState::Waiting,
                AgentState::Failed,
                AgentState::Lost,
                AgentState::Unknown,
            ],
            Self::Ready => vec![AgentState::Idle],
            Self::Finished => vec![AgentState::Stopped],
            Self::Detached => Vec::new(),
        }
    }
}

#[derive(Clone, Debug)]
pub(super) enum ProjectMsg {
    /// One group's page: the read's epoch, whether it appends, the page.
    Loaded(u64, Group, bool, Fallible<AgentPage>),
    /// The group's next page.
    More(Group),
    /// Open this project's check in Settings › Projects.
    EditCheck,
    /// Who works where (`who`): the read's epoch and its reply.
    WhoLoaded(u64, Box<Fallible<eliot_core::api::WhoResponse>>),
    /// Show or hide the finished tasks.
    FinishedToggled,
    /// Show or hide the tasks competing with the manager.
    CompetingToggled,
    /// Read the page again after a failed read.
    Retry,
}

/// One state group as read.
#[derive(Default)]
struct GroupList {
    rows: Vec<UiAgent>,
    next: Option<String>,
    total: usize,
    read: bool,
    reading: bool,
    error: Option<String>,
}

/// The open project and its reads.
#[derive(Default)]
pub(super) struct ProjectState {
    /// The sidebar key (`org:<project>`): the place Back returns to.
    pub(super) key: String,
    id: String,
    checkout: String,
    /// Tags reads: a reply for an older project or refresh is dropped.
    epoch: u64,
    lists: [GroupList; 4],
    /// Who works where, as last read.
    who: Option<Fallible<eliot_core::api::WhoResponse>>,
    who_reading: bool,
    finished_open: bool,
    competing_open: bool,
}

impl DesktopApp {
    pub(super) fn project_update(&mut self, msg: ProjectMsg, context: &ComponentContext<Self>) {
        match msg {
            ProjectMsg::Loaded(epoch, group, append, page) => {
                if epoch != self.project.epoch {
                    return;
                }
                let list = &mut self.project.lists[group as usize];
                list.reading = false;
                match page {
                    Ok(page) => {
                        if append {
                            let seen: HashSet<String> =
                                list.rows.iter().map(|row| row.id.clone()).collect();
                            list.rows.extend(
                                page.rows.into_iter().filter(|row| !seen.contains(&row.id)),
                            );
                        } else {
                            list.rows = page.rows;
                        }
                        list.next = page.next;
                        list.total = page.total;
                        list.read = true;
                        list.error = None;
                    }
                    Err(error) => list.error = Some(error),
                }
            }
            ProjectMsg::More(group) => {
                let list = &self.project.lists[group as usize];
                let after = list.next.clone().filter(|_| !list.reading);
                if after.is_some() {
                    self.read_group(group, after, context);
                }
            }
            ProjectMsg::WhoLoaded(epoch, read) => {
                if epoch == self.project.epoch {
                    self.project.who_reading = false;
                    self.project.who = Some(*read);
                }
            }
            ProjectMsg::FinishedToggled => {
                self.project.finished_open = !self.project.finished_open;
            }
            ProjectMsg::CompetingToggled => {
                self.project.competing_open = !self.project.competing_open;
            }
            ProjectMsg::Retry => self.read_project(context),
            ProjectMsg::EditCheck => {
                let id = self.project.id.clone();
                self.shell_update(ShellMsg::SettingsPage(Page::Projects), context);
                if self.settings.project_open.as_deref() != Some(id.as_str()) {
                    self.settings_update(SettingsMsg::ProjectOpened(id), context);
                }
            }
        }
    }

    /// Whether the sidebar group `key` is a project the page can show.
    pub(super) fn project_exists(&self, key: &str) -> bool {
        self.nav
            .organization
            .groups
            .iter()
            .any(|group| group.project.is_some() && nav::group_key(group.project.as_deref()) == key)
    }

    /// Show the project page of the sidebar group `key`; false when no such
    /// project is known.
    pub(super) fn open_project(&mut self, key: &str, context: &ComponentContext<Self>) -> bool {
        let Some(group) = self.nav.organization.groups.iter().find(|group| {
            group.project.is_some() && nav::group_key(group.project.as_deref()) == key
        }) else {
            return false;
        };
        let id = group.project.clone().unwrap_or_default();
        let checkout = group
            .registration
            .as_ref()
            .map_or_else(String::new, |registration| registration.checkout.clone());
        self.close_popover();
        if self.panel != Panel::Closed {
            self.close_panel();
        }
        if self.project.key != key {
            self.project = ProjectState {
                key: key.to_owned(),
                id,
                checkout,
                epoch: self.project.epoch + 1,
                ..ProjectState::default()
            };
        }
        if self.main != Main::Project {
            self.main = Main::Project;
            self.reprime_fields(context);
        }
        self.read_project(context);
        if !self.settings.loaded {
            self.load_settings(context);
        }
        if !self.automation.read() {
            self.load_rules(context);
        }
        true
    }

    /// Read the state groups again (a refresh keeps the rows already paged).
    /// A group whose read is still running keeps it: on a busy daemon a read
    /// slower than the poll still lands instead of being replaced.
    pub(super) fn read_project(&mut self, context: &ComponentContext<Self>) {
        for group in Group::LISTED {
            if !self.project.lists[group as usize].reading {
                self.read_group(group, None, context);
            }
        }
        self.read_who(context);
    }

    fn read_group(
        &mut self,
        group: Group,
        after: Option<String>,
        context: &ComponentContext<Self>,
    ) {
        let Ok(data_dir) = self.data_dir.clone() else {
            return;
        };
        let append = after.is_some();
        let list = &mut self.project.lists[group as usize];
        list.reading = true;
        let limit = if append {
            PAGE
        } else {
            list.rows.len().clamp(PAGE, MAX_REFRESH)
        };
        let request = AgentListRequest {
            after,
            limit,
            order: AgentListOrder::Recent,
            project: Some(self.project.id.clone()),
            states: Some(group.states()),
            ..Default::default()
        };
        let epoch = self.project.epoch;
        let _task = context.spawn_background(move |_| {
            Message::Project(ProjectMsg::Loaded(
                epoch,
                group,
                append,
                load_agents_blocking(&data_dir, &request, None),
            ))
        });
    }

    /// Read who works where in the project (`who`), with its groups.
    fn read_who(&mut self, context: &ComponentContext<Self>) {
        let Ok(data_dir) = self.data_dir.clone() else {
            return;
        };
        if self.project.who_reading {
            return;
        }
        self.project.who_reading = true;
        let (epoch, id) = (self.project.epoch, self.project.id.clone());
        let _task = context.spawn_background(move |_| {
            let read = client::block_on(config_client::who(&data_dir, id))
                .map_err(|error| error.user_message());
            Message::Project(ProjectMsg::WhoLoaded(epoch, Box::new(read)))
        });
    }

    /// One group's rows: the daemon's, plus loaded chats attached to this
    /// project (their own project label differs, so the filter misses them),
    /// less those working in a detached checkout (their own group).
    fn project_lines(&self, group: Group, detached: &HashSet<String>) -> Vec<TaskLine> {
        let now = crate::handoff::now_ms();
        let line = |agent: &UiAgent| TaskLine {
            agent: agent.clone(),
            title: self.agent_label(agent),
            line: task_line(agent),
            trailing: change_words(agent).unwrap_or_else(|| nav::short_age(now, agent.updated_at)),
        };
        if group == Group::Detached {
            let mut seen: HashSet<&str> = HashSet::new();
            return Group::LISTED
                .iter()
                .flat_map(|listed| self.project.lists[*listed as usize].rows.iter())
                .filter(|agent| in_detached(agent, detached) && seen.insert(agent.id.as_str()))
                .map(line)
                .collect();
        }
        let list = &self.project.lists[group as usize];
        let states = group.states();
        let mut seen: HashSet<&str> = HashSet::new();
        list.rows
            .iter()
            .chain(self.nav.agents.iter().filter(|agent| {
                agent.organization.as_deref() == Some(self.project.id.as_str())
                    && agent.project_id != self.project.id
                    && states.contains(&agent.state)
            }))
            .filter(|agent| {
                !agent.native_child
                    && !in_detached(agent, detached)
                    && seen.insert(agent.id.as_str())
            })
            .map(line)
            .collect()
    }

    /// The page, as its own component: a window render that changes none of
    /// what it shows leaves it as it is.
    pub(super) fn project_view(&self, context: &mut ViewContext<Self>) -> View {
        let state = &self.project;
        let registered = self
            .nav
            .projects
            .iter()
            .find(|project| project.id == state.id);
        let checkouts = registered
            .map(|project| project.git_checkouts.as_slice())
            .unwrap_or_default();
        let detached: HashSet<String> = checkouts
            .iter()
            .filter(|checkout| checkout.branch.is_none())
            .map(|checkout| nav::path_key(&checkout.path))
            .collect();
        let groups: Vec<GroupView> = Group::ALL
            .into_iter()
            .map(|group| {
                let rows = self.project_lines(group, &detached);
                let (more, reading, error, total) = match group {
                    Group::Detached => (false, false, None, rows.len()),
                    listed => {
                        let list = &state.lists[listed as usize];
                        (
                            // The daemon returns a cursor even on its last page.
                            list.next.is_some() && list.rows.len() < list.total,
                            list.reading && !list.read,
                            list.error.clone(),
                            list.total.max(rows.len()),
                        )
                    }
                };
                GroupView {
                    group,
                    total,
                    more,
                    reading,
                    error,
                    rows,
                }
            })
            .collect();
        let org = self
            .nav
            .organization
            .groups
            .iter()
            .find(|group| group.project.as_deref() == Some(state.id.as_str()));
        // The manager card: the assignment the sidebar read carries.
        let manager = org.map(|group| {
            let agent = group.manager.as_ref().map(|manager| {
                let row = self.find_agent(&manager.agent);
                ManagerLine {
                    agent: row.cloned(),
                    title: self.agent_title(&manager.agent),
                    state: manager.state,
                    words: [
                        row.map(|row| {
                            crate::picker::harness_model_label(&row.harness, row.model.as_deref())
                        }),
                        manager.branch.clone(),
                        Some(crate::monitor::state_words(manager.state).to_owned()),
                    ]
                    .into_iter()
                    .flatten()
                    .collect::<Vec<_>>()
                    .join(" · "),
                }
            });
            ManagerView {
                agent,
                assignment: group
                    .assignment
                    .as_ref()
                    .map(|assignment| format!("{:?}", assignment.state).to_lowercase()),
                needs: group.needs_assignment,
                transfer: group.transfer.clone(),
                competing: group
                    .conflicting_roots
                    .iter()
                    .map(|id| self.agent_title(id))
                    .collect(),
            }
        });
        // Queued launches and automation waits for this project.
        let queued: Vec<String> = self
            .nav
            .organization
            .queue
            .as_ref()
            .map(|queue| {
                queue
                    .interactive
                    .iter()
                    .filter(|intent| intent.project == state.id)
                    .map(|intent| match &intent.reason {
                        Some(reason) => {
                            format!("Queued launch · {}", nav::wait_reason_words(reason))
                        }
                        None => "Queued launch".to_owned(),
                    })
                    .chain(
                        queue
                            .assignments
                            .iter()
                            .filter(|wait| wait.project == state.id)
                            .map(|wait| {
                                format!(
                                    "{} waits · {}",
                                    wait.rule,
                                    nav::wait_reason_words(&wait.reason)
                                )
                            }),
                    )
                    .collect()
            })
            .unwrap_or_default();
        let task_count = |key: &str| {
            groups
                .iter()
                .flat_map(|group| &group.rows)
                .filter(|line| {
                    line.agent
                        .workspace
                        .as_deref()
                        .is_some_and(|workspace| nav::path_key(workspace) == key)
                })
                .map(|line| line.title.clone())
                .collect::<Vec<_>>()
        };
        let mut worktrees: Vec<WorktreeLine> = checkouts
            .iter()
            .map(|checkout| {
                let tasks = task_count(&nav::path_key(&checkout.path));
                WorktreeLine {
                    branch: checkout
                        .branch
                        .clone()
                        .unwrap_or_else(|| "Detached".to_owned()),
                    folder: nav::display_path(&checkout.path),
                    path: checkout.path.clone(),
                    state: if checkout.main {
                        "Main checkout".to_owned()
                    } else if checkout.branch.is_none() {
                        "Detached".to_owned()
                    } else {
                        match tasks.len() {
                            0 => "No task".to_owned(),
                            1 => "1 task".to_owned(),
                            count => format!("{count} tasks"),
                        }
                    },
                    main: checkout.main,
                    tasks: tasks.join(", "),
                }
            })
            .collect();
        worktrees.sort_by(|a, b| b.main.cmp(&a.main).then_with(|| a.branch.cmp(&b.branch)));
        let fields = self.settings.policy_of(&state.id);
        let check = (!fields.argv.is_empty()).then(|| CheckView {
            command: fields.argv.join(" "),
            runs: {
                let rounds = if fields.max_rounds.trim().is_empty() {
                    "3 rounds".to_owned()
                } else {
                    format!("{} rounds", fields.max_rounds.trim())
                };
                if fields.at_turn_end {
                    format!("When a turn ends · {rounds}")
                } else {
                    format!("When asked · {rounds}")
                }
            },
            scope: if fields.scope.is_empty() {
                "Whole folder".to_owned()
            } else {
                fields.scope.join(", ")
            },
        });
        let usage = registered.map(|project| {
            let usage = &project.policy_usage;
            if usage.total == 0 {
                "No launches in the last 24 h".to_owned()
            } else {
                format!(
                    "Applied to {} of {} launches in the last 24 h",
                    usage.applied, usage.total
                )
            }
        });
        let capacity = org.and_then(|group| group.capacity.as_ref()).map(|scope| {
            let live: Vec<String> = groups
                .iter()
                .filter(|group| group.group == Group::Working)
                .flat_map(|group| group.rows.iter().map(|line| line.title.clone()))
                .collect();
            CapacityView {
                usage: scope.usage,
                limit: scope.limit,
                live: live.join(", "),
            }
        });
        // This project's automations: its rules and the brief queues that
        // start tasks in its checkout.
        let key = nav::path_key(&state.checkout);
        let cards: Vec<(String, crate::controls::StatusTone)> = self
            .automation_cards()
            .into_iter()
            .filter(|card| {
                card.rule
                    .as_ref()
                    .is_some_and(|rule| rule.key.0.as_deref() == Some(state.id.as_str()))
                    || (card.assignment.is_some()
                        && card.paths.iter().any(|(label, path)| {
                            *label == "Works in" && nav::path_key(path) == key
                        }))
            })
            .map(|card| (card.state.label(), card.state.tone()))
            .collect();
        let who = state.who.clone().map(|read| {
            read.map(|reply| WhoView {
                workers: reply
                    .workers
                    .iter()
                    .map(|worker| who_line(worker, self.agent_title(&worker.agent)))
                    .collect(),
                shared: reply
                    .shared
                    .iter()
                    .map(|shared| SharedLine {
                        folder: nav::display_path(&shared.workspace),
                        agents: shared
                            .agents
                            .iter()
                            .map(|agent| self.agent_title(agent))
                            .collect::<Vec<_>>()
                            .join(", "),
                        count: shared.agents.len(),
                        files: path_words(&shared.dirty, 3),
                    })
                    .collect(),
                truncated: reply.truncated,
            })
        });
        let target = self.row_target(&format!("project:{}", state.key));
        View::component::<ProjectView>(ProjectInput {
            width: self.page_width(tokens::page::WIDTH),
            name: self.place_title(&state.checkout),
            checkout: state.checkout.clone(),
            key: state.key.clone(),
            missing: registered.is_some_and(|project| project.missing),
            can_launch: self.can_launch_here(&state.checkout),
            target,
            groups,
            queued,
            finished_open: state.finished_open,
            competing_open: state.competing_open,
            manager,
            worktrees,
            check,
            usage,
            capacity,
            automations: cards,
            who,
            send: context.forward(),
        })
    }
}

/// Whether `agent` works in one of the detached checkouts.
fn in_detached(agent: &UiAgent, detached: &HashSet<String>) -> bool {
    agent
        .workspace
        .as_deref()
        .is_some_and(|workspace| detached.contains(&nav::path_key(workspace)))
}

/// What a task changed ("+12 −3"), when Git has reported it.
fn change_words(agent: &UiAgent) -> Option<String> {
    let work = agent.work.as_ref().filter(|work| work.available)?;
    let (added, removed) = (work.insertions?, work.deletions?);
    (added + removed > 0).then(|| format!("+{added} −{removed}"))
}

/// A task row's supporting line: its model, then its status line.
fn task_line(agent: &UiAgent) -> String {
    let model = crate::picker::harness_model_label(&agent.harness, agent.model.as_deref());
    match &agent.status_line {
        Some(status) => format!("{model} · {status}"),
        None => model,
    }
}

/// Up to `shown` paths by their file names, then "and N more".
fn path_words(paths: &[String], shown: usize) -> String {
    let names: Vec<&str> = paths
        .iter()
        .take(shown)
        .map(|path| path.rsplit(['/', '\\']).next().unwrap_or(path))
        .collect();
    match paths.len().saturating_sub(shown) {
        0 => names.join(" · "),
        more => format!("{} and {more} more", names.join(" · ")),
    }
}

/// One live Agent's entry in Who works where.
fn who_line(worker: &eliot_core::api::WhoWorker, title: String) -> WhoLine {
    let mut lines = Vec::new();
    let edited: Vec<String> = worker
        .dirty
        .iter()
        .chain(&worker.attributed_edits)
        .cloned()
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect();
    if !edited.is_empty() {
        lines.push(format!("Not committed: {}", path_words(&edited, 2)));
    }
    if !worker.committed_paths.is_empty() {
        lines.push(format!(
            "Committed against the base: {}",
            path_words(&worker.committed_paths, 2)
        ));
    }
    for claim in &worker.claims {
        let until = crate::config_view::local_time(claim.expires_at);
        lines.push(match &claim.note {
            Some(note) => format!("Claims {} · {note} · until {until}", claim.glob),
            None => format!("Claims {} · until {until}", claim.glob),
        });
    }
    if !worker.git_available {
        lines.push("Git facts unavailable for its folder".to_owned());
    } else if !worker.attribution_complete || worker.edits_truncated {
        lines.push("Partial: more edits than listed".to_owned());
    }
    if lines.is_empty() {
        lines.push("No changes yet".to_owned());
    }
    WhoLine {
        title,
        place: if worker.singly_owned {
            "worktree".to_owned()
        } else {
            "shared checkout".to_owned()
        },
        lines,
    }
}

#[derive(Clone, PartialEq)]
struct TaskLine {
    agent: UiAgent,
    title: String,
    line: String,
    trailing: String,
}

#[derive(Clone, PartialEq)]
struct GroupView {
    group: Group,
    total: usize,
    more: bool,
    reading: bool,
    error: Option<String>,
    rows: Vec<TaskLine>,
}

#[derive(Clone, PartialEq)]
struct WorktreeLine {
    branch: String,
    folder: String,
    path: String,
    state: String,
    main: bool,
    tasks: String,
}

#[derive(Clone, PartialEq)]
struct ManagerLine {
    agent: Option<UiAgent>,
    title: String,
    state: AgentState,
    words: String,
}

#[derive(Clone, PartialEq)]
struct ManagerView {
    agent: Option<ManagerLine>,
    assignment: Option<String>,
    needs: bool,
    transfer: Option<String>,
    competing: Vec<String>,
}

#[derive(Clone, PartialEq)]
struct CheckView {
    command: String,
    runs: String,
    scope: String,
}

#[derive(Clone, PartialEq)]
struct CapacityView {
    usage: usize,
    limit: Option<u64>,
    live: String,
}

#[derive(Clone, PartialEq)]
struct WhoLine {
    title: String,
    place: String,
    lines: Vec<String>,
}

#[derive(Clone, PartialEq)]
struct SharedLine {
    folder: String,
    agents: String,
    count: usize,
    files: String,
}

#[derive(Clone, PartialEq)]
struct WhoView {
    workers: Vec<WhoLine>,
    shared: Vec<SharedLine>,
    truncated: bool,
}

/// What the project page shows, read by the window.
#[derive(Clone, PartialEq)]
pub(super) struct ProjectInput {
    /// The page column's width.
    width: Option<f64>,
    name: String,
    checkout: String,
    key: String,
    missing: bool,
    can_launch: bool,
    /// The project's row menu (the sidebar's), when the sidebar lists it.
    target: Option<MenuTarget>,
    groups: Vec<GroupView>,
    queued: Vec<String>,
    finished_open: bool,
    competing_open: bool,
    manager: Option<ManagerView>,
    worktrees: Vec<WorktreeLine>,
    check: Option<CheckView>,
    usage: Option<String>,
    capacity: Option<CapacityView>,
    automations: Vec<(String, crate::controls::StatusTone)>,
    who: Option<Fallible<WhoView>>,
    send: Callback<Message>,
}

pub(super) struct ProjectView {
    send: Callback<Message>,
}

impl Component for ProjectView {
    type Input = ProjectInput;
    type Message = Message;

    fn create(input: &ProjectInput, _context: &ComponentContext<Self>) -> Self {
        Self {
            send: input.send.clone(),
        }
    }

    fn input_changed(&mut self, input: &ProjectInput, _context: &ComponentContext<Self>) {
        self.send = input.send.clone();
    }

    fn update(&mut self, message: Message, _context: &ComponentContext<Self>) {
        self.send.call(message);
    }

    fn view(&self, input: &ProjectInput, context: &mut ViewContext<Self>) -> View {
        let mut actions: Vec<KeyedView> = vec![KeyedView::new(
            format!("new:{}", input.can_launch),
            controls::filled()
                .is_enabled(input.can_launch)
                .automation_name(format!("New task in {}", input.name))
                .on_click(context.message(Message::Shell(ShellMsg::ProjectPicked(
                    input.checkout.clone(),
                ))))
                .content(controls::icon_label(controls::glyph::NEW_TASK, "New task")),
        )];
        if let Some(target) = &input.target {
            actions.push(KeyedView::new(
                "more",
                controls::icon_button()
                    .automation_id(format!(
                        "{}project:{}",
                        super::menu_ui::MORE_ID_PREFIX,
                        input.key
                    ))
                    .automation_name(format!("Project actions for {}", input.name))
                    .on_click(context.message(Message::Menu(MenuMsg::Opened(target.clone()))))
                    .content(controls::icon(controls::glyph::MORE))
                    .tooltip("Project actions"),
            ));
        }
        let path = nav::display_path(&input.checkout);
        let heading =
            StackPanel::new().children((
                page_ui::title(input.name.clone()),
                StackPanel::new()
                    .orientation(Orientation::Horizontal)
                    .spacing(tokens::spacing::XS)
                    .children((
                        controls::secondary(format!("{path} ·"))
                            .vertical_alignment(VerticalAlignment::Center)
                            .text_trimming(TextTrimming::CharacterEllipsis),
                        controls::text_button()
                            .height(tokens::text::CAPTION.size + tokens::spacing::SM)
                            .min_height(0.0)
                            .margin(Thickness::new(
                                -tokens::spacing::ROW,
                                0.0,
                                -tokens::spacing::ROW,
                                0.0,
                            ))
                            .automation_name(format!("Open folder {path}"))
                            .on_click(context.message(Message::Reveal(
                                input.name.clone(),
                                input.checkout.clone(),
                            )))
                            .content(controls::text("Open folder", tokens::text::CAPTION)),
                    )),
            ));
        let mut items = vec![KeyedView::new("header", page_ui::toolbar(heading, actions))];
        if input.missing {
            items.push(KeyedView::new(
                "missing",
                page_ui::read_failed(
                    "Folder not found: the checkout is gone from disk.",
                    "Remove",
                    true,
                    context.message(Message::Shell(ShellMsg::SettingsPage(Page::Projects))),
                ),
            ));
        }
        let left = vec![
            KeyedView::new("manager", manager_panel(input, context)),
            KeyedView::new("tasks", tasks_panel(input, context)),
            KeyedView::new("check", check_panel(input, context)),
        ];
        let right = vec![
            KeyedView::new("who", who_panel(input)),
            KeyedView::new("worktrees", worktrees_panel(input, context)),
            KeyedView::new("capacity", capacity_panel(input, context)),
            KeyedView::new("automations", automations_panel(input, context)),
        ];
        items.push(KeyedView::new(
            "columns",
            page_ui::columns(left, tokens::spacing::MD, right),
        ));
        page_ui::page(&format!("Project {}", input.name), input.width, items)
    }
}

/// A row of the Tasks panel (`.item`): the state mark, the title and its
/// line, the age or +/− at the end; it opens the conversation.
fn task_row(line: &TaskLine, group: &str, context: &mut ViewContext<ProjectView>) -> View {
    let agent = &line.agent;
    let waiting = matches!(agent.state, AgentState::Waiting);
    let content = Grid::new()
        .columns([GridLength::Auto, GridLength::STAR, GridLength::Auto])
        .column_spacing(tokens::spacing::INSET)
        .children((
            Border::new()
                .vertical_alignment(VerticalAlignment::Center)
                .maybe_content(page_ui::agent_mark(
                    agent.state,
                    agent.status_line.as_deref(),
                )),
            Border::new()
                .grid_column(1)
                .content(page_ui::two_lines(&line.title, &line.line)),
            page_ui::age(line.trailing.clone()).grid_column(2),
        ));
    let row = controls::row(false)
        .min_height(tokens::height::TWO_LINE_ROW)
        .automation_name(format!("Open {}, {group}", line.title))
        .on_click(context.message(Message::Shell(ShellMsg::AgentOpened(agent.clone()))))
        .content(content);
    if waiting {
        Border::new()
            .background(ThemeBrush::CardBackground)
            .corner_radius(tokens::radius::CONTAINER)
            .content(row)
            .into()
    } else {
        row.into()
    }
}

/// A group's label row (`.lab`), with an optional action at its end.
fn group_label(text: String, action: Option<View>) -> View {
    Grid::new()
        .min_height(tokens::height::CONTROL)
        .columns([GridLength::STAR, GridLength::Auto])
        .children((
            controls::text(text, tokens::text::LABEL)
                .margin(Thickness::new(tokens::spacing::XS, 0.0, 0.0, 0.0))
                .vertical_alignment(VerticalAlignment::Center),
            crate::views::MaybeContent::maybe_content(Border::new().grid_column(1), action),
        ))
        .into()
}

fn tasks_panel(input: &ProjectInput, context: &mut ViewContext<ProjectView>) -> View {
    let mut rows: Vec<KeyedView> = vec![KeyedView::new("head", page_ui::panel_head("Tasks", None))];
    let mut any = false;
    for group in &input.groups {
        let title = group.group.title();
        let queued = if group.group == Group::Ready {
            input.queued.as_slice()
        } else {
            &[]
        };
        if group.rows.is_empty() && group.error.is_none() && !group.reading && queued.is_empty() {
            continue;
        }
        any = true;
        let finished = group.group == Group::Finished;
        let open = !finished || input.finished_open;
        let action: Option<View> = finished.then(|| {
            controls::text_button()
                .automation_name(format!(
                    "{} finished tasks",
                    if open { "Hide" } else { "Show" }
                ))
                .on_click(context.message(Message::Project(ProjectMsg::FinishedToggled)))
                .content(controls::label(if open { "Hide" } else { "Show" }))
                .into()
        });
        rows.push(KeyedView::new(
            format!("label:{title}:{open}"),
            group_label(format!("{title} · {}", group.total + queued.len()), action),
        ));
        if !open {
            continue;
        }
        if group.reading {
            rows.push(KeyedView::new(
                format!("reading:{title}"),
                page_ui::quiet("Reading…"),
            ));
        }
        if let Some(error) = &group.error {
            rows.push(KeyedView::new(
                format!("error:{title}"),
                page_ui::read_failed(
                    format!("Not read: {error}"),
                    "Retry",
                    true,
                    context.message(Message::Project(ProjectMsg::Retry)),
                ),
            ));
        }
        for line in &group.rows {
            rows.push(KeyedView::new(
                line.agent.id.clone(),
                task_row(line, title, context),
            ));
        }
        for (index, wait) in queued.iter().enumerate() {
            rows.push(KeyedView::new(
                format!("queued:{index}"),
                Grid::new()
                    .min_height(tokens::height::CONTROL)
                    .margin(Thickness::xy(tokens::spacing::ROW, 0.0))
                    .columns([GridLength::Auto, GridLength::STAR])
                    .column_spacing(tokens::spacing::INSET)
                    .children((
                        Border::new()
                            .vertical_alignment(VerticalAlignment::Center)
                            .content(controls::row_mark(controls::RowMark::Queued, "Queued")),
                        controls::secondary(wait.clone())
                            .grid_column(1)
                            .vertical_alignment(VerticalAlignment::Center)
                            .text_wrapping(TextWrapping::Wrap),
                    )),
            ));
        }
        if group.more {
            rows.push(KeyedView::new(
                format!("more:{title}"),
                controls::text_button()
                    .horizontal_alignment(HorizontalAlignment::Left)
                    .automation_name(format!("Show more {title}"))
                    .on_click(context.message(Message::Project(ProjectMsg::More(group.group))))
                    .content(controls::label("Show more")),
            ));
        }
    }
    if !any {
        rows.push(KeyedView::new(
            "none",
            page_ui::quiet("No tasks in the last 24 h"),
        ));
    }
    page_ui::panel(tokens::spacing::ROW_GAP, rows)
}

fn manager_panel(input: &ProjectInput, context: &mut ViewContext<ProjectView>) -> View {
    let mut rows: Vec<KeyedView> =
        vec![KeyedView::new("head", page_ui::panel_head("Manager", None))];
    match &input.manager {
        None => rows.push(KeyedView::new("reading", page_ui::quiet("Reading…"))),
        Some(manager) => {
            match &manager.agent {
                Some(line) => {
                    let content = Grid::new()
                        .columns([GridLength::Auto, GridLength::STAR])
                        .column_spacing(tokens::spacing::INSET)
                        .children((
                            Border::new()
                                .vertical_alignment(VerticalAlignment::Center)
                                .maybe_content(page_ui::state_mark(line.state)),
                            Border::new()
                                .grid_column(1)
                                .content(page_ui::two_lines(&line.title, &line.words)),
                        ));
                    let agent: View = match &line.agent {
                        Some(agent) => controls::row(false)
                            .min_height(tokens::height::TWO_LINE_ROW)
                            .automation_name(format!("Open the manager {}", line.title))
                            .on_click(
                                context
                                    .message(Message::Shell(ShellMsg::AgentOpened(agent.clone()))),
                            )
                            .content(content)
                            .into(),
                        None => content.into(),
                    };
                    rows.push(KeyedView::new("agent", agent));
                }
                None => rows.push(KeyedView::new(
                    "none",
                    page_ui::quiet(if manager.needs {
                        "This project has no manager; its top-level tasks wait for one."
                    } else {
                        "This project has no manager."
                    }),
                )),
            }
            if let Some(assignment) = &manager.assignment
                && assignment != "active"
            {
                rows.push(KeyedView::new(
                    "assignment",
                    page_ui::quiet(format!("Assignment: {assignment}")),
                ));
            }
            if let Some(transfer) = &manager.transfer {
                rows.push(KeyedView::new(
                    "transfer",
                    strip(
                        controls::glyph::MOVE,
                        &format!("Handing over the project (transfer {transfer})"),
                        None,
                    ),
                ));
            }
            if !manager.competing.is_empty() {
                let count = manager.competing.len();
                let open = input.competing_open;
                rows.push(KeyedView::new(
                    format!("competing:{open}"),
                    strip(
                        controls::glyph::GROUP,
                        &format!(
                            "{} compete{} with the manager",
                            eliot_core::count_words(
                                count as u64,
                                "other top-level task",
                                "other top-level tasks"
                            ),
                            if count == 1 { "s" } else { "" }
                        ),
                        Some(
                            controls::text_button()
                                .automation_name(if open {
                                    "Hide the competing tasks"
                                } else {
                                    "Show the competing tasks"
                                })
                                .on_click(
                                    context.message(Message::Project(ProjectMsg::CompetingToggled)),
                                )
                                .content(controls::label(if open { "Hide" } else { "Show" }))
                                .into(),
                        ),
                    ),
                ));
                if open {
                    rows.push(KeyedView::new(
                        "competing-list",
                        controls::secondary(manager.competing.join(", "))
                            .text_wrapping(TextWrapping::Wrap),
                    ));
                }
            }
        }
    }
    page_ui::panel(tokens::spacing::SM, rows)
}

/// An attention strip inside a panel: a card surface, an icon, one line
/// and an optional action.
fn strip(glyph: &str, text: &str, action: Option<View>) -> View {
    Border::new()
        .background(ThemeBrush::CardBackground)
        .border_brush(ThemeBrush::CardStroke)
        .border_thickness(Thickness::uniform(tokens::layout::HAIRLINE))
        .corner_radius(tokens::radius::CONTAINER)
        .min_height(tokens::height::CONTROL + tokens::spacing::SM)
        .padding(Thickness::new(
            tokens::spacing::INSET,
            0.0,
            tokens::spacing::XS,
            0.0,
        ))
        .content(
            Grid::new()
                .columns([GridLength::Auto, GridLength::STAR, GridLength::Auto])
                .column_spacing(tokens::spacing::SM)
                .children((
                    controls::small_icon(glyph),
                    controls::text(text.to_owned(), tokens::text::BODY)
                        .grid_column(1)
                        .vertical_alignment(VerticalAlignment::Center)
                        .text_wrapping(TextWrapping::Wrap),
                    crate::views::MaybeContent::maybe_content(
                        Border::new()
                            .grid_column(2)
                            .vertical_alignment(VerticalAlignment::Center),
                        action,
                    ),
                )),
        )
        .into()
}

fn check_panel(input: &ProjectInput, context: &mut ViewContext<ProjectView>) -> View {
    let edit: View = controls::text_button()
        .automation_name("Edit project check")
        .on_click(context.message(Message::Project(ProjectMsg::EditCheck)))
        .content(controls::label("Edit"))
        .into();
    let mut rows = vec![KeyedView::new(
        "head",
        page_ui::panel_head("Project check", Some(edit)),
    )];
    match &input.check {
        Some(check) => {
            rows.push(KeyedView::new(
                "command",
                page_ui::kv_text("Command", check.command.clone()),
            ));
            rows.push(KeyedView::new(
                "runs",
                page_ui::kv_text("Runs", check.runs.clone()),
            ));
            rows.push(KeyedView::new(
                "scope",
                page_ui::kv_text("Can change", check.scope.clone()),
            ));
        }
        None => rows.push(KeyedView::new("none", page_ui::quiet("No check set"))),
    }
    if let Some(usage) = &input.usage {
        rows.push(KeyedView::new("usage", page_ui::quiet(usage.clone())));
    }
    page_ui::panel(tokens::spacing::SM, rows)
}

fn who_panel(input: &ProjectInput) -> View {
    let mut rows = vec![KeyedView::new(
        "head",
        page_ui::panel_head("Who works where", None),
    )];
    match &input.who {
        None => rows.push(KeyedView::new("reading", page_ui::quiet("Reading…"))),
        Some(Err(error)) => rows.push(KeyedView::new(
            "error",
            page_ui::error_line(format!("Not read: {error}")),
        )),
        Some(Ok(who)) => {
            if who.workers.is_empty() && who.shared.is_empty() {
                rows.push(KeyedView::new(
                    "none",
                    page_ui::quiet("No Agent works here now."),
                ));
            }
            for (index, worker) in who.workers.iter().enumerate() {
                let mut parts: Vec<KeyedView> = vec![KeyedView::new(
                    "head",
                    Grid::new()
                        .columns([GridLength::STAR, GridLength::Auto])
                        .column_spacing(tokens::spacing::SM)
                        .children((
                            controls::text(worker.title.clone(), tokens::page::HEADING)
                                .text_trimming(TextTrimming::CharacterEllipsis),
                            controls::secondary(worker.place.clone()).grid_column(1),
                        )),
                )];
                for (line_index, line) in worker.lines.iter().enumerate() {
                    parts.push(KeyedView::new(
                        line_index,
                        controls::secondary(line.clone())
                            .text_wrapping(TextWrapping::Wrap)
                            .is_text_selection_enabled(true),
                    ));
                }
                rows.push(KeyedView::new(format!("worker:{index}"), who_entry(parts)));
            }
            for (index, shared) in who.shared.iter().enumerate() {
                let mut parts: Vec<KeyedView> = vec![KeyedView::new(
                    "head",
                    Grid::new()
                        .columns([GridLength::STAR, GridLength::Auto])
                        .column_spacing(tokens::spacing::SM)
                        .children((
                            controls::text("Shared checkout", tokens::page::HEADING),
                            controls::secondary(format!("{} Agents", shared.count)).grid_column(1),
                        )),
                )];
                parts.push(KeyedView::new(
                    "agents",
                    controls::secondary(format!("{} · {}", shared.agents, shared.folder))
                        .text_wrapping(TextWrapping::Wrap),
                ));
                if !shared.files.is_empty() {
                    parts.push(KeyedView::new(
                        "files",
                        controls::secondary(format!("Not committed: {}", shared.files))
                            .text_wrapping(TextWrapping::Wrap),
                    ));
                }
                rows.push(KeyedView::new(format!("shared:{index}"), who_entry(parts)));
            }
            if who.truncated {
                rows.push(KeyedView::new(
                    "truncated",
                    page_ui::quiet("More Agents work here than listed."),
                ));
            }
            rows.push(KeyedView::new(
                "source",
                page_ui::quiet("From Git, not from what agents say."),
            ));
        }
    }
    page_ui::panel(tokens::spacing::SM, rows)
}

/// One entry of Who works where (`.who`): a hairline under it.
fn who_entry(parts: Vec<KeyedView>) -> View {
    Border::new()
        .border_brush(ThemeBrush::CardStroke)
        .border_thickness(Thickness::new(0.0, 0.0, 0.0, tokens::layout::HAIRLINE))
        .padding(Thickness::xy(0.0, tokens::spacing::CHIP_GAP))
        .content(
            StackPanel::new()
                .spacing(tokens::spacing::ROW_GAP)
                .keyed_children(parts),
        )
        .into()
}

fn worktrees_panel(input: &ProjectInput, context: &mut ViewContext<ProjectView>) -> View {
    let mut rows = vec![KeyedView::new(
        "head",
        page_ui::panel_head("Branches & worktrees", None),
    )];
    if input.worktrees.is_empty() {
        rows.push(KeyedView::new(
            "none",
            page_ui::quiet("No checkouts read yet."),
        ));
    }
    for line in &input.worktrees {
        let caption = if line.tasks.is_empty() {
            line.folder.clone()
        } else {
            format!("{} · {}", line.tasks, line.folder)
        };
        rows.push(KeyedView::new(
            line.path.clone(),
            controls::row(false)
                .min_height(tokens::height::TWO_LINE_ROW)
                .automation_name(format!("Open folder of {}: {}", line.branch, line.folder))
                .on_click(context.message(Message::Reveal(line.branch.clone(), line.path.clone())))
                .content(
                    StackPanel::new()
                        .vertical_alignment(VerticalAlignment::Center)
                        .children((
                            Grid::new()
                                .columns([
                                    GridLength::STAR,
                                    GridLength::Pixel(tokens::page::BRANCH_STATE),
                                ])
                                .column_spacing(tokens::spacing::SM)
                                .children((
                                    controls::body(line.branch.clone())
                                        .text_trimming(TextTrimming::CharacterEllipsis),
                                    controls::secondary(line.state.clone())
                                        .grid_column(1)
                                        .horizontal_alignment(HorizontalAlignment::Right)
                                        .text_trimming(TextTrimming::CharacterEllipsis),
                                )),
                            controls::secondary(caption)
                                .text_trimming(TextTrimming::CharacterEllipsis),
                        )),
                )
                .tooltip("Open in Explorer"),
        ));
    }
    page_ui::panel(tokens::spacing::ROW_GAP, rows)
}

fn capacity_panel(input: &ProjectInput, context: &mut ViewContext<ProjectView>) -> View {
    let edit: View = controls::text_button()
        .automation_name("Edit capacity in config.toml")
        .on_click(context.message(Message::Settings(SettingsMsg::OpenConfig)))
        .content(controls::link_content("Edit"))
        .tooltip("Capacity is set in config.toml ([capacity])");
    let mut rows = vec![KeyedView::new(
        "head",
        page_ui::panel_head("Capacity", Some(edit)),
    )];
    match &input.capacity {
        None => rows.push(KeyedView::new("none", page_ui::quiet("Not read yet."))),
        Some(capacity) => {
            let at_once = match capacity.limit {
                Some(0) => "Closed: new launches wait".to_owned(),
                Some(limit) => format!("{} of {limit}", capacity.usage),
                None => format!("{} · no limit", capacity.usage),
            };
            let at_once = if capacity.live.is_empty() {
                at_once
            } else {
                format!("{at_once} · {}", capacity.live)
            };
            rows.push(KeyedView::new(
                "at-once",
                page_ui::kv_text("At once", at_once),
            ));
            if let Some(limit) = capacity.limit.filter(|limit| *limit > 0) {
                let share = capacity.usage as f64 * 100.0 / limit as f64;
                rows.push(KeyedView::new(
                    format!("meter:{}", share.round()),
                    controls::meter(share, false),
                ));
            }
        }
    }
    page_ui::panel(tokens::spacing::SM, rows)
}

fn automations_panel(input: &ProjectInput, context: &mut ViewContext<ProjectView>) -> View {
    let manage: View = controls::text_button()
        .automation_name("Manage automations")
        .on_click(context.message(Message::Shell(ShellMsg::PlaceOpened(
            super::page_ui::Place::Automations,
        ))))
        .content(controls::label("Manage"))
        .into();
    let running = input
        .automations
        .iter()
        .filter(|(label, _)| label == "Running")
        .count();
    let attention = input
        .automations
        .iter()
        .filter(|(_, tone)| {
            matches!(
                tone,
                crate::controls::StatusTone::Attention | crate::controls::StatusTone::Failed
            )
        })
        .count();
    let summary = match input.automations.len() {
        0 => "None for this project".to_owned(),
        total => {
            let mut parts = vec![format!("{running} running")];
            if attention > 0 {
                parts.push(format!("{attention} need attention"));
            }
            let rest = total - running - attention;
            if rest > 0 {
                parts.push(format!("{rest} off or paused"));
            }
            parts.join(" · ")
        }
    };
    page_ui::panel(
        tokens::spacing::SM,
        vec![
            KeyedView::new("head", page_ui::panel_head("Automations", Some(manage))),
            KeyedView::new(format!("summary:{summary}"), controls::body(summary)),
        ],
    )
}
