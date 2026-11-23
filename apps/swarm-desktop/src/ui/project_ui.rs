//! The project page, as in the Codex app: the project's tasks by state read
//! from the daemon (`agent.list` with `project`, paged, so a project is never
//! limited to the sidebar's loaded rows), its branches and worktrees, its
//! check and automations, and New task. A project's name opens it; Back
//! returns to where the owner came from.

use super::*;
use crate::theme::StatusTone;
use crate::theme::winui::Palette;
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
}

impl Group {
    const ALL: [Self; 4] = [Self::Working, Self::Waiting, Self::Ready, Self::Finished];

    fn title(self) -> &'static str {
        match self {
            Self::Working => "Working",
            Self::Waiting => "Waiting for a decision",
            Self::Ready => "Ready",
            Self::Finished => "Finished",
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
                let after = self.project.lists[group as usize].next.clone();
                if after.is_some() {
                    self.read_group(group, after, context);
                }
            }
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
        if self.panel == Panel::Details {
            self.close_panel();
        }
        if self.project.key != key {
            self.project = ProjectState {
                key: key.to_owned(),
                id,
                checkout,
                epoch: self.project.epoch,
                lists: Default::default(),
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

    /// Read every state group again (a refresh keeps the rows already paged).
    pub(super) fn read_project(&mut self, context: &ComponentContext<Self>) {
        self.project.epoch += 1;
        for group in Group::ALL {
            self.read_group(group, None, context);
        }
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

    /// One group's rows: the daemon's, plus loaded chats attached to this
    /// project (their own project label differs, so the filter misses them).
    fn project_lines(&self, group: Group) -> Vec<TaskLine> {
        let list = &self.project.lists[group as usize];
        let states = group.states();
        let mut seen: HashSet<&str> = HashSet::new();
        let now = crate::handoff::now_ms();
        list.rows
            .iter()
            .chain(self.nav.agents.iter().filter(|agent| {
                agent.organization.as_deref() == Some(self.project.id.as_str())
                    && agent.project_id != self.project.id
                    && states.contains(&agent.state)
            }))
            .filter(|agent| !agent.native_child && seen.insert(agent.id.as_str()))
            .map(|agent| TaskLine {
                agent: agent.clone(),
                title: self.agent_label(agent),
                trailing: change_words(agent)
                    .unwrap_or_else(|| nav::short_age(now, agent.updated_at)),
            })
            .collect()
    }

    /// The page, as its own component: a window render that changes none of
    /// what it shows leaves it as it is.
    pub(super) fn project_view(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        let state = &self.project;
        let groups: Vec<GroupView> = Group::ALL
            .into_iter()
            .map(|group| {
                let list = &state.lists[group as usize];
                let rows = self.project_lines(group);
                GroupView {
                    group,
                    total: list.total.max(rows.len()),
                    // The daemon returns a cursor even on its last page.
                    more: list.next.is_some() && list.rows.len() < list.total,
                    reading: list.reading && !list.read,
                    error: list.error.clone(),
                    rows,
                }
            })
            .collect();
        let registered = self
            .nav
            .projects
            .iter()
            .find(|project| project.id == state.id);
        let mut worktrees: Vec<WorktreeLine> = registered
            .map(|project| project.git_checkouts.as_slice())
            .unwrap_or_default()
            .iter()
            .map(|checkout| {
                let key = nav::path_key(&checkout.path);
                let tasks = groups
                    .iter()
                    .flat_map(|group| &group.rows)
                    .filter(|line| {
                        line.agent
                            .workspace
                            .as_deref()
                            .is_some_and(|workspace| nav::path_key(workspace) == key)
                    })
                    .count();
                WorktreeLine {
                    branch: checkout
                        .branch
                        .clone()
                        .unwrap_or_else(|| "Detached".to_owned()),
                    folder: nav::folder_name(&nav::display_path(&checkout.path)).to_owned(),
                    path: checkout.path.clone(),
                    main: checkout.main,
                    tasks,
                }
            })
            .collect();
        worktrees.sort_by(|a, b| {
            b.main
                .cmp(&a.main)
                .then(b.tasks.cmp(&a.tasks))
                .then_with(|| a.branch.cmp(&b.branch))
        });
        let fields = self.settings.policy_of(&state.id);
        let check = (!fields.argv.is_empty()).then(|| fields.argv.join(" "));
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
        View::component::<ProjectView>(ProjectInput {
            palette,
            name: self.place_title(&state.checkout),
            checkout: state.checkout.clone(),
            can_launch: self.can_launch_here(&state.checkout),
            groups,
            worktrees,
            check,
            at_turn_end: fields.at_turn_end,
            usage,
            automations: self.project_automations(&state.id, &state.checkout),
            send: context.forward(),
        })
    }
}

/// What a task changed ("+12 −3"), when Git has reported it.
fn change_words(agent: &UiAgent) -> Option<String> {
    let work = agent.work.as_ref().filter(|work| work.available)?;
    let (added, removed) = (work.insertions?, work.deletions?);
    (added + removed > 0).then(|| format!("+{added} −{removed}"))
}

#[derive(Clone, PartialEq)]
struct TaskLine {
    agent: UiAgent,
    title: String,
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
    main: bool,
    tasks: usize,
}

/// What the project page shows, read by the window.
#[derive(Clone, PartialEq)]
pub(super) struct ProjectInput {
    palette: Palette,
    name: String,
    checkout: String,
    can_launch: bool,
    groups: Vec<GroupView>,
    worktrees: Vec<WorktreeLine>,
    check: Option<String>,
    at_turn_end: bool,
    usage: Option<String>,
    automations: Vec<String>,
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
        let _ = self.send.call(message);
    }

    fn view(&self, input: &ProjectInput, context: &mut ViewContext<Self>) -> View {
        let palette = input.palette;
        let summary = input
            .groups
            .iter()
            .map(|group| format!("{} {}", group.total, group.group.title().to_lowercase()))
            .collect::<Vec<_>>()
            .join(" · ");
        let new_task = m3::filled(palette)
            .is_enabled(input.can_launch)
            .automation_name(format!("New task in {}", input.name))
            .on_click(context.message(Message::Shell(ShellMsg::ProjectPicked(
                input.checkout.clone(),
            ))))
            .content(m3::icon_label(m3::glyph::NEW_TASK, "New task"));
        let mut items = vec![KeyedView::new(
            "header",
            m3::page_header(&input.name, &summary, Some(new_task)),
        )];
        for group in &input.groups {
            if group.rows.is_empty() && group.error.is_none() && !group.reading {
                continue;
            }
            items.push(KeyedView::new(
                format!("group:{:?}", group.group),
                group_view(group, context, palette),
            ));
        }
        items.push(KeyedView::new(
            "worktrees",
            worktrees_view(&input.worktrees, context, palette),
        ));
        items.push(KeyedView::new("work", work_view(input, context, palette)));
        ScrollViewer::new()
            .vertical_scroll_bar_visibility(ScrollBarVisibility::Auto)
            .automation_name(format!("Project {}", input.name))
            .content(
                StackPanel::new()
                    .max_width(tokens::layout::SETTINGS_WIDTH)
                    .horizontal_alignment(HorizontalAlignment::Stretch)
                    .spacing(tokens::spacing::MD)
                    .margin(Thickness::new(
                        tokens::spacing::XL,
                        tokens::spacing::LG,
                        tokens::spacing::XL,
                        tokens::spacing::XL,
                    ))
                    .keyed_children(items),
            )
    }
}

fn group_view(group: &GroupView, context: &mut ViewContext<ProjectView>, palette: Palette) -> View {
    let title = group.group.title();
    let mut rows: Vec<KeyedView> = vec![KeyedView::new(
        "title",
        m3::muted(format!("{title} · {}", group.total), tokens::text::LABEL),
    )];
    if group.reading {
        rows.push(KeyedView::new("reading", m3::secondary("Reading…")));
    }
    if let Some(error) = &group.error {
        rows.push(KeyedView::new(
            "error",
            m3::secondary(format!("Not read: {error}"))
                .foreground(palette.attention())
                .text_wrapping(TextWrapping::Wrap),
        ));
    }
    for line in &group.rows {
        let agent = &line.agent;
        let ended = matches!(agent.state, AgentState::Stopped);
        let content = Grid::new()
            .columns([GridLength::Auto, GridLength::STAR])
            .column_spacing(tokens::spacing::ROW)
            .children((
                m3::status_mark(
                    palette,
                    StatusTone::of_state(agent.state),
                    tokens::layout::DOT,
                ),
                Border::new().grid_column(1).content(m3::task_content(
                    0,
                    &line.title,
                    ended,
                    m3::secondary(line.trailing.clone()).into(),
                    None,
                )),
            ));
        rows.push(KeyedView::new(
            agent.id.clone(),
            m3::row(palette, false)
                .automation_name(format!("Open {}, {title}", line.title))
                .on_click(context.message(Message::Shell(ShellMsg::AgentOpened(agent.clone()))))
                .content(content),
        ));
    }
    if group.more {
        rows.push(KeyedView::new(
            "more",
            m3::text_button(palette)
                .automation_name(format!("Show more {title}"))
                .on_click(context.message(Message::Project(ProjectMsg::More(group.group))))
                .content(m3::label("Show more")),
        ));
    }
    StackPanel::new()
        .spacing(tokens::spacing::ROW_GAP)
        .keyed_children(rows)
}

fn worktrees_view(
    worktrees: &[WorktreeLine],
    context: &mut ViewContext<ProjectView>,
    palette: Palette,
) -> View {
    let mut rows: Vec<KeyedView> = vec![KeyedView::new(
        "title",
        m3::muted(
            format!("Branches and worktrees · {}", worktrees.len()),
            tokens::text::LABEL,
        ),
    )];
    for line in worktrees {
        let trailing = match (line.main, line.tasks) {
            (true, 0) => "main checkout".to_owned(),
            (true, tasks) => format!("main checkout · {tasks}"),
            (false, 0) => String::new(),
            (false, 1) => "1 task".to_owned(),
            (false, tasks) => format!("{tasks} tasks"),
        };
        rows.push(KeyedView::new(
            line.path.clone(),
            m3::row(palette, false)
                .automation_name(format!("Open folder of {}", line.branch))
                .on_click(context.message(Message::Reveal(line.branch.clone(), line.path.clone())))
                .content(m3::row_content(
                    Some(m3::glyph::WORKTREE),
                    &line.branch,
                    &line.folder,
                    m3::secondary(trailing).into(),
                )),
        ));
    }
    StackPanel::new()
        .spacing(tokens::spacing::ROW_GAP)
        .keyed_children(rows)
}

fn work_view(
    input: &ProjectInput,
    context: &mut ViewContext<ProjectView>,
    palette: Palette,
) -> View {
    let check_title = input.check.as_deref().unwrap_or("No check set");
    let mut check_note = input.usage.clone().unwrap_or_default();
    if input.check.is_some() && input.at_turn_end {
        check_note = format!("Runs when a turn ends · {check_note}");
    }
    let automations = match input.automations.len() {
        0 => "None for this project".to_owned(),
        count if count <= 3 => input.automations.join(", "),
        count => format!(
            "{} and {} more",
            input.automations[..3].join(", "),
            count - 3
        ),
    };
    m3::settings_card(
        palette,
        vec![
            KeyedView::new(
                "check",
                m3::setting_row(
                    "Project check",
                    &format!("{check_title}\n{check_note}"),
                    m3::text_button(palette)
                        .automation_name("Edit project check")
                        .on_click(context.message(Message::Project(ProjectMsg::EditCheck)))
                        .content(m3::label("Edit")),
                ),
            ),
            KeyedView::new(
                "automation",
                m3::setting_row(
                    "Automations",
                    &automations,
                    m3::text_button(palette)
                        .automation_name("Manage automations")
                        .on_click(
                            context
                                .message(Message::Shell(ShellMsg::SettingsPage(Page::Automations))),
                        )
                        .content(m3::label("Manage")),
                ),
            ),
        ],
    )
}
