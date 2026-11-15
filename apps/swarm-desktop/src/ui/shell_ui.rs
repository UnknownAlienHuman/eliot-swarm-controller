//! Navigation shell: projects, the Agent list, screen selection and the root layout.

use super::*;

#[derive(Clone, Debug)]
pub(super) enum ShellMsg {
    ProjectsLoaded(Fallible<Vec<UiProject>>),
    AgentsLoaded(u64, bool, Fallible<AgentPage>),
    ProjectSelected(Option<usize>),
    AgentSelected(Option<usize>),
    RefreshRequested,
    NewConversation,
    MoreAgentsRequested,
    HighlightAgent(String),
    ScreenSelected(Screen),
}

/// Projects, the Agent list page and the current selection.
pub(super) struct ShellState {
    pub(super) selection: Selection,
    pub(super) projects: Vec<UiProject>,
    pub(super) project_idx: Option<usize>,
    pub(super) agents: Vec<UiAgent>,
    pub(super) agent_idx: Option<usize>,
    pub(super) agents_next: Option<String>,
    pub(super) agents_total: usize,
    pub(super) agents_loading: bool,
    /// Tags agent-list loads; a reply for an older request is dropped.
    pub(super) list_epoch: u64,
    pub(super) pending_select: Option<UiAgent>,
}

impl DesktopApp {
    pub(super) fn shell_update(&mut self, msg: ShellMsg, context: &ComponentContext<Self>) {
        match msg {
            ShellMsg::ProjectsLoaded(projects) => match projects {
                Ok(projects) => {
                    let count = projects.len();
                    self.nav.projects = projects;
                    self.set_status(format!(
                        "{count} registered project(s); listing every Agent in scope."
                    ));
                }
                Err(error) => self.set_status(format!("project.list failed: {error}")),
            },
            ShellMsg::AgentsLoaded(list_epoch, append, page) => {
                if list_epoch != self.nav.list_epoch {
                    return;
                }
                self.nav.agents_loading = false;
                match page {
                    Ok(page) => {
                        let published = self.nav.agents.len();
                        if append {
                            self.nav.agents.extend(page.rows);
                        } else {
                            self.nav.agents = page.rows;
                        }
                        self.nav.agents_next = page.next;
                        self.nav.agents_total = page.total;
                        if let Some(wanted) = self.nav.pending_select.take()
                            && !self.nav.agents.iter().any(|agent| agent.id == wanted.id)
                        {
                            self.nav.agents.push(wanted.clone());
                            self.nav.pending_select = Some(wanted);
                        }
                        let selected = self
                            .nav
                            .pending_select
                            .take()
                            .map(|agent| agent.id)
                            .or_else(|| self.nav.selection.agent.clone());
                        let index = selected
                            .and_then(|id| self.nav.agents.iter().position(|agent| agent.id == id));
                        if let Some(agent) =
                            index.and_then(|index| self.nav.agents.get(index)).cloned()
                            && self.chat.convo.as_ref().map(Conversation::agent) != Some(&agent.id)
                        {
                            self.open_conversation(&agent, context);
                        }
                        // Never point the native SelectedIndex at a row this same
                        // render adds: highlight a new row on the next tick.
                        // Reactor 0.100.0 applies the SelectedIndex before the
                        // new item reaches the ListView; WinUI rejects it with
                        // E_INVALIDARG (0x80070057), which Reactor treats as a
                        // fatal native command failure and the process aborts
                        // with 0xC0000409. A standalone repro on 2026-11-14
                        // aborted on the first same-update select and stayed up
                        // through repeated next-tick selects.
                        match index {
                            Some(index) if index >= published => {
                                self.nav.agent_idx = None;
                                let id = self.nav.agents[index].id.clone();
                                let _task = context.spawn_background(move |_| {
                                    Message::Shell(ShellMsg::HighlightAgent(id))
                                });
                            }
                            other => self.nav.agent_idx = other,
                        }
                    }
                    Err(error) => self.set_status(format!("agent.list failed: {error}")),
                }
            }
            ShellMsg::ProjectSelected(index) => {
                self.nav.project_idx = index;
                self.nav.agents.clear();
                self.nav.agent_idx = None;
                self.nav.agents_next = None;
                self.nav.agents_total = 0;
                match self.selected_project().cloned() {
                    Some(project) => {
                        self.launch.profiles = project.profiles.clone();
                        self.launch.workspace = project.workspace.clone();
                        self.nav.selection.project = Some(project.id.clone());
                    }
                    None => {
                        self.launch.profiles.clear();
                        self.nav.selection.project = None;
                    }
                }
                self.launch.profile_idx = None;
                self.nav.selection.profile = None;
                self.load_agents(context, None);
            }
            ShellMsg::AgentSelected(index) => {
                self.nav.agent_idx = index;
                // A list reload clears the native selection (None); the open
                // conversation and its feed stay as they are.
                if let Some(agent) = self.selected_agent().cloned()
                    && self.chat.convo.as_ref().map(Conversation::agent) != Some(&agent.id)
                {
                    self.open_conversation(&agent, context);
                }
            }
            ShellMsg::RefreshRequested => {
                if let Ok(data_dir) = self.data_dir.clone() {
                    let _task = context.spawn_background(move |_| {
                        Message::Shell(ShellMsg::ProjectsLoaded(load_projects_blocking(&data_dir)))
                    });
                }
                self.load_agents(context, None);
                if self.chat.convo.is_some() {
                    self.chat.failures = 0;
                    self.start_snapshot(context);
                }
            }
            ShellMsg::NewConversation => {
                // Keyboard focus goes back to the composer (no-op while it is unbound).
                let _ = self.chat.composer_ref.request_focus();
                // Leave the conversation locally; the Agent keeps running.
                self.chat.epoch += 1;
                if let Some(task) = self.chat.live_task.take() {
                    task.cancel();
                }
                self.nav.selection.select_agent(None);
                self.nav.agent_idx = None;
                self.chat.convo = None;
                self.chat.detail = None;
                self.chat.live = LiveStatus::Idle;
                self.chat.items = Rc::new(Vec::new());
                self.chat.items_revision = 0;
                self.chat.unseen = 0;
                self.set_status("New conversation: launch an Agent with its first message.");
            }
            ShellMsg::MoreAgentsRequested => {
                if let Some(after) = self.nav.agents_next.clone() {
                    self.load_agents(context, Some(after));
                }
            }
            ShellMsg::HighlightAgent(id) => {
                if self.nav.selection.agent.as_deref() == Some(id.as_str()) {
                    self.nav.agent_idx = self.nav.agents.iter().position(|agent| agent.id == id);
                }
            }
            ShellMsg::ScreenSelected(screen) => {
                self.screen = screen;
                match screen {
                    Screen::Profiles if !self.editor.loaded && !self.editor.busy => {
                        self.load_profile_source(context);
                        self.load_profile_list(context);
                    }
                    Screen::Automation => self.load_rules(context),
                    Screen::Scripts => {
                        if !self.scripts.listed {
                            self.load_scripts(context);
                        }
                        self.load_rules(context);
                    }
                    _ => {}
                }
            }
        }
    }

    pub(super) fn selected_project(&self) -> Option<&UiProject> {
        self.nav
            .project_idx
            .and_then(|index| index.checked_sub(1))
            .and_then(|index| self.nav.projects.get(index))
    }

    pub(super) fn selected_agent(&self) -> Option<&UiAgent> {
        self.nav
            .agent_idx
            .and_then(|index| self.nav.agents.get(index))
    }

    pub(super) fn selected_agent_id(&self) -> Option<String> {
        self.nav.selection.agent.clone()
    }

    pub(super) fn load_agents(&mut self, context: &ComponentContext<Self>, after: Option<String>) {
        let Ok(data_dir) = self.data_dir.clone() else {
            return;
        };
        let append = after.is_some();
        self.nav.list_epoch += 1;
        let list_epoch = self.nav.list_epoch;
        let request = AgentListRequest {
            after,
            limit: AGENT_PAGE,
            project: self.selected_project().map(|project| project.id.clone()),
            ..Default::default()
        };
        self.nav.agents_loading = true;
        let _task = context.spawn_background(move |_| {
            Message::Shell(ShellMsg::AgentsLoaded(
                list_epoch,
                append,
                load_agents_blocking(&data_dir, &request),
            ))
        });
    }

    pub(super) fn nav_view(&self, context: &mut ViewContext<Self>) -> View {
        let mut project_names = vec![ALL_PROJECTS.to_owned()];
        project_names.extend(self.nav.projects.iter().map(|project| project.id.clone()));
        let project_picker = ComboBox::new()
            .items_source(project_names)
            .selected_index(self.nav.project_idx)
            .placeholder_text("Project")
            .automation_name("Project")
            .horizontal_alignment(HorizontalAlignment::Stretch)
            .on_selection_changed(
                context.callback(|value| Message::Shell(ShellMsg::ProjectSelected(value))),
            );

        let agent_items: Vec<KeyedView> = self
            .nav
            .agents
            .iter()
            .map(|agent| {
                KeyedView::new(
                    agent.id.clone(),
                    ListViewItem::new()
                        .automation_name(format!(
                            "Agent {}, {}, {}, generation {}",
                            agent.id, agent.state, agent.harness, agent.generation
                        ))
                        .content(
                            StackPanel::new().spacing(2.0).children((
                                TextBlock::new()
                                    .text(format!("{} · {}", agent.id, agent.state))
                                    .font_weight(FontWeight::SEMI_BOLD),
                                TextBlock::new()
                                    .text(format!("{} · gen {}", agent.harness, agent.generation))
                                    .font_size(theme::type_scale::LABEL)
                                    .opacity(theme::MUTED_OPACITY),
                            )),
                        ),
                )
            })
            .collect();
        let agent_list = ListView::new()
            .grid_row(3)
            .selected_index(self.nav.agent_idx)
            .automation_name("Agents")
            .on_selection_changed(
                context.callback(|value| Message::Shell(ShellMsg::AgentSelected(value))),
            )
            .collection_slot(ListViewSlot::Items, agent_items);

        let counter = TextBlock::new()
            .text(if self.nav.agents_loading {
                "Loading Agents…".to_owned()
            } else {
                format!(
                    "Showing {} of {} Agent(s){}",
                    self.nav.agents.len(),
                    self.nav.agents_total,
                    if self.nav.agents_next.is_some()
                        && self.nav.agents.len() < self.nav.agents_total
                    {
                        "; more exist"
                    } else {
                        ""
                    }
                )
            })
            .font_size(theme::type_scale::LABEL)
            .opacity(theme::MUTED_OPACITY);
        let more = Button::new()
            .grid_row(5)
            .is_enabled(!self.nav.agents_loading && self.nav.agents.len() < self.nav.agents_total)
            .on_click(context.message(Message::Shell(ShellMsg::MoreAgentsRequested)))
            .content(TextBlock::new().text("More Agents"));

        let picker_profiles = self.picker_profiles();
        let selected_summary = self
            .launch
            .profile_idx
            .and_then(|index| picker_profiles.get(index).copied());
        let picker_mode = ComboBox::new()
            .items_source(vec![
                "Configured profile".to_owned(),
                "Ad-hoc harness".to_owned(),
            ])
            .selected_index(Some(usize::from(self.launch.picker_adhoc)))
            .automation_name("Launch mode")
            .on_selection_changed(
                context.callback(|value| Message::Launch(LaunchMsg::PickerModeSelected(value))),
            );
        let profile_input: View = if self.launch.picker_adhoc {
            StackPanel::new().spacing(theme::spacing::XS).children((
                ComboBox::new()
                    .items_source(ADHOC_HARNESSES.map(str::to_owned).to_vec())
                    .selected_index(self.launch.adhoc_harness_idx)
                    .automation_name("Ad-hoc harness")
                    .on_selection_changed(
                        context.callback(|value| {
                            Message::Launch(LaunchMsg::AdhocHarnessSelected(value))
                        }),
                    ),
                TextBox::new()
                    .text(self.launch.adhoc_model.clone())
                    .placeholder_text("model (unset: harness default)")
                    .automation_name("Ad-hoc model")
                    .on_text_changed(
                        context
                            .callback(|value| Message::Launch(LaunchMsg::AdhocModelChanged(value))),
                    ),
                TextBox::new()
                    .text(self.launch.adhoc_effort.clone())
                    .placeholder_text("effort (unset: harness default)")
                    .automation_name("Ad-hoc effort")
                    .on_text_changed(
                        context.callback(|value| {
                            Message::Launch(LaunchMsg::AdhocEffortChanged(value))
                        }),
                    ),
                Button::new()
                    .on_click(context.message(Message::Launch(LaunchMsg::HarnessModelsRequested)))
                    .content(TextBlock::new().text("Harness defaults")),
                TextBlock::new()
                    .text(self.launch.harness_hint.clone())
                    .font_size(theme::type_scale::LABEL)
                    .text_wrapping(TextWrapping::Wrap)
                    .opacity(theme::MUTED_OPACITY),
            ))
        } else if !self.launch.profile_summaries.is_empty() {
            StackPanel::new()
                .spacing(theme::spacing::XS)
                .children((
                    ComboBox::new()
                        .items_source(
                            picker_profiles
                                .iter()
                                .map(|profile| profile.name.clone())
                                .collect::<Vec<_>>(),
                        )
                        .selected_index(self.launch.profile_idx)
                        .placeholder_text("Profile")
                        .automation_name("Profile")
                        .on_selection_changed(context.callback(|value| Message::Launch(LaunchMsg::ProfileSelected(value)))),
                    TextBlock::new()
                        .text(selected_summary.map_or_else(
                            || "Requested settings appear here; the Agent detail shows the effective readback after launch.".to_owned(),
                            config_view::summary_line,
                        ))
                        .font_size(theme::type_scale::LABEL)
                        .text_wrapping(TextWrapping::Wrap)
                        .opacity(theme::MUTED_OPACITY),
                ))
        } else if self.launch.profiles.is_empty() {
            TextBox::new()
                .text(self.launch.profile_text.clone())
                .placeholder_text("Configured profile name")
                .automation_name("Profile")
                .on_text_changed(
                    context.callback(|value| Message::Launch(LaunchMsg::ProfileTextChanged(value))),
                )
                .into()
        } else {
            ComboBox::new()
                .items_source(self.launch.profiles.clone())
                .selected_index(self.launch.profile_idx)
                .placeholder_text("Profile")
                .automation_name("Profile")
                .on_selection_changed(
                    context.callback(|value| Message::Launch(LaunchMsg::ProfileSelected(value))),
                )
                .into()
        };
        let workspace_box = TextBox::new()
            .text(self.launch.workspace.clone())
            .placeholder_text("Workspace path")
            .automation_name("Workspace")
            .on_text_changed(
                context.callback(|value| Message::Launch(LaunchMsg::WorkspaceChanged(value))),
            );
        let refresh = Button::new()
            .automation_name("Refresh")
            .on_click(context.message(Message::Shell(ShellMsg::RefreshRequested)))
            .content(TextBlock::new().text("Refresh"));
        let new_chat = Button::new()
            .automation_name("New conversation")
            .on_click(context.message(Message::Shell(ShellMsg::NewConversation)))
            .content(TextBlock::new().text("New"));

        let section = |text: &str| {
            TextBlock::new()
                .text(text.to_owned())
                .font_size(theme::type_scale::LABEL)
                .font_weight(FontWeight::SEMI_BOLD)
        };
        Grid::new()
            .grid_column(0)
            .rows([
                GridLength::Auto,
                GridLength::Auto,
                GridLength::Auto,
                GridLength::STAR,
                GridLength::Auto,
                GridLength::Auto,
                GridLength::Auto,
                GridLength::Auto,
                GridLength::Auto,
                GridLength::Auto,
                GridLength::Auto,
            ])
            .row_spacing(theme::spacing::SM)
            .children((
                section("Project").grid_row(0),
                project_picker.grid_row(1),
                section("Agents").grid_row(2),
                agent_list,
                counter.grid_row(4),
                more,
                section("Launch").grid_row(6),
                StackPanel::new()
                    .grid_row(7)
                    .spacing(theme::spacing::XS)
                    .children((picker_mode, profile_input)),
                section("Workspace").grid_row(8),
                workspace_box.grid_row(9),
                StackPanel::new()
                    .grid_row(10)
                    .orientation(Orientation::Horizontal)
                    .spacing(theme::spacing::SM)
                    .children((refresh, new_chat)),
            ))
    }
}
