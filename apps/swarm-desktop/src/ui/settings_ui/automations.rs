//! Settings › Automations: one card per automation in plain words (the
//! Codex/Antigravity settings pattern), the short form that creates or
//! edits an assignment, and the Scripts card. The card model and Run now
//! are in [`super::super::automation_ui`]; the assignment writes are here,
//! beside the settings draft they change, and save at once.

use super::super::automation_ui::{
    AssignmentForm, AssignmentView, AutomationMsg, Card, CardInputs, FormField, cards, interval_ms,
    run_key,
};
use super::*;

/// The label column of a card's path lines.
const PATH_LABEL_WIDTH: f64 = 96.0;

impl DesktopApp {
    pub(super) fn automation_section(
        &self,
        context: &mut ViewContext<Self>,
        palette: Palette,
    ) -> View {
        StackPanel::new().spacing(tokens::spacing::MD).children((
            self.automations_view(context, palette),
            m3::card_title("Scripts"),
            self.scripts_view(context, palette),
        ))
    }

    /// The settings draft's assignments, as the cards read them.
    fn assignment_views(&self) -> Vec<AssignmentView> {
        let automation = &self.settings.automation;
        model::assignment_keys(automation)
            .into_iter()
            .map(|key| AssignmentView {
                folder: model::assignment_text(automation, &key, AssignmentField::Folder),
                workspace: model::assignment_text(automation, &key, AssignmentField::Workspace),
                role: model::assignment_role(automation, &key),
                busy: model::assignment_busy(automation, &key),
                interval: model::assignment_text(automation, &key, AssignmentField::Interval),
                enabled: model::assignment_enabled(automation, &key),
                key,
            })
            .collect()
    }

    /// Registered projects: (id, checkout, label).
    fn automation_projects(&self) -> Vec<(String, String, String)> {
        self.nav
            .projects
            .iter()
            .map(|project| {
                (
                    project.id.clone(),
                    project.workspace.clone(),
                    self.project_label(project),
                )
            })
            .collect()
    }

    /// The form's role list: the routing roles, and the form's own role
    /// when routing no longer lists it.
    pub(in crate::ui) fn assignment_roles(&self) -> Vec<String> {
        let mut roles = model::routing_roles(&self.settings.routing);
        if let Some(form) = &self.automation.form
            && !roles.contains(&form.role)
        {
            roles.push(form.role.clone());
        }
        roles
    }

    /// The form's project list: (checkout, label) of every registered
    /// project, and the form's own folder when it is not one of them.
    pub(in crate::ui) fn assignment_workspaces(&self) -> Vec<(String, String)> {
        let mut workspaces: Vec<(String, String)> = self
            .automation_projects()
            .into_iter()
            .map(|(_, checkout, label)| (nav::display_path(&checkout).to_owned(), label))
            .collect();
        if let Some(form) = &self.automation.form
            && !form.workspace.is_empty()
            && !workspaces
                .iter()
                .any(|(path, _)| nav::path_key(path) == nav::path_key(&form.workspace))
        {
            workspaces.push((form.workspace.clone(), form.workspace.clone()));
        }
        workspaces
    }

    /// Open the form, filled from the assignment it edits (or with the
    /// defaults of a new one: the first project, Worker, 1 at once, 30 s).
    pub(in crate::ui) fn open_assignment_form(&mut self, key: Option<String>) {
        let automation = &self.settings.automation;
        self.automation.form = Some(match key {
            Some(key) => AssignmentForm {
                name: key.clone(),
                folder: nav::display_path(&model::assignment_text(
                    automation,
                    &key,
                    AssignmentField::Folder,
                ))
                .to_owned(),
                workspace: nav::display_path(&model::assignment_text(
                    automation,
                    &key,
                    AssignmentField::Workspace,
                ))
                .to_owned(),
                role: model::assignment_role(automation, &key),
                busy: model::assignment_busy(automation, &key).unwrap_or(1),
                interval: model::assignment_text(automation, &key, AssignmentField::Interval),
                editing: Some(key),
                error: None,
            },
            None => AssignmentForm {
                workspace: self
                    .nav
                    .projects
                    .first()
                    .map(|project| nav::display_path(&project.workspace).to_owned())
                    .unwrap_or_default(),
                role: "worker".to_owned(),
                busy: 1,
                interval: "30s".to_owned(),
                ..AssignmentForm::default()
            },
        });
    }

    /// Check the form, write it into the settings draft and save at once.
    pub(in crate::ui) fn save_assignment_form(&mut self, context: &ComponentContext<Self>) {
        let Some(form) = self.automation.form.as_mut() else {
            return;
        };
        let folder = form.folder.trim();
        let key = form
            .editing
            .clone()
            .unwrap_or_else(|| form.name.trim().to_owned());
        let problem = if key.is_empty() {
            Some("Give it a name.")
        } else if folder.is_empty()
            || !Path::new(folder).is_absolute()
            || !Path::new(folder).is_dir()
        {
            Some("Choose an existing folder by its full path, for example C:\\briefs\\queue.")
        } else if !Path::new(form.workspace.trim()).is_absolute() {
            Some("Choose the project the tasks work in.")
        } else if interval_ms(&form.interval).is_none() {
            Some("Write how often to check, for example 30s, 5m or 1h.")
        } else if !(1..=1000).contains(&form.busy) {
            Some("At most at once is a number from 1 to 1000.")
        } else {
            None
        };
        if let Some(problem) = problem {
            form.error = Some(problem.to_owned());
            return;
        }
        let draft = model::AssignmentDraft {
            folder: folder.to_owned(),
            workspace: form.workspace.trim().to_owned(),
            role: form.role.clone(),
            max_busy: form.busy,
            interval: form.interval.trim().to_owned(),
        };
        let new = form.editing.is_none();
        match model::put_assignment(&mut self.settings.automation, &key, &draft, new) {
            Ok(()) => {
                self.automation.form = None;
                self.autosave(Page::Automations, context);
                self.reload_rules_soon(context);
            }
            Err(error) => form.error = Some(error),
        }
    }

    pub(in crate::ui) fn remove_assignment(&mut self, key: &str, context: &ComponentContext<Self>) {
        model::remove_assignment(&mut self.settings.automation, key);
        self.autosave(Page::Automations, context);
    }

    /// Resume (true) or pause an assignment: its `enabled` key, saved at
    /// once.
    pub(in crate::ui) fn set_assignment_on(
        &mut self,
        key: &str,
        on: bool,
        context: &ComponentContext<Self>,
    ) {
        if model::assignment_enabled(&self.settings.automation, key) != on {
            model::set_assignment_enabled(&mut self.settings.automation, key, on);
            self.autosave(Page::Automations, context);
        }
    }

    /// The cards, the form, and an empty state that says what automations
    /// are for.
    fn automations_view(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        let projects = self.automation_projects();
        let data_dir = self.data_dir.as_ref().ok().map(PathBuf::as_path);
        let cards = cards(&CardInputs {
            assignments: self.assignment_views(),
            worker_down: self.automation.worker_down.as_deref(),
            namespaces: &self.automation.rules,
            projects: &projects,
            data_dir,
        });
        let form = self.automation.form.as_ref();
        let new_button = |context: &mut ViewContext<Self>| -> View {
            m3::filled(palette)
                .horizontal_alignment(HorizontalAlignment::Left)
                .automation_name("New automation")
                .on_click(context.message(Message::Automation(AutomationMsg::FormOpened(None))))
                .content(m3::label("New automation"))
        };
        let mut parts: Vec<KeyedView> = Vec::new();
        if cards.is_empty() && form.is_none() {
            parts.push(KeyedView::new(
                "empty",
                m3::card(palette).content(
                    // Before the first read lands only; a later re-read keeps
                    // the page as it is.
                    StackPanel::new().spacing(tokens::spacing::MD).children((
                        m3::body(if self.automation.read() {
                            "Automations start work for you: Swarm takes the briefs in a folder one by one and starts a task for each."
                        } else {
                            "Reading automations…"
                        })
                        .text_wrapping(TextWrapping::Wrap),
                        if self.automation.read() {
                            new_button(context)
                        } else {
                            View::empty()
                        },
                    )),
                ),
            ));
        } else {
            parts.push(KeyedView::new(
                "heading",
                m3::section_heading(
                    "Your automations",
                    "Each one starts tasks for you. Pause one to stop new tasks; tasks already running finish.",
                    Some(
                        StackPanel::new()
                            .orientation(Orientation::Horizontal)
                            .spacing(tokens::spacing::SM)
                            .children((
                                m3::text_button(palette)
                                    .automation_name("Refresh automations")
                                    .on_click(context.message(Message::Automation(
                                        AutomationMsg::AutomationRefreshRequested,
                                    )))
                                    .content(m3::label("Refresh")),
                                if form.is_none() {
                                    new_button(context)
                                } else {
                                    View::empty()
                                },
                            )),
                    ),
                ),
            ));
        }
        if let Some(form) = form.filter(|form| form.editing.is_none()) {
            parts.push(KeyedView::new(
                "form:new",
                self.assignment_form_view(form, context, palette),
            ));
        }
        for card in &cards {
            let editing = form.filter(|form| {
                form.editing.is_some()
                    && card.assignment.as_ref().map(|(key, _)| key) == form.editing.as_ref()
            });
            parts.push(KeyedView::new(
                card.key.clone(),
                match editing {
                    Some(form) => self.assignment_form_view(form, context, palette),
                    None => self.automation_card(card, context, palette),
                },
            ));
        }
        for namespace in &self.automation.rules {
            let note = match &namespace.rules {
                Err(error) => format!(
                    "{}: not read ({})",
                    namespace.label,
                    crate::state::plain_error(error)
                ),
                Ok(_) if namespace.more => {
                    format!(
                        "{}: more rules exist than this page shows.",
                        namespace.label
                    )
                }
                Ok(_) => continue,
            };
            parts.push(KeyedView::new(
                format!("note:{}", namespace.label),
                m3::secondary(note)
                    .foreground(palette.error())
                    .text_wrapping(TextWrapping::Wrap)
                    .is_text_selection_enabled(true),
            ));
        }
        StackPanel::new()
            .spacing(tokens::spacing::SM)
            .keyed_children(parts)
    }

    /// One automation: what it does, its state, last and next run, its
    /// folders, the actions the daemon supports and its technical details.
    fn automation_card(
        &self,
        card: &Card,
        context: &mut ViewContext<Self>,
        palette: Palette,
    ) -> View {
        let mut lines: Vec<KeyedView> = vec![
            KeyedView::new(
                "title",
                Grid::new()
                    .columns([GridLength::STAR, GridLength::Auto])
                    .column_spacing(tokens::spacing::SM)
                    .children((
                        m3::section_title(card.title.clone())
                            .text_trimming(TextTrimming::CharacterEllipsis)
                            .vertical_alignment(VerticalAlignment::Center),
                        StackPanel::new()
                            .grid_column(1)
                            .orientation(Orientation::Horizontal)
                            .spacing(tokens::spacing::XS)
                            .vertical_alignment(VerticalAlignment::Center)
                            .children((
                                m3::dot(palette.header_dot(card.state.tone())),
                                m3::secondary(card.state.label())
                                    .vertical_alignment(VerticalAlignment::Center),
                            )),
                    )),
            ),
            KeyedView::new(
                "sentence",
                m3::body(card.sentence.clone())
                    .text_wrapping(TextWrapping::Wrap)
                    .is_text_selection_enabled(true),
            ),
        ];
        if let Some(reason) = card.state.reason() {
            lines.push(KeyedView::new(
                "reason",
                m3::secondary(reason.to_owned())
                    .foreground(palette.error())
                    .text_wrapping(TextWrapping::Wrap)
                    .is_text_selection_enabled(true),
            ));
        }
        if !card.times.is_empty() {
            lines.push(KeyedView::new("times", m3::secondary(card.times.clone())));
        }
        for (label, path) in &card.paths {
            lines.push(KeyedView::new(
                format!("path:{label}"),
                Grid::new()
                    .columns([GridLength::Pixel(PATH_LABEL_WIDTH), GridLength::STAR])
                    .children((
                        m3::secondary(label.to_string())
                            .vertical_alignment(VerticalAlignment::Center),
                        Border::new()
                            .grid_column(1)
                            .content(self.path_link(context, palette, path)),
                    )),
            ));
        }
        let mut actions: Vec<KeyedView> = Vec::new();
        if let Some((key, on)) = &card.assignment {
            let label = if *on { "Pause" } else { "Resume" };
            let button = if *on {
                m3::outlined(palette)
            } else {
                m3::filled(palette)
            };
            actions.push(KeyedView::new(
                format!("pause:{on}"),
                button
                    .automation_name(format!("{label} {}", card.title))
                    .on_click(context.message(Message::Automation(AutomationMsg::Enabled(
                        key.clone(),
                        !on,
                    ))))
                    .content(m3::label(label)),
            ));
        }
        let run = card.run.as_ref().map(|(project, rule)| {
            (
                project.clone(),
                rule.clone(),
                self.automation.runs.get(&run_key(project.as_deref(), rule)),
            )
        });
        if let Some((project, rule, line)) = &run {
            actions.push(KeyedView::new(
                "run",
                m3::outlined(palette)
                    .is_enabled(!line.is_some_and(|line| line.busy) && !self.submitting())
                    .automation_name(format!("Run {} now", card.title))
                    .on_click(context.message(Message::Automation(AutomationMsg::RunNow(
                        project.clone(),
                        rule.clone(),
                    ))))
                    .content(m3::label("Run now")),
            ));
        }
        if let Some((key, _)) = &card.assignment {
            actions.push(KeyedView::new(
                "edit",
                m3::text_button(palette)
                    .automation_name(format!("Edit {}", card.title))
                    .on_click(
                        context.message(Message::Automation(AutomationMsg::FormOpened(Some(
                            key.clone(),
                        )))),
                    )
                    .content(m3::label("Edit")),
            ));
        }
        let open = self.automation.technical.contains(&card.key);
        lines.push(KeyedView::new(
            "actions",
            Grid::new()
                .columns([GridLength::STAR, GridLength::Auto])
                .column_spacing(tokens::spacing::SM)
                .children((
                    StackPanel::new()
                        .orientation(Orientation::Horizontal)
                        .spacing(tokens::spacing::SM)
                        .keyed_children(actions),
                    m3::text_button(palette)
                        .grid_column(1)
                        .automation_name(format!(
                            "Technical details of {} ({})",
                            card.title,
                            if open { "expanded" } else { "collapsed" }
                        ))
                        .on_click(context.message(Message::Automation(
                            AutomationMsg::TechnicalToggled(card.key.clone()),
                        )))
                        .content(m3::icon_label(
                            if open {
                                m3::glyph::CHEVRON_DOWN
                            } else {
                                m3::glyph::CHEVRON_RIGHT
                            },
                            "Technical details",
                        )),
                )),
        ));
        if let Some((_, _, Some(line))) = &run {
            let text = m3::secondary(line.text.clone())
                .text_wrapping(TextWrapping::Wrap)
                .is_text_selection_enabled(true);
            lines.push(KeyedView::new(
                "run-line",
                if line.error {
                    text.foreground(palette.error())
                } else {
                    text
                },
            ));
        }
        if open {
            lines.push(KeyedView::new(
                "technical",
                m3::secondary(card.technical.join("\n"))
                    .text_wrapping(TextWrapping::Wrap)
                    .is_text_selection_enabled(true),
            ));
        }
        m3::card(palette)
            .automation_name(format!("Automation {}", card.title))
            .content(
                StackPanel::new()
                    .spacing(tokens::spacing::SM)
                    .keyed_children(lines),
            )
    }

    /// The short form: labelled fields with a line of help each.
    fn assignment_form_view(
        &self,
        form: &AssignmentForm,
        context: &mut ViewContext<Self>,
        palette: Palette,
    ) -> View {
        let text_row = |context: &mut ViewContext<Self>,
                        field: FormField,
                        label: &str,
                        value: &str,
                        hint: &str,
                        help: &str| {
            KeyedView::new(
                label.to_owned(),
                StackPanel::new().children((
                    m3::form_row(
                        label,
                        m3::field()
                            .text(value.to_owned())
                            .placeholder_text(hint)
                            .automation_name(label.to_owned())
                            .on_text_changed(context.callback(move |text| {
                                Message::Automation(AutomationMsg::FormText(field, text))
                            }))
                            .into(),
                        None,
                    ),
                    m3::help(help),
                )),
            )
        };
        let mut rows: Vec<KeyedView> = vec![
            KeyedView::new(
                "title",
                m3::section_title(match &form.editing {
                    Some(key) => format!("Edit {key}"),
                    None => "New automation".to_owned(),
                }),
            ),
            KeyedView::new(
                "about",
                m3::secondary(
                    "Swarm takes the .md briefs in a folder in filename order and starts one task for each.",
                )
                .text_wrapping(TextWrapping::Wrap),
            ),
        ];
        if form.editing.is_none() {
            rows.push(text_row(
                context,
                FormField::Name,
                "Name",
                &form.name,
                "For example backlog",
                "Letters, digits, - and _. Swarm keeps its place in the folder under this name.",
            ));
        }
        rows.push(text_row(
            context,
            FormField::Folder,
            "Briefs folder",
            &form.folder,
            "C:\\briefs\\queue",
            "Each .md file is one task. Name new ones so they sort after the last one started.",
        ));
        let workspaces = self.assignment_workspaces();
        let workspace_index = workspaces
            .iter()
            .position(|(path, _)| nav::path_key(path) == nav::path_key(&form.workspace));
        let labels: Vec<String> = workspaces.iter().map(|(_, label)| label.clone()).collect();
        rows.push(KeyedView::new(
            "workspace",
            StackPanel::new().children((
                m3::form_row(
                    "Work in",
                    Grid::new().keyed_children(vec![KeyedView::new(
                        format!("{}:{workspace_index:?}", labels.join("\u{1f}")),
                        m3::enum_width(ComboBox::new())
                            .items_source(labels)
                            .selected_index(workspace_index)
                            .automation_name("Work in")
                            .on_selection_changed(context.callback(|index| {
                                Message::Automation(AutomationMsg::FormWorkspace(index))
                            })),
                    )]),
                    None,
                ),
                m3::help("The project the tasks start in."),
            )),
        ));
        let roles = self.assignment_roles();
        let role_index = roles.iter().position(|role| *role == form.role);
        rows.push(KeyedView::new(
            "role",
            StackPanel::new().children((
                m3::form_row(
                    "Start as",
                    Grid::new().keyed_children(vec![KeyedView::new(
                        format!("{}:{role_index:?}", roles.join("\u{1f}")),
                        m3::enum_width(ComboBox::new())
                            .items_source(roles)
                            .selected_index(role_index)
                            .automation_name("Start as")
                            .on_selection_changed(context.callback(|index| {
                                Message::Automation(AutomationMsg::FormRole(index))
                            })),
                    )]),
                    None,
                ),
                m3::help("The kind of task. Settings › Routing picks its model for this role."),
            )),
        ));
        rows.push(KeyedView::new(
            "busy",
            StackPanel::new().children((
                m3::form_row(
                    "At most at once",
                    NumberBox::new()
                        .width(tokens::layout::NUMBER_WIDTH)
                        .horizontal_alignment(HorizontalAlignment::Left)
                        .minimum(1.0)
                        .maximum(1000.0)
                        .value(Some(form.busy as f64))
                        .automation_name("At most at once")
                        .on_value_changed(
                            context.callback(|value| {
                                Message::Automation(AutomationMsg::FormBusy(value))
                            }),
                        )
                        .into(),
                    None,
                ),
                m3::help("How many of its tasks may run at the same time."),
            )),
        ));
        rows.push(text_row(
            context,
            FormField::Interval,
            "Check every",
            &form.interval,
            "30s",
            "How often Swarm looks for the next brief: 30s, 5m or 1h.",
        ));
        if let Some(error) = &form.error {
            rows.push(KeyedView::new(
                "error",
                m3::body(error.clone())
                    .foreground(palette.error())
                    .text_wrapping(TextWrapping::Wrap),
            ));
        }
        let mut buttons: Vec<KeyedView> = vec![
            KeyedView::new(
                "save",
                m3::filled(palette)
                    .automation_name(if form.editing.is_some() {
                        "Save automation"
                    } else {
                        "Create automation"
                    })
                    .on_click(context.message(Message::Automation(AutomationMsg::FormSaved)))
                    .content(m3::label(if form.editing.is_some() {
                        "Save"
                    } else {
                        "Create"
                    })),
            ),
            KeyedView::new(
                "cancel",
                m3::text_button(palette)
                    .automation_name("Cancel")
                    .on_click(context.message(Message::Automation(AutomationMsg::FormClosed)))
                    .content(m3::label("Cancel")),
            ),
        ];
        if let Some(key) = &form.editing {
            buttons.push(KeyedView::new(
                "remove",
                m3::destructive(palette)
                    .automation_name(format!("Remove automation {key}"))
                    .on_click(
                        context.message(Message::Automation(AutomationMsg::Removed(key.clone()))),
                    )
                    .content(m3::label("Remove automation")),
            ));
        }
        rows.push(KeyedView::new(
            "buttons",
            StackPanel::new()
                .orientation(Orientation::Horizontal)
                .spacing(tokens::spacing::SM)
                .margin(Thickness::new(
                    tokens::layout::LABEL_WIDTH + tokens::spacing::SM,
                    tokens::spacing::XS,
                    0.0,
                    0.0,
                ))
                .keyed_children(buttons),
        ));
        m3::card(palette).content(
            StackPanel::new()
                .spacing(tokens::spacing::SM)
                .keyed_children(rows),
        )
    }
}
