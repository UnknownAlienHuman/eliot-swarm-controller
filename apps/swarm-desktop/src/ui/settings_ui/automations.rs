//! The Automations page (`Automations.dc.html`): its rows in plain words
//! by group beside the editor of the row chosen (a rule's sentence, or the
//! short form of a brief queue), and the Scripts tab. The card model and Run now
//! are in [`super::super::automation_ui`]; the assignment writes are here,
//! beside the settings draft they change, and save at once.

use super::super::automation_ui::{
    AssignmentForm, AssignmentView, AutoGroup, AutomationMsg, Card, EditorField, FormField,
    RuleEditor, RuleMsg, WhenKind, action_labels, interval_ms, when_labels,
};
use super::super::page_ui;
use super::*;

impl DesktopApp {
    /// Saved assignments. The timer's effective state comes from rule.list.
    pub(in crate::ui) fn assignment_views(&self) -> Vec<AssignmentView> {
        let automation = &self.settings.base(Section::Automation);
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
    pub(in crate::ui) fn automation_projects(&self) -> Vec<(String, String, String)> {
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
                saving: false,
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
        if self.settings.saving == Some(Page::Automations)
            || self.settings.queued.contains(&Page::Automations)
        {
            return;
        }
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
                if !self.settings.dirty(Page::Automations) {
                    self.automation.form = None;
                    return;
                }
                form.error = None;
                form.saving = true;
                self.settings.notes.remove(&Page::Automations);
                self.autosave(Page::Automations, context);
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
    /// The Automations page (`Automations.dc.html`): the title and New
    /// automation, the Automations / Scripts tabs, then the groups (Checks,
    /// Reactions, After work, Schedules) beside the editor of the row
    /// chosen. The project check itself stays in Settings › Projects.
    pub(in crate::ui) fn automations_page(&self, context: &mut ViewContext<Self>) -> View {
        let scripts = self.automation.scripts_tab;
        let busy = self
            .automation
            .editor
            .as_ref()
            .is_some_and(|editor| editor.saving)
            || self
                .automation
                .form
                .as_ref()
                .is_some_and(|form| form.saving);
        let new_menu = vec![
            MenuItem::item("rule", "New rule…"),
            MenuItem::item("queue", "New brief queue…"),
        ];
        let mut items: Vec<KeyedView> = vec![KeyedView::new(
            "title",
            page_ui::toolbar(
                page_ui::title("Automations"),
                vec![
                    KeyedView::new(
                        "refresh",
                        controls::icon_button()
                            .automation_name("Refresh automations")
                            .on_click(context.message(Message::Automation(
                                AutomationMsg::AutomationRefreshRequested,
                            )))
                            .content(controls::icon(controls::glyph::REFRESH))
                            .tooltip("Refresh automations"),
                    ),
                    KeyedView::new(
                        format!("new:{busy}"),
                        controls::filled()
                            .is_enabled(!busy)
                            .automation_name("New automation")
                            .content(controls::icon_label(
                                controls::glyph::NEW_TASK,
                                "New automation",
                            ))
                            .menu(Menu::new(
                                new_menu,
                                crate::views::menu_keys(context.callback(|key: String| {
                                    if key == "queue" {
                                        Message::Automation(AutomationMsg::FormOpened(None))
                                    } else {
                                        Message::Automation(AutomationMsg::Rule(RuleMsg::New))
                                    }
                                })),
                            )),
                    ),
                ],
            ),
        )];
        let tabs: Vec<KeyedView> = [("Automations", false), ("Scripts", true)]
            .into_iter()
            .map(|(label, tab)| {
                let selected = scripts == tab;
                KeyedView::new(
                    format!("{label}:{selected}"),
                    controls::pane_tab(selected)
                        .automation_name(if selected {
                            format!("{label} (shown)")
                        } else {
                            format!("Show {label}")
                        })
                        .on_click(context.message(Message::Automation(AutomationMsg::Rule(
                            RuleMsg::TabChosen(tab),
                        ))))
                        .content(controls::pane_tab_label(label)),
                )
            })
            .collect();
        items.push(KeyedView::new(
            "tabs",
            Border::new()
                .border_brush(ThemeBrush::CardStroke)
                .border_thickness(Thickness::new(0.0, 0.0, 0.0, tokens::layout::HAIRLINE))
                .padding(Thickness::new(0.0, 0.0, 0.0, tokens::spacing::XS))
                .content(
                    StackPanel::new()
                        .orientation(Orientation::Horizontal)
                        .keyed_children(tabs),
                ),
        ));
        if scripts {
            items.push(KeyedView::new("scripts", self.scripts_view(context)));
            return page_ui::page("Automations", tokens::page::WIDTH, items);
        }
        let cards = self.automation_cards();
        let mut left: Vec<KeyedView> = Vec::new();
        if !self.automation.read() {
            left.push(KeyedView::new(
                "reading",
                page_ui::quiet("Reading automations…"),
            ));
        } else if cards.is_empty() {
            left.push(KeyedView::new(
                "empty",
                page_ui::quiet("No automations yet."),
            ));
        }
        for namespace in &self.automation.rules {
            match &namespace.rules {
                Err(error) => left.push(KeyedView::new(
                    format!("error:{}", namespace.label),
                    page_ui::read_failed(
                        format!(
                            "{}: not read ({})",
                            namespace.label,
                            crate::state::plain_error(error)
                        ),
                        "Retry",
                        true,
                        context.message(Message::Automation(
                            AutomationMsg::AutomationRefreshRequested,
                        )),
                    ),
                )),
                Ok(_) if namespace.more => left.push(KeyedView::new(
                    format!("more:{}", namespace.label),
                    page_ui::quiet(format!(
                        "{}: more rules exist than this page shows.",
                        namespace.label
                    )),
                )),
                Ok(_) => {}
            }
        }
        for group in AutoGroup::ALL {
            let rows: Vec<KeyedView> = cards
                .iter()
                .filter(|card| card.group == group)
                .map(|card| KeyedView::new(card.key.clone(), self.automation_row(card, context)))
                .collect();
            if rows.is_empty() {
                continue;
            }
            left.push(KeyedView::new(
                format!("group:{}", group.title()),
                page_ui::heading(group.title()),
            ));
            left.push(KeyedView::new(
                format!("rows:{}", group.title()),
                controls::settings_card(rows),
            ));
        }
        let mut right: Vec<KeyedView> = Vec::new();
        if let Some(form) = self.automation.form.as_ref() {
            right.push(KeyedView::new(
                format!("queue:{:?}", form.editing),
                self.assignment_form_view(form, context),
            ));
        } else if let Some(editor) = self.automation.editor.as_ref() {
            right.push(KeyedView::new(
                format!("editor:{:?}:{}", editor.card, editor.toml.is_some()),
                self.rule_editor_view(editor, context),
            ));
        } else {
            right.push(KeyedView::new(
                "pick",
                page_ui::panel(
                    tokens::spacing::SM,
                    vec![KeyedView::new(
                        "hint",
                        page_ui::quiet("Select an automation"),
                    )],
                ),
            ));
        }
        // A brief queue saves into the settings: its save line.
        if let Some(note) = self
            .settings
            .notes
            .get(&Page::Automations)
            .filter(|_| self.automation.form.is_some())
        {
            right.push(KeyedView::new(
                format!("note:{}", note.text),
                if note.error {
                    page_ui::error_line(note.text.clone())
                } else {
                    page_ui::quiet(note.text.clone())
                },
            ));
        }
        // The open row's technical details (its keys, revisions, cursor).
        let open_card = self
            .automation
            .open
            .as_ref()
            .and_then(|key| cards.iter().find(|card| &card.key == key))
            .or_else(|| {
                let editing = self.automation.form.as_ref()?.editing.as_ref()?;
                cards
                    .iter()
                    .find(|card| card.assignment.as_ref().map(|(key, _)| key) == Some(editing))
            });
        if let Some(card) = open_card {
            let open = self.automation.technical.contains(&card.key);
            right.push(KeyedView::new(
                format!("technical:{}:{open}", card.key),
                StackPanel::new()
                    .spacing(tokens::spacing::XS)
                    .keyed_children(crate::views::slots([
                        Some(
                            controls::text_button()
                                .horizontal_alignment(HorizontalAlignment::Left)
                                .automation_name(format!(
                                    "Technical details of {} ({})",
                                    card.title,
                                    if open { "expanded" } else { "collapsed" }
                                ))
                                .on_click(context.message(Message::Automation(
                                    AutomationMsg::TechnicalToggled(card.key.clone()),
                                )))
                                .content(controls::icon_label(
                                    if open {
                                        controls::glyph::CHEVRON_DOWN
                                    } else {
                                        controls::glyph::CHEVRON_RIGHT
                                    },
                                    "Technical details",
                                ))
                                .into(),
                        ),
                        if open {
                            Some(
                                controls::secondary(card.technical.join("\n"))
                                    .text_wrapping(TextWrapping::Wrap)
                                    .is_text_selection_enabled(true)
                                    .into(),
                            )
                        } else {
                            None
                        },
                    ])),
            ));
        }
        items.push(KeyedView::new(
            "columns",
            page_ui::columns(left, tokens::spacing::SM, right),
        ));
        page_ui::page("Automations", tokens::page::WIDTH, items)
    }

    /// One row (`.srow.arow`): its plain name, the trigger with the last
    /// outcome or refusal (the next run for a timer), the state pill, the
    /// switch and the menu (Run now for a timer, Edit, Remove).
    fn automation_row(&self, card: &Card, context: &mut ViewContext<Self>) -> View {
        let selected = self.automation.open.as_deref() == Some(card.key.as_str())
            || self
                .automation
                .form
                .as_ref()
                .and_then(|form| form.editing.as_ref())
                .is_some_and(|key| card.assignment.as_ref().map(|(own, _)| own) == Some(key));
        let status = card
            .rule
            .as_ref()
            .and_then(|rule| self.automation.status.get(&rule.key));
        let last = match status {
            Some(Ok(show)) => match (&show.last_outcome, &show.last_refusal) {
                (outcome, Some(refusal))
                    if outcome
                        .as_ref()
                        .is_none_or(|outcome| refusal.at >= outcome.settled_at) =>
                {
                    format!(
                        "last refusal: {} {}",
                        refusal.reason,
                        crate::config_view::local_time(refusal.at)
                    )
                }
                (Some(outcome), _) => format!(
                    "last: {} {}",
                    format!("{:?}", outcome.state).to_lowercase(),
                    crate::config_view::local_time(outcome.settled_at)
                ),
                (None, _) => "last: no runs yet".to_owned(),
            },
            Some(Err(error)) => format!("last outcome not read ({error})"),
            None => String::new(),
        };
        let mut line = card.trigger.clone();
        for part in [last, card.times.clone()] {
            if !part.is_empty() {
                line = format!("{line} · {part}");
            }
        }
        let toggle_error = self.automation.toggle_errors.get(&card.key);
        let mut text: Vec<KeyedView> = Vec::new();
        if let Some(reason) = card.state.reason() {
            text.push(KeyedView::new("reason", page_ui::error_line(reason)));
        }
        if let Some(error) = toggle_error {
            text.push(KeyedView::new(
                "toggle",
                page_ui::error_line(format!("Not switched: {error}")),
            ));
        }
        if let Some(run) = card
            .run
            .as_ref()
            .and_then(|key| self.automation.runs.get(key))
        {
            text.push(KeyedView::new(
                "run",
                if run.error {
                    page_ui::error_line(run.text.clone())
                } else {
                    page_ui::quiet(run.text.clone())
                },
            ));
        }
        // The switch: an assignment's or a file rule's `enabled`; a rule
        // Swarm generates from the settings has no file to switch.
        let (on, switchable, why) = match (&card.assignment, &card.rule) {
            (Some((_, on)), _) => (*on, true, None),
            (None, Some(rule)) if rule.file.is_some() => (rule.enabled, true, None),
            (None, Some(rule)) => (
                rule.enabled,
                false,
                Some("Swarm makes this rule from the settings; change it in Settings"),
            ),
            (None, None) => (false, false, None),
        };
        let toggling = self.automation.toggling.contains(&card.key);
        let key = card.key.clone();
        let switch: View = Grid::new()
            .keyed_children(vec![KeyedView::new(
                format!("switch:{on}:{switchable}:{toggling}"),
                ToggleSwitch::new()
                    .is_on(on)
                    .is_enabled(switchable && !toggling)
                    .min_width(0.0)
                    .automation_name(match why {
                        Some(why) => format!("{} enabled: {why}", card.title),
                        None => format!("{} enabled", card.title),
                    })
                    .on_toggled(context.callback(move |on: bool| {
                        Message::Automation(AutomationMsg::Rule(RuleMsg::Toggled(key.clone(), on)))
                    })),
            )])
            .into();
        let switch = match why {
            Some(why) => Border::new()
                .background(Brush::Solid(Color::argb(0, 0, 0, 0)))
                .content(switch)
                .tooltip(why),
            None => switch,
        };
        let mut menu: Vec<MenuItem> = Vec::new();
        if card.run.is_some() {
            menu.push(MenuItem::item(format!("run\u{1f}{}", card.key), "Run now"));
        }
        menu.push(MenuItem::item(format!("edit\u{1f}{}", card.key), "Edit"));
        if card.assignment.is_some() || card.rule.as_ref().is_some_and(|rule| rule.file.is_some()) {
            menu.push(MenuItem::item(
                format!("remove\u{1f}{}", card.key),
                "Remove",
            ));
        }
        let content = Grid::new()
            .columns([
                GridLength::STAR,
                GridLength::Auto,
                GridLength::Auto,
                GridLength::Auto,
            ])
            .column_spacing(tokens::spacing::SM)
            .children((
                controls::row(false)
                    .height(tokens::height::CONTROL + tokens::spacing::LG)
                    .automation_name(format!("Edit {}: {line}", card.title))
                    .on_click(context.message(Message::Automation(AutomationMsg::Rule(
                        RuleMsg::Opened(card.key.clone()),
                    ))))
                    .content(page_ui::two_lines(&card.title, &line)),
                Border::new()
                    .grid_column(1)
                    .vertical_alignment(VerticalAlignment::Center)
                    .content(controls::status_pill(
                        card.state.tone(),
                        &card.state.label(),
                    )),
                Border::new()
                    .grid_column(2)
                    .vertical_alignment(VerticalAlignment::Center)
                    .content(switch),
                controls::icon_button()
                    .grid_column(3)
                    .vertical_alignment(VerticalAlignment::Center)
                    .automation_name(format!("Actions for {}", card.title))
                    .content(controls::icon(controls::glyph::MORE_VERTICAL))
                    .menu(Menu::new(
                        menu,
                        crate::views::menu_keys(context.callback(|key: String| {
                            Message::Automation(AutomationMsg::Rule(RuleMsg::Menu(key)))
                        })),
                    )),
            ));
        let row: View = if text.is_empty() {
            content.into()
        } else {
            StackPanel::new()
                .children((
                    content,
                    StackPanel::new()
                        .margin(Thickness::new(
                            tokens::spacing::ROW,
                            0.0,
                            0.0,
                            tokens::spacing::SM,
                        ))
                        .keyed_children(text),
                ))
                .into()
        };
        if selected {
            // The ring spans the card's row (`.arow.sel`): out over the
            // card's side padding.
            Border::new()
                .border_brush(ThemeBrush::Accent)
                .border_thickness(Thickness::uniform(2.0 * tokens::layout::HAIRLINE))
                .corner_radius(tokens::radius::CONTAINER)
                .margin(Thickness::xy(-tokens::spacing::MD, 0.0))
                .padding(Thickness::xy(
                    tokens::spacing::MD - 2.0 * tokens::layout::HAIRLINE,
                    0.0,
                ))
                .content(row)
                .into()
        } else {
            row
        }
    }

    /// The rule editor (`.panel.editor`): the sentence When · Only if · Do
    /// · Repeat, the last outcome and refusal, then Delete, Edit as TOML,
    /// Cancel and Save; check errors under their field.
    fn rule_editor_view(&self, editor: &RuleEditor, context: &mut ViewContext<Self>) -> View {
        let title = match &editor.rule {
            Some(rule) => rule.key.1.clone(),
            None => "New rule".to_owned(),
        };
        let enabled = !editor.saving && !editor.reading;
        let generated = editor.rule.as_ref().is_some_and(|rule| rule.file.is_none());
        let mut parts: Vec<KeyedView> =
            vec![KeyedView::new("head", page_ui::panel_head(&title, None))];
        let field_error = |field: EditorField| -> Option<View> {
            match &editor.field_error {
                Some((at, error)) if *at == field => Some(page_ui::error_line(error.clone())),
                _ => None,
            }
        };
        let word = |text: &str, row: i32| -> View {
            controls::text(text.to_owned(), tokens::page::HEADING)
                .grid_row(row)
                .vertical_alignment(VerticalAlignment::Center)
                .into()
        };
        if generated {
            parts.push(KeyedView::new(
                "generated",
                page_ui::quiet(
                    "Swarm makes this rule from the settings (an attention reaction or a brief queue). Change it there.",
                ),
            ));
        } else if editor.reading {
            parts.push(KeyedView::new(
                "reading",
                page_ui::quiet("Reading the rule…"),
            ));
        } else if let Some(toml) = &editor.toml {
            parts.push(KeyedView::new(
                "toml",
                controls::field()
                    .text(toml.clone())
                    .accepts_return(true)
                    .text_wrapping(TextWrapping::Wrap)
                    .min_height(tokens::layout::SOURCE_MIN_HEIGHT)
                    .max_height(tokens::layout::SOURCE_MAX_HEIGHT)
                    .is_enabled(enabled)
                    .automation_name(format!("{title} as TOML"))
                    .on_text_changed(crate::views::text_edits(context.callback(|text| {
                        Message::Automation(AutomationMsg::Rule(RuleMsg::TomlText(text)))
                    }))),
            ));
        } else {
            let sentence = &editor.sentence;
            let mut rows: Vec<KeyedView> = Vec::new();
            let mut row = 0;
            if editor.rule.is_none() {
                rows.push(KeyedView::new("name-word", word("Name", row)));
                rows.push(KeyedView::new(
                    "name",
                    StackPanel::new()
                        .grid_row(row)
                        .grid_column(1)
                        .keyed_children(crate::views::slots([
                            Some(
                                controls::field()
                                    .text(editor.id.clone())
                                    .placeholder_text("check-after-turn")
                                    .is_enabled(enabled)
                                    .automation_name("Rule name")
                                    .on_text_changed(crate::views::text_edits(context.callback(
                                        |text| {
                                            Message::Automation(AutomationMsg::Rule(RuleMsg::Id(
                                                text,
                                            )))
                                        },
                                    )))
                                    .into(),
                            ),
                            field_error(EditorField::Name),
                        ])),
                ));
                row += 1;
            }
            let when_labels = when_labels();
            let when_index = sentence.when_index();
            let mut when: Vec<KeyedView> = vec![KeyedView::new(
                format!("when:{when_index}"),
                ComboBox::new()
                    .items_source(when_labels)
                    .selected_index(Some(when_index))
                    .horizontal_alignment(HorizontalAlignment::Stretch)
                    .is_enabled(enabled)
                    .automation_name("When")
                    .on_selection_changed(context.callback(|index| {
                        Message::Automation(AutomationMsg::Rule(RuleMsg::When(index)))
                    })),
            )];
            if sentence.when_typed() {
                when.push(KeyedView::new(
                    "when-value",
                    controls::field()
                        .text(sentence.when_value.clone())
                        .placeholder_text(match sentence.when {
                            WhenKind::Event => "turn.completed",
                            WhenKind::Every => "30m",
                            WhenKind::Cron => "0 9 * * 1-5",
                            WhenKind::Once => "2026-12-01T09:00:00Z",
                        })
                        .is_enabled(enabled)
                        .automation_name("When, typed")
                        .on_text_changed(crate::views::text_edits(context.callback(|text| {
                            Message::Automation(AutomationMsg::Rule(RuleMsg::WhenValue(text)))
                        }))),
                ));
            }
            if sentence.when == WhenKind::Cron {
                when.push(KeyedView::new(
                    "zone",
                    controls::field()
                        .text(sentence.zone.clone())
                        .placeholder_text("Time zone (UTC when empty)")
                        .is_enabled(enabled)
                        .automation_name("Time zone")
                        .on_text_changed(crate::views::text_edits(context.callback(|text| {
                            Message::Automation(AutomationMsg::Rule(RuleMsg::Zone(text)))
                        }))),
                ));
            }
            when.extend(
                field_error(EditorField::When).map(|view| KeyedView::new("when-error", view)),
            );
            rows.push(KeyedView::new("when-word", word("When", row)));
            rows.push(KeyedView::new(
                "when",
                StackPanel::new()
                    .grid_row(row)
                    .grid_column(1)
                    .spacing(tokens::spacing::XS)
                    .keyed_children(when),
            ));
            row += 1;
            rows.push(KeyedView::new("if-word", word("Only if", row)));
            rows.push(KeyedView::new(
                "if",
                StackPanel::new()
                    .grid_row(row)
                    .grid_column(1)
                    .keyed_children(crate::views::slots([
                        Some(
                            controls::field()
                                .text(sentence.only_if.clone())
                                .placeholder_text("Always")
                                .is_enabled(enabled)
                                .automation_name("Only if")
                                .on_text_changed(crate::views::text_edits(context.callback(
                                    |text| {
                                        Message::Automation(AutomationMsg::Rule(RuleMsg::OnlyIf(
                                            text,
                                        )))
                                    },
                                )))
                                .into(),
                        ),
                        field_error(EditorField::OnlyIf),
                    ])),
            ));
            row += 1;
            let action_labels = action_labels(sentence);
            let action_index = sentence.action_index();
            let mut act: Vec<KeyedView> = vec![KeyedView::new(
                format!("do:{action_index}:{}", action_labels.len()),
                ComboBox::new()
                    .items_source(action_labels)
                    .selected_index(Some(action_index))
                    .horizontal_alignment(HorizontalAlignment::Stretch)
                    .is_enabled(enabled)
                    .automation_name("Do")
                    .on_selection_changed(context.callback(|index| {
                        Message::Automation(AutomationMsg::Rule(RuleMsg::Action(index)))
                    })),
            )];
            if let Some(text) = &sentence.text {
                act.push(KeyedView::new(
                    "text",
                    controls::field()
                        .text(text.clone())
                        .placeholder_text("The text it sends")
                        .is_enabled(enabled)
                        .automation_name("Text")
                        .on_text_changed(crate::views::text_edits(context.callback(|text| {
                            Message::Automation(AutomationMsg::Rule(RuleMsg::Text(text)))
                        }))),
                ));
            }
            if sentence.more_actions > 0 {
                act.push(KeyedView::new(
                    "more",
                    controls::secondary(format!(
                        "and {} more (Edit as TOML)",
                        eliot_core::count_words(sentence.more_actions as u64, "action", "actions")
                    )),
                ));
            }
            act.extend(field_error(EditorField::Do).map(|view| KeyedView::new("do-error", view)));
            rows.push(KeyedView::new("do-word", word("Do", row)));
            rows.push(KeyedView::new(
                "do",
                StackPanel::new()
                    .grid_row(row)
                    .grid_column(1)
                    .spacing(tokens::spacing::XS)
                    .keyed_children(act),
            ));
            row += 1;
            rows.push(KeyedView::new("repeat-word", word("Repeat", row)));
            let repeat: View = match (sentence.repeat, &sentence.limit_words) {
                (Some(max), words) => StackPanel::new()
                    .spacing(tokens::spacing::XS)
                    .keyed_children(crate::views::slots([
                        Some(
                            NumberBox::new()
                                .width(tokens::page::REPEAT_FIELD)
                                .horizontal_alignment(HorizontalAlignment::Left)
                                .minimum(1.0)
                                .value(Some(max as f64))
                                .is_enabled(enabled)
                                .automation_name("Repeat at most")
                                .on_value_changed(context.callback(|value| {
                                    Message::Automation(AutomationMsg::Rule(RuleMsg::Repeat(value)))
                                }))
                                .into(),
                        ),
                        Some(
                            controls::secondary(match words {
                                Some(words) if !words.is_empty() => {
                                    format!("times at most ({words}), then it waits")
                                }
                                _ => "times at most, then it waits".to_owned(),
                            })
                            .text_wrapping(TextWrapping::Wrap)
                            .into(),
                        ),
                        field_error(EditorField::Repeat),
                    ]))
                    .into(),
                (None, _) => controls::secondary("No limit · Edit as TOML to add one")
                    .vertical_alignment(VerticalAlignment::Center)
                    .into(),
            };
            rows.push(KeyedView::new(
                "repeat",
                Border::new().grid_row(row).grid_column(1).content(repeat),
            ));
            row += 1;
            parts.push(KeyedView::new(
                "sentence",
                Grid::new()
                    .columns([
                        GridLength::Pixel(tokens::page::SENTENCE_WORD),
                        GridLength::STAR,
                    ])
                    .rows((0..row).map(|_| GridLength::Auto).collect::<Vec<_>>())
                    .column_spacing(tokens::spacing::INSET)
                    .row_spacing(tokens::spacing::SM)
                    .keyed_children(rows),
            ));
        }
        if let Some(rule) = &editor.rule {
            let (outcome, refusal) = match self.automation.status.get(&rule.key) {
                Some(Ok(show)) => (
                    show.last_outcome.as_ref().map_or_else(
                        || "None yet".to_owned(),
                        |outcome| {
                            format!(
                                "{:?} · {}",
                                outcome.state,
                                crate::config_view::local_time(outcome.settled_at)
                            )
                        },
                    ),
                    show.last_refusal.as_ref().map_or_else(
                        || "None".to_owned(),
                        |refusal| {
                            format!(
                                "{} · {}",
                                refusal.reason,
                                crate::config_view::local_time(refusal.at)
                            )
                        },
                    ),
                ),
                Some(Err(error)) => (format!("Not read ({error})"), String::new()),
                None => ("Reading…".to_owned(), String::new()),
            };
            parts.push(KeyedView::new(
                "outcome",
                page_ui::kv_text("Last outcome", outcome),
            ));
            if !refusal.is_empty() {
                parts.push(KeyedView::new(
                    "refusal",
                    page_ui::kv_text("Last refusal", refusal),
                ));
            }
        }
        if let Some((note, error)) = &editor.note {
            parts.push(KeyedView::new(
                "note",
                if *error {
                    page_ui::error_line(note.clone())
                } else {
                    page_ui::quiet(note.clone())
                },
            ));
        }
        let can_edit = enabled && !generated;
        let mut left: Vec<KeyedView> = Vec::new();
        if editor.rule.as_ref().is_some_and(|rule| rule.file.is_some()) {
            left.push(KeyedView::new(
                "delete",
                controls::destructive()
                    .is_enabled(can_edit && editor.revision.is_some())
                    .automation_name(format!("Delete {title}"))
                    .on_click(
                        context.message(Message::Automation(AutomationMsg::Rule(RuleMsg::Delete))),
                    )
                    .content(controls::label("Delete")),
            ));
            left.push(KeyedView::new(
                format!("toml:{}", editor.toml.is_some()),
                controls::text_button()
                    .is_enabled(can_edit)
                    .automation_name(if editor.toml.is_some() {
                        "Edit as a sentence"
                    } else {
                        "Edit as TOML"
                    })
                    .on_click(context.message(Message::Automation(AutomationMsg::Rule(
                        RuleMsg::TomlToggled,
                    ))))
                    .content(controls::label(if editor.toml.is_some() {
                        "Edit as a sentence"
                    } else {
                        "Edit as TOML"
                    })),
            ));
        }
        parts.push(KeyedView::new(
            "actions",
            page_ui::toolbar(
                StackPanel::new()
                    .orientation(Orientation::Horizontal)
                    .spacing(tokens::spacing::XS)
                    .keyed_children(left),
                vec![
                    KeyedView::new(
                        "cancel",
                        controls::outlined()
                            .is_enabled(!editor.saving)
                            .automation_name("Cancel")
                            .on_click(
                                context.message(Message::Automation(AutomationMsg::Rule(
                                    RuleMsg::Cancel,
                                ))),
                            )
                            .content(controls::label("Cancel")),
                    ),
                    KeyedView::new(
                        "save",
                        controls::filled()
                            .is_enabled(can_edit && (editor.dirty() || editor.rule.is_none()))
                            .automation_name(format!("Save {title}"))
                            .on_click(
                                context.message(Message::Automation(AutomationMsg::Rule(
                                    RuleMsg::Save,
                                ))),
                            )
                            .content(controls::label(if editor.saving {
                                "Saving…"
                            } else {
                                "Save"
                            })),
                    ),
                ],
            ),
        ));
        page_ui::panel(tokens::spacing::SM, parts)
    }

    /// The short form: labelled fields, each one's purpose in its
    /// placeholder and tooltip (as the reference apps put it).
    fn assignment_form_view(&self, form: &AssignmentForm, context: &mut ViewContext<Self>) -> View {
        let text_row = |context: &mut ViewContext<Self>,
                        field: FormField,
                        label: &str,
                        value: &str,
                        hint: &str,
                        help: &str| {
            KeyedView::new(
                label.to_owned(),
                controls::form_row(
                    label,
                    controls::field()
                        .is_enabled(!form.saving)
                        .text(value)
                        .placeholder_text(hint)
                        .automation_name(label)
                        .on_text_changed(crate::views::text_edits(context.callback(move |text| {
                            Message::Automation(AutomationMsg::FormText(field, text))
                        })))
                        .automation_help_text(help)
                        .tooltip(help),
                    None,
                ),
            )
        };
        let mut rows: Vec<KeyedView> = vec![KeyedView::new(
            "title",
            controls::section_title(match &form.editing {
                Some(key) => format!("Edit {key}"),
                None => "New automation".to_owned(),
            }),
        )];
        if form.editing.is_none() {
            rows.push(text_row(
                context,
                FormField::Name,
                "Name",
                &form.name,
                "For example backlog",
                "Letters, digits, - and _. Swarm keeps its place in the folder under this name",
            ));
        }
        rows.push(text_row(
            context,
            FormField::Folder,
            "Briefs folder",
            &form.folder,
            "C:\\briefs\\queue",
            "Each .md file is one task, started in filename order",
        ));
        let workspaces = self.assignment_workspaces();
        let workspace_index = workspaces
            .iter()
            .position(|(path, _)| nav::path_key(path) == nav::path_key(&form.workspace));
        let labels: Vec<String> = workspaces.iter().map(|(_, label)| label.clone()).collect();
        rows.push(KeyedView::new(
            "workspace",
            controls::form_row(
                "Work in",
                Grid::new()
                    .keyed_children(vec![KeyedView::new(
                        format!("{}:{workspace_index:?}", labels.join("\u{1f}")),
                        controls::enum_width(ComboBox::new())
                            .is_enabled(!form.saving)
                            .items_source(labels)
                            .selected_index(workspace_index)
                            .automation_name("Work in")
                            .on_selection_changed(context.callback(|index| {
                                Message::Automation(AutomationMsg::FormWorkspace(index))
                            })),
                    )])
                    .automation_help_text("The project the tasks start in")
                    .tooltip("The project the tasks start in"),
                None,
            ),
        ));
        let roles = self.assignment_roles();
        let role_index = roles.iter().position(|role| *role == form.role);
        rows.push(KeyedView::new(
            "role",
            controls::form_row(
                "Start as",
                Grid::new()
                    .keyed_children(vec![KeyedView::new(
                        format!("{}:{role_index:?}", roles.join("\u{1f}")),
                        controls::enum_width(ComboBox::new())
                            .is_enabled(!form.saving)
                            .items_source(roles)
                            .selected_index(role_index)
                            .automation_name("Start as")
                            .on_selection_changed(context.callback(|index| {
                                Message::Automation(AutomationMsg::FormRole(index))
                            })),
                    )])
                    .automation_help_text("The task's role: Settings › Routing picks its model")
                    .tooltip("The task's role: Settings › Routing picks its model"),
                None,
            ),
        ));
        rows.push(KeyedView::new(
            "busy",
            controls::form_row(
                "At most at once",
                NumberBox::new()
                    .is_enabled(!form.saving)
                    .width(tokens::layout::NUMBER_WIDTH)
                    .horizontal_alignment(HorizontalAlignment::Left)
                    .minimum(1.0)
                    .maximum(1000.0)
                    .value(Some(form.busy as f64))
                    .automation_name("At most at once")
                    .on_value_changed(
                        context
                            .callback(|value| Message::Automation(AutomationMsg::FormBusy(value))),
                    )
                    .automation_help_text("How many of its tasks may run at the same time")
                    .tooltip("How many of its tasks may run at the same time"),
                None,
            ),
        ));
        rows.push(text_row(
            context,
            FormField::Interval,
            "Check every",
            &form.interval,
            "30s",
            "How often Swarm looks for the next brief: 30s, 5m or 1h",
        ));
        if let Some(error) = &form.error {
            rows.push(KeyedView::new(
                "error",
                controls::body(error.clone())
                    .foreground(ThemeBrush::SystemCritical)
                    .text_wrapping(TextWrapping::Wrap),
            ));
        }
        let mut buttons: Vec<KeyedView> = vec![
            KeyedView::new(
                "save",
                controls::filled()
                    .is_enabled(!form.saving)
                    .automation_name(if form.editing.is_some() {
                        "Save automation"
                    } else {
                        "Create automation"
                    })
                    .on_click(context.message(Message::Automation(AutomationMsg::FormSaved)))
                    .content(controls::label(if form.editing.is_some() {
                        "Save"
                    } else {
                        "Create"
                    })),
            ),
            KeyedView::new(
                "cancel",
                controls::text_button()
                    .is_enabled(!form.saving)
                    .automation_name("Cancel")
                    .on_click(context.message(Message::Automation(AutomationMsg::FormClosed)))
                    .content(controls::label("Cancel")),
            ),
        ];
        if let Some(key) = &form.editing {
            buttons.push(KeyedView::new(
                "remove",
                controls::destructive()
                    .is_enabled(!form.saving)
                    .automation_name(format!("Remove automation {key}"))
                    .on_click(
                        context.message(Message::Automation(AutomationMsg::Removed(key.clone()))),
                    )
                    .content(controls::label("Remove automation")),
            ));
        }
        rows.push(KeyedView::new(
            "buttons",
            StackPanel::new()
                .orientation(Orientation::Horizontal)
                .spacing(tokens::spacing::SM)
                .margin(Thickness::new(
                    tokens::layout::FIELD_INDENT,
                    tokens::spacing::XS,
                    0.0,
                    0.0,
                ))
                .keyed_children(buttons),
        ));
        page_ui::panel(tokens::spacing::SM, rows)
    }
}
