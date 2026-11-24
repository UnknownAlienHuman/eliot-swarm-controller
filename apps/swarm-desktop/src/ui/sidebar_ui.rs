//! The sidebar of the approved design (`Main.dc.html`) as its own
//! component: the window computes its rows from the listed Agents
//! ([`DesktopApp::sidebar_input`]: `nav::sections`, the owner's names, the
//! Monitor's reports, each row's age and work line), and [`Sidebar`] draws
//! them. Reactor 0.100.0 re-renders the window for every message, but a
//! component whose input compares equal is skipped: a keystroke, a
//! conversation delivery or a Monitor read leaves the sidebar as it is,
//! and a sidebar refresh redraws the sidebar, not the conversation.

use super::menu_ui::Target;
use super::*;
use crate::nav::Row;
use crate::theme::StatusTone;
use crate::theme::winui::Palette;

/// The sidebar's input: everything it draws, compared by value. Ages and
/// work lines are read at the window's render to the minute, so an
/// unchanged list compares equal.
#[derive(Clone, PartialEq)]
pub(super) struct SidebarInput {
    pub(super) palette: Palette,
    pub(super) can_back: bool,
    pub(super) can_forward: bool,
    /// New task is the place shown.
    pub(super) home: bool,
    /// The Settings page shown, if any (its nav row is selected).
    pub(super) page: Option<settings_ui::Page>,
    /// The Search row opened into its field.
    pub(super) search: Option<SearchField>,
    /// A search query lists what it found, flat in its projects.
    pub(super) filtered: bool,
    /// The row under the pointer (its actions show).
    pub(super) hovered: Option<String>,
    /// The row holding keyboard focus (its actions show too).
    pub(super) focused: Option<String>,
    /// A failed organization or queue read, in words.
    pub(super) errors: Vec<String>,
    /// The line shown without projects.
    pub(super) empty: Option<&'static str>,
    pub(super) places: Vec<PlaceEntry>,
    /// "Show more (N of M)": its label and whether it is enabled.
    pub(super) more: Option<(String, bool)>,
    pub(super) show_archived: bool,
    pub(super) fleet: FleetEntry,
    pub(super) inbox: InboxEntry,
    /// The rename or tag editor in a row's place.
    pub(super) renaming: Option<Renaming>,
    /// No bulk action is open (a missing folder's Remove is enabled).
    pub(super) bulk_idle: bool,
    pub(super) send: Callback<Message>,
}

/// The search field: its text, the suggestions' labels and its element.
#[derive(Clone, PartialEq)]
pub(super) struct SearchField {
    pub(super) text: String,
    pub(super) results: Vec<String>,
    pub(super) field: ElementRef<AutoSuggestBox>,
}

/// One project or subproject with its rows.
#[derive(Clone, PartialEq)]
pub(super) struct PlaceEntry {
    /// The section as `nav::sections` built it, its rows moved to `tasks`.
    pub(super) section: nav::Section,
    /// The owner's name for it, else the section's.
    pub(super) name: String,
    /// A linked worktree's subproject, as the owner renamed it.
    pub(super) subproject: Option<String>,
    /// Its name opens the project page.
    pub(super) opens: bool,
    /// Its project page is the one shown.
    pub(super) selected: bool,
    /// A missing folder's registration, which Remove removes.
    pub(super) registered: Option<String>,
    pub(super) tasks: Vec<TaskEntry>,
}

/// One task or chat row, read at the window's render.
#[derive(Clone, PartialEq)]
pub(super) struct TaskEntry {
    pub(super) row: Row,
    /// The owner's name for it, else its title.
    pub(super) label: String,
    pub(super) selected: bool,
    /// The Monitor's open report in words (it needs attention).
    pub(super) reported: Option<String>,
    /// A running task's work line and its words for the accessible name.
    pub(super) work: Option<(String, String)>,
    /// How long ago it changed ("3m").
    pub(super) age: String,
}

/// The fleet row: its line, its counts in words, its tone and whether the
/// Monitor is open.
#[derive(Clone, PartialEq)]
pub(super) struct FleetEntry {
    pub(super) line: String,
    pub(super) summary: String,
    pub(super) tone: StatusTone,
    pub(super) open: bool,
}

/// The Inbox row: the per-kind counts, their total and whether the Inbox
/// pane is open.
#[derive(Clone, PartialEq)]
pub(super) struct InboxEntry {
    pub(super) counts: String,
    pub(super) total: usize,
    pub(super) open: bool,
}

/// The target being renamed (or tagged), the text typed so far and the
/// tag editor's fields.
#[derive(Clone, PartialEq)]
pub(super) struct Renaming {
    pub(super) target: Target,
    pub(super) typed: String,
    pub(super) tags: Vec<String>,
    pub(super) field: ElementRef<TextBox>,
}

/// The sidebar component; its controls go to the window.
pub(super) struct Sidebar {
    send: Callback<Message>,
}

impl Component for Sidebar {
    type Input = SidebarInput;
    type Message = Message;

    fn create(input: &SidebarInput, _context: &ComponentContext<Self>) -> Self {
        Self {
            send: input.send.clone(),
        }
    }

    fn input_changed(&mut self, input: &SidebarInput, _context: &ComponentContext<Self>) {
        self.send = input.send.clone();
    }

    fn update(&mut self, message: Message, _context: &ComponentContext<Self>) {
        let _ = self.send.call(message);
    }

    /// The app title (home) with Back, Forward and Hide; the nav rows New
    /// task, Search, Automations and Usage; the projects with their tasks
    /// nested inside them (the open one included, never hoisted); then the
    /// fleet row and Settings at the bottom. Every row is a 32-DIP pill on
    /// one content edge (its icon 10 DIPs inside the 8-DIP gutter); nested
    /// rows indent by one 28-DIP step; a row's actions show on hover or
    /// selection.
    fn view(&self, input: &SidebarInput, context: &mut ViewContext<Self>) -> View {
        let palette = input.palette;
        let gutter = tokens::layout::SIDEBAR_GUTTER;
        let gap = tokens::spacing::ROW_GAP;
        let header = Grid::new()
            .height(tokens::height::TITLE_ROW)
            .margin(Thickness::new(
                gutter,
                tokens::layout::SIDEBAR_PADDING,
                gutter + gap,
                gap,
            ))
            .columns([
                GridLength::STAR,
                GridLength::Auto,
                GridLength::Auto,
                GridLength::Auto,
            ])
            .column_spacing(gap)
            .children((
                m3::text_chip(palette)
                    .horizontal_alignment(HorizontalAlignment::Left)
                    .automation_name("Eliot Swarm: home")
                    .on_click(context.message(Message::Shell(ShellMsg::NewTask)))
                    .content(m3::text("Eliot Swarm", tokens::text::TITLE))
                    .tooltip("Home (Ctrl+N)"),
                m3::icon_button(palette)
                    .grid_column(1)
                    .is_enabled(input.can_back)
                    .automation_name("Back (Alt+Left)")
                    .on_click(context.message(Message::Shell(ShellMsg::Back)))
                    .content(m3::icon(m3::glyph::BACK)),
                m3::icon_button(palette)
                    .grid_column(2)
                    .is_enabled(input.can_forward)
                    .automation_name("Forward (Alt+Right)")
                    .on_click(context.message(Message::Shell(ShellMsg::Forward)))
                    .content(m3::icon(m3::glyph::FORWARD)),
                m3::icon_button(palette)
                    .grid_column(3)
                    .automation_name("Hide sidebar")
                    .on_click(context.message(Message::Shell(ShellMsg::SidebarToggled)))
                    .content(m3::icon(m3::glyph::HIDE_SIDEBAR)),
            ));

        // The nav rows. A selectable row is keyed by its selection: WinUI
        // reads a button's colors only when its template is applied.
        let home = input.home;
        let mut nav: Vec<KeyedView> = vec![KeyedView::new(
            format!("new-task:{home}"),
            m3::new_task_button(palette)
                .automation_name("New task (Ctrl+N)")
                .on_click(context.message(Message::Shell(ShellMsg::NewTask)))
                .content(m3::nav_content(m3::glyph::NEW_TASK, "New task", "Ctrl+N")),
        )];
        nav.push(match &input.search {
            Some(search) => KeyedView::new(
                "search-field",
                Grid::new()
                    .columns([GridLength::STAR, GridLength::Auto])
                    .column_spacing(gap)
                    .children((
                        AutoSuggestBox::new()
                            .element_ref(&search.field)
                            .placeholder_text("Search tasks")
                            .text(search.text.clone())
                            .items_source(search.results.iter().cloned())
                            .on_text_changed(
                                context
                                    .callback(|text| Message::Shell(ShellMsg::SearchChanged(text))),
                            )
                            .on_suggestion_chosen(
                                context.callback(|label| {
                                    Message::Shell(ShellMsg::SearchChosen(label))
                                }),
                            )
                            .automation_name("Search tasks")
                            .height(tokens::height::CONTROL),
                        m3::icon_button(palette)
                            .grid_column(1)
                            .automation_name("Close search")
                            .on_click(
                                context.message(Message::Shell(ShellMsg::SearchToggled(false))),
                            )
                            .content(m3::icon(m3::glyph::CLOSE)),
                    )),
            ),
            None => KeyedView::new(
                "search",
                m3::row(palette, false)
                    .automation_name("Search tasks (Ctrl+K)")
                    .on_click(context.message(Message::Shell(ShellMsg::SearchToggled(true))))
                    .content(m3::nav_content(m3::glyph::SEARCH, "Search", "Ctrl+K")),
            ),
        });
        for (page, glyph, label) in [
            (
                settings_ui::Page::Automations,
                m3::glyph::AUTOMATION,
                "Automations",
            ),
            (settings_ui::Page::Usage, m3::glyph::USAGE, "Usage"),
        ] {
            let shown = input.page == Some(page);
            nav.push(KeyedView::new(
                format!("{label}:{shown}"),
                m3::row(palette, shown)
                    .automation_name(label.to_owned())
                    .on_click(context.message(Message::Shell(ShellMsg::SettingsPage(page))))
                    .content(m3::nav_content(glyph, label, "")),
            ));
        }
        let nav = StackPanel::new()
            .grid_row(1)
            .spacing(gap)
            .margin(Thickness::xy(gutter, 0.0))
            .keyed_children(nav);

        let filtered = input.filtered;
        let caption = if filtered {
            "Matching tasks"
        } else {
            "Projects"
        };
        let caption_key = format!("caption:{caption}");
        let caption_over = input.hovered.as_deref() == Some(caption_key.as_str());
        let caption_actions: View = if filtered || !caption_over {
            View::empty()
        } else {
            // Bulk actions over every project (k142): archive, delete,
            // remove the unused ones, each after one confirmation.
            let items: Vec<MenuItem> = super::bulk_ui::ALL_MENU
                .iter()
                .map(|(id, label)| MenuItem::item(*id, *label))
                .collect();
            m3::icon_button(palette)
                .horizontal_alignment(HorizontalAlignment::Right)
                .vertical_alignment(VerticalAlignment::Bottom)
                .automation_name("Actions for all projects")
                .content(m3::icon(m3::glyph::MORE))
                .menu(Menu::new(
                    items,
                    // Capture-free: one identity across renders.
                    context.callback(|id: String| Message::Bulk(BulkMsg::Menu(id))),
                ))
        };
        let mut items: Vec<KeyedView> = vec![KeyedView::new(
            caption_key.clone(),
            hover_area(
                caption_key,
                Grid::new().children((m3::section_row(caption), caption_actions)),
                context,
            ),
        )];
        // A failed organization or queue read says so; the rows keep the
        // group the daemon placed them in.
        for error in &input.errors {
            items.push(KeyedView::new(
                format!("organization-error:{error}"),
                nested_note(error, 0, Some(palette.attention())),
            ));
        }
        if let Some(empty) = input.empty {
            items.push(KeyedView::new("no-projects", nested_note(empty, 0, None)));
        }
        // A project row is never the selection: its home is New task's
        // (Main.dc.html), with the project named in the hero and the chip.
        for place in &input.places {
            let section = &place.section;
            items.push(place_item(place, input, context));
            if section.collapsed {
                continue;
            }
            // Queued launches say why they wait; nothing else under the
            // project but its tasks (owner review 2026-11-19: no team or
            // totals lines).
            for (index, line) in section.waits.iter().enumerate() {
                items.push(KeyedView::new(
                    format!("wait:{}:{index}:{line}", section.key),
                    nested_note(line, 1, None),
                ));
            }
            if place.tasks.is_empty() && section.waits.is_empty() {
                items.push(KeyedView::new(
                    format!("empty:{}", section.key),
                    nested_note(
                        if section.older > 0 {
                            "No recent tasks"
                        } else {
                            "No tasks yet"
                        },
                        1,
                        None,
                    ),
                ));
            }
            for task in &place.tasks {
                items.push(task_item(task, input, context));
            }
            if section.older > 0 {
                let label = if section.older_shown {
                    "Hide older".to_owned()
                } else {
                    format!("Show {} older", section.older)
                };
                items.push(KeyedView::new(
                    format!("older:{}:{}", section.key, section.older_shown),
                    m3::sub_row(palette, false)
                        .automation_name(format!("{label} in {}", section.name))
                        .on_click(
                            context.message(Message::Shell(ShellMsg::OlderToggled(
                                section.key.clone(),
                            ))),
                        )
                        .content(m3::task_content(1, &label, true, View::empty(), None)),
                ));
            }
        }
        // Show more pages the list shown: the server's matches while
        // searching, the recent history otherwise.
        if let Some((label, enabled)) = &input.more {
            let message = if filtered {
                ShellMsg::MoreSearchRequested
            } else {
                ShellMsg::MoreAgentsRequested
            };
            items.push(KeyedView::new(
                format!("more:{filtered}"),
                m3::sub_row(palette, false)
                    .is_enabled(*enabled)
                    .on_click(context.message(Message::Shell(message)))
                    .content(m3::task_content(1, label, true, View::empty(), None)),
            ));
        }
        let show_archived = input.show_archived;
        items.push(KeyedView::new(
            format!("archived:{show_archived}"),
            m3::row(palette, show_archived)
                .margin(Thickness::new(0.0, tokens::spacing::INSET, 0.0, 0.0))
                .automation_name(if show_archived {
                    "Hide archived (showing archived)"
                } else {
                    "Show archived"
                })
                .on_click(context.message(Message::Menu(MenuMsg::ArchivedToggled)))
                .content(m3::nav_content(
                    m3::glyph::ARCHIVE,
                    if show_archived {
                        "Hide archived"
                    } else {
                        "Show archived"
                    },
                    "",
                )),
        ));
        let list = ScrollViewer::new()
            .grid_row(2)
            .vertical_scroll_bar_visibility(ScrollBarVisibility::Auto)
            .automation_name("Projects and Agents")
            .content(
                StackPanel::new()
                    .spacing(gap)
                    .margin(Thickness::new(gutter, 0.0, gutter, gutter))
                    .keyed_children(items),
            );

        let bottom = StackPanel::new()
            .grid_row(3)
            .spacing(gap)
            .margin(Thickness::new(
                gutter,
                0.0,
                gutter,
                tokens::layout::SIDEBAR_PADDING,
            ))
            .keyed_children(vec![
                KeyedView::new("divider", m3::divider(palette)),
                KeyedView::new(
                    format!("inbox:{}", input.inbox.open),
                    Border::new()
                        .margin(Thickness::new(0.0, tokens::spacing::SM - gap, 0.0, 0.0))
                        .content(inbox_row(&input.inbox, context, palette)),
                ),
                KeyedView::new(
                    format!("fleet:{}", input.fleet.open),
                    Border::new()
                        .margin(Thickness::new(0.0, tokens::spacing::SM - gap, 0.0, 0.0))
                        .content(fleet_row(&input.fleet, context, palette)),
                ),
                KeyedView::new(
                    "settings",
                    m3::row(palette, false)
                        .automation_name("Settings")
                        .on_click(context.message(Message::Shell(ShellMsg::SettingsOpened)))
                        .content(m3::nav_content(m3::glyph::SETTINGS, "Settings", "")),
                ),
            ]);

        Grid::new()
            .rows([
                GridLength::Auto,
                GridLength::Auto,
                GridLength::STAR,
                GridLength::Auto,
            ])
            .children((header, nav, list, bottom))
    }
}

/// A project or subproject row with its right-click menu, or the rename
/// editor in its place.
fn place_item(
    place: &PlaceEntry,
    input: &SidebarInput,
    context: &mut ViewContext<Sidebar>,
) -> KeyedView {
    let section = &place.section;
    let renaming = input.renaming.as_ref().filter(
        |renaming| matches!(&renaming.target, Target::Project { key, .. } if *key == section.key),
    );
    let row = match renaming {
        _ if section.key == nav::OTHER_LOCATIONS => place_row(place, input, context),
        Some(renaming) => rename_row(renaming, 0.0, context, input.palette),
        None => with_menu(
            place_row(place, input, context),
            Target::Project {
                key: section.key.clone(),
                name: place.name.clone(),
                checkout: section.checkout.clone(),
                checkouts: section.checkouts.clone(),
                collapsed: section.collapsed,
            },
            context,
        ),
    };
    KeyedView::new(
        format!(
            "project:{}:{}:{}:{}:{}:{}:{}",
            section.key,
            section.collapsed,
            place.name,
            place.subproject.as_deref().unwrap_or_default(),
            section.running,
            section.missing,
            input.renaming.is_some()
        ),
        row,
    )
}

/// A project row (folder icon): its chevron and name collapse or expand
/// it; tasks working now (the server's team totals) show as a count on
/// the right, collapsed or not; a registered folder that is gone says
/// "missing" with a one-click Remove. The trailing button starts a new
/// task in it.
fn place_row(place: &PlaceEntry, input: &SidebarInput, context: &mut ViewContext<Sidebar>) -> View {
    let (section, palette, filtered) = (&place.section, input.palette, input.filtered);
    let name = place.name.as_str();
    // The full place, for the tooltip and the accessible name.
    let full = match &place.subproject {
        Some(subproject) => format!("{name} › {subproject}"),
        None => name.to_owned(),
    };
    let count: View = if section.missing {
        m3::secondary("missing")
            .foreground(palette.attention())
            .into()
    } else if section.running > 0 {
        m3::secondary(section.running.to_string()).into()
    } else {
        View::empty()
    };
    let kind = "project";
    let mut label = format!(
        "{full}, {kind}, {}",
        if section.collapsed {
            "collapsed"
        } else {
            "expanded"
        }
    );
    if section.running > 0 {
        label.push_str(&format!(", {} working", section.running));
    }
    if section.missing {
        label.push_str(", folder missing");
    }
    // The name opens the project page, its tasks in view; the few places
    // with no one checkout fold.
    let toggle = Message::Shell(ShellMsg::ProjectToggled(section.key.clone()));
    let opens = place.opens;
    let row = m3::row(palette, place.selected)
        .automation_name(if opens {
            format!("Open {label}")
        } else {
            label
        })
        .is_enabled(opens || !filtered)
        .on_click(context.message(if opens {
            Message::Shell(ShellMsg::ProjectOpened(
                section.checkout.clone(),
                section.key.clone(),
            ))
        } else {
            toggle.clone()
        }));
    let hover_key = format!("project:{}", section.key);
    let over = shows_actions(input, &hover_key);
    let mut actions: Vec<KeyedView> = Vec::new();
    if over {
        actions.push(KeyedView::new(
            "fold",
            m3::icon_button(palette)
                .is_enabled(!filtered)
                .automation_name(format!(
                    "{} {name}",
                    if section.collapsed {
                        "Expand"
                    } else {
                        "Collapse"
                    }
                ))
                .on_click(context.message(toggle))
                .content(m3::icon(if section.collapsed {
                    m3::glyph::CHEVRON_RIGHT
                } else {
                    m3::glyph::CHEVRON_DOWN
                })),
        ));
    }
    if section.missing {
        if over {
            actions.push(KeyedView::new(
                "remove",
                m3::icon_button(palette)
                    .is_enabled(place.registered.is_some() && input.bulk_idle)
                    .automation_name(format!("Remove missing {kind} {name}"))
                    .on_click(context.message(Message::Bulk(BulkMsg::Start(
                        super::bulk_ui::BulkOp::RemoveProjects,
                        super::bulk_ui::Pick::Missing {
                            project: place.registered.clone().unwrap_or_default(),
                            checkout: section.checkout.clone(),
                        },
                        name.to_owned(),
                    ))))
                    .content(m3::icon(m3::glyph::DELETE)),
            ));
        }
    } else if over && section.key != nav::OTHER_LOCATIONS {
        actions.push(KeyedView::new(
            "more",
            m3::icon_button(palette)
                .automation_id(format!(
                    "{}project:{}",
                    super::menu_ui::MORE_ID_PREFIX,
                    section.key
                ))
                .automation_name(format!("Project actions for {name}"))
                .on_click(
                    context.message(Message::Menu(MenuMsg::Opened(Target::Project {
                        key: section.key.clone(),
                        name: name.to_owned(),
                        checkout: section.checkout.clone(),
                        checkouts: section.checkouts.clone(),
                        collapsed: section.collapsed,
                    }))),
                )
                .content(m3::icon(m3::glyph::MORE))
                .tooltip("Project actions"),
        ));
    }
    let reserve = (tokens::height::CONTROL * actions.len() as f64 - tokens::spacing::ROW).max(0.0);
    let row = row
        .content(
            Border::new()
                .padding(Thickness::new(0.0, 0.0, reserve, 0.0))
                .content(m3::project_content(
                    name,
                    place.subproject.as_deref(),
                    count,
                )),
        )
        .tooltip(full);
    hover_area(hover_key, row_with_actions(row, actions), context)
}

/// One task or chat row: its right-click menu, or the rename editor.
fn task_item(
    task: &TaskEntry,
    input: &SidebarInput,
    context: &mut ViewContext<Sidebar>,
) -> KeyedView {
    let row = &task.row;
    let renaming = input.renaming.as_ref().filter(|renaming| {
        matches!(&renaming.target, Target::Agent(agent) | Target::Tags(agent) if agent.id == row.agent.id)
    });
    let view = match renaming {
        Some(renaming) => rename_row(
            renaming,
            row.depth as f64 * tokens::layout::TREE_INDENT + tokens::layout::TREE_INDENT,
            context,
            input.palette,
        ),
        None => with_menu(
            agent_row(task, input, context),
            Target::Agent(row.agent.clone()),
            context,
        ),
    };
    KeyedView::new(
        format!(
            "agent:{}:{}:{}:{}:{}:{}",
            row.agent.id,
            task.selected,
            row.expanded,
            task.label,
            row.agent.archived,
            input.renaming.is_some()
        ),
        view,
    )
}

/// A task row under its project: indented 28 DIPs per level, so its title
/// lines up with the project's label: the title and a state dot only for a
/// task that runs or needs attention, else its age. A running task adds
/// its work line under the title. A manager with workers shows its expand
/// chevron among the hover actions. Its accessible name says
/// "Harness · model" in full.
fn agent_row(task: &TaskEntry, input: &SidebarInput, context: &mut ViewContext<Sidebar>) -> View {
    let (row, palette, selected) = (&task.row, input.palette, task.selected);
    let agent = &row.agent;
    let title = task.label.as_str();
    let runs = crate::picker::harness_model_label(&agent.harness, agent.model.as_deref());
    let mut name = format!(
        "{title}, {runs}, {}, Agent {}",
        eliot_core::api::agent_state_name(agent.state),
        agent.id
    );
    if row.depth > 0 {
        name.push_str(&format!(", worker level {}", row.depth));
    }
    if let Some(note) = &row.note {
        name.push_str(&format!(", {note}"));
    }
    // The Monitor's incident report leads, then projected status and work.
    if let Some(words) = &task.reported {
        name.push_str(&format!(", needs attention: {words}"));
    } else if let Some(status) = &agent.status_line {
        name.push_str(&format!(", {status}"));
    } else if let Some((_, words)) = &task.work {
        name.push_str(&format!(", {words}"));
    }
    let line = task
        .reported
        .clone()
        .map(|words| (words, true))
        .or_else(|| {
            agent
                .status_line
                .as_ref()
                .map(|status| (status.clone(), agent.needs_attention()))
        })
        .or_else(|| task.work.as_ref().map(|(line, _)| (line.clone(), false)));
    // A live task shows its dot (working, attention: a reported one in the
    // attention tone); a quiet one its age, its title in the variant tone.
    let dot = if line.as_ref().is_some_and(|(_, attention)| *attention) {
        Some(m3::status_mark(
            palette,
            StatusTone::Attention,
            tokens::layout::DOT,
        ))
    } else {
        task_dot(palette, agent)
    };
    let quiet = dot.is_none();
    let trailing: View = dot.unwrap_or_else(|| m3::secondary(task.age.clone()).into());
    let hover_key = format!("agent:{}", agent.id);
    let over = selected || shows_actions(input, &hover_key);
    let mut actions: Vec<KeyedView> = Vec::new();
    if over && row.expandable() {
        actions.push(KeyedView::new(
            "workers",
            m3::icon_button(palette)
                .automation_name(if row.expanded {
                    format!("Collapse workers of {title}")
                } else {
                    format!(
                        "Show {} of {title}",
                        eliot_core::count_words(row.children as u64, "worker", "workers")
                    )
                })
                .on_click(context.message(Message::Shell(ShellMsg::ExpandToggled(
                    agent.id.clone(),
                    !row.expanded,
                ))))
                .content(m3::icon(if row.expanded {
                    m3::glyph::CHEVRON_DOWN
                } else {
                    m3::glyph::CHEVRON_RIGHT
                })),
        ));
    }
    // The design and Codex: a row's actions show under the pointer, with
    // keyboard focus in the row and on the open task, in place of its dot
    // or age; a right click, Shift+F10 or the context-menu key opens the
    // same menu at any time.
    if over {
        actions.push(KeyedView::new(
            "more",
            m3::icon_button(palette)
                .automation_id(format!(
                    "{}agent:{}",
                    super::menu_ui::MORE_ID_PREFIX,
                    agent.id
                ))
                .automation_name(format!("Task actions for {title}, Agent {}", agent.id))
                .on_click(
                    context.message(Message::Menu(MenuMsg::Opened(Target::Agent(agent.clone())))),
                )
                .content(m3::icon(m3::glyph::MORE))
                .tooltip("Task actions"),
        ));
    }
    let trailing: View = if over {
        Border::new()
            .width(tokens::height::CONTROL * actions.len() as f64 - tokens::spacing::ROW)
            .into()
    } else {
        trailing
    };
    let row_button = m3::sub_row(palette, selected);
    let row_button = if line.is_some() {
        row_button
            .height(tokens::height::TWO_LINE_ROW)
            .min_height(tokens::height::TWO_LINE_ROW)
    } else {
        row_button
    };
    let row_button = row_button
        .automation_name(name)
        .on_click(context.message(Message::Shell(ShellMsg::AgentOpened(agent.clone()))))
        .content(m3::task_content(
            row.depth + 1,
            title,
            quiet,
            trailing,
            line,
        ));
    hover_area(hover_key, row_with_actions(row_button, actions), context)
}

/// The Owner inbox in one row: its glyph, "Inbox", the per-kind counts
/// ("2 requests · 1 stuck", "Nothing waiting") and the total; it opens the
/// Inbox pane.
fn inbox_row(inbox: &InboxEntry, context: &mut ViewContext<Sidebar>, palette: Palette) -> View {
    let open = inbox.open;
    m3::row(palette, open)
        .automation_name(format!(
            "Inbox: {} ({})",
            inbox.counts,
            if open { "open" } else { "closed" }
        ))
        .on_click(context.message(Message::Shell(ShellMsg::PanelToggled(Panel::Inbox))))
        .content(
            Grid::new()
                .columns([GridLength::Auto, GridLength::STAR, GridLength::Auto])
                .column_spacing(tokens::spacing::ROW)
                .children((
                    Border::new()
                        .width(tokens::icon::ROW)
                        .vertical_alignment(VerticalAlignment::Center)
                        .content(m3::icon(m3::glyph::INBOX)),
                    StackPanel::new()
                        .grid_column(1)
                        .vertical_alignment(VerticalAlignment::Center)
                        .children((
                            TextBlock::new()
                                .text("Inbox")
                                .text_trimming(TextTrimming::CharacterEllipsis),
                            m3::secondary(inbox.counts.clone())
                                .text_trimming(TextTrimming::CharacterEllipsis),
                        )),
                    if inbox.total > 0 {
                        m3::badge(palette, &inbox.total.to_string())
                    } else {
                        View::empty()
                    },
                )),
        )
}

/// The fleet in one calm row ("2 working · 1 needs attention", "All
/// quiet"): its dot in the icon slot, "Monitor" at the end; it opens the
/// Monitor, and its name carries every count.
fn fleet_row(fleet: &FleetEntry, context: &mut ViewContext<Sidebar>, palette: Palette) -> View {
    let open = fleet.open;
    m3::row(palette, open)
        .automation_name(format!(
            "Monitor: {} ({})",
            fleet.summary,
            if open { "open" } else { "closed" }
        ))
        .on_click(context.message(Message::Shell(ShellMsg::PanelToggled(Panel::Monitor))))
        .content(
            Grid::new()
                .columns([GridLength::Auto, GridLength::STAR, GridLength::Auto])
                .column_spacing(tokens::spacing::ROW)
                .children((
                    Border::new()
                        .width(tokens::icon::ROW)
                        .vertical_alignment(VerticalAlignment::Center)
                        .content(m3::status_mark(palette, fleet.tone, tokens::layout::DOT)),
                    TextBlock::new()
                        .text(fleet.line.clone())
                        .grid_column(1)
                        .vertical_alignment(VerticalAlignment::Center)
                        .text_trimming(TextTrimming::CharacterEllipsis),
                    m3::secondary("Monitor")
                        .grid_column(2)
                        .vertical_alignment(VerticalAlignment::Center),
                )),
        )
}

/// The inline rename editor in place of a row's title, or the tag editor
/// for [`Target::Tags`].
fn rename_row(
    renaming: &Renaming,
    indent: f64,
    context: &mut ViewContext<Sidebar>,
    palette: Palette,
) -> View {
    if matches!(renaming.target, Target::Tags(_)) {
        return tags_row(renaming, indent, context, palette);
    }
    let (placeholder, field, save, cancel) = ("Name", "Rename", "Save name", "Cancel rename");
    Grid::new()
        .margin(Thickness::new(
            indent,
            tokens::spacing::XS,
            0.0,
            tokens::spacing::XS,
        ))
        .columns([GridLength::STAR, GridLength::Auto, GridLength::Auto])
        .column_spacing(tokens::spacing::XS)
        .children((
            m3::field()
                .element_ref(&renaming.field)
                .text(renaming.typed.clone())
                .placeholder_text(placeholder)
                .automation_name(field)
                .on_text_changed(
                    context.callback(|text| Message::Menu(MenuMsg::RenameChanged(text))),
                ),
            m3::icon_button(palette)
                .grid_column(1)
                .width(tokens::height::CONTROL)
                .height(tokens::height::CONTROL)
                .automation_name(save)
                .on_click(context.message(Message::Menu(MenuMsg::RenameCommitted)))
                .content(m3::icon(m3::glyph::CHECK)),
            m3::icon_button(palette)
                .grid_column(2)
                .width(tokens::height::CONTROL)
                .height(tokens::height::CONTROL)
                .automation_name(cancel)
                .on_click(context.message(Message::Menu(MenuMsg::RenameCancelled)))
                .content(m3::icon(m3::glyph::CLOSE)),
        ))
}

/// The inline tag editor in place of a row's title: one field per tag
/// with its remove button, an empty field for a new tag, then Save and
/// Cancel (Enter saves through the window's keyboard hook). Nothing is
/// split, so every existing tag round-trips as it is.
fn tags_row(
    renaming: &Renaming,
    indent: f64,
    context: &mut ViewContext<Sidebar>,
    palette: Palette,
) -> View {
    let fields = &renaming.tags;
    let blank = String::new();
    let mut rows: Vec<KeyedView> = fields
        .iter()
        .chain(std::iter::once(&blank))
        .enumerate()
        .map(|(index, tag)| {
            let new = index == fields.len();
            let mut field = m3::field()
                .text(tag.clone())
                .placeholder_text(if new { "New tag" } else { "Tag" })
                .automation_name(if new {
                    "New tag".to_owned()
                } else {
                    format!("Tag {}", index + 1)
                })
                .on_text_changed(
                    context.callback(move |text| Message::Menu(MenuMsg::TagChanged(index, text))),
                );
            if index == 0 {
                field = field.element_ref(&renaming.field);
            }
            let remove: View = if new {
                View::empty()
            } else {
                m3::icon_button(palette)
                    .grid_column(1)
                    .width(tokens::height::CONTROL)
                    .height(tokens::height::CONTROL)
                    .automation_name(format!("Remove tag {}", index + 1))
                    .on_click(context.message(Message::Menu(MenuMsg::TagRemoved(index))))
                    .content(m3::icon(m3::glyph::CLOSE))
            };
            KeyedView::new(
                format!("tag:{index}"),
                Grid::new()
                    .columns([GridLength::STAR, GridLength::Pixel(tokens::height::CONTROL)])
                    .column_spacing(tokens::spacing::XS)
                    .children((field, remove)),
            )
        })
        .collect();
    rows.push(KeyedView::new(
        "tag-actions",
        StackPanel::new()
            .orientation(Orientation::Horizontal)
            .horizontal_alignment(HorizontalAlignment::Right)
            .spacing(tokens::spacing::XS)
            .children((
                m3::icon_button(palette)
                    .width(tokens::height::CONTROL)
                    .height(tokens::height::CONTROL)
                    .automation_name("Save tags")
                    .on_click(context.message(Message::Menu(MenuMsg::RenameCommitted)))
                    .content(m3::icon(m3::glyph::CHECK)),
                m3::icon_button(palette)
                    .width(tokens::height::CONTROL)
                    .height(tokens::height::CONTROL)
                    .automation_name("Cancel tags")
                    .on_click(context.message(Message::Menu(MenuMsg::RenameCancelled)))
                    .content(m3::icon(m3::glyph::CLOSE)),
            )),
    ));
    StackPanel::new()
        .margin(Thickness::new(
            indent,
            tokens::spacing::XS,
            0.0,
            tokens::spacing::XS,
        ))
        .spacing(tokens::spacing::XS)
        .keyed_children(rows)
}

/// A row wrapped so its right press opens the menu for `target`.
fn with_menu(row: View, target: Target, context: &mut ViewContext<Sidebar>) -> View {
    Border::new()
        .on_pointer_pressed(context.callback(move |info: PointerEventInfo| {
            if info.is_right_button_pressed {
                Message::Menu(MenuMsg::Requested(
                    target.clone(),
                    info.window_x,
                    info.window_y,
                ))
            } else {
                Message::Menu(MenuMsg::Ignored)
            }
        }))
        .content(row)
}

/// A row's pointer area: entering it shows its actions (hover or
/// selection), leaving hides them. The row inside keeps its identity, so a
/// click that follows the hover is never lost.
/// A row's actions show under the pointer and with keyboard focus inside it.
fn shows_actions(input: &SidebarInput, key: &str) -> bool {
    input.hovered.as_deref() == Some(key) || input.focused.as_deref() == Some(key)
}

/// A row's pointer area; its automation id lets `crate::motion` find the
/// row that holds keyboard focus.
fn hover_area(key: String, view: impl Into<View>, context: &mut ViewContext<Sidebar>) -> View {
    Border::new()
        .automation_id(format!("{}{key}", crate::motion::ROW_ID_PREFIX))
        .on_pointer_entered(context.callback(move |_: PointerEventInfo| {
            Message::Menu(MenuMsg::Hovered(Some(key.clone())))
        }))
        .on_pointer_exited(
            context.callback(|_: PointerEventInfo| Message::Menu(MenuMsg::Hovered(None))),
        )
        .content(view)
}

/// A full-width row with its actions over its trailing end (the pill spans
/// the row; the '…' sits at one fixed place in every row).
fn row_with_actions(row: impl Into<View>, actions: Vec<KeyedView>) -> View {
    let mut cells = vec![KeyedView::new("row", row.into())];
    if !actions.is_empty() {
        cells.push(KeyedView::new(
            "actions",
            StackPanel::new()
                .orientation(Orientation::Horizontal)
                .horizontal_alignment(HorizontalAlignment::Right)
                .keyed_children(actions),
        ));
    }
    Grid::new().keyed_children(cells)
}

/// A quiet caption under a project (a queued launch's wait, "No tasks
/// yet"): where a task's title would start, `depth` steps in.
fn nested_note(text: &str, depth: usize, tone: Option<Brush>) -> View {
    let note = m3::secondary(text.to_owned())
        .text_wrapping(TextWrapping::Wrap)
        .vertical_alignment(VerticalAlignment::Center);
    let note = match tone {
        Some(tone) => note.foreground(tone),
        None => note,
    };
    Border::new()
        .min_height(tokens::height::CONTROL)
        .padding(Thickness::new(
            tokens::spacing::ROW + tokens::layout::TREE_INDENT * depth as f64,
            0.0,
            tokens::spacing::ROW,
            0.0,
        ))
        .content(note)
}

/// A task's state dot, only while it runs or needs attention: a calm list
/// carries no marks.
fn task_dot(palette: Palette, agent: &UiAgent) -> Option<View> {
    (agent.running() || agent.needs_attention()).then(|| m3::list_dot(palette, agent.state))
}
