//! The sidebar of the approved design (`Sidebar.dc.html`) as its own
//! component: the window computes its rows from the listed Agents
//! ([`DesktopApp::sidebar_input`]: `nav::sections`, the owner's names, the
//! Monitor's reports, each row's age and work line), and [`Sidebar`] draws
//! them. Reactor 0.100.0 re-renders the window for every message, but a
//! component whose input compares equal is skipped: a keystroke, a
//! conversation delivery or a Monitor read leaves the sidebar as it is,
//! and a sidebar refresh redraws the sidebar, not the conversation.

use super::menu_ui::{Action, Target};
use super::*;
use crate::nav::Row;
use crate::theme::winui::Palette;

/// The sidebar's input: everything it draws, compared by value. Ages and
/// work lines are read at the window's render to the minute, so an
/// unchanged list compares equal.
#[derive(Clone, PartialEq)]
pub(super) struct SidebarInput {
    pub(super) palette: Palette,
    pub(super) can_back: bool,
    pub(super) can_forward: bool,
    /// The Settings page shown, if any (its nav row is selected).
    pub(super) page: Option<settings_ui::Page>,
    /// Inbox, Agents or Automations shown in the main card, if any.
    pub(super) place: Option<super::page_ui::Place>,
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
    pub(super) agents: AgentsEntry,
    pub(super) inbox: InboxEntry,
    /// The footer's Usage row, built by the window: it carries the Usage
    /// popover's flyout while that is open (captureless callbacks only, so
    /// it compares equal from one render to the next).
    pub(super) usage: View,
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
    /// The Monitor's open report (it needs attention): its one word for
    /// the row's end and its words in full.
    pub(super) reported: Option<(&'static str, String)>,
    /// A running task's work line and its words for the accessible name.
    pub(super) work: Option<(String, String)>,
    /// How long ago it changed ("3m").
    pub(super) age: String,
}

/// The Agents row: the fleet's line (at most two counts, the most urgent
/// first: "2 working · 1 needs attention"), its counts in full and whether
/// the fleet view (the Monitor pane) is open.
#[derive(Clone, PartialEq)]
pub(super) struct AgentsEntry {
    pub(super) line: String,
    pub(super) summary: String,
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

/// The Projects label's "…": the bulk actions over every project (k142),
/// each after one confirmation, as typed menu actions.
pub(super) const BULK_MENU: [Action; 3] = [
    Action::ArchiveAllFinished,
    Action::DeleteAllStopped,
    Action::RemoveUnused,
];

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

    /// `Sidebar.dc.html` top to bottom, 2 DIPs apart inside an 8-DIP
    /// padding: the title row (the brand mark and "Eliot Swarm" for Home,
    /// Back, Forward, Hide); New task; the places Search, Inbox, Agents and
    /// Automations; the scrolling Projects (their rows and tasks, workers
    /// indented under their manager) and Chats; then, over a hairline, Show
    /// archived, Usage and Settings. Every row is a 32-DIP pill; a row's
    /// actions show on hover, keyboard focus or selection.
    fn view(&self, input: &SidebarInput, context: &mut ViewContext<Self>) -> View {
        let palette = input.palette;
        let gap = tokens::spacing::ROW_GAP;
        let header = title_row(input, context);

        // The nav rows. A selectable row is keyed by its selection: WinUI
        // reads a button's colors only when its template is applied.
        let mut nav: Vec<KeyedView> = vec![KeyedView::new(
            "new-task",
            new_task_button(palette, context),
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
                    .content(m3::sidebar_row(
                        m3::icon(m3::glyph::SEARCH),
                        "Search",
                        false,
                        0.0,
                        false,
                        None,
                        m3::trailing("Ctrl+K", false).into(),
                    )),
            ),
        });
        nav.push(inbox_row(&input.inbox, context, palette));
        nav.push(agents_row(&input.agents, context, palette));
        let automations = input.place == Some(super::page_ui::Place::Automations);
        nav.push(KeyedView::new(
            format!("automations:{automations}"),
            m3::row(palette, automations)
                .automation_name("Automations")
                .on_click(context.message(Message::Shell(ShellMsg::PlaceOpened(
                    super::page_ui::Place::Automations,
                ))))
                .content(m3::sidebar_row(
                    m3::nav_icon(m3::glyph::AUTOMATION, automations),
                    "Automations",
                    automations,
                    0.0,
                    false,
                    None,
                    View::empty(),
                )),
        ));
        let nav = StackPanel::new()
            .grid_row(1)
            .spacing(gap)
            .margin(Thickness::new(0.0, gap, 0.0, 0.0))
            .keyed_children(nav);

        let filtered = input.filtered;
        let mut items: Vec<KeyedView> = vec![projects_label(input, context)];
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
        // Projects first, then the no-project bucket as Chats.
        let (chats, projects): (Vec<&PlaceEntry>, Vec<&PlaceEntry>) = input
            .places
            .iter()
            .partition(|place| place.section.key == nav::OTHER_LOCATIONS);
        for place in projects {
            items.push(place_item(place, input, context));
            if place.section.collapsed {
                continue;
            }
            place_rows(place, input, context, &mut items);
        }
        for place in chats {
            items.push(KeyedView::new(
                "chats-label",
                m3::section_label_row("Chats", View::empty()),
            ));
            place_rows(place, input, context, &mut items);
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
                    .content(m3::sidebar_row(
                        mark_spacer(),
                        label,
                        false,
                        0.0,
                        false,
                        Some(palette.variant()),
                        View::empty(),
                    )),
            ));
        }
        let list = ScrollViewer::new()
            .grid_row(2)
            .margin(Thickness::new(0.0, gap, 0.0, 0.0))
            .vertical_scroll_bar_visibility(ScrollBarVisibility::Auto)
            .automation_name("Projects and Agents")
            .content(StackPanel::new().spacing(gap).keyed_children(items));

        let show_archived = input.show_archived;
        let footer = Border::new()
            .grid_row(3)
            .margin(Thickness::new(0.0, gap, 0.0, 0.0))
            .border_brush(palette.outline())
            .border_thickness(Thickness::new(0.0, tokens::layout::HAIRLINE, 0.0, 0.0))
            .padding(Thickness::new(0.0, tokens::spacing::SM, 0.0, 0.0))
            .content(StackPanel::new().spacing(gap).keyed_children(vec![
                KeyedView::new(
                    format!("archived:{show_archived}"),
                    m3::row(palette, show_archived)
                        .automation_name(if show_archived {
                            "Hide archived (showing archived)"
                        } else {
                            "Show archived"
                        })
                        .on_click(context.message(Message::Menu(MenuMsg::ArchivedToggled)))
                        .content(m3::sidebar_row(
                            m3::nav_icon(m3::glyph::INVENTORY, show_archived),
                            if show_archived {
                                "Hide archived"
                            } else {
                                "Show archived"
                            },
                            show_archived,
                            0.0,
                            false,
                            None,
                            View::empty(),
                        )),
                ),
                KeyedView::new("usage", input.usage.clone()),
                KeyedView::new(
                    "settings",
                    m3::row(palette, false)
                        .automation_name("Settings (Ctrl+,)")
                        .on_click(context.message(Message::Shell(ShellMsg::SettingsOpened)))
                        .content(m3::sidebar_row(
                            m3::icon(m3::glyph::SETTINGS),
                            "Settings",
                            false,
                            0.0,
                            false,
                            None,
                            View::empty(),
                        )),
                ),
            ]));

        Grid::new()
            .margin(Thickness::uniform(tokens::layout::SIDEBAR_GUTTER))
            .rows([
                GridLength::Auto,
                GridLength::Auto,
                GridLength::STAR,
                GridLength::Auto,
            ])
            .children((header, nav, list, footer))
    }
}

/// The title row (`.brandrow`): the brand mark and "Eliot Swarm" as one
/// Home button (the mark takes its `primary-text` content color, the name
/// stays `on-surface`), then Back, Forward and Hide sidebar.
fn title_row(input: &SidebarInput, context: &mut ViewContext<Sidebar>) -> View {
    let palette = input.palette;
    // The design's 8 between the buttons leaves "Eliot Swarm" too little of
    // the 248-DIP row; they keep the half step.
    let gap = tokens::spacing::SM;
    let buttons_gap = tokens::spacing::XS;
    let home = StackPanel::new()
        .orientation(Orientation::Horizontal)
        .spacing(gap)
        // The pill's end inset is the row's 4-DIP end padding.
        .margin(Thickness::new(
            0.0,
            0.0,
            tokens::spacing::XS - tokens::spacing::ROW,
            0.0,
        ))
        .children((
            m3::shape(crate::brand::SHAPE_MARK, tokens::layout::MARK),
            m3::text("Eliot Swarm", tokens::text::TITLE)
                .foreground(palette.text())
                .vertical_alignment(VerticalAlignment::Center),
        ));
    Grid::new()
        .height(tokens::height::TITLE_ROW)
        .margin(Thickness::new(0.0, 0.0, tokens::spacing::XS, 0.0))
        .columns([
            GridLength::STAR,
            GridLength::Auto,
            GridLength::Auto,
            GridLength::Auto,
        ])
        .column_spacing(buttons_gap)
        .children((
            m3::text_button(palette)
                .horizontal_alignment(HorizontalAlignment::Left)
                .automation_name("Eliot Swarm: home")
                .on_click(context.message(Message::Shell(ShellMsg::NewTask)))
                .content(home)
                .tooltip("Home"),
            m3::icon_button(palette)
                .grid_column(1)
                .is_enabled(input.can_back)
                .automation_name("Back (Alt+Left)")
                .on_click(context.message(Message::Shell(ShellMsg::Back)))
                .content(m3::icon(m3::glyph::BACK))
                .tooltip("Back"),
            m3::icon_button(palette)
                .grid_column(2)
                .is_enabled(input.can_forward)
                .automation_name("Forward (Alt+Right)")
                .on_click(context.message(Message::Shell(ShellMsg::Forward)))
                .content(m3::icon(m3::glyph::FORWARD))
                .tooltip("Forward"),
            m3::icon_button(palette)
                .grid_column(3)
                .automation_name("Hide sidebar (Ctrl+B)")
                .on_click(context.message(Message::Shell(ShellMsg::SidebarToggled)))
                .content(m3::icon(m3::glyph::HIDE_SIDEBAR))
                .tooltip("Hide sidebar"),
        ))
}

/// New task (`.newtask`): `primary-container`, 40 DIPs, the `add` icon,
/// the label emphasized, Ctrl+N at the shortcut opacity at its end, 16
/// DIPs in.
fn new_task_button(palette: Palette, context: &mut ViewContext<Sidebar>) -> View {
    let inset = tokens::spacing::MD - tokens::spacing::INSET;
    m3::new_task_button(palette)
        .automation_name("New task (Ctrl+N)")
        .on_click(context.message(Message::Shell(ShellMsg::NewTask)))
        .content(
            Grid::new()
                .margin(Thickness::xy(inset, 0.0))
                .columns([GridLength::Auto, GridLength::STAR, GridLength::Auto])
                .column_spacing(tokens::spacing::ROW)
                .children((
                    m3::icon(m3::glyph::NEW_TASK),
                    m3::text("New task", tokens::text::EMPHASIZED)
                        .grid_column(1)
                        .vertical_alignment(VerticalAlignment::Center),
                    m3::text("Ctrl+N", tokens::text::TRAILING)
                        .grid_column(2)
                        .opacity(tokens::state::SHORTCUT)
                        .vertical_alignment(VerticalAlignment::Center),
                )),
        )
}

/// The Projects label (`.lab`) with, on hover or keyboard focus, Add
/// project (+) and the bulk actions over every project (…); while a
/// search lists matches, "Matching tasks" and no actions.
fn projects_label(input: &SidebarInput, context: &mut ViewContext<Sidebar>) -> KeyedView {
    let palette = input.palette;
    let filtered = input.filtered;
    let caption = if filtered {
        "Matching tasks"
    } else {
        "Projects"
    };
    let caption_key = format!("caption:{caption}");
    let over = shows_actions(input, &caption_key);
    let actions: View = if filtered || !over {
        View::empty()
    } else {
        let items = super::menu_ui::menu_items(
            &BULK_MENU
                .iter()
                .map(|action| super::menu_ui::Entry::item(*action))
                .collect::<Vec<_>>(),
        );
        StackPanel::new()
            .orientation(Orientation::Horizontal)
            .children((
                m3::icon_button(palette)
                    .automation_name("Add project")
                    .on_click(context.message(Message::Shell(ShellMsg::AddProject)))
                    .content(m3::icon(m3::glyph::NEW_TASK))
                    .tooltip("Add project"),
                m3::icon_button(palette)
                    .automation_name("Actions for all projects")
                    .content(m3::icon(m3::glyph::MORE))
                    .menu(Menu::new(
                        items,
                        // Capture-free: one identity across renders.
                        context.callback(|label: String| {
                            Message::Bulk(BulkMsg::Menu(Action::from_label(&label)))
                        }),
                    )),
            ))
    };
    KeyedView::new(
        caption_key.clone(),
        hover_area(
            caption_key,
            m3::section_label_row(caption, actions),
            context,
        ),
    )
}

/// A project's (or the Chats bucket's) rows: its queued launches, its
/// tasks with their workers, "No tasks yet" and Show older.
fn place_rows(
    place: &PlaceEntry,
    input: &SidebarInput,
    context: &mut ViewContext<Sidebar>,
    items: &mut Vec<KeyedView>,
) {
    let (section, palette) = (&place.section, input.palette);
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
    // Queued launches and automation waits: the Queued row, why it waits
    // in its tooltip and accessible name.
    for (index, line) in section.waits.iter().enumerate() {
        items.push(KeyedView::new(
            format!("wait:{}:{index}:{line}", section.key),
            queued_row(line, palette),
        ));
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
                    context.message(Message::Shell(ShellMsg::OlderToggled(section.key.clone()))),
                )
                .content(m3::sidebar_row(
                    mark_spacer(),
                    &label,
                    false,
                    0.0,
                    false,
                    Some(palette.variant()),
                    View::empty(),
                )),
        ));
    }
}

/// An empty mark slot: a row without a mark keeps its title where the
/// task titles start.
fn mark_spacer() -> View {
    Border::new().width(tokens::layout::MARK_SLOT).into()
}

/// A queued launch or automation wait: the `schedule` icon, what waits,
/// "Queued", all in the variant tone (a row with no action, so a disabled
/// one); the reason and who can lift it in its tooltip and name.
fn queued_row(line: &str, palette: Palette) -> View {
    let (what, _) = line.split_once(": ").unwrap_or((line, ""));
    Border::new()
        .content(
            m3::row(palette, false)
                .is_enabled(false)
                .automation_name(line.to_owned())
                .content(m3::sidebar_row(
                    m3::row_mark(palette, m3::RowMark::Queued, "Queued"),
                    what,
                    false,
                    0.0,
                    false,
                    None,
                    m3::trailing("Queued", false).into(),
                )),
        )
        .tooltip(line.to_owned())
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
            "project:{}:{}:{}:{}:{}:{}:{}:{}",
            section.key,
            section.collapsed,
            place.name,
            place.subproject.as_deref().unwrap_or_default(),
            section.running,
            section.missing,
            place.selected,
            input.renaming.is_some()
        ),
        row,
    )
}

/// A project row (`Sidebar.dc.html`): its chevron (collapse or expand, a
/// button of its own on the row's icon), its name (the project page) and
/// the tasks working now on the right; a registered folder that is gone
/// says "missing" with a one-click Remove; "…" opens the project menu.
fn place_row(place: &PlaceEntry, input: &SidebarInput, context: &mut ViewContext<Sidebar>) -> View {
    let (section, palette, filtered) = (&place.section, input.palette, input.filtered);
    let name = place.name.as_str();
    let selected = place.selected;
    // The full place, for the tooltip and the accessible name.
    let full = match &place.subproject {
        Some(subproject) => format!("{name} › {subproject}"),
        None => name.to_owned(),
    };
    let label = match &place.subproject {
        Some(subproject) => format!("{name} › {subproject}"),
        None => name.to_owned(),
    };
    let kind = "project";
    let mut spoken = format!(
        "{full}, {kind}, {}",
        if section.collapsed {
            "collapsed"
        } else {
            "expanded"
        }
    );
    if section.running > 0 {
        spoken.push_str(&format!(", {} working", section.running));
    }
    if section.missing {
        spoken.push_str(", folder missing");
    }
    let hover_key = format!("project:{}", section.key);
    let over = shows_actions(input, &hover_key);
    let mut actions: Vec<KeyedView> = Vec::new();
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
    } else if over {
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
    let trailing: View = if !actions.is_empty() {
        Border::new()
            .width(tokens::height::CONTROL * actions.len() as f64 - tokens::spacing::SM)
            .into()
    } else if section.missing {
        m3::trailing("missing", selected)
            .foreground(palette.attention())
            .into()
    } else if section.running > 0 {
        m3::trailing(section.running.to_string(), selected).into()
    } else {
        View::empty()
    };
    // The name opens the project page, its tasks in view; a place with no
    // one checkout folds instead.
    let toggle = Message::Shell(ShellMsg::ProjectToggled(section.key.clone()));
    let opens = place.opens;
    let row = m3::row(palette, selected)
        .automation_name(if opens {
            format!("Open {spoken}")
        } else {
            spoken
        })
        .is_enabled(opens || !filtered)
        .on_click(context.message(if opens {
            Message::Shell(ShellMsg::ProjectOpened(
                section.checkout.clone(),
                section.key.clone(),
            ))
        } else {
            toggle.clone()
        }))
        .content(m3::sidebar_row(
            Border::new().width(tokens::icon::ROW).into(),
            &label,
            selected,
            0.0,
            false,
            None,
            trailing,
        ))
        .tooltip(full);
    // The chevron is the row's icon: its own button, centred on it.
    let chevron = m3::icon_button(palette)
        .resource_overrides(if selected {
            palette.row(true)
        } else {
            palette.icon_button()
        })
        .is_enabled(!filtered)
        .horizontal_alignment(HorizontalAlignment::Left)
        .margin(Thickness::new(
            tokens::spacing::ROW + tokens::icon::ROW / 2.0 - tokens::height::CONTROL / 2.0,
            0.0,
            0.0,
            0.0,
        ))
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
        }));
    let mut cells = vec![
        KeyedView::new("row", row),
        KeyedView::new(format!("chevron:{selected}"), chevron),
    ];
    if !actions.is_empty() {
        cells.push(KeyedView::new(
            "actions",
            StackPanel::new()
                .orientation(Orientation::Horizontal)
                .horizontal_alignment(HorizontalAlignment::Right)
                .keyed_children(actions),
        ));
    }
    hover_area(hover_key, Grid::new().keyed_children(cells), context)
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
            row.depth as f64 * tokens::layout::KID_INDENT,
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

/// A task's mark, its one word for the row's end (attention first) and
/// whether its title takes the variant tone (stopped), as the brand book's
/// status table says.
fn task_look(task: &TaskEntry) -> (m3::RowMark, Option<&'static str>, bool) {
    use eliot_core::AgentState;
    let agent = &task.row.agent;
    let held = agent.status_line.as_deref().is_some_and(|line| {
        line.starts_with("Provider hold") || line.starts_with("Waiting for the provider")
    });
    if let Some((word, _)) = task.reported {
        return (m3::RowMark::Waiting, Some(word), false);
    }
    match agent.state {
        AgentState::Created | AgentState::Starting | AgentState::Busy if held => {
            (m3::RowMark::Waiting, Some("Provider hold"), false)
        }
        AgentState::Created | AgentState::Starting | AgentState::Busy => {
            (m3::RowMark::Working, None, false)
        }
        _ if held => (m3::RowMark::Waiting, Some("Provider hold"), false),
        AgentState::Waiting => (m3::RowMark::Waiting, Some("Waiting"), false),
        AgentState::Idle => (m3::RowMark::Idle, None, false),
        AgentState::Failed => (m3::RowMark::Failed, Some("Failed"), false),
        AgentState::Lost => (m3::RowMark::Failed, Some("Lost"), false),
        AgentState::Unknown => (m3::RowMark::Failed, Some("Unknown"), false),
        AgentState::Stopped => (m3::RowMark::None, None, true),
    }
}

/// A task row (`.row`): its state mark, its title and, at the end, one
/// attention word, else its `+N −M`, else its age; workers indent one step
/// under their manager, their title at the kid size in the variant tone.
/// Hovering (or keyboard focus, or the open task) shows the workers'
/// chevron and "…" in place of the end. The tooltip carries the work line
/// or the Monitor's report; the accessible name says "Harness · model" in
/// full.
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
    let line = task
        .reported
        .as_ref()
        .map(|(_, words)| words.clone())
        .or_else(|| agent.status_line.clone())
        .or_else(|| task.work.as_ref().map(|(line, _)| line.clone()));
    if let Some(words) = task.reported.as_ref().map(|(_, words)| words) {
        name.push_str(&format!(", needs attention: {words}"));
    } else if let Some(status) = &agent.status_line {
        name.push_str(&format!(", {status}"));
    } else if let Some((_, words)) = &task.work {
        name.push_str(&format!(", {words}"));
    }
    let (mark, word, quiet) = task_look(task);
    let diff = agent
        .work
        .as_deref()
        .filter(|work| work.available)
        .and_then(|work| match (work.insertions, work.deletions) {
            (Some(add), Some(del)) if add > 0 || del > 0 => Some((add, del)),
            _ => None,
        });
    let end: View = match (word, diff) {
        (Some(word), _) => {
            let block = m3::trailing(word, selected);
            if mark == m3::RowMark::Failed && !selected {
                block.foreground(palette.attention()).into()
            } else {
                block.into()
            }
        }
        (None, Some((add, del))) => m3::diff_counts(add, del, selected),
        (None, None) => m3::trailing(task.age.clone(), selected).into(),
    };
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
    // keyboard focus in the row and on the open task, in place of its end;
    // a right click, Shift+F10 or the context-menu key opens the same menu
    // at any time.
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
    let end: View = if over {
        Border::new()
            .width(tokens::height::CONTROL * actions.len() as f64 - tokens::spacing::SM)
            .into()
    } else {
        end
    };
    let kid = row.depth > 0;
    let tone = (!selected && (quiet || kid)).then(|| palette.variant());
    let leading = m3::row_mark(palette, mark, word.unwrap_or("Failed"));
    let row_button = m3::sub_row(palette, selected)
        .automation_name(name)
        .on_click(context.message(Message::Shell(ShellMsg::AgentOpened(agent.clone()))))
        .content(m3::sidebar_row(
            leading,
            title,
            selected,
            row.depth as f64 * tokens::layout::KID_INDENT,
            kid,
            tone,
            end,
        ));
    let row_button: View = match line {
        Some(line) => row_button.tooltip(line),
        None => row_button,
    };
    hover_area(hover_key, row_with_actions(row_button, actions), context)
}

/// The Inbox row: the `inbox` icon, "Inbox" and the count badge; it opens
/// the Inbox pane. Its name carries the per-kind counts.
fn inbox_row(
    inbox: &InboxEntry,
    context: &mut ViewContext<Sidebar>,
    palette: Palette,
) -> KeyedView {
    let open = inbox.open;
    KeyedView::new(
        format!("inbox:{open}"),
        m3::row(palette, open)
            .automation_name(format!(
                "Inbox: {} ({})",
                inbox.counts,
                if open { "open" } else { "closed" }
            ))
            .on_click(context.message(Message::Shell(ShellMsg::PlaceOpened(
                super::page_ui::Place::Inbox,
            ))))
            .content(m3::sidebar_row(
                m3::nav_icon(m3::glyph::INBOX, open),
                "Inbox",
                open,
                0.0,
                false,
                None,
                if inbox.total > 0 {
                    m3::count_badge(palette, inbox.total)
                } else {
                    View::empty()
                },
            ))
            .tooltip(inbox.counts.clone()),
    )
}

/// The Agents row: the `hub` icon, "Agents" and the fleet's line ("3
/// working", as on main: the most urgent two counts); it opens the fleet
/// view (the Monitor pane). Its name carries every count.
fn agents_row(
    agents: &AgentsEntry,
    context: &mut ViewContext<Sidebar>,
    palette: Palette,
) -> KeyedView {
    let open = agents.open;
    KeyedView::new(
        format!("agents:{open}"),
        m3::row(palette, open)
            .automation_name(format!(
                "Agents: {} ({})",
                agents.summary,
                if open { "open" } else { "closed" }
            ))
            .on_click(context.message(Message::Shell(ShellMsg::PlaceOpened(
                super::page_ui::Place::Agents,
            ))))
            .content(m3::sidebar_row(
                m3::nav_icon(m3::glyph::HUB, open),
                "Agents",
                open,
                0.0,
                false,
                None,
                m3::trailing(agents.line.clone(), open)
                    .text_trimming(TextTrimming::CharacterEllipsis)
                    .into(),
            ))
            .tooltip(agents.summary.clone()),
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
                    false,
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
            tokens::spacing::ROW + tokens::layout::KID_INDENT * depth as f64,
            0.0,
            tokens::spacing::ROW,
            0.0,
        ))
        .content(note)
}
