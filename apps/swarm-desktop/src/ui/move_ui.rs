//! "Move under another task…" (k113): a task's ⋯ menu in the conversation
//! header and its right-click menu in the sidebar open one picker of the
//! tasks it can work under ([`reparent::eligible_parents`]: the same
//! project, running or ready), with a search box. Picking a task and
//! pressing Move is the one confirmation.
//!
//! The picker is drawn in the window, over a scrim, in the window's own
//! theme (the delete confirmation's pattern; a `ContentDialog` follows the
//! Windows theme). The move is the existing `swarm.reparent` with both
//! generations bound at the click; the daemon's checks decide, and a
//! refusal stays in the picker in words. An uncertain reply keeps the
//! ledger entry, so Move again sends the same mutation id.

use super::*;
use crate::theme::winui::Palette;

/// The picker list's height before it scrolls.
const LIST_HEIGHT: f64 = 320.0;
/// The picker's width.
const DIALOG_WIDTH: f64 = 480.0;

#[derive(Clone, Debug)]
pub(super) enum MoveMsg {
    /// Open the picker for this task, bound to the generation shown.
    Open(UiAgent),
    QueryChanged(String),
    /// A task was picked to work under (its id).
    Picked(String),
    /// Move: the one confirmation, bound to both shown generations.
    Confirmed(reparent::MoveBinding),
    Settled(reparent::MoveBinding, MutationResult<ReparentResponse>),
    Closed,
}

/// The open picker: the task to move, the search text, the picked task
/// and the last refusal in words.
#[derive(Clone, Debug)]
pub(super) struct MoveDialog {
    agent: UiAgent,
    query: String,
    picked: Option<String>,
    error: Option<String>,
}

impl DesktopApp {
    pub(super) fn move_update(&mut self, msg: MoveMsg, context: &ComponentContext<Self>) {
        match msg {
            MoveMsg::Open(agent) => {
                self.close_menu();
                self.menu.moving = Some(MoveDialog {
                    agent,
                    query: String::new(),
                    picked: None,
                    error: None,
                });
            }
            MoveMsg::QueryChanged(text) => {
                if let Some(dialog) = self.menu.moving.as_mut()
                    && dialog.query != text
                {
                    dialog.query = text;
                }
            }
            MoveMsg::Picked(id) => {
                if let Some(dialog) = self.menu.moving.as_mut() {
                    dialog.picked = Some(id);
                    dialog.error = None;
                }
            }
            MoveMsg::Confirmed(binding) => {
                if self.submitting() {
                    if let Some(dialog) = self.menu.moving.as_mut() {
                        dialog.error = Some("Waiting for the last action to settle.".to_owned());
                    }
                    return;
                }
                let Ok(data_dir) = self.data_dir.clone() else {
                    return;
                };
                let key = binding.key();
                let mutation_id = self.ledger.id_for(&key);
                self.in_flight_key = Some(key);
                self.set_status(format!("Move ({mutation_id})…"));
                if let Some(dialog) = self.menu.moving.as_mut() {
                    dialog.error = None;
                }
                let request = binding.request(mutation_id);
                let _task = context.spawn_background(move |_| {
                    let result = client::block_on(client::reparent(&data_dir, &request));
                    Message::Move(MoveMsg::Settled(binding, result))
                });
            }
            MoveMsg::Settled(binding, result) => {
                let uncertain = result.as_ref().err().is_some_and(crate::state::uncertain);
                self.settle_mutation(!uncertain);
                let source = self.agent_title(&binding.source);
                let parent = self.agent_title(&binding.parent);
                match result {
                    Ok(_) => {
                        self.menu.moving = None;
                        self.set_status(format!("Moved “{source}” under “{parent}”."));
                    }
                    Err(error) => {
                        let line = if uncertain {
                            format!(
                                "The move is not confirmed yet: {}. Move again sends the same request.",
                                error.user_message()
                            )
                        } else {
                            format!("Not moved: {}", error.user_message())
                        };
                        match self.menu.moving.as_mut() {
                            Some(dialog) => {
                                dialog.error = Some(line.clone());
                                self.set_status(line);
                            }
                            None => self.alert(line),
                        }
                    }
                }
                if self.panel != Panel::Closed {
                    self.spawn_monitor(context, 0);
                }
                // The sidebar's tree follows the new parent.
                self.shell_update(ShellMsg::RefreshRequested, context);
            }
            MoveMsg::Closed => self.menu.moving = None,
        }
    }

    /// The picker: what moves, the search box, the tasks it can work under
    /// (the picked one selected), what Move will do, and Cancel / Move.
    pub(super) fn move_dialog(
        &self,
        dialog: &MoveDialog,
        context: &mut ViewContext<Self>,
        palette: Palette,
    ) -> View {
        let title = self.agent_label(&dialog.agent);
        let eligible = reparent::eligible_parents(&self.nav.agents, &dialog.agent);
        let words: Vec<String> = dialog
            .query
            .split_whitespace()
            .map(str::to_lowercase)
            .collect();
        let matching: Vec<&UiAgent> = eligible
            .iter()
            .copied()
            .filter(|agent| {
                let haystack = format!(
                    "{} {} {}",
                    self.agent_label(agent),
                    agent.title,
                    crate::picker::harness_label(&agent.harness)
                )
                .to_lowercase();
                words.iter().all(|word| haystack.contains(word))
            })
            .collect();
        let picked = dialog
            .picked
            .as_deref()
            .and_then(|id| eligible.iter().copied().find(|agent| agent.id == id));
        let rows: Vec<KeyedView> = matching
            .iter()
            .map(|agent| {
                let selected = picked.is_some_and(|picked| picked.id == agent.id);
                let label = self.agent_label(agent);
                let mut note = format!(
                    "{} · {}",
                    crate::picker::harness_label(&agent.harness),
                    monitor::state_words(agent.state)
                );
                if let Some(parent) = &agent.parent {
                    note.push_str(&format!(" · works under “{}”", self.agent_title(parent)));
                }
                KeyedView::new(
                    format!("{}:{selected}", agent.id),
                    m3::row(palette, selected)
                        .height(tokens::height::TWO_LINE_ROW)
                        .min_height(tokens::height::TWO_LINE_ROW)
                        .automation_name(format!("{label}, {note}"))
                        .on_click(context.message(Message::Move(MoveMsg::Picked(agent.id.clone()))))
                        .content(
                            StackPanel::new()
                                .vertical_alignment(VerticalAlignment::Center)
                                .children((
                                    m3::body(label).text_trimming(TextTrimming::CharacterEllipsis),
                                    m3::secondary(note)
                                        .text_trimming(TextTrimming::CharacterEllipsis),
                                )),
                        ),
                )
            })
            .collect();
        let list: View = if eligible.is_empty() {
            m3::secondary(
                "No other task in this project is running or ready, so there is none to move it under.",
            )
            .text_wrapping(TextWrapping::Wrap)
            .into()
        } else if rows.is_empty() {
            m3::secondary(format!("No task matches “{}”.", dialog.query.trim()))
                .text_wrapping(TextWrapping::Wrap)
                .into()
        } else {
            ScrollViewer::new()
                .max_height(LIST_HEIGHT)
                .vertical_scroll_bar_visibility(ScrollBarVisibility::Auto)
                .automation_name("Tasks to move under")
                .content(StackPanel::new().keyed_children(rows))
        };
        let summary: View = match (&dialog.error, picked) {
            (Some(error), _) => m3::body(error.clone())
                .foreground(palette.error())
                .text_wrapping(TextWrapping::Wrap)
                .is_text_selection_enabled(true)
                .into(),
            (None, Some(parent)) => m3::secondary(format!(
                "“{title}” will work under “{}”. Its own workers move with it.",
                self.agent_label(parent)
            ))
            .text_wrapping(TextWrapping::Wrap)
            .into(),
            (None, None) => View::empty(),
        };
        let confirm: View = match picked {
            Some(parent) => m3::filled(palette)
                .is_enabled(!self.submitting())
                .automation_name(format!("Move under: {}", self.agent_label(parent)))
                .on_click(context.message(Message::Move(MoveMsg::Confirmed(
                    reparent::MoveBinding {
                        source: dialog.agent.id.clone(),
                        source_generation: dialog.agent.generation,
                        parent: parent.id.clone(),
                        parent_generation: parent.generation,
                    },
                ))))
                .content(m3::label(if self.submitting() {
                    "Moving…"
                } else {
                    "Move"
                })),
            None => m3::filled(palette)
                .is_enabled(false)
                .automation_name("Move (pick a task first)")
                .content(m3::label("Move")),
        };
        let heading = format!("Move “{title}” under another task");
        Grid::new().children((
            Border::new().background(palette.scrim()),
            m3::card_high(palette)
                .width(DIALOG_WIDTH)
                .horizontal_alignment(HorizontalAlignment::Center)
                .vertical_alignment(VerticalAlignment::Center)
                .automation_name(heading.clone())
                .content(
                    StackPanel::new().spacing(tokens::spacing::MD).children((
                        m3::section_title(heading).text_wrapping(TextWrapping::Wrap),
                        m3::secondary(
                            "Pick the task it should work under. It then shows under that task in the sidebar and the Monitor.",
                        )
                        .text_wrapping(TextWrapping::Wrap),
                        m3::field()
                            .text(dialog.query.clone())
                            .placeholder_text("Search tasks")
                            .automation_name("Search tasks to move under")
                            .on_text_changed(
                                context.callback(|text| Message::Move(MoveMsg::QueryChanged(text))),
                            ),
                        list,
                        summary,
                        StackPanel::new()
                            .orientation(Orientation::Horizontal)
                            .horizontal_alignment(HorizontalAlignment::Right)
                            .spacing(tokens::spacing::SM)
                            .children((
                                m3::text_button(palette)
                                    .automation_name("Cancel move")
                                    .on_click(context.message(Message::Move(MoveMsg::Closed)))
                                    .content(m3::label("Cancel")),
                                confirm,
                            )),
                    )),
                ),
        ))
    }
}
