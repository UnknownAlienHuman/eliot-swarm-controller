//! "Move under another task…" (k113): a task's ⋯ menu in the conversation
//! header and its right-click menu in the sidebar open one picker of the
//! tasks it can work under ([`reparent::eligible_parents`]: the same
//! project, or none for a chat, running or ready), with a search box. Picking a task and
//! pressing Move is the one confirmation. The picker reads its own
//! candidates ([`reparent::search_managers`], every page up to a bound),
//! not the sidebar's single page, and says when it left some unread.
//!
//! The picker uses the shared M3 dialog and native modal focus host.
//! The move is the existing `swarm.reparent` with both
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
    /// The candidates read for this task and generation.
    Searched(String, i64, Result<reparent::Managers, String>),
    /// A task was picked to work under (its id).
    Picked(String),
    /// Move: the one confirmation, bound to both shown generations.
    Confirmed(reparent::MoveBinding),
    Settled(reparent::MoveBinding, MutationResult<ReparentResponse>),
    Closed,
}

/// The open picker: the task to move, its candidates, the search text,
/// the picked task and the last refusal in words.
#[derive(Clone, Debug)]
pub(super) struct MoveDialog {
    agent: UiAgent,
    search: Search,
    query: String,
    picked: Option<String>,
    error: Option<String>,
}

/// The picker's candidates: being read, read, or the read's error in words.
#[derive(Clone, Debug)]
enum Search {
    Reading,
    Read(reparent::Managers),
    Failed(String),
}

impl MoveDialog {
    /// The candidate row with this id, once read.
    fn candidate(&self, id: &str) -> Option<&UiAgent> {
        match &self.search {
            Search::Read(managers) => managers.agents.iter().find(|agent| agent.id == id),
            _ => None,
        }
    }
}

impl DesktopApp {
    pub(super) fn move_update(&mut self, msg: MoveMsg, context: &ComponentContext<Self>) {
        match msg {
            MoveMsg::Open(agent) => {
                self.close_menu();
                self.spawn_move_search(&agent, context);
                self.menu.moving = Some(MoveDialog {
                    agent,
                    search: Search::Reading,
                    query: String::new(),
                    picked: None,
                    error: None,
                });
            }
            MoveMsg::Searched(source, generation, result) => {
                if let Some(dialog) = self.menu.moving.as_mut()
                    && dialog.agent.id == source
                    && dialog.agent.generation == generation
                {
                    dialog.search = match result {
                        Ok(managers) => Search::Read(managers),
                        Err(error) => Search::Failed(error),
                    };
                }
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
                let parent = match self
                    .menu
                    .moving
                    .as_ref()
                    .and_then(|dialog| dialog.candidate(&binding.parent))
                {
                    Some(agent) => self.agent_label(agent),
                    None => self.agent_title(&binding.parent),
                };
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
                                // A refusal may follow a moved generation or
                                // state: read the candidates again.
                                let agent = (!uncertain).then(|| dialog.agent.clone());
                                self.set_status(line);
                                if let Some(agent) = agent {
                                    self.spawn_move_search(&agent, context);
                                }
                            }
                            None => self.alert(line),
                        }
                    }
                }
                if self.panel != Panel::Closed {
                    self.start_monitor(context);
                }
                // The sidebar's tree follows the new parent.
                self.shell_update(ShellMsg::RefreshRequested, context);
            }
            MoveMsg::Closed => self.menu.moving = None,
        }
    }

    /// Read the tasks `agent` can move under on a worker; the reply lands
    /// only on the picker still open for that task and generation.
    fn spawn_move_search(&self, agent: &UiAgent, context: &ComponentContext<Self>) {
        let source = agent.id.clone();
        let generation = agent.generation;
        let project = reparent::search_project(agent).to_owned();
        let data_dir = self.data_dir.clone();
        let _task = context.spawn_background(move |_| {
            let result = match data_dir {
                Ok(data_dir) => client::block_on(reparent::search_managers(
                    &project,
                    reparent::SEARCH_PAGE,
                    reparent::SEARCH_BOUND,
                    |request| {
                        let data_dir = data_dir.clone();
                        async move {
                            let reply = client::list_agents(&data_dir, &request).await?;
                            Ok(reparent::Page {
                                rows: reply
                                    .agents
                                    .iter()
                                    .map(|row| UiAgent::from_summary(&row.summary))
                                    .collect(),
                                next: reply.next,
                                total: reply.total,
                            })
                        }
                    },
                ))
                .map_err(|error: eliot_core::Error| error.user_message()),
                Err(error) => Err(error),
            };
            Message::Move(MoveMsg::Searched(source, generation, result))
        });
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
        let (candidates, unread): (&[UiAgent], usize) = match &dialog.search {
            Search::Read(managers) => (&managers.agents, managers.unread),
            _ => (&[], 0),
        };
        let eligible = reparent::eligible_parents(candidates, &self.nav.agents, &dialog.agent);
        let scope = reparent::Scope::of(&dialog.agent.project_id);
        // A parent past the sidebar's page is named from the candidates.
        let title_of = |id: &str| {
            dialog
                .candidate(id)
                .map_or_else(|| self.agent_title(id), |agent| self.agent_label(agent))
        };
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
                    note.push_str(&format!(" · works under “{}”", title_of(parent)));
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
        let list: View = match &dialog.search {
            Search::Reading => {
                m3::secondary(format!("Reading running or ready tasks {}…", scope.read())).into()
            }
            Search::Failed(error) => m3::body(format!(
                "Could not read the tasks {}: {error}",
                scope.read()
            ))
            .foreground(palette.error())
            .text_wrapping(TextWrapping::Wrap)
            .is_text_selection_enabled(true)
            .into(),
            Search::Read(_) if eligible.is_empty() && unread == 0 => m3::secondary(format!(
                "No other {} is running or ready, so there is none to move it under.",
                scope.offered()
            ))
            .text_wrapping(TextWrapping::Wrap)
            .into(),
            Search::Read(_) if eligible.is_empty() => {
                m3::secondary("None of the tasks read can take it.")
                    .text_wrapping(TextWrapping::Wrap)
                    .into()
            }
            Search::Read(_) if rows.is_empty() => {
                m3::secondary(format!("No task matches “{}”.", dialog.query.trim()))
                    .text_wrapping(TextWrapping::Wrap)
                    .into()
            }
            Search::Read(_) => ScrollViewer::new()
                .max_height(LIST_HEIGHT)
                .vertical_scroll_bar_visibility(ScrollBarVisibility::Auto)
                .automation_name("Tasks to move under")
                .content(StackPanel::new().keyed_children(rows)),
        };
        // The search is bounded: say what it left unread.
        let incomplete: View =
            match reparent::unread_words(&dialog.agent.project_id, candidates.len(), unread) {
                Some(words) => m3::secondary(words)
                    .text_wrapping(TextWrapping::Wrap)
                    .into(),
                None => View::empty(),
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
                .element_ref(&self.dialog.primary)
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
        super::dialog_ui::surface(
            context,
            self.dialog.epoch,
            palette,
            heading,
            StackPanel::new().spacing(tokens::spacing::MD).children((
                m3::secondary(format!(
                    "Pick a running or ready {}. Its workers move with it.",
                    scope.offered()
                ))
                .text_wrapping(TextWrapping::Wrap),
                m3::field()
                    .text(dialog.query.clone())
                    .placeholder_text("Search tasks")
                    .automation_name("Search tasks to move under")
                    .on_text_changed(
                        context.callback(|text| Message::Move(MoveMsg::QueryChanged(text))),
                    ),
                list,
                incomplete,
                summary,
            )),
            super::dialog_ui::actions(
                m3::text_button(palette)
                    .element_ref(&self.dialog.close)
                    .automation_name("Cancel move")
                    .on_click(context.message(Message::Move(MoveMsg::Closed)))
                    .content(m3::label("Cancel")),
                confirm,
            ),
            DIALOG_WIDTH,
        )
    }
}
