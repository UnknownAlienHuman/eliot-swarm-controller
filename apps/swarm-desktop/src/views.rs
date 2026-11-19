//! WinUI views for the retained conversation.
//!
//! Native controls only: `RichTextBlock` for Markdown paragraphs (runs carry
//! bold/italic, links carry only validated http/https URIs), separate
//! `TextBlock`s for headings and code, real `Button`/`ToggleButton` controls
//! for Copy and the collapsed tool groups. `RichText` holds an `Rc`, so it is
//! built here on the UI thread from the owned blocks a worker parsed, and
//! cached per part key + content revision: an unchanged part hands Reactor
//! the same `RichText`, so its native block is not recreated and a selection
//! inside it survives updates to other parts.

use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use windows_reactor::*;

use crate::chat::{self, Part, PartKind, TurnItem, text_hash};
use crate::markdown::{Block, Doc, Inline};
use crate::theme;
use crate::theme::winui::Palette;
use crate::ui::tokens;

/// One display block, ready for the UI thread.
#[derive(Clone)]
pub enum RenderedBlock {
    Rich(RichText),
    /// Consecutive list item paragraphs: (depth, marker, text) each.
    List(Vec<(usize, String, RichText)>),
    Heading(u8, String),
    Code(usize, String),
    Rule,
}

/// One rendered part; immutable once built, replaced on a new revision.
pub struct RenderedPart {
    pub key: String,
    pub kind: PartKind,
    pub label: String,
    pub plain: String,
    pub blocks: Option<Vec<RenderedBlock>>,
    pub detail: Vec<String>,
    pub file_paths: Vec<String>,
    pub detail_paths: Vec<String>,
    /// Files the owner attached (an input's closing block).
    pub attachments: Vec<String>,
}

/// One virtualized turn item.
pub struct RenderedItem {
    pub key: String,
    pub parts: Vec<Rc<RenderedPart>>,
}

/// Bounded cache of rendered parts keyed by (part key, revision); rebuilt to
/// hold exactly the resident parts on every change.
#[derive(Default)]
pub struct RenderCache {
    parts: HashMap<(String, u64), Rc<RenderedPart>>,
}

fn rich_inlines(inlines: &[Inline]) -> Vec<RichTextInline> {
    inlines
        .iter()
        .map(|inline| match inline {
            Inline::Text { text, bold, italic } => RichTextInline::Run(RichTextRun {
                text: text.clone(),
                is_bold: *bold,
                is_italic: *italic,
            }),
            Inline::Link { text, url } => RichTextInline::Hyperlink(RichTextHyperlink {
                text: text.clone(),
                uri: url.clone(),
            }),
            Inline::Break => RichTextInline::LineBreak,
        })
        .collect()
}

/// The display blocks of one document: each paragraph its own text block
/// (the part's spacing sets paragraphs apart, as Codex does), consecutive
/// list items one list.
fn render_doc(doc: &Doc) -> Vec<RenderedBlock> {
    let mut blocks = Vec::new();
    let mut code_index = 0;
    for block in &doc.blocks {
        match block {
            Block::Paragraph(inlines) => blocks.push(RenderedBlock::Rich(
                RichText::single_paragraph(rich_inlines(inlines)),
            )),
            Block::Item {
                depth,
                marker,
                inlines,
                opens,
            } => {
                let item = (
                    *depth,
                    marker.clone(),
                    RichText::single_paragraph(rich_inlines(inlines)),
                );
                match blocks.last_mut() {
                    Some(RenderedBlock::List(items)) if !opens => items.push(item),
                    _ => blocks.push(RenderedBlock::List(vec![item])),
                }
            }
            Block::Heading { level, text } => {
                blocks.push(RenderedBlock::Heading(*level, text.clone()));
            }
            Block::Code { text, .. } => {
                blocks.push(RenderedBlock::Code(code_index, text.clone()));
                code_index += 1;
            }
            Block::Rule => blocks.push(RenderedBlock::Rule),
        }
    }
    blocks
}

/// One selectable Markdown text block in the body role.
fn rich_block(text: &RichText) -> RichTextBlock {
    RichTextBlock::new()
        .paragraphs(text.clone())
        .font_size(tokens::text::BODY.size)
        .text_wrapping(TextWrapping::Wrap)
        .is_text_selection_enabled(true)
}

/// A list: one row per item paragraph, its marker in a fixed column and
/// its text wrapping under itself (a hanging indent), each level one step
/// in.
fn list_view(items: &[(usize, String, RichText)]) -> View {
    StackPanel::new()
        .spacing(tokens::spacing::XS)
        .keyed_children(
            items
                .iter()
                .enumerate()
                .map(|(index, (depth, marker, text))| {
                    KeyedView::new(
                        index,
                        Grid::new()
                            .margin(Thickness::new(
                                tokens::layout::LIST_INDENT * (*depth as f64 + 0.5),
                                0.0,
                                0.0,
                                0.0,
                            ))
                            .columns([
                                GridLength::Pixel(tokens::layout::LIST_MARKER_WIDTH),
                                GridLength::STAR,
                            ])
                            .children((
                                TextBlock::new()
                                    .text(marker.clone())
                                    .font_size(tokens::text::BODY.size),
                                rich_block(text).grid_column(1),
                            )),
                    )
                }),
        )
}

fn render_part(part: &Part, docs: &HashMap<u64, Doc>) -> RenderedPart {
    let markdown = matches!(part.kind, PartKind::Final | PartKind::Progress);
    let doc = markdown.then(|| docs.get(&text_hash(&part.text))).flatten();
    // The owner's message shows its attachments as chips, not as text.
    let (shown, attachments) = if part.kind == PartKind::Input {
        crate::attachments::split(&part.text)
    } else {
        (part.text.as_str(), Vec::new())
    };
    RenderedPart {
        key: part.key.clone(),
        kind: part.kind,
        label: part.label.clone(),
        attachments,
        plain: chat::preview(shown),
        blocks: doc.map(render_doc),
        detail: part.detail.clone(),
        file_paths: chat::file_references(&part.text),
        detail_paths: chat::file_references(&part.detail.join("\n")),
    }
}

impl RenderCache {
    /// Render the resident items, reusing every unchanged part.
    pub fn render(
        &mut self,
        items: &[TurnItem],
        docs: &HashMap<u64, Doc>,
    ) -> Vec<Rc<RenderedItem>> {
        let mut next = HashMap::with_capacity(self.parts.len());
        let rendered = items
            .iter()
            .map(|item| {
                let parts = item
                    .parts
                    .iter()
                    .map(|part| {
                        let id = (part.key.clone(), part.revision);
                        let reused = self.parts.get(&id).filter(|cached| {
                            // A Markdown doc that arrived after a plain fallback re-renders.
                            cached.blocks.is_some()
                                || !matches!(part.kind, PartKind::Final | PartKind::Progress)
                                || !docs.contains_key(&text_hash(&part.text))
                        });
                        let rendered = match reused {
                            Some(cached) => Rc::clone(cached),
                            None => Rc::new(render_part(part, docs)),
                        };
                        next.insert(id, Rc::clone(&rendered));
                        rendered
                    })
                    .collect();
                Rc::new(RenderedItem {
                    key: item.key.clone(),
                    parts,
                })
            })
            .collect();
        self.parts = next;
        rendered
    }
}

/// What every part is drawn with: the callbacks (each made from a
/// captureless mapper, so equal across renders) and the path context.
#[derive(Clone, PartialEq)]
pub struct RowCallbacks {
    pub copy_part: Callback<String>,
    pub open_path: Callback<(String, String)>,
    pub path_scope: String,
    pub path_failure: Option<(String, String)>,
    pub workspace: String,
    /// Retry a local input that did not start (the failure part's key).
    pub retry: Callback<String>,
    pub pick_model: Option<Callback<()>>,
    pub copy_code: Callback<(String, usize)>,
    pub toggle_tools: Callback<(String, bool)>,
}

fn label(text: &str) -> TextBlock {
    crate::m3::secondary(text.to_owned())
}

/// Message text: body.
fn body_text(text: &str, foreground: Brush) -> TextBlock {
    TextBlock::new()
        .text(text.to_owned())
        .font_size(tokens::text::BODY.size)
        .text_wrapping(TextWrapping::Wrap)
        .is_text_selection_enabled(true)
        .foreground(foreground)
}

/// An inline icon button is pulled back by this so its glyph lines up
/// with the text around it (a 32-DIP icon button centres a 16-DIP glyph).
const INSET: f64 = (tokens::height::CONTROL - tokens::icon::ROW) / 2.0;

/// Copy for `part`: an icon button while the part is under the pointer;
/// otherwise the same place holds a keyboard stop whose glyph shows only
/// with keyboard focus ([`crate::m3::reveal_button`]). Never a permanently
/// visible icon.
fn copy_button(name: &str, hovered: bool, palette: Palette, callback: impl Fn() + 'static) -> View {
    if hovered {
        crate::m3::icon_button(palette)
            .automation_name(name.to_owned())
            .on_click(callback)
            .content(copy_glyph())
    } else {
        crate::m3::reveal_button()
            .automation_name(name.to_owned())
            .on_click(callback)
            .content(copy_glyph())
    }
}

/// The glyph of a copy button.
fn copy_glyph() -> View {
    crate::m3::icon(crate::m3::glyph::COPY)
}

fn block_views(
    part: &RenderedPart,
    callbacks: &RowCallbacks,
    hovered: bool,
    palette: Palette,
) -> Vec<KeyedView> {
    let Some(blocks) = part.blocks.as_ref() else {
        return vec![KeyedView::new(
            "plain",
            body_text(&part.plain, palette.text()),
        )];
    };
    blocks
        .iter()
        .enumerate()
        .map(|(index, block)| {
            let view: View = match block {
                RenderedBlock::Rich(text) => rich_block(text).into(),
                RenderedBlock::List(items) => list_view(items),
                RenderedBlock::Heading(level, text) => crate::m3::text(
                    text.clone(),
                    match level {
                        1 => theme::typography::TITLE,
                        _ => theme::typography::HEADER,
                    },
                )
                .text_wrapping(TextWrapping::Wrap)
                .is_text_selection_enabled(true)
                .automation_heading_level(match level {
                    1 => AutomationHeadingLevel::Level1,
                    2 => AutomationHeadingLevel::Level2,
                    _ => AutomationHeadingLevel::Level3,
                })
                .into(),
                RenderedBlock::Code(code_index, text) => {
                    let copy = callbacks.copy_code.clone();
                    let key = part.key.clone();
                    let code_index = *code_index;
                    StackPanel::new().spacing(tokens::spacing::XS).children((
                        Border::new()
                            .background(palette.composer())
                            .corner_radius(tokens::radius::CONTAINER)
                            .padding(Thickness::uniform(tokens::spacing::MD))
                            .content(
                                ScrollViewer::new()
                                    .horizontal_scroll_bar_visibility(ScrollBarVisibility::Auto)
                                    .vertical_scroll_bar_visibility(ScrollBarVisibility::Disabled)
                                    .content(
                                        TextBlock::new()
                                            .text(text.clone())
                                            .font_size(tokens::text::BODY.size)
                                            .text_wrapping(TextWrapping::NoWrap)
                                            .is_text_selection_enabled(true),
                                    ),
                            ),
                        Border::new()
                            .horizontal_alignment(HorizontalAlignment::Left)
                            .margin(Thickness::new(-INSET, 0.0, 0.0, 0.0))
                            .content(copy_button("Copy code", hovered, palette, move || {
                                let _ = copy.call((key.clone(), code_index));
                            })),
                    ))
                }
                RenderedBlock::Rule => crate::m3::divider(palette).into(),
            };
            KeyedView::new(index, view)
        })
        .collect()
}

/// One style for every part, as in a chat: the user's message is a
/// secondary-container bubble on the right; the answer is plain body
/// text on the surface; tool groups are one low-emphasis expandable row;
/// interim notes and notices are quiet lines; copy is a small icon button.
fn part_view(
    part: &RenderedPart,
    expanded: bool,
    hovered: bool,
    callbacks: &RowCallbacks,
    palette: Palette,
) -> View {
    let copy = callbacks.copy_part.clone();
    let key = part.key.clone();
    let copy_message = move || {
        let _ = copy.call(key.clone());
    };
    match part.kind {
        PartKind::Input => StackPanel::new()
            .spacing(tokens::spacing::XS)
            .horizontal_alignment(HorizontalAlignment::Right)
            .max_width(tokens::layout::READING_WIDTH * tokens::layout::BUBBLE_SHARE)
            .children((
                crate::m3::bubble(palette, body_text(&part.plain, palette.text()), None),
                if part.attachments.is_empty() {
                    View::empty()
                } else {
                    attached_chips(&part.attachments, palette)
                },
                Border::new()
                    .horizontal_alignment(HorizontalAlignment::Right)
                    .margin(Thickness::new(0.0, 0.0, -INSET, 0.0))
                    .content(copy_button("Copy message", hovered, palette, copy_message)),
            )),
        // A steered message stays in the turn it steered.
        PartKind::Steer => Border::new()
            .horizontal_alignment(HorizontalAlignment::Right)
            .max_width(tokens::layout::READING_WIDTH * tokens::layout::BUBBLE_SHARE)
            .automation_name(format!("Steered: {}", part.plain))
            .content(crate::m3::steer_bubble(palette, &part.plain, None)),
        // The assistant's interim notes read as its text.
        PartKind::Progress => StackPanel::new()
            .spacing(tokens::spacing::SM)
            .keyed_children(block_views(part, callbacks, hovered, palette)),
        PartKind::Tools => {
            let toggle = callbacks.toggle_tools.clone();
            let key = part.key.clone();
            // Name, status, duration and start (`chat::TOOL_FIELDS`
            // apart): the name, then the status with its elapsed seconds
            // while it runs ("Running · 12 s", in the primary role) or the
            // duration at the end.
            let mut fields = part.plain.split(crate::chat::TOOL_FIELDS);
            let name = fields.next().unwrap_or_default().to_owned();
            let status = fields.next().unwrap_or_default().to_owned();
            let duration = fields.next().unwrap_or_default().to_owned();
            let started = fields.next().and_then(|ms| ms.parse::<i64>().ok());
            let running = status == "Running";
            // A running row counts its seconds in its own component
            // ([`Elapsed`]): the tick redraws that caption only.
            let trailing: View = match (status.as_str(), started) {
                ("Running", Some(started_ms)) => View::component::<Elapsed>(ElapsedInput {
                    started_ms,
                    palette,
                }),
                ("Running", None) => {
                    crate::m3::tool_caption(status.clone(), Some(palette.primary())).into()
                }
                ("Completed", _) => crate::m3::tool_caption(duration.clone(), None).into(),
                _ => crate::m3::tool_caption(status.clone(), None).into(),
            };
            let glyph = match part.label.as_str() {
                "Command" => crate::m3::glyph::TERMINAL,
                "Edit" => crate::m3::glyph::EDIT,
                _ => crate::m3::glyph::TOOLS,
            };
            let row = crate::m3::tool_row(palette, running)
                .automation_name(format!(
                    "{name} · {} ({})",
                    if running {
                        status.clone()
                    } else {
                        format!("{status} · {duration}")
                    },
                    if expanded { "expanded" } else { "collapsed" }
                ))
                .on_click(move || {
                    let _ = toggle.call((key.clone(), !expanded));
                })
                .content(crate::m3::tool_content(glyph, &name, trailing));
            let detail: View = if expanded {
                StackPanel::new()
                    .spacing(tokens::spacing::XS)
                    .margin(Thickness::xy(tokens::spacing::INSET, 0.0))
                    .children((
                        crate::m3::secondary(part.detail.join("\n"))
                            .text_wrapping(TextWrapping::Wrap)
                            .is_text_selection_enabled(true),
                        path_actions(&part.key, &part.detail_paths, callbacks, palette),
                    ))
            } else {
                View::empty()
            };
            StackPanel::new()
                .spacing(tokens::spacing::XS)
                .children((row, detail))
        }

        // The answer: plain text on the surface; a label only when the
        // outcome is not a plain success.
        PartKind::Final => {
            let note: View = if part.label == "Assistant" {
                View::empty()
            } else {
                label(&part.label).into()
            };
            StackPanel::new().spacing(tokens::spacing::SM).children((
                note,
                StackPanel::new()
                    .spacing(tokens::spacing::SM)
                    .keyed_children(block_views(part, callbacks, hovered, palette)),
                Border::new()
                    .horizontal_alignment(HorizontalAlignment::Left)
                    .margin(Thickness::new(-INSET, 0.0, 0.0, 0.0))
                    .content(copy_button("Copy message", hovered, palette, copy_message)),
            ))
        }
        PartKind::Working => StackPanel::new().spacing(tokens::spacing::XS).children((
            label(&part.label),
            ProgressBar::new()
                .is_indeterminate(true)
                .width(tokens::layout::PROGRESS_WIDTH)
                .horizontal_alignment(HorizontalAlignment::Left),
        )),
        // A start that did not happen: why (`swarm why`), its origin, and
        // Retry when the failure is definite.
        PartKind::Failed | PartKind::TurnFailed | PartKind::Unconfirmed => {
            let action: View = if part.kind == PartKind::Failed {
                let retry = callbacks.retry.clone();
                let key = part.key.clone();
                crate::m3::outlined(palette)
                    .horizontal_alignment(HorizontalAlignment::Left)
                    .automation_name("Retry")
                    .on_click(move || {
                        let _ = retry.call(key.clone());
                    })
                    .content(crate::m3::label("Retry"))
            } else if let (PartKind::TurnFailed, Some(pick_model)) =
                (part.kind, callbacks.pick_model.clone())
            {
                crate::m3::outlined(palette)
                    .horizontal_alignment(HorizontalAlignment::Left)
                    .automation_name("Pick another model")
                    .on_click(move || {
                        let _ = pick_model.call(());
                    })
                    .content(crate::m3::label("Pick another model"))
            } else {
                View::empty()
            };
            StackPanel::new().spacing(tokens::spacing::XS).children((
                TextBlock::new()
                    .text(format!("{} · {}", part.label, part.plain))
                    .font_size(tokens::text::BODY.size)
                    .text_wrapping(TextWrapping::Wrap)
                    .is_text_selection_enabled(true)
                    .foreground(palette.attention()),
                label(&part.detail.join(" · ")),
                action,
            ))
        }
        PartKind::Notice => crate::m3::secondary(format!("{} · {}", part.label, part.plain))
            .text_wrapping(TextWrapping::Wrap)
            .into(),
    }
}

/// An `Rc` compared by identity: a component input whose content is rebuilt
/// only on a real change ([`RenderCache`]) compares in O(1).
pub struct Shared<T>(pub Rc<T>);

impl<T> Clone for Shared<T> {
    fn clone(&self) -> Self {
        Self(Rc::clone(&self.0))
    }
}

impl<T> PartialEq for Shared<T> {
    fn eq(&self, other: &Self) -> bool {
        Rc::ptr_eq(&self.0, &other.0)
    }
}

/// Parts built in the first frame of a newly shown conversation: enough to
/// fill the window at its end, where the anchored transcript opens.
const FIRST_PARTS: usize = 30;
/// Older parts added per later frame until every resident part is built.
const GROW_PARTS: usize = 40;
/// Parts of a conversation just left removed per later frame: Reactor
/// 0.100.0 tears a subtree down node by node, and 588 parts at once held
/// the window for 1.7 s.
const DRAIN_PARTS: usize = 10;
/// Between two of those frames: input and painting run in between.
const STEP_DELAY_MS: u64 = 15;

/// The transcript's input: the resident turns (rebuilt only when the
/// conversation changes) and what they are drawn with. A window render for
/// anything else (the sidebar refresh, a Monitor tick, typing) compares
/// equal and leaves the transcript as it is.
#[derive(Clone, PartialEq)]
pub struct TranscriptInput {
    /// The open Agent ("" for none): another one is built from its end
    /// again, and the one it replaces is removed over later frames.
    pub conversation: String,
    pub items: Shared<Vec<Rc<RenderedItem>>>,
    pub expanded: Rc<HashSet<String>>,
    pub callbacks: Rc<RowCallbacks>,
    pub palette: Palette,
    pub column: Option<f64>,
    /// "Load older messages" when more history exists: enabled, and its
    /// callback.
    pub load_older: Option<(bool, Callback<()>)>,
    /// The line shown without turns ("Loading…", "No messages yet.").
    pub empty: Option<&'static str>,
    pub automation_id: &'static str,
}

/// A conversation that was left: drawn as it was (hidden, so its parts
/// are kept, not rebuilt) until its parts are removed.
struct Leaving {
    /// Its container's key.
    serial: u64,
    input: TranscriptInput,
    /// Its oldest parts already removed (or never built).
    gone: usize,
    parts: usize,
}

/// Build more of the shown conversation, or remove more of the left ones.
#[derive(Clone, Copy, Debug)]
pub enum TranscriptStep {
    /// The conversation (its serial) the next older parts were planned
    /// for.
    Grow(u64),
    Drain,
}

/// The conversation's transcript: its own component, so it renders only
/// when its input changes. A newly shown conversation builds its newest
/// parts first and the older ones over the next frames (the transcript is
/// anchored at its end, so they arrive above the window's view); the one
/// it replaces is removed over later frames too.
pub struct Transcript {
    /// Counts the conversations shown: each one's container key.
    serial: u64,
    /// The conversation's first turns arrived.
    filled: bool,
    /// The oldest resident parts not built yet.
    hidden: usize,
    /// The input last drawn.
    shown: TranscriptInput,
    /// Conversations left, the latest first.
    leaving: Vec<Leaving>,
}

fn part_count(items: &[Rc<RenderedItem>]) -> usize {
    items.iter().map(|item| item.parts.len()).sum()
}

fn step_later(step: TranscriptStep, context: &ComponentContext<Transcript>) {
    let _task = context.spawn_background(move |cancel| {
        let _ = crate::feed::backoff(STEP_DELAY_MS, &|| cancel.is_cancelled());
        step
    });
}

impl Transcript {
    fn settle(&mut self, input: &TranscriptInput, context: &ComponentContext<Self>) {
        if input.conversation != self.shown.conversation {
            let left = std::mem::replace(&mut self.shown, input.clone());
            let parts = part_count(&left.items.0);
            if parts > self.hidden {
                if self.leaving.is_empty() {
                    step_later(TranscriptStep::Drain, context);
                }
                self.leaving.insert(
                    0,
                    Leaving {
                        serial: self.serial,
                        input: left,
                        gone: self.hidden,
                        parts,
                    },
                );
            }
            self.serial += 1;
            self.filled = false;
            self.hidden = 0;
        }
        self.shown = input.clone();
        if self.filled || input.items.0.is_empty() {
            return;
        }
        self.filled = true;
        self.hidden = part_count(&input.items.0).saturating_sub(FIRST_PARTS);
        if self.hidden > 0 {
            step_later(TranscriptStep::Grow(self.serial), context);
        }
    }
}

impl Component for Transcript {
    type Input = TranscriptInput;
    type Message = TranscriptStep;

    fn create(input: &TranscriptInput, context: &ComponentContext<Self>) -> Self {
        let mut transcript = Self {
            serial: 0,
            filled: false,
            hidden: 0,
            shown: input.clone(),
            leaving: Vec::new(),
        };
        transcript.settle(input, context);
        transcript
    }

    fn input_changed(&mut self, input: &TranscriptInput, context: &ComponentContext<Self>) {
        self.settle(input, context);
    }

    fn update(&mut self, step: TranscriptStep, context: &ComponentContext<Self>) {
        match step {
            TranscriptStep::Grow(serial) => {
                if serial != self.serial || self.hidden == 0 {
                    return;
                }
                self.hidden = self.hidden.saturating_sub(GROW_PARTS);
                if self.hidden > 0 {
                    step_later(step, context);
                }
            }
            TranscriptStep::Drain => {
                if let Some(oldest) = self.leaving.last_mut() {
                    oldest.gone += DRAIN_PARTS;
                    if oldest.gone >= oldest.parts {
                        self.leaving.pop();
                    }
                }
                if !self.leaving.is_empty() {
                    step_later(step, context);
                }
            }
        }
    }

    fn view(&self, input: &TranscriptInput, _context: &mut ViewContext<Self>) -> View {
        debug_render(1, "transcript"); // k210-debug
        let palette = input.palette;
        let load_older: View = match &input.load_older {
            Some((enabled, load)) if self.hidden == 0 => crate::m3::text_button(palette)
                .is_enabled(*enabled)
                .horizontal_alignment(HorizontalAlignment::Center)
                .margin(Thickness::new(0.0, 0.0, 0.0, tokens::spacing::MD))
                .on_click(load.clone())
                .content(crate::m3::label("Load older messages")),
            _ => View::empty(),
        };
        let empty: View = match input.empty {
            Some(line) => crate::m3::secondary(line)
                .horizontal_alignment(HorizontalAlignment::Center)
                .into(),
            None => View::empty(),
        };
        // One container per conversation, keyed by its serial: the shown one
        // first, then the ones left (zero height, transparent and clipped),
        // so a switch inserts the new one in front and moves nothing.
        let mut conversations = vec![KeyedView::new(
            self.serial,
            StackPanel::new().keyed_children(turn_views(input, self.hidden)),
        )];
        conversations.extend(self.leaving.iter().map(|left| {
            KeyedView::new(
                left.serial,
                StackPanel::new()
                    .height(0.0)
                    .opacity(0.0)
                    .keyed_children(turn_views(&left.input, left.gone)),
            )
        }));
        // The app's one ScrollView: its implicit style anchors it at the
        // far (bottom) edge (`app_resources::styles_xaml`), so it opens at
        // the newest turn and stays there while earlier turns are built or
        // new activity arrives, with no scroll after the first frame.
        ScrollView::new()
            .grid_row(1)
            .vertical_scroll_bar_visibility(ScrollingScrollBarVisibility::Auto)
            .automation_name("Conversation")
            .automation_id(input.automation_id)
            .content(
                StackPanel::new()
                    .width(input.column)
                    .max_width(tokens::layout::READING_WIDTH)
                    .horizontal_alignment(HorizontalAlignment::Center)
                    .margin(Thickness::xy(0.0, tokens::spacing::MD))
                    .children((
                        load_older,
                        empty,
                        StackPanel::new().keyed_children(conversations),
                    )),
            )
    }
}

/// The turns of one conversation, its oldest `skip` parts left out; each
/// part its own [`PartView`].
fn turn_views(input: &TranscriptInput, skip: usize) -> Vec<KeyedView> {
    // A tool row is keyed by whether it runs: WinUI reads a button's colors
    // only when its template is applied, so a finished row would keep the
    // running tint.
    let running = format!(
        "{}Running{}",
        crate::chat::TOOL_FIELDS,
        crate::chat::TOOL_FIELDS
    );
    let mut skip = skip;
    input
        .items
        .0
        .iter()
        .filter_map(|item| {
            if skip >= item.parts.len() {
                skip -= item.parts.len();
                return None;
            }
            let shown = &item.parts[skip..];
            skip = 0;
            let parts: Vec<KeyedView> = shown
                .iter()
                .map(|part| {
                    let key = if part.kind == PartKind::Tools && part.plain.contains(&running) {
                        format!("{}:running", part.key)
                    } else {
                        part.key.clone()
                    };
                    KeyedView::new(
                        key,
                        View::component::<PartView>(PartInput {
                            part: Shared(Rc::clone(part)),
                            expanded: input.expanded.contains(&part.key),
                            callbacks: Rc::clone(&input.callbacks),
                            palette: input.palette,
                        }),
                    )
                })
                .collect();
            // A turn takes the reading column's width explicitly: a short
            // turn's bubble still reaches the right edge.
            Some(KeyedView::new(
                item.key.clone(),
                StackPanel::new()
                    .width(input.column)
                    .spacing(tokens::spacing::SM)
                    .margin(Thickness::new(0.0, 0.0, 0.0, tokens::spacing::LG))
                    .keyed_children(parts),
            ))
        })
        .collect()
}

/// One part's input: the rendered part (replaced only on a new revision)
/// and how it is drawn.
#[derive(Clone, PartialEq)]
pub struct PartInput {
    pub part: Shared<RenderedPart>,
    pub expanded: bool,
    pub callbacks: Rc<RowCallbacks>,
    pub palette: Palette,
}

/// One transcript part as its own component: a transcript render rebuilds
/// only the parts whose input changed, and the pointer over a message shows
/// its copy actions without leaving the part.
pub struct PartView {
    /// The pointer is over the part: its copy actions show.
    hovered: bool,
}

impl Component for PartView {
    type Input = PartInput;
    /// The pointer entered (`true`) or left the part.
    type Message = bool;

    fn create(_input: &PartInput, _context: &ComponentContext<Self>) -> Self {
        Self { hovered: false }
    }

    fn update(&mut self, over: bool, _context: &ComponentContext<Self>) {
        self.hovered = over;
    }

    fn view(&self, input: &PartInput, context: &mut ViewContext<Self>) -> View {
        debug_render(2, "part"); // k210-debug
        let part = &*input.part.0;
        // k210-debug: large parts
        {
            let blocks: usize = part.blocks.as_ref().map_or(0, |b| {
                b.iter()
                    .map(|b| match b {
                        RenderedBlock::Code(_, t) | RenderedBlock::Heading(_, t) => t.len(),
                        _ => 0,
                    })
                    .sum()
            });
            let detail: usize = part.detail.iter().map(String::len).sum();
            if part.plain.len() + blocks + detail > 2_000 {
                eprintln!(
                    "k210dbg part {} {:?} plain={} code={} blocks={} detail={} paths={}",
                    part.key,
                    part.kind,
                    part.plain.len(),
                    blocks,
                    part.blocks.as_ref().map_or(0, Vec::len),
                    detail,
                    part.file_paths.len()
                );
            }
        }
        let (callbacks, palette) = (&*input.callbacks, input.palette);
        let body = StackPanel::new().spacing(tokens::spacing::XS).children((
            part_view(part, input.expanded, self.hovered, callbacks, palette),
            // The assistant's files are actions; the owner's own message
            // only names them.
            if matches!(part.kind, PartKind::Progress | PartKind::Final) {
                path_actions(&part.key, &part.file_paths, callbacks, palette)
            } else {
                View::empty()
            },
        ));
        // A message with Copy is a hover area: its actions show while the
        // pointer is over it.
        if matches!(
            part.kind,
            PartKind::Input | PartKind::Progress | PartKind::Final
        ) {
            Border::new()
                .background(Brush::Solid(Color::transparent()))
                .on_pointer_entered(context.callback(|_: PointerEventInfo| true))
                .on_pointer_exited(context.callback(|_: PointerEventInfo| false))
                .content(body)
        } else {
            body
        }
    }
}

/// Between two redraws of a running tool row's seconds.
const ELAPSED_TICK_MS: u64 = 1_000;

/// A running tool row's start and the palette its caption takes.
#[derive(Clone, PartialEq)]
pub struct ElapsedInput {
    pub started_ms: i64,
    pub palette: Palette,
}

/// "Running · 12 s": its own one-second clock, so the tick redraws this
/// caption only, never the transcript or the window around it.
pub struct Elapsed {
    now_ms: i64,
}

fn tick_later(context: &ComponentContext<Elapsed>) {
    let _task = context.spawn_background(|cancel| {
        let _ = crate::feed::backoff(ELAPSED_TICK_MS, &|| cancel.is_cancelled());
    });
}

impl Component for Elapsed {
    type Input = ElapsedInput;
    type Message = ();

    fn create(_input: &ElapsedInput, context: &ComponentContext<Self>) -> Self {
        tick_later(context);
        Self {
            now_ms: crate::handoff::now_ms(),
        }
    }

    fn update(&mut self, (): (), context: &ComponentContext<Self>) {
        self.now_ms = crate::handoff::now_ms();
        tick_later(context);
    }

    fn view(&self, input: &ElapsedInput, _context: &mut ViewContext<Self>) -> View {
        debug_render(3, "elapsed"); // k210-debug
        let seconds = (self.now_ms - input.started_ms).max(0) / 1_000;
        crate::m3::tool_caption(
            format!("Running \u{b7} {seconds} s"),
            Some(input.palette.primary()),
        )
        .into()
    }
}

/// Most file chips one message shows before "+N more".
const FILE_CHIPS: usize = 6;

/// The files a message names, as compact chips under it (file glyph and
/// name, the full path in the tooltip; one chip per file however often it
/// is named). A click opens it in Explorer (`ui::reveal`); Reactor 0.100.0
/// hyperlinks only navigate URIs, and prose selection is kept.
fn path_actions(part: &str, paths: &[String], callbacks: &RowCallbacks, palette: Palette) -> View {
    let mut seen = HashSet::new();
    let files: Vec<(String, String)> = paths
        .iter()
        .filter_map(|reference| {
            let path = std::path::Path::new(reference);
            if path.is_relative() && callbacks.workspace.is_empty() {
                return None;
            }
            let resolved = if path.is_absolute() {
                path.to_path_buf()
            } else {
                std::path::Path::new(&callbacks.workspace).join(path)
            };
            let resolved = resolved.to_string_lossy().into_owned();
            seen.insert(resolved.to_lowercase())
                .then(|| (reference.clone(), resolved))
        })
        .collect();
    if files.is_empty() {
        return View::empty();
    }
    let mut note = View::empty();
    let mut chips: Vec<KeyedView> = Vec::new();
    for (reference, resolved) in files.iter().take(FILE_CHIPS) {
        let item = format!("{}{part}:{reference}", callbacks.path_scope);
        if let Some((failed, text)) = &callbacks.path_failure
            && failed == &item
        {
            note = crate::m3::secondary(text.clone())
                .foreground(palette.attention())
                .text_wrapping(TextWrapping::Wrap)
                .is_text_selection_enabled(true)
                .into();
        }
        let open = callbacks.open_path.clone();
        let target = resolved.clone();
        let name = crate::attachments::file_name(resolved).to_owned();
        chips.push(KeyedView::new(
            resolved.clone(),
            crate::m3::chip(palette)
                .automation_name(format!("Open {resolved} in Explorer"))
                .on_click(move || {
                    let _ = open.call((item.clone(), target.clone()));
                })
                .content(crate::m3::chip_content(
                    Some(file_glyph(resolved)),
                    &name,
                    false,
                    tokens::layout::PROJECT_CHIP_WIDTH,
                ))
                .tooltip(resolved.clone()),
        ));
    }
    if files.len() > FILE_CHIPS {
        chips.push(KeyedView::new(
            "more",
            crate::m3::secondary(format!("+{} more", files.len() - FILE_CHIPS))
                .vertical_alignment(VerticalAlignment::Center),
        ));
    }
    StackPanel::new().spacing(tokens::spacing::XS).children((
        StackPanel::new()
            .orientation(Orientation::Horizontal)
            .spacing(6.0)
            .keyed_children(chips),
        note,
    ))
}

/// A folder's glyph for a folder, else a file's.
fn file_glyph(path: &str) -> &'static str {
    if std::path::Path::new(path).is_dir() {
        crate::m3::glyph::FOLDER
    } else {
        crate::m3::glyph::FILE
    }
}

/// The files the owner attached to a message: chips under the bubble, at
/// its right edge (`attachments`).
pub fn attached_chips(files: &[String], palette: Palette) -> View {
    StackPanel::new()
        .orientation(Orientation::Horizontal)
        .spacing(6.0)
        .horizontal_alignment(HorizontalAlignment::Right)
        .keyed_children(
            files
                .iter()
                .map(|path| {
                    KeyedView::new(
                        path.clone(),
                        crate::m3::fixed_chip(
                            palette,
                            false,
                            Some(crate::m3::glyph::FILE),
                            crate::attachments::file_name(path),
                            &format!("Attached file {path}"),
                        )
                        .tooltip(path.clone()),
                    )
                })
                .collect::<Vec<_>>(),
        )
}

// k210-debug: render counters for the live proof; removed before the final commit.
pub static DEBUG_RENDERS: [std::sync::atomic::AtomicU64; 5] =
    [const { std::sync::atomic::AtomicU64::new(0) }; 5];
pub fn debug_render(slot: usize, what: &str) {
    use std::sync::atomic::Ordering::Relaxed;
    DEBUG_RENDERS[slot].fetch_add(1, Relaxed);
    if slot != 2 {
        eprintln!(
            "k210dbg {} {what} root={} transcript={} parts={} elapsed={} monitor={}",
            crate::handoff::now_ms(),
            DEBUG_RENDERS[0].load(Relaxed),
            DEBUG_RENDERS[1].load(Relaxed),
            DEBUG_RENDERS[2].load(Relaxed),
            DEBUG_RENDERS[3].load(Relaxed),
            DEBUG_RENDERS[4].load(Relaxed),
        );
    }
}
