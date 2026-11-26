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
//!
//! The transcript, each part and a running tool row's seconds are Reactor
//! components ([`Transcript`], [`PartView`], [`Elapsed`]): Reactor 0.100.0
//! re-renders the whole window for every message, but a component whose
//! `Input` compares equal is skipped, so a sidebar refresh, a Monitor tick
//! or the one-second clock never rebuilds the conversation.

use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use windows_reactor::*;

use crate::chat::{self, OutputBound, Part, PartKind, StepDetail, TurnEnd, TurnItem, text_hash};
use crate::markdown::{Block, Doc, Inline};
use crate::ui::tokens;

/// Children with optional slots. Reactor 0.101 has no empty view, so an
/// absent slot is left out, and each present child is keyed by its slot
/// index: the key a positional child at that index gets, so a slot that
/// fills or empties does not shift its siblings' identity.
pub fn slots<const N: usize>(children: [Option<View>; N]) -> Vec<KeyedView> {
    children
        .into_iter()
        .enumerate()
        .filter_map(|(index, child)| child.map(|view| KeyedView::new(index, view)))
        .collect()
}

/// Optional content: Reactor 0.101 has no empty view, so a control whose
/// slot is empty keeps no child.
pub trait MaybeContent: Sized {
    fn maybe_content(self, content: Option<View>) -> Self;
}

impl MaybeContent for Border {
    fn maybe_content(self, content: Option<View>) -> Self {
        match content {
            Some(content) => self.content(content),
            None => self,
        }
    }
}

/// A content column (§6.3): stretched across its slot less the 32-DIP side
/// gutters, at most `widest` wide and at least [`tokens::layout::MIN_COLUMN`];
/// WinUI centres a stretched element its MaxWidth caps, so no width is
/// derived from the window. A column is never a ScrollViewer's direct
/// content: the default ScrollViewer style aligns content Left, which places
/// a capped child by its desired width, off centre; a plain Border between
/// them takes the viewport width and centres the column.
pub trait Column: Sized {
    fn column(self, widest: f64, top: f64, bottom: f64) -> Self;
}

macro_rules! column {
    ($($control:ty),*) => {$(
        impl Column for $control {
            fn column(self, widest: f64, top: f64, bottom: f64) -> Self {
                self.min_width(crate::tokens::layout::MIN_COLUMN)
                    .max_width(widest)
                    .horizontal_alignment(HorizontalAlignment::Stretch)
                    .margin(Thickness::new(
                        crate::tokens::spacing::XL,
                        top,
                        crate::tokens::spacing::XL,
                        bottom,
                    ))
            }
        }
    )*};
}

column!(Border, StackPanel, Grid);

/// An optional maximum width: Reactor 0.101 takes a plain `f64`; an
/// omitted size leaves the control at its default, as `None` did in 0.100.
pub trait MaybeSize: Sized {
    fn maybe_max_width(self, width: Option<f64>) -> Self;
}

macro_rules! maybe_size {
    ($($control:ty),*) => {$(
        impl MaybeSize for $control {
            fn maybe_max_width(self, width: Option<f64>) -> Self {
                match width {
                    Some(width) => self.max_width(width),
                    None => self,
                }
            }
        }
    )*};
}

maybe_size!(Border, Grid, StackPanel);

/// A text box's edits for a `String` handler: Reactor 0.101 reports them as
/// `Rc<str>`.
pub fn text_edits(callback: Callback<String>) -> impl Fn(Rc<str>) + 'static {
    move |text| callback.call(text.to_string())
}

/// A menu's picks for a `String` handler: Reactor 0.101 reports the picked
/// item's `Key` (0.100 reported its label); every menu here keys its items
/// by string.
pub fn menu_keys(callback: Callback<String>) -> impl Fn(Key) + 'static {
    move |key| {
        if let Some(key) = key.as_str() {
            callback.call(key.to_owned());
        }
    }
}

/// A two-state check's changes for a `bool` handler: Reactor 0.101 reports
/// `IsChecked` as `Option<bool>`; indeterminate reads as unchecked.
pub fn check_changes(callback: Callback<bool>) -> impl Fn(Option<bool>) + 'static {
    move |checked| callback.call(checked == Some(true))
}

/// A code block's font: the system monospace face, Cascadia Mono, then
/// Consolas where it is missing. Set on the code `TextBlock` alone; no
/// application font key changes.
const CODE_FONT: &str = "Cascadia Mono, Consolas";

/// One display block, ready for the UI thread.
#[derive(Clone)]
pub enum RenderedBlock {
    Rich(RichText),
    /// Consecutive list item paragraphs: (depth, marker, text) each.
    List(Vec<(usize, String, RichText)>),
    Heading(u8, String),
    Code(usize, String),
    Rule,
    /// A table, bounded and shared: its grid is its own component, built
    /// once per message revision.
    Table(Rc<TableData>),
}

/// Most body rows and columns a table draws; the rest are counted and Copy
/// table copies them all.
const TABLE_ROWS: usize = 200;
const TABLE_COLUMNS: usize = 20;

/// One answer table as drawn: at most [`TABLE_ROWS`] × [`TABLE_COLUMNS`],
/// how many rows and columns were left out, and the whole table as
/// tab-separated text.
pub struct TableData {
    head: Vec<String>,
    rows: Vec<Vec<String>>,
    hidden_rows: usize,
    hidden_columns: usize,
    text: String,
}

impl TableData {
    fn new(head: &[String], rows: &[Vec<String>]) -> Self {
        let columns = head.len().max(rows.iter().map(Vec::len).max().unwrap_or(0));
        let cut = |row: &[String]| row.iter().take(TABLE_COLUMNS).cloned().collect::<Vec<_>>();
        let text = std::iter::once(head)
            .chain(rows.iter().map(Vec::as_slice))
            .filter(|row| !row.is_empty())
            .map(|row| row.join("\t"))
            .collect::<Vec<_>>()
            .join("\n");
        Self {
            head: cut(head),
            rows: rows.iter().take(TABLE_ROWS).map(|row| cut(row)).collect(),
            hidden_rows: rows.len().saturating_sub(TABLE_ROWS),
            hidden_columns: columns.saturating_sub(TABLE_COLUMNS),
            text,
        }
    }
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
    pub end: Option<TurnEnd>,
    pub step: Option<StepDetail>,
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
            Block::Table { head, rows } => {
                blocks.push(RenderedBlock::Table(Rc::new(TableData::new(head, rows))));
            }
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
                            // At least the marker column; a longer marker
                            // ("100.") widens its own row instead of clipping.
                            .columns([GridLength::Auto, GridLength::STAR])
                            .column_spacing(tokens::spacing::XS)
                            .children((
                                crate::controls::body(marker.clone())
                                    .min_width(tokens::layout::LIST_MARKER_WIDTH),
                                rich_block(text).grid_column(1),
                            )),
                    )
                }),
        )
        .into()
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
        end: part.end,
        step: part.step.clone(),
    }
}

/// The running turn's live message (`turn.preview.watch`), drawn at the end
/// of its turn as the answer it becomes when the turn ends with it, so the
/// answer takes its place without a jump. It is never one of the
/// transcript's items.
#[derive(Clone, PartialEq)]
pub struct Draft {
    /// The key of its turn's item (`turn:<generation>:<turn>`).
    pub item: String,
    pub part: Shared<RenderedPart>,
    /// The daemon kept only the start of the message.
    pub truncated: bool,
}

/// The draft's part, rendered with the Markdown `doc` a worker parsed from
/// its text, as every committed message is.
pub fn draft_part(part: &Part, doc: Doc) -> RenderedPart {
    render_part(part, &HashMap::from([(text_hash(&part.text), doc)]))
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
    /// Copy a step's retained output (the step part's key).
    pub copy_output: Callback<String>,
    pub toggle_tools: Callback<(String, bool)>,
    /// View changes: the right pane's Changes tab on the last turn.
    pub view_changes: Callback<()>,
}

fn label(text: &str) -> TextBlock {
    crate::controls::secondary(text.to_owned())
}

/// Message text: body.
fn body_text(text: &str) -> TextBlock {
    crate::controls::body(text.to_owned())
        .text_wrapping(TextWrapping::Wrap)
        .is_text_selection_enabled(true)
}

/// An inline icon button is pulled back by this so its glyph lines up
/// with the text around it (a 32-DIP icon button centres a 16-DIP glyph).
const INSET: f64 = (tokens::height::CONTROL - tokens::icon::ROW) / 2.0;

/// Copy for `part`: an icon button while the part is under the pointer;
/// otherwise the same place holds a keyboard stop whose glyph shows only
/// with keyboard focus ([`crate::controls::reveal_button`]). Never a permanently
/// visible icon.
fn copy_button(name: &str, hovered: bool, callback: impl Fn() + 'static) -> View {
    if hovered {
        crate::controls::icon_button()
            .automation_name(name)
            .on_click(callback)
            .content(copy_glyph())
            .into()
    } else {
        crate::controls::reveal_button()
            .automation_name(name)
            .on_click(callback)
            .content(copy_glyph())
            .into()
    }
}

/// The glyph of a copy button.
fn copy_glyph() -> View {
    crate::controls::icon(crate::controls::glyph::COPY)
}

/// Code or output on the code ground: monospace, selectable, its long
/// lines scrolled sideways rather than cut.
fn code_ground(text: &str) -> Border {
    Border::new()
        .background(ThemeBrush::CardBackground)
        .border_brush(ThemeBrush::CardStroke)
        .border_thickness(Thickness::uniform(tokens::layout::HAIRLINE))
        .corner_radius(tokens::radius::CONTAINER)
        .padding(Thickness::xy(tokens::spacing::INSET, tokens::spacing::SM))
        .content(
            ScrollViewer::new()
                .horizontal_scroll_bar_visibility(ScrollBarVisibility::Auto)
                .vertical_scroll_bar_visibility(ScrollBarVisibility::Disabled)
                .content(
                    crate::controls::body(text.to_owned())
                        .font_family(CODE_FONT)
                        .text_wrapping(TextWrapping::NoWrap)
                        .is_text_selection_enabled(true),
                ),
        )
}

fn block_views(part: &RenderedPart, callbacks: &RowCallbacks, hovered: bool) -> Vec<KeyedView> {
    let Some(blocks) = part.blocks.as_ref() else {
        return vec![KeyedView::new("plain", body_text(&part.plain))];
    };
    blocks
        .iter()
        .enumerate()
        .map(|(index, block)| {
            let view: View = match block {
                RenderedBlock::Rich(text) => rich_block(text).into(),
                RenderedBlock::List(items) => list_view(items),
                RenderedBlock::Heading(level, text) => crate::controls::text(
                    text.clone(),
                    match level {
                        1 => tokens::text::TITLE,
                        _ => tokens::text::HEADER,
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
                    StackPanel::new()
                        .spacing(tokens::spacing::XS)
                        .children((
                            code_ground(text),
                            Border::new()
                                .horizontal_alignment(HorizontalAlignment::Left)
                                .margin(Thickness::new(-INSET, 0.0, 0.0, 0.0))
                                .content(copy_button("Copy code", hovered, move || {
                                    copy.call((key.clone(), code_index));
                                })),
                        ))
                        .into()
                }
                RenderedBlock::Rule => crate::controls::divider().into(),
                RenderedBlock::Table(table) => View::component::<TableView>(TableInput {
                    table: Shared(Rc::clone(table)),
                }),
            };
            KeyedView::new(index, view)
        })
        .collect()
}

/// One style for every part (`Conversation.dc.html`, `Finished.dc.html`):
/// the owner's message is a card-surface bubble on the right with
/// its time and label under the text; the answer is Markdown on the
/// surface; the work log is one head line over its notes and step rows;
/// service lines are quiet centred text with an icon.
fn part_view(
    part: &RenderedPart,
    expanded: bool,
    hovered: bool,
    callbacks: &RowCallbacks,
) -> Option<View> {
    let copy = callbacks.copy_part.clone();
    let key = part.key.clone();
    let copy_message = move || {
        copy.call(key.clone());
    };
    Some(match part.kind {
        PartKind::Input => owner_side(
            StackPanel::new()
                .spacing(tokens::spacing::XS)
                .horizontal_alignment(HorizontalAlignment::Right)
                .keyed_children(crate::views::slots([
                    Some(crate::controls::bubble(
                        bubble_content(&part.plain, &part.label),
                        None,
                    )),
                    (!part.attachments.is_empty()).then(|| attached_chips(&part.attachments)),
                    Some(
                        Border::new()
                            .horizontal_alignment(HorizontalAlignment::Right)
                            .margin(Thickness::new(0.0, 0.0, -INSET, 0.0))
                            .content(copy_button("Copy message", hovered, copy_message))
                            .into(),
                    ),
                ]))
                .into(),
        ),
        // A steered message stays in the turn it steered, its label under
        // the text ("Steer · 14:02").
        PartKind::Steer => owner_side(
            Border::new()
                .horizontal_alignment(HorizontalAlignment::Right)
                .automation_name(format!("Steered: {}", part.plain))
                .content(crate::controls::bubble(
                    bubble_content(&part.plain, &part.label),
                    None,
                ))
                .into(),
        ),
        // The agent's commentary between steps (`.note`).
        PartKind::Progress => StackPanel::new()
            .spacing(tokens::spacing::SM)
            .margin(Thickness::xy(tokens::spacing::ROW, tokens::spacing::XS))
            .keyed_children(block_views(part, callbacks, hovered))
            .into(),
        PartKind::Tools => step_view(part, expanded, callbacks),
        PartKind::Log | PartKind::Edited => return None,
        // The answer: Markdown on the surface; its outcome beside it only
        // when it is not a plain success ("result not confirmed" stays in
        // view over the answer text).
        PartKind::Final => {
            let note: Option<View> = match part.end {
                None | Some(TurnEnd::Answered) => None,
                Some(end) => Some(outcome_line(&part.label, end)),
            };
            StackPanel::new()
                .spacing(tokens::spacing::SM)
                .keyed_children(crate::views::slots([
                    note,
                    Some(
                        StackPanel::new()
                            .spacing(tokens::spacing::SM)
                            .keyed_children(block_views(part, callbacks, hovered))
                            .into(),
                    ),
                    Some(
                        Border::new()
                            .horizontal_alignment(HorizontalAlignment::Left)
                            .margin(Thickness::new(-INSET, 0.0, 0.0, 0.0))
                            .content(copy_button("Copy message", hovered, copy_message))
                            .into(),
                    ),
                ]))
                .into()
        }
        // A local input waiting for its turn ("Sending…", "Starting
        // Codex…"): its words and the progress bar.
        PartKind::Working => StackPanel::new()
            .spacing(tokens::spacing::XS)
            .keyed_children(crate::views::slots([
                Some(label(&part.label).into()),
                // Why the check of its outcome failed, when it did.
                (!part.detail.is_empty()).then(|| {
                    label(&part.detail.join(" · "))
                        .foreground(ThemeBrush::SystemCritical)
                        .text_wrapping(TextWrapping::Wrap)
                        .is_text_selection_enabled(true)
                        .into()
                }),
                Some(
                    ProgressBar::new()
                        .is_indeterminate(true)
                        .width(tokens::layout::PROGRESS_WIDTH)
                        .horizontal_alignment(HorizontalAlignment::Left)
                        .into(),
                ),
            ]))
            .into(),
        // A start that did not happen: why (`swarm why`), its origin, and
        // Retry when the failure is definite.
        PartKind::Failed | PartKind::TurnFailed | PartKind::Unconfirmed => {
            let action: Option<View> = if part.kind == PartKind::Failed {
                let retry = callbacks.retry.clone();
                let key = part.key.clone();
                Some(
                    crate::controls::outlined()
                        .horizontal_alignment(HorizontalAlignment::Left)
                        .automation_name("Retry")
                        .on_click(move || {
                            retry.call(key.clone());
                        })
                        .content(crate::controls::label("Retry"))
                        .into(),
                )
            } else if let (PartKind::TurnFailed, Some(pick_model)) =
                (part.kind, callbacks.pick_model.clone())
            {
                Some(
                    crate::controls::outlined()
                        .horizontal_alignment(HorizontalAlignment::Left)
                        .automation_name("Pick another model")
                        .on_click(move || {
                            pick_model.call(());
                        })
                        .content(crate::controls::label("Pick another model"))
                        .into(),
                )
            } else {
                None
            };
            StackPanel::new()
                .spacing(tokens::spacing::XS)
                .keyed_children(crate::views::slots([
                    Some(
                        crate::controls::body(format!("{} · {}", part.label, part.plain))
                            .text_wrapping(TextWrapping::Wrap)
                            .is_text_selection_enabled(true)
                            .foreground(ThemeBrush::SystemCritical)
                            .into(),
                    ),
                    Some(label(&part.detail.join(" · ")).into()),
                    action,
                ]))
                .into()
        }
        // What Swarm inserted (a Swarm note, service or peer mail): never
        // the owner's bubble. One step row with its fixed frame line, the
        // quoted content below it when expanded.
        PartKind::Note => {
            let toggle = callbacks.toggle_tools.clone();
            let key = part.key.clone();
            let row = crate::controls::step_row(false)
                .automation_name(format!(
                    "{} ({})",
                    part.label,
                    if expanded { "expanded" } else { "collapsed" }
                ))
                .on_click(move || {
                    toggle.call((key.clone(), !expanded));
                })
                .content(crate::controls::step_content(
                    crate::controls::icon(crate::controls::glyph::INFO),
                    &part.label,
                    None,
                ));
            let detail: Option<View> = (expanded && !part.plain.is_empty()).then(|| {
                Border::new()
                    .background(ThemeBrush::CardBackground)
                    .corner_radius(tokens::radius::CONTAINER)
                    .padding(Thickness::xy(tokens::spacing::INSET, tokens::spacing::SM))
                    .content(
                        crate::controls::secondary(part.plain.clone())
                            .text_wrapping(TextWrapping::Wrap)
                            .is_text_selection_enabled(true)
                            .automation_name(format!("Swarm note content: {}", part.plain)),
                    )
                    .into()
            });
            StackPanel::new()
                .spacing(tokens::spacing::XS)
                .keyed_children(crate::views::slots([Some(row.into()), detail]))
                .into()
        }
        // The end of a turn without an answer is a service line: the
        // owner's stop says only that ("Stopped by you after 3m"); any other
        // end keeps its outcome in view ("Assistant · result not confirmed
        // · …"), with a warning unless the turn succeeded.
        PartKind::Notice if part.end.is_some() => match part.end {
            Some(TurnEnd::StoppedByYou) => {
                service_line(&part.plain, crate::controls::glyph::STOP_CIRCLE)
            }
            end => service_line(
                &format!("{} · {}", part.label, part.plain),
                end_glyph(end.unwrap_or(TurnEnd::Answered)),
            ),
        },
        PartKind::Notice => crate::controls::secondary(format!("{} · {}", part.label, part.plain))
            .text_wrapping(TextWrapping::Wrap)
            .margin(Thickness::xy(tokens::spacing::ROW, 0.0))
            .into(),
        // Where a new generation starts: one quiet line, read out by name.
        PartKind::Generation => service_line(&part.label, generation_glyph(&part.label)),
    })
}

/// The icon of how a turn ended.
fn end_glyph(end: TurnEnd) -> &'static str {
    match end {
        TurnEnd::Answered => crate::controls::glyph::INFO,
        TurnEnd::StoppedByYou => crate::controls::glyph::STOP_CIRCLE,
        TurnEnd::Interrupted | TurnEnd::Failed | TurnEnd::Unconfirmed => {
            crate::controls::glyph::WARNING
        }
    }
}

/// An answer's outcome over its text ("Assistant · result not
/// confirmed"): the icon and the words, read out together.
fn outcome_line(text: &str, end: TurnEnd) -> View {
    StackPanel::new()
        .orientation(Orientation::Horizontal)
        .spacing(tokens::spacing::SM)
        .automation_name(text)
        .children((
            crate::controls::muted_icon(end_glyph(end)),
            label(text)
                .vertical_alignment(VerticalAlignment::Center)
                .text_wrapping(TextWrapping::Wrap),
        ))
        .into()
}

/// The owner's side of the reading column (§9.1): the message at the
/// right, as wide as its text up to the bubble's share of the column (a
/// star cell, not a fixed width), so a short message stays short.
fn owner_side(message: View) -> View {
    Grid::new()
        .columns([
            GridLength::Star(1.0 - tokens::layout::BUBBLE_SHARE),
            GridLength::Star(tokens::layout::BUBBLE_SHARE),
        ])
        .children((Border::new().grid_column(1).content(message),))
        .into()
}

/// A message bubble's text and, under it, its time and label
/// ("You · 13:58", "Steer · 14:02") in smaller type.
fn bubble_content(text: &str, meta: &str) -> View {
    StackPanel::new()
        .spacing(tokens::spacing::XS)
        .children((
            crate::controls::body(text.to_owned())
                .text_wrapping(TextWrapping::Wrap)
                .is_text_selection_enabled(true),
            crate::controls::text(meta.to_owned(), tokens::text::TRAILING),
        ))
        .into()
}

/// A service line (`.svc`): quiet centred text after a 16-DIP icon.
fn service_line(text: &str, glyph: &str) -> View {
    StackPanel::new()
        .orientation(Orientation::Horizontal)
        .spacing(tokens::spacing::SM)
        .horizontal_alignment(HorizontalAlignment::Center)
        .automation_name(text)
        .children((
            crate::controls::muted_icon(glyph),
            crate::controls::secondary(text.to_owned())
                .vertical_alignment(VerticalAlignment::Center)
                .text_wrapping(TextWrapping::Wrap),
        ))
        .into()
}

/// The icon of a generation's line: a restart, a resume, a move.
fn generation_glyph(line: &str) -> &'static str {
    if line.contains("restart") {
        crate::controls::glyph::RESTART
    } else if line.starts_with("Resumed") {
        crate::controls::glyph::RESUME
    } else {
        crate::controls::glyph::WORKTREE
    }
}

/// A table's input: its bounded data (compared by identity).
#[derive(Clone, PartialEq)]
pub struct TableInput {
    pub table: Shared<TableData>,
}

/// An answer table (`.md table`) as its own component: a hover or another
/// part's change re-renders its message, never its grid. The header in
/// muted, the cells in body type, a hairline
/// under every row; past the bound, how many rows and columns are left out
/// and Copy table (every row, tab-separated).
pub struct TableView;

impl Component for TableView {
    type Input = TableInput;
    /// Copy table.
    type Message = ();

    fn create(_input: &TableInput, _context: &ComponentContext<Self>) -> Self {
        Self
    }

    fn update(&mut self, (): (), _context: &ComponentContext<Self>) {}

    fn view(&self, input: &TableInput, context: &mut ViewContext<Self>) -> View {
        let table = &*input.table.0;
        let grid = table_grid(&table.head, &table.rows);
        if table.hidden_rows == 0 && table.hidden_columns == 0 {
            return grid;
        }
        let mut left_out = Vec::new();
        if table.hidden_rows > 0 {
            left_out.push(crate::chat::count_words(
                table.hidden_rows,
                "more row",
                "more rows",
            ));
        }
        if table.hidden_columns > 0 {
            left_out.push(crate::chat::count_words(
                table.hidden_columns,
                "more column",
                "more columns",
            ));
        }
        let text = table.text.clone();
        let copy = context.callback(move |()| {
            let _ = crate::clipboard::set_text(&text);
        });
        StackPanel::new()
            .spacing(tokens::spacing::XS)
            .children((
                grid,
                StackPanel::new()
                    .orientation(Orientation::Horizontal)
                    .spacing(tokens::spacing::SM)
                    .children((
                        crate::controls::secondary(left_out.join(" \u{b7} "))
                            .vertical_alignment(VerticalAlignment::Center),
                        crate::controls::text_button()
                            .automation_name("Copy table: every row, tab-separated")
                            .on_click(copy)
                            .content(crate::controls::label("Copy table"))
                            .tooltip("Copies every row, tab-separated"),
                    )),
            ))
            .into()
    }
}

/// The grid of a bounded table: one hairline per row, one cell per drawn
/// cell.
fn table_grid(head: &[String], rows: &[Vec<String>]) -> View {
    let columns = head
        .len()
        .max(rows.iter().map(Vec::len).max().unwrap_or(0))
        .max(1);
    let mut cells: Vec<KeyedView> = Vec::new();
    let mut grid_rows = Vec::new();
    let all = std::iter::once((true, head)).chain(rows.iter().map(|row| (false, row.as_slice())));
    for (index, (header, row)) in all.enumerate() {
        if header && row.is_empty() {
            continue;
        }
        let at = grid_rows.len() as i32;
        grid_rows.push(GridLength::Auto);
        cells.push(KeyedView::new(
            format!("rule:{index}"),
            Border::new()
                .grid_row(at)
                .grid_column_span(columns as i32)
                .vertical_alignment(VerticalAlignment::Bottom)
                .height(tokens::layout::HAIRLINE)
                .background(ThemeBrush::CardStroke),
        ));
        for (column, cell) in row.iter().enumerate() {
            let text = if header {
                crate::controls::text(cell.clone(), tokens::text::BUTTON)
            } else {
                crate::controls::body(cell.clone())
            };
            cells.push(KeyedView::new(
                format!("{index}:{column}"),
                text.grid_row(at)
                    .grid_column(column as i32)
                    .margin(Thickness::xy(tokens::spacing::ROW, TABLE_CELL_INSET))
                    .text_wrapping(TextWrapping::Wrap)
                    .is_text_selection_enabled(true),
            ));
        }
    }
    Grid::new()
        .rows(grid_rows)
        .columns(vec![GridLength::STAR; columns])
        .automation_name("Table")
        .keyed_children(cells)
        .into()
}

/// A table cell's top and bottom inset.
const TABLE_CELL_INSET: f64 = 6.0;

/// A work-log step (`.step`): the kind's icon (the loader while it runs,
/// the error icon in `error` when it failed), verb and object, the time on
/// the right; a click shows what the adapter retained ([`step_detail`]).
fn step_view(part: &RenderedPart, expanded: bool, callbacks: &RowCallbacks) -> View {
    let toggle = callbacks.toggle_tools.clone();
    let key = part.key.clone();
    // Name, status, duration, start and class (`chat::TOOL_FIELDS` apart).
    let mut fields = part.plain.split(crate::chat::TOOL_FIELDS);
    let name = fields.next().unwrap_or_default().to_owned();
    let status = fields.next().unwrap_or_default().to_owned();
    let duration = fields.next().unwrap_or_default().to_owned();
    let started = fields.next().and_then(|ms| ms.parse::<i64>().ok());
    let class = fields.next().unwrap_or_default();
    let running = class == "run";
    // A running step counts its seconds in its own component
    // ([`Elapsed`]): the tick redraws that caption only.
    let trailing: View = match started {
        Some(started_ms) if running => View::component::<Elapsed>(ElapsedInput {
            started_ms,
            words: None,
        }),
        _ => crate::controls::tool_caption(duration.clone(), None).into(),
    };
    let glyph = match part.label.as_str() {
        "Command" => crate::controls::glyph::TERMINAL,
        "Edit" => crate::controls::glyph::EDIT,
        "Read" => crate::controls::glyph::FILE,
        "Search" | "Web" => crate::controls::glyph::SEARCH,
        "Worker" => crate::controls::glyph::HUB,
        "Plan" => crate::controls::glyph::LIST,
        _ => crate::controls::glyph::TOOLS,
    };
    let lead: View = match class {
        "run" => crate::controls::row_mark(crate::controls::RowMark::Working, "Running"),
        "fail" => crate::controls::critical_icon(crate::controls::glyph::ERROR, tokens::icon::ROW),
        _ => crate::controls::icon(glyph),
    };
    let row = crate::controls::step_row(running)
        .automation_name(format!(
            "{name} · {status}{} ({})",
            if running {
                String::new()
            } else {
                format!(" · {duration}")
            },
            if expanded { "expanded" } else { "collapsed" }
        ))
        .on_click(move || {
            toggle.call((key.clone(), !expanded));
        })
        .content(crate::controls::step_content(lead, &name, trailing));
    // The row is keyed by its class: WinUI reads a button's colors only
    // when its template is applied, so a finished row would keep the
    // running tint. The open detail keeps its own key, so a selection in
    // it survives the step's updates.
    let mut views = vec![KeyedView::new(format!("row:{class}"), row)];
    if expanded {
        views.push(KeyedView::new("detail", step_detail(part, callbacks)));
    }
    StackPanel::new().keyed_children(views).into()
}

/// A step's disclosure (§9.2): the whole command on the code ground, the
/// folder it ran in with its state and exit code, the output its adapter
/// retained with Copy and how much of it this is, the changed paths, and
/// View changes for an edit.
fn step_detail(part: &RenderedPart, callbacks: &RowCallbacks) -> View {
    let step = part.step.clone().unwrap_or_default();
    let quiet = |text: String| -> View {
        crate::controls::secondary(text)
            .text_wrapping(TextWrapping::Wrap)
            .is_text_selection_enabled(true)
            .into()
    };
    let mut rows: Vec<KeyedView> = Vec::new();
    if let Some(command) = step.command.as_deref() {
        rows.push(KeyedView::new("command", code_ground(command)));
        if step.command_truncated {
            rows.push(KeyedView::new(
                "command-cut",
                quiet("Command truncated".to_owned()),
            ));
        }
    }
    // "In crates/eliot-core · Failed · exit 2".
    let facts: Vec<String> = step
        .folder
        .as_deref()
        .map(|folder| folder_words(folder, &callbacks.workspace))
        .into_iter()
        .chain((!step.state.is_empty()).then(|| step.state.clone()))
        .collect();
    if !facts.is_empty() {
        rows.push(KeyedView::new("facts", quiet(facts.join(" \u{b7} "))));
    }
    match &step.output {
        Some((output, bound)) => {
            let copy = callbacks.copy_output.clone();
            let key = part.key.clone();
            rows.push(KeyedView::new(
                "output-head",
                crate::controls::row_grid(
                    None,
                    crate::controls::secondary(match bound {
                        OutputBound::Whole => "Output",
                        OutputBound::Tail => "Output \u{b7} last lines",
                        OutputBound::Truncated => "Output truncated",
                    })
                    .vertical_alignment(VerticalAlignment::Center)
                    .into(),
                    Some(
                        crate::controls::text_button()
                            .automation_name("Copy output")
                            .on_click(move || {
                                copy.call(key.clone());
                            })
                            .content(crate::controls::label("Copy output"))
                            .into(),
                    ),
                    tokens::spacing::SM,
                ),
            ));
            rows.push(KeyedView::new("output", code_ground(output)));
        }
        None => rows.push(KeyedView::new(
            "output",
            quiet("Output not retained".to_owned()),
        )),
    }
    if !part.detail.is_empty() {
        rows.push(KeyedView::new("paths", quiet(part.detail.join("\n"))));
    }
    if let Some(actions) = path_actions(&part.key, &part.detail_paths, callbacks) {
        rows.push(KeyedView::new("path-actions", actions));
    }
    if step.changes {
        rows.push(KeyedView::new(
            "changes",
            crate::controls::text_button()
                .horizontal_alignment(HorizontalAlignment::Left)
                .automation_name("View changes")
                .on_click(callbacks.view_changes.clone())
                .content(crate::controls::label("View changes")),
        ));
    }
    StackPanel::new()
        .spacing(tokens::spacing::XS)
        .margin(Thickness::new(
            tokens::spacing::ROW + tokens::icon::ROW + tokens::spacing::ROW,
            0.0,
            tokens::spacing::ROW,
            tokens::spacing::XS,
        ))
        .keyed_children(rows)
        .into()
}

/// Where a command ran, in words: inside the task's folder as a relative
/// path ("In crates/eliot-core", "In the task folder"), elsewhere in full.
fn folder_words(folder: &str, workspace: &str) -> String {
    let trim = |path: &str| path.trim_end_matches(['\\', '/']).to_owned();
    let (folder, workspace) = (trim(folder), trim(workspace));
    let same = |a: &str, b: &str| {
        a.len() == b.len()
            && a.bytes().zip(b.bytes()).all(|(x, y)| {
                x.eq_ignore_ascii_case(&y)
                    || (matches!(x, b'\\' | b'/') && matches!(y, b'\\' | b'/'))
            })
    };
    if !workspace.is_empty() && folder.is_char_boundary(workspace.len()) {
        let (head, rest) = folder.split_at(workspace.len());
        if same(head, &workspace) {
            if rest.is_empty() {
                return "In the task folder".to_owned();
            }
            if let Some(rest) = rest.strip_prefix(['\\', '/']) {
                return format!("In {rest}");
            }
        }
    }
    format!("In {folder}")
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

thread_local! {
    static NO_ITEMS: Rc<Vec<Rc<RenderedItem>>> = Rc::new(Vec::new());
}

/// The transcript's items without a conversation: one list, so it compares
/// equal from one window render to the next.
pub fn no_items() -> Shared<Vec<Rc<RenderedItem>>> {
    NO_ITEMS.with(|items| Shared(Rc::clone(items)))
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
    /// "Load older messages" when more history exists: enabled, and its
    /// callback.
    pub load_older: Option<(bool, Callback<()>)>,
    /// The line shown without turns ("Loading…", "No messages yet.").
    pub empty: Option<&'static str>,
    /// The running turn's live message, after its turn's parts.
    pub draft: Option<Draft>,
    /// What the loaded history leaves out and why, over its oldest turn.
    pub boundary: Option<String>,
    pub automation_id: &'static str,
    /// The ScrollView's reference, for the scroll to the end after a send.
    pub scroll: ElementRef<ScrollView>,
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

/// How far from the far edge the view still counts as at the end (DIPs):
/// layout rounding, not a reading position.
const END_SLACK: f64 = 1.0;

fn at_end(metrics: &ScrollViewMetrics) -> bool {
    metrics.extent_height - metrics.viewport_height - metrics.vertical_offset <= END_SLACK
}

/// The first built part's key: the oldest part drawn.
fn first_built(items: &[Rc<RenderedItem>], hidden: usize) -> Option<&str> {
    items
        .iter()
        .flat_map(|item| item.parts.iter())
        .nth(hidden)
        .map(|part| part.key.as_str())
}

/// The parts added after the owner left the end: those of the item that
/// was last then beyond its count then, and every later item's. `None`
/// once that item left the list (the count would be a guess).
fn new_parts(items: &[Rc<RenderedItem>], last: &str, seen: usize) -> Option<usize> {
    let at = items.iter().position(|item| item.key == last)?;
    Some(
        items[at].parts.len().saturating_sub(seen)
            + items[at + 1..]
                .iter()
                .map(|item| item.parts.len())
                .sum::<usize>(),
    )
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
    /// Counts the times parts were built above the oldest drawn one
    /// ([`Follow`] keeps the reading position over them).
    built_above: u64,
}

/// Whether only the top of the same conversation changed: Load older or
/// the boundary line came or went, or an earlier item's parts changed,
/// while the last item and the item count stayed as they were. A change at
/// the end as well is ambiguous and counts as none.
fn changed_above(shown: &TranscriptInput, input: &TranscriptInput) -> bool {
    let (before, after) = (&shown.items.0, &input.items.0);
    let same_parts = |left: &RenderedItem, right: &RenderedItem| {
        left.key == right.key
            && left.parts.len() == right.parts.len()
            && left
                .parts
                .iter()
                .zip(&right.parts)
                .all(|(left, right)| Rc::ptr_eq(left, right))
    };
    let end_kept = before.len() == after.len()
        && match (before.last(), after.last()) {
            (Some(left), Some(right)) => same_parts(left, right),
            (None, None) => true,
            _ => false,
        };
    let top_changed = shown.load_older.as_ref().map(|(enabled, _)| *enabled)
        != input.load_older.as_ref().map(|(enabled, _)| *enabled)
        || shown.boundary != input.boundary
        || before
            .iter()
            .zip(after.iter())
            .rev()
            .skip(1)
            .any(|(left, right)| !same_parts(left, right));
    end_kept && top_changed
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
        if input.conversation == self.shown.conversation
            && let Some(first) = first_built(&self.shown.items.0, self.hidden)
            && first_built(&input.items.0, self.hidden) != Some(first)
            && input
                .items
                .0
                .iter()
                .any(|item| item.parts.iter().any(|part| part.key == first))
        {
            // An older page arrived above the oldest drawn part.
            self.built_above += 1;
        } else if input.conversation == self.shown.conversation && changed_above(&self.shown, input)
        {
            // The top changed and the end did not: Load older left (or its
            // page filled the older turns), the boundary line changed.
            self.built_above += 1;
        }
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
            built_above: 0,
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
                self.built_above += 1;
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
        let boundary: Option<View> = input.boundary.as_ref().map(|line| {
            crate::controls::secondary(line.clone())
                .horizontal_alignment(HorizontalAlignment::Center)
                .text_wrapping(TextWrapping::Wrap)
                .margin(Thickness::new(0.0, 0.0, 0.0, tokens::spacing::MD))
                .into()
        });
        let load_older: Option<View> = match &input.load_older {
            Some((enabled, load)) if self.hidden == 0 => Some(
                crate::controls::text_button()
                    .is_enabled(*enabled)
                    .horizontal_alignment(HorizontalAlignment::Center)
                    .margin(Thickness::new(0.0, 0.0, 0.0, tokens::spacing::MD))
                    .on_click(load.clone())
                    .content(crate::controls::label("Load older messages"))
                    .into(),
            ),
            _ => None,
        };
        let empty: Option<View> = input.empty.map(|line| {
            crate::controls::secondary(line)
                .horizontal_alignment(HorizontalAlignment::Center)
                .into()
        });
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
        // Anchored at the far (bottom) edge, it opens at the newest turn and
        // stays there while earlier turns are built or new activity
        // arrives, with no scroll after the first frame; read further up,
        // the anchor keeps the reading position.
        let scroll: View = ScrollView::new()
            .element_ref(&input.scroll)
            .vertical_anchor_ratio(1.0)
            .vertical_scroll_bar_visibility(ScrollingScrollBarVisibility::Auto)
            .automation_name("Conversation")
            .automation_id(input.automation_id)
            .content(
                StackPanel::new()
                    .column(
                        tokens::layout::READING_WIDTH,
                        tokens::spacing::MD,
                        tokens::spacing::MD,
                    )
                    .keyed_children(crate::views::slots([
                        boundary,
                        load_older,
                        empty,
                        Some(StackPanel::new().keyed_children(conversations).into()),
                    ])),
            )
            .into();
        // Over the transcript's foot: Jump to latest while it is read
        // higher up, in its own component, so a scroll redraws only it.
        let follow = View::component::<Follow>(FollowInput {
            scroll: input.scroll.clone(),
            conversation: input.conversation.clone(),
            items: input.items.clone(),
            built_above: self.built_above,
        });
        Grid::new().grid_row(1).children((scroll, follow)).into()
    }
}

/// What [`Follow`] watches: the transcript's view, the open conversation
/// and its items, and how often parts were built above the oldest one.
#[derive(Clone, PartialEq)]
pub struct FollowInput {
    pub scroll: ElementRef<ScrollView>,
    pub conversation: String,
    pub items: Shared<Vec<Rc<RenderedItem>>>,
    pub built_above: u64,
}

/// The view's metrics changed, or Jump to latest was pressed.
#[derive(Clone, Copy, Debug)]
pub enum FollowStep {
    Scrolled(ScrollViewMetrics),
    Jump,
}

/// Whether the owner reads at the end of the transcript (§9.3), from the
/// ScrollView's own metrics (`observe_view`), never from wheel events or
/// timers: at the end the far-edge anchor (`VerticalAnchorRatio=1`)
/// carries new events into view; read higher up, Jump to latest shows with
/// the count of parts added since, and parts built above keep the text in
/// view where it was (the view has no anchor element of its own).
pub struct Follow {
    /// The input last seen: the view to scroll, the items whose last one
    /// marks where the owner left the end.
    shown: FollowInput,
    metrics: Option<ScrollViewMetrics>,
    /// Read higher up: the item that was last when the view left the end,
    /// and its part count then.
    away: Option<(String, usize)>,
    /// Parts added since the view left the end (Jump to latest's count),
    /// kept with `away` and the items, not recounted on every render.
    fresh: Option<usize>,
    /// The top changed while read higher up (parts built above, Load
    /// older's page): the extent and viewport size before, so the next
    /// extent change at that size moves the offset by as much. Metrics
    /// with the same extent leave it waiting (the new parts are not laid
    /// out yet, or added no height); a viewport size change drops it (the
    /// text wraps again, so the extent change is not the top's), and any
    /// other input clears it (content changing at the end would make that
    /// next extent change ambiguous).
    above: Option<Above>,
    _observation: ElementObservation,
}

/// The extent and viewport size when the top changed ([`Follow`]).
#[derive(Clone, Copy)]
struct Above {
    extent: f64,
    width: f64,
    height: f64,
}

impl Follow {
    /// Count the parts added since the view left the end.
    fn count_fresh(&mut self) {
        self.fresh = self
            .away
            .as_ref()
            .and_then(|(last, seen)| new_parts(&self.shown.items.0, last, *seen));
    }
}

impl Component for Follow {
    type Input = FollowInput;
    type Message = FollowStep;

    fn create(input: &FollowInput, context: &ComponentContext<Self>) -> Self {
        let scrolled = context.callback(FollowStep::Scrolled);
        Self {
            shown: input.clone(),
            metrics: None,
            away: None,
            fresh: None,
            above: None,
            _observation: input
                .scroll
                .observe_view(move |metrics| scrolled.call(metrics)),
        }
    }

    fn input_changed(&mut self, input: &FollowInput, _context: &ComponentContext<Self>) {
        if input.conversation != self.shown.conversation {
            // Another conversation opens at its end.
            self.away = None;
            self.above = None;
        } else if input.built_above != self.shown.built_above && self.away.is_some() {
            // Called before the new tree reaches WinUI: the metrics still
            // hold the extent before the parts built above.
            self.above = self.above.or(self.metrics.map(|metrics| Above {
                extent: metrics.extent_height,
                width: metrics.viewport_width,
                height: metrics.viewport_height,
            }));
        } else {
            self.above = None;
        }
        self.shown = input.clone();
        self.count_fresh();
    }

    fn update(&mut self, step: FollowStep, _context: &ComponentContext<Self>) {
        match step {
            FollowStep::Scrolled(metrics) => {
                let before = self.metrics.replace(metrics);
                // The first extent change at the same viewport size after
                // the top changed moves the offset by the height it added
                // or removed; the same extent waits for the layout.
                if let Some(above) = self.above {
                    if metrics.viewport_width != above.width
                        || metrics.viewport_height != above.height
                    {
                        self.above = None;
                    } else if metrics.extent_height != above.extent {
                        self.above = None;
                        let _ = self.shown.scroll.request_scroll_to(
                            0.0,
                            metrics.vertical_offset + (metrics.extent_height - above.extent),
                            false,
                            |_| {},
                        );
                        self.count_fresh();
                        return;
                    }
                }
                if at_end(&metrics) {
                    self.away = None;
                } else if self.away.is_none()
                    && before.is_none_or(|before| metrics.vertical_offset < before.vertical_offset)
                {
                    // The owner scrolled up (or the first metrics already
                    // find the view off its end): content growing at the
                    // end moves only the end, never the offset back.
                    self.away = self
                        .shown
                        .items
                        .0
                        .last()
                        .map(|item| (item.key.clone(), item.parts.len()));
                }
                self.count_fresh();
            }
            FollowStep::Jump => {
                // WinUI clamps the offset to the end of the content.
                let _ = self
                    .shown
                    .scroll
                    .request_scroll_to(0.0, f64::MAX, false, |_| {});
            }
        }
    }

    fn view(&self, _input: &FollowInput, context: &mut ViewContext<Self>) -> View {
        if self.away.is_none() {
            // Nothing over the transcript at the end (no background: the
            // pointer reaches the view below).
            return Border::new()
                .horizontal_alignment(HorizontalAlignment::Center)
                .vertical_alignment(VerticalAlignment::Bottom)
                .into();
        }
        let words = match self.fresh {
            Some(fresh) if fresh > 0 => format!("Jump to latest \u{b7} {fresh} new"),
            _ => "Jump to latest".to_owned(),
        };
        crate::controls::outlined()
            .horizontal_alignment(HorizontalAlignment::Center)
            .vertical_alignment(VerticalAlignment::Bottom)
            .margin(Thickness::new(0.0, 0.0, 0.0, tokens::spacing::MD))
            .automation_name(words.clone())
            .on_click(context.message(FollowStep::Jump))
            .content(crate::controls::icon_label(
                crate::controls::glyph::ARROW_DOWN,
                &words,
            ))
            .into()
    }
}

/// How many steps a running turn's open log shows before "Show all N".
const LIVE_STEPS: usize = 5;
/// How many files the Edited files card lists before "Show N more".
const EDITED_FILES: usize = 5;

/// The expanded-set keys of a work log: a finished turn's log is open
/// with `open:`, a running one closed with `shut:`, and a running one
/// shows every step with `all:`.
pub fn log_key(prefix: &str, log: &str) -> String {
    format!("{prefix}:{log}")
}

/// Whether a part ends a turn's work log: the answer, a failure, the end
/// notice (each carries how the turn ended), the edited files.
fn ends_log(part: &RenderedPart) -> bool {
    part.end.is_some()
        || matches!(
            part.kind,
            PartKind::Final | PartKind::TurnFailed | PartKind::Edited
        )
}

/// The turns of one conversation, its oldest `skip` parts left out; each
/// part its own [`PartView`], a turn's work log folded under its head.
fn turn_views(input: &TranscriptInput, skip: usize) -> Vec<KeyedView> {
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
            let first = skip;
            skip = 0;
            let parts = &item.parts;
            // The log's body: the parts after its head up to the answer.
            let log = parts.iter().position(|part| part.kind == PartKind::Log);
            let end = log.map(|at| {
                parts[at + 1..]
                    .iter()
                    .position(|part| ends_log(part))
                    .map_or(parts.len(), |offset| at + 1 + offset)
            });
            let mut hidden: HashSet<usize> = HashSet::new();
            let mut head: Option<View> = None;
            if let (Some(at), Some(end)) = (log, end) {
                let part = &parts[at];
                let mut fields = part.plain.split(crate::chat::TOOL_FIELDS);
                let running = fields.next() == Some("running");
                let started = fields.next().and_then(|ms| ms.parse::<i64>().ok());
                let elapsed = fields.next().and_then(|ms| ms.parse::<i64>().ok());
                let doing = fields.next().unwrap_or_default().to_owned();
                let open = if running {
                    !input.expanded.contains(&log_key("shut", &part.key))
                } else {
                    input.expanded.contains(&log_key("open", &part.key))
                };
                let body: Vec<usize> = (at + 1..end).collect();
                let steps: Vec<usize> = body
                    .iter()
                    .copied()
                    .filter(|index| parts[*index].kind == PartKind::Tools)
                    .collect();
                let all = input.expanded.contains(&log_key("all", &part.key));
                let mut more = None;
                if !open {
                    hidden.extend(body.iter().copied());
                } else if running && !all && steps.len() > LIVE_STEPS {
                    // A running log shows its last five steps (and what
                    // came after the first of them).
                    let from = steps[steps.len() - LIVE_STEPS];
                    hidden.extend(body.iter().copied().filter(|index| *index < from));
                    more = Some(steps.len());
                }
                head = Some(log_head(
                    part,
                    LogHead {
                        running,
                        open,
                        started,
                        elapsed,
                        doing,
                        more,
                    },
                    &input.callbacks,
                ));
            }
            let mut views: Vec<KeyedView> = Vec::new();
            for (index, part) in parts.iter().enumerate().skip(first) {
                if hidden.contains(&index) {
                    continue;
                }
                match part.kind {
                    PartKind::Log => {
                        if let Some(head) = head.take() {
                            views.push(KeyedView::new(part.key.clone(), head));
                        }
                    }
                    PartKind::Edited => views.push(KeyedView::new(
                        part.key.clone(),
                        edited_card(
                            part,
                            input.expanded.contains(&log_key("more", &part.key)),
                            &input.callbacks,
                        ),
                    )),
                    _ => {
                        // Every part keeps its key while it streams: an
                        // open step and a selection survive its updates.
                        views.push(KeyedView::new(
                            part.key.clone(),
                            View::component::<PartView>(PartInput {
                                part: Shared(Rc::clone(part)),
                                expanded: input.expanded.contains(&part.key),
                                callbacks: Rc::clone(&input.callbacks),
                            }),
                        ));
                    }
                }
            }
            // The live draft where its committed message will be: after the
            // turn's last part, keyed by its message, so each revision
            // updates the same block (no live region: nothing is announced
            // per revision).
            if let Some(draft) = input.draft.as_ref().filter(|draft| draft.item == item.key) {
                views.push(KeyedView::new(
                    draft.part.0.key.clone(),
                    View::component::<PartView>(PartInput {
                        part: draft.part.clone(),
                        expanded: false,
                        callbacks: Rc::clone(&input.callbacks),
                    }),
                ));
                if draft.truncated {
                    views.push(KeyedView::new(
                        "draft-truncated",
                        label("The full message appears when the turn ends.")
                            .text_wrapping(TextWrapping::Wrap),
                    ));
                }
            }
            // A turn stretches across the reading column: a short turn's
            // bubble still reaches the right edge.
            Some(KeyedView::new(
                item.key.clone(),
                StackPanel::new()
                    .spacing(tokens::spacing::SM)
                    .margin(Thickness::new(0.0, 0.0, 0.0, tokens::spacing::LG))
                    .keyed_children(views),
            ))
        })
        .collect()
}

/// What a work log's head shows.
struct LogHead {
    running: bool,
    open: bool,
    started: Option<i64>,
    elapsed: Option<i64>,
    /// What the running step does ("Running commands").
    doing: String,
    /// A running log hides its earlier steps: the count of all of them.
    more: Option<usize>,
}

/// A work log's head (`.lg-head`): the chevron, "Worked for 4m 12s · 8
/// commands · 3 files changed"; while the
/// turn runs, "Working for …" counting and what the running step does, with
/// the loader, on the right. A click opens or closes the log.
fn log_head(part: &RenderedPart, head: LogHead, callbacks: &RowCallbacks) -> View {
    let words = part.label.clone();
    let summary = |time: Option<String>| -> String {
        let verb = if head.running { "Working" } else { "Worked" };
        match (time, words.is_empty()) {
            (Some(time), true) => format!("{verb} for {time}"),
            (Some(time), false) => format!("{verb} for {time} \u{b7} {words}"),
            (None, true) => verb.to_owned(),
            (None, false) => format!("{verb} \u{b7} {words}"),
        }
    };
    let text: View = match (head.running, head.started) {
        (true, Some(started_ms)) => View::component::<Elapsed>(ElapsedInput {
            started_ms,
            words: Some(words.clone()),
        }),
        _ => crate::controls::text(
            summary(head.elapsed.map(crate::chat::short_duration)),
            tokens::text::BUTTON,
        )
        .vertical_alignment(VerticalAlignment::Center)
        .text_trimming(TextTrimming::CharacterEllipsis)
        .into(),
    };
    // An open log shows the running step itself: the label only stands in
    // for it while the log is closed.
    let doing: Option<View> = (head.running && !head.open && !head.doing.is_empty()).then(|| {
        StackPanel::new()
            .orientation(Orientation::Horizontal)
            .spacing(tokens::spacing::SM)
            .grid_column(2)
            .vertical_alignment(VerticalAlignment::Center)
            .children((
                crate::controls::row_mark(crate::controls::RowMark::Working, &head.doing),
                crate::controls::text(head.doing.clone(), tokens::text::LABEL)
                    .foreground(ThemeBrush::AccentText)
                    .vertical_alignment(VerticalAlignment::Center),
            ))
            .into()
    });
    let toggle = callbacks.toggle_tools.clone();
    let (state_key, set) = if head.running {
        (log_key("shut", &part.key), head.open)
    } else {
        (log_key("open", &part.key), !head.open)
    };
    let button = crate::controls::step_row(false)
        .automation_name(format!(
            "{} ({})",
            summary(head.elapsed.map(crate::chat::short_duration)),
            if head.open { "open" } else { "closed" }
        ))
        .on_click(move || {
            toggle.call((state_key.clone(), set));
        })
        .content(
            Grid::new()
                .columns([GridLength::Auto, GridLength::STAR, GridLength::Auto])
                .column_spacing(tokens::spacing::SM)
                .keyed_children(crate::views::slots([
                    Some(crate::controls::icon(if head.open {
                        crate::controls::glyph::CHEVRON_UP
                    } else {
                        crate::controls::glyph::CHEVRON_DOWN
                    })),
                    Some(
                        Border::new()
                            .grid_column(1)
                            .vertical_alignment(VerticalAlignment::Center)
                            .content(text)
                            .into(),
                    ),
                    doing,
                ])),
        );
    let more: Option<View> = head.more.map(|steps| {
        let toggle = callbacks.toggle_tools.clone();
        let key = log_key("all", &part.key);
        crate::controls::text_button()
            .horizontal_alignment(HorizontalAlignment::Left)
            .automation_name(format!("Show all {steps} steps"))
            .on_click(move || {
                toggle.call((key.clone(), true));
            })
            .content(crate::controls::label(&format!("Show all {steps}")))
            .into()
    });
    StackPanel::new()
        .spacing(tokens::spacing::ROW_GAP)
        .keyed_children(crate::views::slots([Some(button.into()), more]))
        .into()
}

/// The changed files under an answer (§9.1): "N files changed" with View
/// changes (the Changes tab on Last turn), up to five paths, "Show N
/// more"; a plain list, no card.
fn edited_card(part: &RenderedPart, all: bool, callbacks: &RowCallbacks) -> View {
    let shown = if all {
        part.detail.len()
    } else {
        part.detail.len().min(EDITED_FILES)
    };
    // The caption and View changes on one line, the paths under it: a
    // plain list under the answer, no card around it.
    let mut rows: Vec<KeyedView> = vec![KeyedView::new(
        "caption",
        crate::controls::row_grid(
            None,
            crate::controls::secondary(crate::chat::count_words(
                part.detail.len(),
                "file changed",
                "files changed",
            ))
            .vertical_alignment(VerticalAlignment::Center)
            .into(),
            Some(
                crate::controls::text_button()
                    .automation_name("View changes")
                    .on_click(callbacks.view_changes.clone())
                    .content(crate::controls::label("View changes"))
                    .into(),
            ),
            tokens::spacing::SM,
        ),
    )];
    for path in part.detail.iter().take(shown) {
        rows.push(KeyedView::new(
            path.clone(),
            Grid::new()
                .columns([GridLength::Auto, GridLength::STAR])
                .column_spacing(tokens::spacing::SM)
                .automation_name(format!("Edited {path}"))
                .children((
                    crate::controls::muted_icon(crate::controls::glyph::EDIT),
                    crate::controls::body(path.clone())
                        .grid_column(1)
                        .vertical_alignment(VerticalAlignment::Center)
                        .text_trimming(TextTrimming::CharacterEllipsis)
                        .is_text_selection_enabled(true),
                )),
        ));
    }
    if part.detail.len() > EDITED_FILES && !all {
        let toggle = callbacks.toggle_tools.clone();
        let key = log_key("more", &part.key);
        let hidden = part.detail.len() - EDITED_FILES;
        rows.push(KeyedView::new(
            "more",
            crate::controls::text_button()
                .horizontal_alignment(HorizontalAlignment::Left)
                .automation_name(format!("Show {hidden} more edited files"))
                .on_click(move || {
                    toggle.call((key.clone(), true));
                })
                .content(crate::controls::label(&format!("Show {hidden} more"))),
        ));
    }
    StackPanel::new()
        .spacing(tokens::spacing::XS)
        .keyed_children(rows)
        .into()
}

/// One part's input: the rendered part (replaced only on a new revision)
/// and how it is drawn.
#[derive(Clone, PartialEq)]
pub struct PartInput {
    pub part: Shared<RenderedPart>,
    pub expanded: bool,
    pub callbacks: Rc<RowCallbacks>,
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
        let part = &*input.part.0;
        let callbacks = &*input.callbacks;
        let body = StackPanel::new()
            .spacing(tokens::spacing::XS)
            .keyed_children(crate::views::slots([
                part_view(part, input.expanded, self.hovered, callbacks),
                // The assistant's files are actions; the owner's own message
                // only names them.
                if matches!(part.kind, PartKind::Progress | PartKind::Final) {
                    path_actions(&part.key, &part.file_paths, callbacks)
                } else {
                    None
                },
            ]));
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
                .into()
        } else {
            body.into()
        }
    }
}

/// The conversation's one clock: a single ticker thread advances it once a
/// second while the conversation is shown and the window is not minimized,
/// and every running step's and log head's time waits on it, so nothing
/// ticks while it cannot be seen.
pub mod clock {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Condvar, Mutex, Once};
    use std::time::Duration;

    static TICK: Mutex<u64> = Mutex::new(0);
    static NEXT: Condvar = Condvar::new();
    static SHOWN: AtomicBool = AtomicBool::new(false);
    static STARTED: Once = Once::new();

    const PERIOD: Duration = Duration::from_secs(1);

    /// Whether a conversation is on screen (the window says so each render).
    pub fn set_shown(shown: bool) {
        SHOWN.store(shown, Ordering::Relaxed);
    }

    fn minimized() -> bool {
        crate::app_window::window_minimized()
    }

    fn start() {
        STARTED.call_once(|| {
            let _ = std::thread::Builder::new()
                .name("conversation-clock".to_owned())
                .spawn(|| {
                    loop {
                        std::thread::sleep(PERIOD);
                        if !SHOWN.load(Ordering::Relaxed) || minimized() {
                            continue;
                        }
                        if let Ok(mut tick) = TICK.lock() {
                            *tick += 1;
                        }
                        NEXT.notify_all();
                    }
                });
        });
    }

    /// The clock's count now.
    pub fn now() -> u64 {
        start();
        TICK.lock().map_or(0, |tick| *tick)
    }

    /// Wait until the clock passes `seen`, or `cancelled`: the new count.
    pub fn next(seen: u64, cancelled: &dyn Fn() -> bool) -> u64 {
        start();
        let Ok(mut tick) = TICK.lock() else {
            return seen;
        };
        while *tick <= seen && !cancelled() {
            match NEXT.wait_timeout(tick, PERIOD) {
                Ok((guard, _)) => tick = guard,
                Err(_) => return seen,
            }
        }
        *tick
    }
}

/// A running tool row's start and its caption.
#[derive(Clone, PartialEq)]
pub struct ElapsedInput {
    pub started_ms: i64,
    /// A work log's head: "Working for 3m 12s · {words}"; a step's time
    /// otherwise.
    pub words: Option<String>,
}

/// A running step's time or a running log's head, redrawn on the shared
/// [`clock`]: the tick redraws this caption only, never the transcript or
/// the window around it, and stops while the conversation is not shown.
pub struct Elapsed {
    now_ms: i64,
}

fn tick_later(seen: u64, context: &ComponentContext<Elapsed>) {
    let _task =
        context.spawn_background(move |cancel| clock::next(seen, &|| cancel.is_cancelled()));
}

impl Component for Elapsed {
    type Input = ElapsedInput;
    /// The shared clock's new count.
    type Message = u64;

    fn create(_input: &ElapsedInput, context: &ComponentContext<Self>) -> Self {
        tick_later(clock::now(), context);
        Self {
            now_ms: crate::handoff::now_ms(),
        }
    }

    fn update(&mut self, tick: u64, context: &ComponentContext<Self>) {
        self.now_ms = crate::handoff::now_ms();
        tick_later(tick, context);
    }

    fn view(&self, input: &ElapsedInput, _context: &mut ViewContext<Self>) -> View {
        let time = crate::chat::short_duration(self.now_ms - input.started_ms);
        match &input.words {
            Some(words) => crate::controls::text(
                if words.is_empty() {
                    format!("Working for {time}")
                } else {
                    format!("Working for {time} \u{b7} {words}")
                },
                tokens::text::BUTTON,
            )
            .vertical_alignment(VerticalAlignment::Center)
            .text_trimming(TextTrimming::CharacterEllipsis)
            .into(),
            None => crate::controls::tool_caption(time, None).into(),
        }
    }
}

/// Most file chips one message shows before "+N more".
const FILE_CHIPS: usize = 6;

/// The files a message names, as compact chips under it (file glyph and
/// name, the full path in the tooltip; one chip per file however often it
/// is named). A click opens it in Explorer (`ui::reveal`); Reactor 0.100.0
/// hyperlinks only navigate URIs, and prose selection is kept.
fn path_actions(part: &str, paths: &[String], callbacks: &RowCallbacks) -> Option<View> {
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
        return None;
    }
    let mut note: Option<View> = None;
    let mut chips: Vec<KeyedView> = Vec::new();
    for (reference, resolved) in files.iter().take(FILE_CHIPS) {
        let item = format!("{}{part}:{reference}", callbacks.path_scope);
        if let Some((failed, text)) = &callbacks.path_failure
            && failed == &item
        {
            note = Some(
                crate::controls::secondary(text.clone())
                    .foreground(ThemeBrush::SystemCritical)
                    .text_wrapping(TextWrapping::Wrap)
                    .is_text_selection_enabled(true)
                    .into(),
            );
        }
        let open = callbacks.open_path.clone();
        let target = resolved.clone();
        let name = crate::attachments::file_name(resolved).to_owned();
        chips.push(KeyedView::new(
            resolved.clone(),
            crate::controls::file_chip()
                .automation_name(format!("Open {resolved} in Explorer"))
                .on_click(move || {
                    open.call((item.clone(), target.clone()));
                })
                .content(crate::controls::file_chip_content(
                    file_glyph(resolved),
                    &name,
                ))
                .tooltip(resolved.clone()),
        ));
    }
    if files.len() > FILE_CHIPS {
        chips.push(KeyedView::new(
            "more",
            crate::controls::secondary(format!("+{} more", files.len() - FILE_CHIPS))
                .vertical_alignment(VerticalAlignment::Center),
        ));
    }
    Some(
        StackPanel::new()
            .spacing(tokens::spacing::XS)
            .keyed_children(crate::views::slots([
                Some(
                    StackPanel::new()
                        .orientation(Orientation::Horizontal)
                        .spacing(tokens::spacing::CHIP_GAP)
                        .keyed_children(chips)
                        .into(),
                ),
                note,
            ]))
            .into(),
    )
}

/// A folder's glyph for a folder, else a file's.
fn file_glyph(path: &str) -> &'static str {
    if std::path::Path::new(path).is_dir() {
        crate::controls::glyph::FOLDER
    } else {
        crate::controls::glyph::FILE
    }
}

/// The files the owner attached to a message: chips under the bubble, at
/// its right edge (`attachments`).
pub fn attached_chips(files: &[String]) -> View {
    StackPanel::new()
        .orientation(Orientation::Horizontal)
        .spacing(tokens::spacing::CHIP_GAP)
        .horizontal_alignment(HorizontalAlignment::Right)
        .keyed_children(
            files
                .iter()
                .map(|path| {
                    KeyedView::new(
                        path.clone(),
                        crate::controls::fixed_chip(
                            false,
                            Some(crate::controls::glyph::FILE),
                            crate::attachments::file_name(path),
                            &format!("Attached file {path}"),
                        )
                        .tooltip(path.clone()),
                    )
                })
                .collect::<Vec<_>>(),
        )
        .into()
}
