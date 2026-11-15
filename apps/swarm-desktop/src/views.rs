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

/// One display block, ready for the UI thread.
#[derive(Clone)]
pub enum RenderedBlock {
    Rich(RichText),
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
    pub code_blocks: usize,
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

fn render_doc(doc: &Doc) -> Vec<RenderedBlock> {
    let mut blocks = Vec::new();
    let mut paragraphs: Vec<RichTextParagraph> = Vec::new();
    let mut code_index = 0;
    let flush = |paragraphs: &mut Vec<RichTextParagraph>, blocks: &mut Vec<RenderedBlock>| {
        if !paragraphs.is_empty() {
            blocks.push(RenderedBlock::Rich(RichText::new(std::mem::take(
                paragraphs,
            ))));
        }
    };
    for block in &doc.blocks {
        match block {
            Block::Paragraph(inlines) => {
                paragraphs.push(RichTextParagraph::new(rich_inlines(inlines)))
            }
            Block::Heading { level, text } => {
                flush(&mut paragraphs, &mut blocks);
                blocks.push(RenderedBlock::Heading(*level, text.clone()));
            }
            Block::Code { text, .. } => {
                flush(&mut paragraphs, &mut blocks);
                blocks.push(RenderedBlock::Code(code_index, text.clone()));
                code_index += 1;
            }
            Block::Rule => {
                flush(&mut paragraphs, &mut blocks);
                blocks.push(RenderedBlock::Rule);
            }
        }
    }
    flush(&mut paragraphs, &mut blocks);
    blocks
}

fn render_part(part: &Part, docs: &HashMap<u64, Doc>) -> RenderedPart {
    let markdown = matches!(part.kind, PartKind::Final | PartKind::Progress);
    let doc = markdown.then(|| docs.get(&text_hash(&part.text))).flatten();
    RenderedPart {
        key: part.key.clone(),
        kind: part.kind,
        label: part.label.clone(),
        plain: chat::preview(&part.text),
        code_blocks: doc.map_or(0, |doc| doc.code_blocks().len()),
        blocks: doc.map(render_doc),
        detail: part.detail.clone(),
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

/// Callbacks a realized row needs; cloned into the virtual source.
#[derive(Clone)]
pub struct RowCallbacks {
    pub copy_part: Callback<String>,
    pub copy_code: Callback<(String, usize)>,
    pub toggle_tools: Callback<(String, bool)>,
}

fn label(text: &str) -> TextBlock {
    TextBlock::new()
        .text(text.to_owned())
        .font_size(theme::type_scale::LABEL_MEDIUM.size)
        .font_weight(FontWeight::SEMI_BOLD)
        .opacity(theme::MUTED_OPACITY)
}

fn body_text(text: &str, palette: Palette) -> TextBlock {
    TextBlock::new()
        .text(text.to_owned())
        .font_size(theme::type_scale::BODY_MEDIUM.size)
        .text_wrapping(TextWrapping::Wrap)
        .is_text_selection_enabled(true)
        .foreground(palette.on_surface())
}

fn copy_button(name: &str, palette: Palette, callback: impl Fn() + 'static) -> Button {
    crate::m3::text_button(palette)
        .automation_name(name.to_owned())
        .on_click(callback)
}

/// The label of a copy button.
fn copy_label(name: &str) -> TextBlock {
    TextBlock::new()
        .text(name.to_owned())
        .font_size(theme::type_scale::LABEL_MEDIUM.size)
}

fn block_views(part: &RenderedPart, callbacks: &RowCallbacks, palette: Palette) -> Vec<KeyedView> {
    let Some(blocks) = part.blocks.as_ref() else {
        return vec![KeyedView::new("plain", body_text(&part.plain, palette))];
    };
    blocks
        .iter()
        .enumerate()
        .map(|(index, block)| {
            let view: View = match block {
                RenderedBlock::Rich(text) => RichTextBlock::new()
                    .paragraphs(text.clone())
                    .font_size(theme::type_scale::BODY_MEDIUM.size)
                    .text_wrapping(TextWrapping::Wrap)
                    .is_text_selection_enabled(true)
                    .into(),
                RenderedBlock::Heading(level, text) => TextBlock::new()
                    .text(text.clone())
                    .font_size(match level {
                        1 => theme::type_scale::TITLE_LARGE.size,
                        2 => theme::type_scale::TITLE_MEDIUM.size,
                        _ => theme::type_scale::TITLE_SMALL.size,
                    })
                    .font_weight(FontWeight::SEMI_BOLD)
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
                    StackPanel::new().spacing(theme::spacing::XS).children((
                        Border::new()
                            .background(palette.surface_container_highest())
                            .corner_radius(theme::shape::MD)
                            .padding(Thickness::uniform(theme::spacing::MD))
                            .content(
                                ScrollViewer::new()
                                    .horizontal_scroll_bar_visibility(ScrollBarVisibility::Auto)
                                    .vertical_scroll_bar_visibility(ScrollBarVisibility::Disabled)
                                    .content(
                                        TextBlock::new()
                                            .text(text.clone())
                                            .font_size(theme::type_scale::BODY_SMALL.size + 1.0)
                                            .text_wrapping(TextWrapping::NoWrap)
                                            .is_text_selection_enabled(true),
                                    ),
                            ),
                        copy_button("Copy code", palette, move || {
                            let _ = copy.call((key.clone(), code_index));
                        })
                        .content(copy_label("Copy code")),
                    ))
                }
                RenderedBlock::Rule => Border::new()
                    .height(1.0)
                    .background(palette.outline_variant())
                    .into(),
            };
            KeyedView::new(index, view)
        })
        .collect()
}

/// `anchor`, when given, binds the part's own button (Copy message or the
/// tool group header): the end anchor of the transcript.
fn part_view(
    part: &RenderedPart,
    expanded: bool,
    callbacks: &RowCallbacks,
    palette: Palette,
    anchor: Option<&ElementRef<Button>>,
) -> View {
    let anchored = |button: Button| match anchor {
        Some(anchor) => button.element_ref(anchor),
        None => button,
    };
    let copy = callbacks.copy_part.clone();
    let key = part.key.clone();
    let copy_message = move || {
        let _ = copy.call(key.clone());
    };
    match part.kind {
        // The user's message: a secondary-container bubble on the right.
        PartKind::Input => StackPanel::new()
            .spacing(theme::spacing::XS)
            .horizontal_alignment(HorizontalAlignment::Right)
            .max_width(theme::layout::READING_WIDTH * 0.8)
            .children((
                label(&part.label).horizontal_alignment(HorizontalAlignment::Right),
                Border::new()
                    .background(palette.secondary_container())
                    .corner_radius(CornerRadius::new(20.0, 4.0, 20.0, 20.0))
                    .padding(Thickness::xy(theme::spacing::LG, theme::spacing::MD))
                    .content(
                        body_text(&part.plain, palette)
                            .foreground(palette.on_secondary_container()),
                    ),
                anchored(copy_button("Copy message", palette, copy_message))
                    .horizontal_alignment(HorizontalAlignment::Right)
                    .content(copy_label("Copy message")),
            )),
        // Interim notes stay unboxed and quiet.
        PartKind::Progress => StackPanel::new()
            .spacing(theme::spacing::XS)
            .opacity(theme::MUTED_OPACITY)
            .children((
                label(&part.label),
                StackPanel::new()
                    .spacing(theme::spacing::SM)
                    .keyed_children(block_views(part, callbacks, palette)),
            )),
        PartKind::Tools => {
            let toggle = callbacks.toggle_tools.clone();
            let key = part.key.clone();
            // An outlined button rather than a ToggleButton: Reactor 0.100.0
            // themes only Buttons, so a checked toggle would show the system
            // accent. The accessible name carries the state.
            let header = anchored(crate::m3::outlined(palette))
                .automation_name(format!(
                    "Tool operations: {} ({})",
                    part.plain,
                    if expanded { "expanded" } else { "collapsed" }
                ))
                .on_click(move || {
                    let _ = toggle.call((key.clone(), !expanded));
                })
                .content(
                    TextBlock::new()
                        .text(format!(
                            "{} {}",
                            if expanded { "▾" } else { "▸" },
                            part.plain
                        ))
                        .font_size(theme::type_scale::LABEL_MEDIUM.size),
                );
            if expanded {
                StackPanel::new().spacing(theme::spacing::XS).children((
                    header,
                    Border::new()
                        .background(palette.surface_container_highest())
                        .corner_radius(theme::shape::MD)
                        .padding(Thickness::uniform(theme::spacing::MD))
                        .content(
                            TextBlock::new()
                                .text(part.detail.join("\n"))
                                .font_size(theme::type_scale::BODY_SMALL.size)
                                .text_wrapping(TextWrapping::Wrap)
                                .is_text_selection_enabled(true)
                                .opacity(theme::MUTED_OPACITY),
                        ),
                ))
            } else {
                header
            }
        }
        // The assistant's answer: a tonal container on the left.
        PartKind::Final => {
            let mut actions: Vec<KeyedView> = vec![KeyedView::new(
                "copy",
                anchored(copy_button("Copy message", palette, copy_message))
                    .content(copy_label("Copy message")),
            )];
            if part.code_blocks > 0 {
                actions.push(KeyedView::new(
                    "codes",
                    label(&format!("{} code block(s)", part.code_blocks))
                        .vertical_alignment(VerticalAlignment::Center),
                ));
            }
            Border::new()
                .background(palette.surface_container_low())
                .corner_radius(CornerRadius::new(4.0, 20.0, 20.0, 20.0))
                .padding(Thickness::new(
                    theme::spacing::LG,
                    theme::spacing::MD,
                    theme::spacing::LG,
                    theme::spacing::XS,
                ))
                .content(
                    StackPanel::new().spacing(theme::spacing::SM).children((
                        TextBlock::new()
                            .text(part.label.clone())
                            .font_size(theme::type_scale::LABEL_MEDIUM.size)
                            .font_weight(FontWeight::SEMI_BOLD)
                            .foreground(palette.primary()),
                        StackPanel::new()
                            .spacing(theme::spacing::SM)
                            .keyed_children(block_views(part, callbacks, palette)),
                        StackPanel::new()
                            .orientation(Orientation::Horizontal)
                            .spacing(theme::spacing::SM)
                            .keyed_children(actions),
                    )),
                )
        }
        PartKind::Working => StackPanel::new().spacing(theme::spacing::XS).children((
            label(&part.label),
            ProgressBar::new()
                .is_indeterminate(true)
                .width(160.0)
                .horizontal_alignment(HorizontalAlignment::Left),
        )),
        PartKind::Notice => StackPanel::new().spacing(theme::spacing::XS).children((
            label(&part.label),
            TextBlock::new()
                .text(part.plain.clone())
                .font_size(theme::type_scale::BODY_SMALL.size)
                .text_wrapping(TextWrapping::Wrap)
                .opacity(theme::MUTED_OPACITY),
        )),
    }
}

/// One realized turn: keyed parts, so new activity appends children instead
/// of replacing the item. `anchor` binds the last part that has a button of
/// its own (see [`part_view`]).
pub fn item_view(
    item: &RenderedItem,
    expanded: &HashSet<String>,
    callbacks: &RowCallbacks,
    palette: Palette,
    anchor: Option<&ElementRef<Button>>,
) -> View {
    let anchored = anchor.and_then(|_| {
        item.parts.iter().rposition(|part| {
            matches!(
                part.kind,
                PartKind::Input | PartKind::Tools | PartKind::Final
            )
        })
    });
    let parts: Vec<KeyedView> = item
        .parts
        .iter()
        .enumerate()
        .map(|(index, part)| {
            KeyedView::new(
                part.key.clone(),
                part_view(
                    part,
                    expanded.contains(&part.key),
                    callbacks,
                    palette,
                    anchor.filter(|_| anchored == Some(index)),
                ),
            )
        })
        .collect();
    StackPanel::new()
        .spacing(theme::spacing::MD)
        .margin(Thickness::new(0.0, 0.0, 0.0, theme::spacing::XL))
        .keyed_children(parts)
}

/// Order-sensitive hash of the item keys: Reactor's `key_revision` must
/// change exactly when the key list changes.
pub fn key_revision(items: &[Rc<RenderedItem>]) -> u64 {
    items.iter().fold(0xcbf2_9ce4_8422_2325_u64, |hash, item| {
        (hash ^ text_hash(&item.key)).wrapping_mul(0x0100_0000_01b3)
    })
}
