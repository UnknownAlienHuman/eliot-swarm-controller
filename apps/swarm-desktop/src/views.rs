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

/// Message text: M3 body-large.
fn body_text(text: &str, foreground: Brush) -> TextBlock {
    TextBlock::new()
        .text(text.to_owned())
        .font_size(theme::type_scale::BODY_LARGE.size)
        .text_wrapping(TextWrapping::Wrap)
        .is_text_selection_enabled(true)
        .foreground(foreground)
}

/// An inline control is pulled back by this so its content lines up with
/// the text around it (a 32-DIP icon button centres a 16-DIP glyph).
const INSET: f64 = 8.0;

/// A small icon button for Copy, named for screen readers.
fn copy_button(name: &str, palette: Palette, callback: impl Fn() + 'static) -> Button {
    crate::m3::icon_button(palette)
        .width(32.0)
        .height(32.0)
        .automation_name(name.to_owned())
        .on_click(callback)
}

/// The glyph of a copy button.
fn copy_glyph() -> View {
    crate::m3::icon_sized(crate::m3::glyph::COPY, 16.0)
}

fn block_views(part: &RenderedPart, callbacks: &RowCallbacks, palette: Palette) -> Vec<KeyedView> {
    let Some(blocks) = part.blocks.as_ref() else {
        return vec![KeyedView::new(
            "plain",
            body_text(&part.plain, palette.on_surface()),
        )];
    };
    blocks
        .iter()
        .enumerate()
        .map(|(index, block)| {
            let view: View = match block {
                RenderedBlock::Rich(text) => RichTextBlock::new()
                    .paragraphs(text.clone())
                    .font_size(theme::type_scale::BODY_LARGE.size)
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
                    .font_weight(FontWeight::MEDIUM)
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
                            .background(palette.raised())
                            .corner_radius(theme::shape::MD)
                            .padding(Thickness::uniform(theme::spacing::MD))
                            .content(
                                ScrollViewer::new()
                                    .horizontal_scroll_bar_visibility(ScrollBarVisibility::Auto)
                                    .vertical_scroll_bar_visibility(ScrollBarVisibility::Disabled)
                                    .content(
                                        TextBlock::new()
                                            .text(text.clone())
                                            .font_size(theme::type_scale::BODY_MEDIUM.size)
                                            .text_wrapping(TextWrapping::NoWrap)
                                            .is_text_selection_enabled(true),
                                    ),
                            ),
                        copy_button("Copy code", palette, move || {
                            let _ = copy.call((key.clone(), code_index));
                        })
                        .margin(Thickness::new(-INSET, 0.0, 0.0, 0.0))
                        .content(copy_glyph()),
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

/// `anchor`, when given, binds the part's own button (Copy or the tool
/// group header): the end anchor of the transcript.
///
/// One style for every part, as in a chat: the user's message is a
/// secondary-container bubble on the right; the answer is plain body-large
/// text on the surface; tool groups are one low-emphasis expandable row;
/// interim notes and notices are quiet lines; copy is a small icon button.
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
        PartKind::Input => StackPanel::new()
            .spacing(2.0)
            .horizontal_alignment(HorizontalAlignment::Right)
            .max_width(theme::layout::READING_WIDTH * 0.8)
            .children((
                Border::new()
                    .background(palette.bubble())
                    // M3 large corners, the small one toward the sender.
                    .corner_radius(CornerRadius::new(
                        theme::shape::LG,
                        theme::shape::LG,
                        theme::shape::XS,
                        theme::shape::LG,
                    ))
                    .padding(Thickness::xy(theme::spacing::MD, 10.0))
                    .content(body_text(&part.plain, palette.on_bubble())),
                anchored(copy_button("Copy message", palette, copy_message))
                    .horizontal_alignment(HorizontalAlignment::Right)
                    .margin(Thickness::new(0.0, 0.0, -INSET, 0.0))
                    .content(copy_glyph()),
            )),
        // Interim notes stay quiet.
        PartKind::Progress => StackPanel::new()
            .spacing(theme::spacing::SM)
            .opacity(theme::MUTED_OPACITY)
            .keyed_children(block_views(part, callbacks, palette)),
        PartKind::Tools => {
            let toggle = callbacks.toggle_tools.clone();
            let key = part.key.clone();
            // A low-emphasis button rather than a ToggleButton: Reactor
            // 0.100.0 themes only Buttons, so a checked toggle would show the
            // system accent. The accessible name carries the state.
            let header = anchored(
                Button::new()
                    .resource_overrides(palette.icon_button())
                    .height(36.0),
            )
            .horizontal_alignment(HorizontalAlignment::Left)
            .margin(Thickness::new(-12.0, 0.0, 0.0, 0.0))
            .automation_name(format!(
                "Tool operations: {} ({})",
                part.plain,
                if expanded { "expanded" } else { "collapsed" }
            ))
            .on_click(move || {
                let _ = toggle.call((key.clone(), !expanded));
            })
            .content(
                StackPanel::new()
                    .orientation(Orientation::Horizontal)
                    .spacing(theme::spacing::SM)
                    .margin(Thickness::xy(-12.0, 0.0))
                    .children((
                        crate::m3::icon_sized(crate::m3::glyph::TOOLS, 16.0),
                        TextBlock::new()
                            .text(part.plain.clone())
                            .font_size(theme::type_scale::BODY_MEDIUM.size)
                            .font_weight(FontWeight::NORMAL)
                            .vertical_alignment(VerticalAlignment::Center),
                        crate::m3::icon_sized(
                            if expanded {
                                crate::m3::glyph::CHEVRON_DOWN
                            } else {
                                crate::m3::glyph::CHEVRON_RIGHT
                            },
                            10.0,
                        ),
                    )),
            );
            if expanded {
                StackPanel::new().spacing(theme::spacing::XS).children((
                    header,
                    Border::new()
                        .background(palette.card())
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
        // The answer: plain text on the surface; a label only when the
        // outcome is not a plain success.
        PartKind::Final => {
            let note: View = if part.label == "Assistant" {
                View::empty()
            } else {
                label(&part.label).into()
            };
            StackPanel::new().spacing(theme::spacing::SM).children((
                note,
                StackPanel::new()
                    .spacing(theme::spacing::SM)
                    .keyed_children(block_views(part, callbacks, palette)),
                anchored(copy_button("Copy message", palette, copy_message))
                    .horizontal_alignment(HorizontalAlignment::Left)
                    .margin(Thickness::new(-INSET, 0.0, 0.0, 0.0))
                    .content(copy_glyph()),
            ))
        }
        PartKind::Working => StackPanel::new().spacing(theme::spacing::XS).children((
            label(&part.label),
            ProgressBar::new()
                .is_indeterminate(true)
                .width(160.0)
                .horizontal_alignment(HorizontalAlignment::Left),
        )),
        PartKind::Notice => TextBlock::new()
            .text(format!("{} · {}", part.label, part.plain))
            .font_size(theme::type_scale::BODY_SMALL.size)
            .text_wrapping(TextWrapping::Wrap)
            .opacity(theme::MUTED_OPACITY)
            .into(),
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
    width: Option<f64>,
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
    // An ItemsRepeater arranges a turn at its content width, so the turn
    // takes the reading column's width explicitly: a short turn's bubble
    // still reaches the right edge.
    StackPanel::new()
        .width(width)
        .spacing(theme::spacing::SM)
        .margin(Thickness::new(0.0, 0.0, 0.0, theme::spacing::LG))
        .keyed_children(parts)
}

/// Order-sensitive hash of the item keys: Reactor's `key_revision` must
/// change exactly when the key list changes.
pub fn key_revision(items: &[Rc<RenderedItem>]) -> u64 {
    items.iter().fold(0xcbf2_9ce4_8422_2325_u64, |hash, item| {
        (hash ^ text_hash(&item.key)).wrapping_mul(0x0100_0000_01b3)
    })
}
