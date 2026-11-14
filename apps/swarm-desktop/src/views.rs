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
        .font_size(theme::type_scale::LABEL)
        .font_weight(FontWeight::SEMI_BOLD)
        .opacity(theme::MUTED_OPACITY)
}

fn body_text(text: &str) -> TextBlock {
    TextBlock::new()
        .text(text.to_owned())
        .font_size(theme::type_scale::BODY)
        .text_wrapping(TextWrapping::Wrap)
        .is_text_selection_enabled(true)
        .foreground(theme::winui::text_primary())
}

fn copy_button(name: &str, callback: impl Fn() + 'static) -> View {
    Button::new()
        .style(ButtonStyle::Subtle)
        .automation_name(name.to_owned())
        .on_click(callback)
        .content(
            TextBlock::new()
                .text(name.to_owned())
                .font_size(theme::type_scale::LABEL),
        )
}

fn block_views(part: &RenderedPart, callbacks: &RowCallbacks) -> Vec<KeyedView> {
    let Some(blocks) = part.blocks.as_ref() else {
        return vec![KeyedView::new("plain", body_text(&part.plain))];
    };
    blocks
        .iter()
        .enumerate()
        .map(|(index, block)| {
            let view: View = match block {
                RenderedBlock::Rich(text) => RichTextBlock::new()
                    .paragraphs(text.clone())
                    .font_size(theme::type_scale::BODY)
                    .text_wrapping(TextWrapping::Wrap)
                    .is_text_selection_enabled(true)
                    .into(),
                RenderedBlock::Heading(level, text) => TextBlock::new()
                    .text(text.clone())
                    .font_size(match level {
                        1 => theme::type_scale::TITLE,
                        2 => theme::type_scale::HEADING,
                        _ => theme::type_scale::SUBHEADING,
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
                            .background(theme::winui::surface())
                            .border_brush(theme::winui::stroke())
                            .border_thickness(1.0)
                            .corner_radius(theme::radius::SM)
                            .padding(Thickness::uniform(theme::spacing::SM))
                            .content(
                                ScrollViewer::new()
                                    .horizontal_scroll_bar_visibility(ScrollBarVisibility::Auto)
                                    .vertical_scroll_bar_visibility(ScrollBarVisibility::Disabled)
                                    .content(
                                        TextBlock::new()
                                            .text(text.clone())
                                            .font_size(theme::type_scale::LABEL + 1.0)
                                            .text_wrapping(TextWrapping::NoWrap)
                                            .is_text_selection_enabled(true),
                                    ),
                            ),
                        copy_button("Copy code", move || {
                            let _ = copy.call((key.clone(), code_index));
                        }),
                    ))
                }
                RenderedBlock::Rule => Border::new()
                    .height(1.0)
                    .background(theme::winui::stroke())
                    .into(),
            };
            KeyedView::new(index, view)
        })
        .collect()
}

fn part_view(part: &RenderedPart, expanded: bool, callbacks: &RowCallbacks) -> View {
    let copy = callbacks.copy_part.clone();
    let key = part.key.clone();
    let copy_message = move || {
        let _ = copy.call(key.clone());
    };
    match part.kind {
        PartKind::Input => Border::new()
            .background(theme::winui::surface())
            .border_brush(theme::winui::stroke())
            .border_thickness(1.0)
            .corner_radius(theme::radius::MD)
            .padding(Thickness::uniform(theme::spacing::MD))
            .content(StackPanel::new().spacing(theme::spacing::XS).children((
                label(&part.label),
                body_text(&part.plain),
                copy_button("Copy message", copy_message),
            ))),
        PartKind::Progress => StackPanel::new()
            .spacing(theme::spacing::XS)
            .opacity(0.9)
            .children((
                label(&part.label),
                StackPanel::new()
                    .spacing(theme::spacing::SM)
                    .keyed_children(block_views(part, callbacks)),
            )),
        PartKind::Tools => {
            let toggle = callbacks.toggle_tools.clone();
            let key = part.key.clone();
            let header = ToggleButton::new()
                .is_checked(expanded)
                .automation_name(format!("Tool operations: {}", part.plain))
                .on_is_checked_changed(move |checked: bool| {
                    let _ = toggle.call((key.clone(), checked));
                })
                .content(
                    TextBlock::new()
                        .text(format!(
                            "{} {}",
                            if expanded { "▾" } else { "▸" },
                            part.plain
                        ))
                        .font_size(theme::type_scale::LABEL),
                );
            if expanded {
                StackPanel::new().spacing(theme::spacing::XS).children((
                    header,
                    TextBlock::new()
                        .text(part.detail.join("\n"))
                        .font_size(theme::type_scale::LABEL)
                        .is_text_selection_enabled(true)
                        .opacity(theme::MUTED_OPACITY),
                ))
            } else {
                header
            }
        }
        PartKind::Final => {
            let mut actions: Vec<KeyedView> = vec![KeyedView::new(
                "copy",
                copy_button("Copy message", copy_message),
            )];
            if part.code_blocks > 0 {
                actions.push(KeyedView::new(
                    "codes",
                    label(&format!("{} code block(s)", part.code_blocks)),
                ));
            }
            StackPanel::new().spacing(theme::spacing::SM).children((
                TextBlock::new()
                    .text(part.label.clone())
                    .font_size(theme::type_scale::LABEL)
                    .font_weight(FontWeight::BOLD)
                    .foreground(theme::winui::text_accent()),
                StackPanel::new()
                    .spacing(theme::spacing::SM)
                    .keyed_children(block_views(part, callbacks)),
                StackPanel::new()
                    .orientation(Orientation::Horizontal)
                    .spacing(theme::spacing::SM)
                    .keyed_children(actions),
            ))
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
                .font_size(theme::type_scale::LABEL)
                .text_wrapping(TextWrapping::Wrap)
                .opacity(theme::MUTED_OPACITY),
        )),
    }
}

/// One realized turn: keyed parts, so new activity appends children instead
/// of replacing the item.
pub fn item_view(
    item: &RenderedItem,
    expanded: &HashSet<String>,
    callbacks: &RowCallbacks,
) -> View {
    let parts: Vec<KeyedView> = item
        .parts
        .iter()
        .map(|part| {
            KeyedView::new(
                part.key.clone(),
                part_view(part, expanded.contains(&part.key), callbacks),
            )
        })
        .collect();
    StackPanel::new()
        .spacing(theme::spacing::MD)
        .margin(Thickness::new(
            0.0,
            0.0,
            theme::spacing::MD,
            theme::spacing::LG,
        ))
        .keyed_children(parts)
}

/// Order-sensitive hash of the item keys: Reactor's `key_revision` must
/// change exactly when the key list changes.
pub fn key_revision(items: &[Rc<RenderedItem>]) -> u64 {
    items.iter().fold(0xcbf2_9ce4_8422_2325_u64, |hash, item| {
        (hash ^ text_hash(&item.key)).wrapping_mul(0x0100_0000_01b3)
    })
}
