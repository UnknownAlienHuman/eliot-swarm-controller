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

/// Callbacks a realized row needs; cloned into the virtual source.
#[derive(Clone)]
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
    /// The part under the pointer: its copy actions show.
    pub hovered: Option<String>,
    /// The pointer entered (`true`) or left a part.
    pub hover: Callback<(String, bool)>,
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
fn copy_button(
    name: &str,
    part: &str,
    callbacks: &RowCallbacks,
    palette: Palette,
    callback: impl Fn() + 'static,
) -> View {
    if callbacks.hovered.as_deref() == Some(part) {
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

fn block_views(part: &RenderedPart, callbacks: &RowCallbacks, palette: Palette) -> Vec<KeyedView> {
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
                RenderedBlock::Rich(text) => RichTextBlock::new()
                    .paragraphs(text.clone())
                    .font_size(tokens::text::BODY.size)
                    .text_wrapping(TextWrapping::Wrap)
                    .is_text_selection_enabled(true)
                    .into(),
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
                            .content(copy_button(
                                "Copy code",
                                &part.key,
                                callbacks,
                                palette,
                                move || {
                                    let _ = copy.call((key.clone(), code_index));
                                },
                            )),
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
                    .content(copy_button(
                        "Copy message",
                        &part.key,
                        callbacks,
                        palette,
                        copy_message,
                    )),
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
            .keyed_children(block_views(part, callbacks, palette)),
        PartKind::Tools => {
            let toggle = callbacks.toggle_tools.clone();
            let key = part.key.clone();
            // "Run command · Completed · ~0.3 s": the name, then the status
            // (Running in the primary role) or the duration at the end.
            let mut fields = part.plain.split(" · ");
            let name = fields.next().unwrap_or_default().to_owned();
            let status = fields.next().unwrap_or_default().to_owned();
            let duration = fields.next().unwrap_or_default().to_owned();
            let running = status == "Running";
            let trailing = match status.as_str() {
                "Running" => "Running".to_owned(),
                "Completed" => duration,
                _ => status,
            };
            let glyph = match name.as_str() {
                "Run command" => crate::m3::glyph::TERMINAL,
                edit if edit.starts_with("Edit") => crate::m3::glyph::EDIT,
                _ => crate::m3::glyph::TOOLS,
            };
            let row = crate::m3::tool_row(palette, running)
                .automation_name(format!(
                    "{} ({})",
                    part.plain,
                    if expanded { "expanded" } else { "collapsed" }
                ))
                .on_click(move || {
                    let _ = toggle.call((key.clone(), !expanded));
                })
                .content(crate::m3::tool_content(
                    glyph,
                    &name,
                    &trailing,
                    running.then(|| palette.primary()),
                ));
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
                    .keyed_children(block_views(part, callbacks, palette)),
                Border::new()
                    .horizontal_alignment(HorizontalAlignment::Left)
                    .margin(Thickness::new(-INSET, 0.0, 0.0, 0.0))
                    .content(copy_button(
                        "Copy message",
                        &part.key,
                        callbacks,
                        palette,
                        copy_message,
                    )),
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

/// One realized turn: keyed parts, so new activity appends children instead
/// of replacing the item.
pub fn item_view(
    item: &RenderedItem,
    expanded: &HashSet<String>,
    callbacks: &RowCallbacks,
    palette: Palette,
    width: Option<f64>,
) -> View {
    let parts: Vec<KeyedView> = item
        .parts
        .iter()
        .map(|part| {
            // A tool row is keyed by whether it runs: WinUI reads a button's
            // colors only when its template is applied, so a finished row
            // would keep the running tint.
            let key = if matches!(part.kind, PartKind::Tools) && part.plain.contains(" · Running")
            {
                format!("{}:running", part.key)
            } else {
                part.key.clone()
            };
            let body = StackPanel::new().spacing(tokens::spacing::XS).children((
                part_view(part, expanded.contains(&part.key), callbacks, palette),
                // The assistant's files are actions; the owner's own
                // message only names them.
                if matches!(part.kind, PartKind::Progress | PartKind::Final) {
                    path_actions(&part.key, &part.file_paths, callbacks, palette)
                } else {
                    View::empty()
                },
            ));
            // A message with Copy is a hover area: its actions show while
            // the pointer is over it (a late exit does not hide the next).
            let view: View = if matches!(
                part.kind,
                PartKind::Input | PartKind::Progress | PartKind::Final
            ) {
                let (enter, leave) = (callbacks.hover.clone(), callbacks.hover.clone());
                let (over, out) = (part.key.clone(), part.key.clone());
                Border::new()
                    .background(Brush::Solid(Color::transparent()))
                    .on_pointer_entered(move |_: PointerEventInfo| {
                        let _ = enter.call((over.clone(), true));
                    })
                    .on_pointer_exited(move |_: PointerEventInfo| {
                        let _ = leave.call((out.clone(), false));
                    })
                    .content(body)
            } else {
                body
            };
            KeyedView::new(key, view)
        })
        .collect();
    // An ItemsRepeater arranges a turn at its content width, so the turn
    // takes the reading column's width explicitly: a short turn's bubble
    // still reaches the right edge.
    StackPanel::new()
        .width(width)
        .spacing(tokens::spacing::SM)
        .margin(Thickness::new(0.0, 0.0, 0.0, tokens::spacing::LG))
        .keyed_children(parts)
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
