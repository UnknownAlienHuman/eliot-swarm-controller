//! Markdown to owned display blocks, parsed off the UI thread.
//!
//! Donor: `pulldown-cmark` 0.13.1 (https://github.com/pulldown-cmark/pulldown-cmark),
//! MIT, used directly as a pull parser; no donor source is copied. The output
//! is plain owned data (`Send`), so workers parse and the UI thread builds the
//! native `RichText` values. This is not a Markdown engine: paragraphs,
//! emphasis, lists, quotes, headings, fenced code and links are mapped onto
//! the controls Reactor 0.100.0 exposes; everything else stays readable text.
//! Reactor exposes no font family, so code keeps its own block and Copy action
//! but is not monospace, and inline code keeps its backticks.

use pulldown_cmark::{CodeBlockKind, Event, HeadingLevel, Options, Parser, Tag, TagEnd};

/// Bodies above this size parse only their prefix and say so.
pub const MAX_PARSE_BYTES: usize = 64 * 1024;
/// Upper bound on blocks per document; the rest is reported, not rendered.
pub const MAX_BLOCKS: usize = 1_500;
/// Longest URL accepted as a navigable link.
const MAX_URL_BYTES: usize = 2_048;

/// One inline span inside a paragraph.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Inline {
    Text {
        text: String,
        bold: bool,
        italic: bool,
    },
    /// Only validated absolute `http`/`https` destinations become links.
    Link {
        text: String,
        url: String,
    },
    Break,
}

/// One display block.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Block {
    Paragraph(Vec<Inline>),
    Heading { level: u8, text: String },
    Code { info: String, text: String },
    Rule,
}

/// Parsed document: owned, `Send`, cheap to cache by content hash.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Doc {
    pub blocks: Vec<Block>,
}

impl Doc {
    /// Text of each fenced/indented code block, in order, for Copy code.
    pub fn code_blocks(&self) -> Vec<&str> {
        self.blocks
            .iter()
            .filter_map(|block| match block {
                Block::Code { text, .. } => Some(text.as_str()),
                _ => None,
            })
            .collect()
    }
}

/// Accept only absolute `http`/`https` URLs with a host and no embedded
/// credentials. Everything else (file paths, `javascript:`, `ms-settings:`,
/// relative links) stays copyable text, because WinUI cannot cancel a
/// hyperlink's `NavigateUri` activation once it exists.
pub fn safe_link(destination: &str) -> Option<String> {
    let destination = destination.trim();
    if destination.is_empty()
        || destination.len() > MAX_URL_BYTES
        || destination.chars().any(char::is_control)
    {
        return None;
    }
    let url = url::Url::parse(destination).ok()?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none_or(str::is_empty)
        || !url.username().is_empty()
        || url.password().is_some()
    {
        return None;
    }
    Some(url.to_string())
}

fn prefix_at_boundary(text: &str, max: usize) -> &str {
    if text.len() <= max {
        return text;
    }
    let mut end = max;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}

#[derive(Default)]
struct Builder {
    blocks: Vec<Block>,
    paragraph: Option<Vec<Inline>>,
    bold: u32,
    italic: u32,
    lists: Vec<Option<u64>>,
    item_marker: Option<String>,
    quote_depth: usize,
    heading: Option<(u8, String)>,
    code: Option<(String, String)>,
    link: Option<(String, String)>,
    image: Option<(String, String)>,
    table_row: Option<Vec<String>>,
    table_cell: Option<String>,
    table_head: bool,
    dropped: usize,
}

impl Builder {
    fn push_block(&mut self, block: Block) {
        if self.blocks.len() < MAX_BLOCKS {
            self.blocks.push(block);
        } else {
            self.dropped += 1;
        }
    }

    fn open_paragraph(&mut self) {
        if self.paragraph.is_some() {
            return;
        }
        let mut inlines = Vec::new();
        let mut lead = String::new();
        if self.quote_depth > 0 {
            lead.push_str(&"│ ".repeat(self.quote_depth));
        }
        if let Some(marker) = self.item_marker.take() {
            let depth = self.lists.len().saturating_sub(1);
            lead.push_str(&"\u{00A0}\u{00A0}\u{00A0}\u{00A0}".repeat(depth));
            lead.push_str(&marker);
        } else if !self.lists.is_empty() {
            lead.push_str(&"\u{00A0}\u{00A0}\u{00A0}\u{00A0}".repeat(self.lists.len()));
        }
        if !lead.is_empty() {
            inlines.push(Inline::Text {
                text: lead,
                bold: false,
                italic: false,
            });
        }
        self.paragraph = Some(inlines);
    }

    fn close_paragraph(&mut self) {
        if let Some(inlines) = self.paragraph.take()
            && !inlines.is_empty()
        {
            self.push_block(Block::Paragraph(inlines));
        }
    }

    fn text(&mut self, text: &str) {
        if let Some((_, code)) = self.code.as_mut() {
            code.push_str(text);
        } else if let Some((_, heading)) = self.heading.as_mut() {
            heading.push_str(text);
        } else if let Some((_, alt)) = self.image.as_mut() {
            alt.push_str(text);
        } else if let Some((_, label)) = self.link.as_mut() {
            label.push_str(text);
        } else if let Some(cell) = self.table_cell.as_mut() {
            cell.push_str(text);
        } else {
            self.open_paragraph();
            let (bold, italic) = (self.bold > 0, self.italic > 0);
            if let Some(inlines) = self.paragraph.as_mut() {
                inlines.push(Inline::Text {
                    text: text.to_owned(),
                    bold,
                    italic,
                });
            }
        }
    }

    fn inline(&mut self, inline: Inline) {
        if let Some(cell) = self.table_cell.as_mut() {
            match inline {
                Inline::Text { text, .. } | Inline::Link { text, .. } => cell.push_str(&text),
                Inline::Break => cell.push(' '),
            }
            return;
        }
        self.open_paragraph();
        if let Some(inlines) = self.paragraph.as_mut() {
            inlines.push(inline);
        }
    }

    fn start(&mut self, tag: Tag<'_>) {
        match tag {
            Tag::Paragraph => self.open_paragraph(),
            Tag::Heading { level, .. } => {
                self.close_paragraph();
                self.heading = Some((heading_level(level), String::new()));
            }
            Tag::BlockQuote(_) => {
                self.close_paragraph();
                self.quote_depth += 1;
            }
            Tag::CodeBlock(kind) => {
                self.close_paragraph();
                let info = match kind {
                    CodeBlockKind::Fenced(info) => info.to_string(),
                    CodeBlockKind::Indented => String::new(),
                };
                self.code = Some((info, String::new()));
            }
            Tag::List(start) => {
                self.close_paragraph();
                self.lists.push(start);
            }
            Tag::Item => {
                self.close_paragraph();
                let marker = match self.lists.last_mut() {
                    Some(Some(next)) => {
                        let marker = format!("{next}. ");
                        *next += 1;
                        marker
                    }
                    _ => "• ".to_owned(),
                };
                self.item_marker = Some(marker);
            }
            Tag::Emphasis => self.italic += 1,
            Tag::Strong => self.bold += 1,
            Tag::Strikethrough => self.text("~~"),
            Tag::Link { dest_url, .. } => self.link = Some((dest_url.to_string(), String::new())),
            Tag::Image { dest_url, .. } => {
                self.image = Some((dest_url.to_string(), String::new()));
            }
            Tag::Table(_) => self.close_paragraph(),
            Tag::TableHead => {
                self.table_head = true;
                self.table_row = Some(Vec::new());
            }
            Tag::TableRow => self.table_row = Some(Vec::new()),
            Tag::TableCell => self.table_cell = Some(String::new()),
            _ => {}
        }
    }

    fn end(&mut self, tag: TagEnd) {
        match tag {
            TagEnd::Paragraph => self.close_paragraph(),
            TagEnd::Heading(_) => {
                if let Some((level, text)) = self.heading.take() {
                    self.push_block(Block::Heading { level, text });
                }
            }
            TagEnd::BlockQuote(_) => {
                self.close_paragraph();
                self.quote_depth = self.quote_depth.saturating_sub(1);
            }
            TagEnd::CodeBlock => {
                if let Some((info, mut text)) = self.code.take() {
                    if text.ends_with('\n') {
                        text.pop();
                    }
                    self.push_block(Block::Code { info, text });
                }
            }
            TagEnd::List(_) => {
                self.close_paragraph();
                self.lists.pop();
            }
            TagEnd::Item => {
                // Tight list items carry text without a paragraph.
                self.close_paragraph();
                self.item_marker = None;
            }
            TagEnd::Emphasis => self.italic = self.italic.saturating_sub(1),
            TagEnd::Strong => self.bold = self.bold.saturating_sub(1),
            TagEnd::Strikethrough => self.text("~~"),
            TagEnd::Link => {
                if let Some((destination, label)) = self.link.take() {
                    let label = if label.trim().is_empty() {
                        destination.clone()
                    } else {
                        label
                    };
                    match safe_link(&destination) {
                        Some(url) => {
                            let shows_destination = label == destination || label == url;
                            self.inline(Inline::Link { text: label, url });
                            if !shows_destination {
                                self.text(&format!(" ({destination})"));
                            }
                        }
                        None if label == destination => self.text(&label),
                        None => self.text(&format!("{label} ({destination})")),
                    }
                }
            }
            TagEnd::Image => {
                if let Some((destination, alt)) = self.image.take() {
                    // Remote images are never loaded; show what they were.
                    self.text(&format!("[image: {alt}] ({destination})"));
                }
            }
            TagEnd::TableCell => {
                if let (Some(cell), Some(row)) = (self.table_cell.take(), self.table_row.as_mut()) {
                    row.push(cell.trim().to_owned());
                }
            }
            TagEnd::TableHead | TagEnd::TableRow => {
                if let Some(row) = self.table_row.take() {
                    let bold = std::mem::take(&mut self.table_head);
                    self.push_block(Block::Paragraph(vec![Inline::Text {
                        text: row.join("  │  "),
                        bold,
                        italic: false,
                    }]));
                }
            }
            _ => {}
        }
    }
}

fn heading_level(level: HeadingLevel) -> u8 {
    match level {
        HeadingLevel::H1 => 1,
        HeadingLevel::H2 => 2,
        HeadingLevel::H3 => 3,
        HeadingLevel::H4 => 4,
        HeadingLevel::H5 => 5,
        HeadingLevel::H6 => 6,
    }
}

/// Parse one message body into display blocks. Unsupported constructs stay
/// readable text: HTML is shown literally and never executed.
pub fn parse(text: &str) -> Doc {
    let source = prefix_at_boundary(text, MAX_PARSE_BYTES);
    let options =
        Options::ENABLE_TABLES | Options::ENABLE_STRIKETHROUGH | Options::ENABLE_TASKLISTS;
    let mut builder = Builder::default();
    for event in Parser::new_ext(source, options) {
        match event {
            Event::Start(tag) => builder.start(tag),
            Event::End(tag) => builder.end(tag),
            Event::Text(text) => builder.text(&text),
            Event::Code(code) => builder.text(&format!("`{code}`")),
            Event::InlineMath(math) => builder.text(&format!("${math}$")),
            Event::DisplayMath(math) => builder.text(&format!("$${math}$$")),
            Event::Html(html) | Event::InlineHtml(html) => builder.text(&html),
            Event::FootnoteReference(label) => builder.text(&format!("[^{label}]")),
            Event::SoftBreak => builder.text(" "),
            Event::HardBreak => builder.inline(Inline::Break),
            Event::Rule => {
                builder.close_paragraph();
                builder.push_block(Block::Rule);
            }
            Event::TaskListMarker(done) => builder.text(if done { "[x] " } else { "[ ] " }),
        }
    }
    builder.close_paragraph();
    if let Some((info, text)) = builder.code.take() {
        builder.push_block(Block::Code { info, text });
    }
    let mut notes = Vec::new();
    if source.len() < text.len() {
        notes.push(format!(
            "… formatted preview covers the first {} KiB; Copy message copies the retained text.",
            MAX_PARSE_BYTES / 1024
        ));
    }
    if builder.dropped > 0 {
        notes.push(format!("… {} more blocks not rendered.", builder.dropped));
    }
    for note in notes {
        builder.blocks.push(Block::Paragraph(vec![Inline::Text {
            text: note,
            bold: false,
            italic: true,
        }]));
    }
    Doc {
        blocks: builder.blocks,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn plain(doc: &Doc) -> Vec<String> {
        doc.blocks
            .iter()
            .map(|block| match block {
                Block::Paragraph(inlines) => inlines
                    .iter()
                    .map(|inline| match inline {
                        Inline::Text { text, .. } | Inline::Link { text, .. } => text.clone(),
                        Inline::Break => "\n".to_owned(),
                    })
                    .collect(),
                Block::Heading { level, text } => format!("h{level}:{text}"),
                Block::Code { info, text } => format!("code[{info}]:{text}"),
                Block::Rule => "---".to_owned(),
            })
            .collect()
    }

    #[test]
    fn emphasis_and_strong_become_run_flags() {
        let doc = parse("plain *it* **bold**");
        let Block::Paragraph(inlines) = &doc.blocks[0] else {
            panic!("paragraph expected");
        };
        assert!(inlines.contains(&Inline::Text {
            text: "it".to_owned(),
            bold: false,
            italic: true
        }));
        assert!(inlines.contains(&Inline::Text {
            text: "bold".to_owned(),
            bold: true,
            italic: false
        }));
    }

    #[test]
    fn headings_code_and_rules_are_separate_blocks() {
        let doc = parse("# Title\n\n```rust\nfn main() {}\n```\n\n---\n");
        assert_eq!(
            plain(&doc),
            vec!["h1:Title", "code[rust]:fn main() {}", "---"]
        );
        assert_eq!(doc.code_blocks(), vec!["fn main() {}"]);
    }

    #[test]
    fn tight_and_ordered_lists_keep_markers() {
        let doc = parse("- one\n- two\n\n3. three\n4. four\n");
        assert_eq!(plain(&doc), vec!["• one", "• two", "3. three", "4. four"]);
    }

    #[test]
    fn only_http_links_navigate() {
        let doc = parse("[site](https://example.com/a) [x](javascript:alert(1)) [f](file:///C:/x)");
        let Block::Paragraph(inlines) = &doc.blocks[0] else {
            panic!("paragraph expected");
        };
        let links: Vec<_> = inlines
            .iter()
            .filter_map(|inline| match inline {
                Inline::Link { url, .. } => Some(url.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(links, vec!["https://example.com/a"]);
        let text = plain(&doc).join("");
        assert!(text.contains("(https://example.com/a)"));
        assert!(text.contains("x (javascript:alert(1))"));
        assert!(text.contains("f (file:///C:/x)"));
    }

    #[test]
    fn safe_link_rejects_credentials_and_relative() {
        assert!(safe_link("https://user:pw@example.com").is_none());
        assert!(safe_link("/relative/path").is_none());
        assert!(safe_link("C:\\Windows").is_none());
        assert!(safe_link("ftp://example.com").is_none());
        assert_eq!(
            safe_link("http://example.com").as_deref(),
            Some("http://example.com/")
        );
    }

    #[test]
    fn html_stays_literal_and_images_do_not_load() {
        let doc = parse("<script>x()</script>\n\n![logo](https://example.com/l.png)");
        let text = plain(&doc).join("\n");
        assert!(text.contains("<script>x()</script>"));
        assert!(text.contains("[image: logo] (https://example.com/l.png)"));
    }

    #[test]
    fn oversized_bodies_parse_a_prefix_and_say_so() {
        let body = "word ".repeat(MAX_PARSE_BYTES);
        let doc = parse(&body);
        let text = plain(&doc).join("");
        assert!(text.contains("formatted preview covers the first"));
    }

    #[test]
    fn tables_render_as_readable_rows() {
        let doc = parse("| a | b |\n|---|---|\n| 1 | 2 |\n");
        assert_eq!(plain(&doc), vec!["a  │  b", "1  │  2"]);
    }
}
