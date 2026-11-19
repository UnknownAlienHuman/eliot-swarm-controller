//! Files the owner attaches to a message. The daemon's input is text, so an
//! attachment travels as its absolute path in a closing block of the
//! message (one `- path` line each under [`HEADER`]); the agent reads the
//! files itself. The transcript lifts that block back out of the owner's
//! message and shows it as file chips.

/// The line that opens the attachment block.
pub const HEADER: &str = "Attached files:";

/// Most files one message carries.
pub const MAX: usize = 12;

/// `text` with `files` attached as its closing block (unchanged without
/// files).
pub fn compose(text: &str, files: &[String]) -> String {
    if files.is_empty() {
        return text.to_owned();
    }
    let list: Vec<String> = files.iter().map(|path| format!("- {path}")).collect();
    let body = text.trim_end();
    if body.is_empty() {
        format!("{HEADER}\n{}", list.join("\n"))
    } else {
        format!("{body}\n\n{HEADER}\n{}", list.join("\n"))
    }
}

/// The message without its attachment block, and the attached paths. A
/// block counts only when it closes the message and every line under the
/// header is `- ` and an absolute path; anything else is plain text.
pub fn split(text: &str) -> (&str, Vec<String>) {
    let trimmed = text.trim_end();
    let start = if trimmed.starts_with(HEADER) {
        Some(0)
    } else {
        trimmed
            .rfind(&format!("\n{HEADER}\n"))
            .map(|index| index + 1)
    };
    let Some(start) = start else {
        return (text, Vec::new());
    };
    let mut lines = trimmed[start..].lines();
    lines.next();
    let mut files = Vec::new();
    for line in lines {
        let Some(path) = line.strip_prefix("- ") else {
            return (text, Vec::new());
        };
        if !std::path::Path::new(path).is_absolute() {
            return (text, Vec::new());
        }
        files.push(path.to_owned());
    }
    if files.is_empty() || files.len() > MAX {
        return (text, Vec::new());
    }
    (trimmed[..start].trim_end(), files)
}

/// A path's file name, as a chip names it.
pub fn file_name(path: &str) -> &str {
    std::path::Path::new(path)
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or(path)
}
