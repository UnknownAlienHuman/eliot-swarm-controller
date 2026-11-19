//! Profile draft editing as pure text operations.
//!
//! Models and profiles keeps one document draft: the `profiles` settings
//! section as `[profiles.*]` TOML text (see
//! [`crate::settings_model::profiles_toml`]). The form and the source view
//! edit that same text, so there is never a second model to reconcile. Form
//! edits change one `key = "value"` line inside one profile block; duplicate
//! copies a block under a new name. Validation and saving stay with the
//! daemon (`settings.save`, dry run first), which runs its own parser and
//! launch resolution; nothing here decides validity.

/// The first choice of an enumerated field: the key is left out, so the
/// harness default applies.
pub const DEFAULT_CHOICE: &str = "Default";

/// One enumerated form field of a profile: its key, label and the values
/// the profile's harness accepts.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FormField {
    pub key: &'static str,
    pub label: &'static str,
    pub values: Vec<String>,
}

/// Claude Code's and the ad-hoc launch's effort vocabulary.
const EFFORTS: [&str; 5] = ["low", "medium", "high", "xhigh", "max"];

/// The enumerated fields a profile of `harness` takes besides its model,
/// with that harness's real values (the daemon's profile validation in
/// `eliot-daemon` `config.rs` and `docs/spec/configuration.md`):
///
/// * Codex: effort, `permissions` (sandbox) and `approval_policy`;
/// * Claude Code: effort and `approval` (its permission mode);
/// * Muse: effort, `approval` and `permissions` (no read-only);
/// * Antigravity: `mode` (no effort, permissions or approval presets);
/// * OpenCode: none (its slice maps no preset).
///
/// `efforts` are the catalogue's efforts of the profile's model, when it
/// reports them (Codex and Muse pass a profile's effort to it); Claude
/// keeps its closed vocabulary.
pub fn form_fields(harness: &str, efforts: Option<&[String]>) -> Vec<FormField> {
    let list = |values: &[&str]| values.iter().map(|value| (*value).to_owned()).collect();
    let effort = |fallback: &[&str]| FormField {
        key: "effort",
        label: "Effort",
        values: efforts
            .filter(|efforts| !efforts.is_empty())
            .map_or_else(|| list(fallback), <[String]>::to_vec),
    };
    match harness {
        "codex" => vec![
            effort(&["low", "medium", "high", "xhigh"]),
            FormField {
                key: "permissions",
                label: "Permissions",
                values: list(&["read-only", "workspace-write", "danger-full-access"]),
            },
            FormField {
                key: "approval_policy",
                label: "Approval policy",
                values: list(&["never", "on-request"]),
            },
        ],
        "claude" => vec![
            FormField {
                key: "effort",
                label: "Effort",
                values: list(&EFFORTS),
            },
            FormField {
                key: "approval",
                label: "Permission mode",
                values: list(&[
                    "manual",
                    "auto",
                    "acceptEdits",
                    "plan",
                    "dontAsk",
                    "bypassPermissions",
                ]),
            },
        ],
        "muse" => vec![
            effort(&["low", "medium", "high", "xhigh"]),
            FormField {
                key: "approval",
                label: "Approval",
                values: list(&["onRequest", "promptUnmatched", "denyUnmatched", "allowAll"]),
            },
            FormField {
                key: "permissions",
                label: "Permissions",
                values: list(&["workspace-write", "danger-full-access"]),
            },
        ],
        "antigravity" => vec![FormField {
            key: "mode",
            label: "Mode",
            values: list(&["plan", "auto", "accept-edits"]),
        }],
        _ => Vec::new(),
    }
}

/// The dropdown choices of an enumerated field: [`DEFAULT_CHOICE`], its
/// values, and the current value when it is none of them, so a form edit
/// never drops what the source says.
pub fn choices(values: &[String], current: Option<&str>) -> Vec<String> {
    let mut all = vec![DEFAULT_CHOICE.to_owned()];
    all.extend(values.iter().cloned());
    if let Some(current) = current.filter(|current| !values.iter().any(|value| value == current)) {
        all.push(current.to_owned());
    }
    all
}

/// The value a dropdown index sets: `None` (remove the key) for the
/// default choice or an index outside `choices`.
pub fn choice_value(choices: &[String], index: usize) -> Option<&str> {
    choices.get(index).filter(|_| index > 0).map(String::as_str)
}

/// WinUI `TextBox` reports line breaks as a bare carriage return; TOML
/// (and the daemon) need line feeds. Normalize every edit before it
/// enters a draft.
pub fn normalize_newlines(text: &str) -> String {
    text.replace("\r\n", "\n").replace('\r', "\n")
}

/// Header table path of a line (`[a.b]`), `None` otherwise. Mirrors the
/// daemon's rule: only whitespace or a comment may follow the bracket.
fn header_path(line: &str) -> Option<Vec<String>> {
    let trimmed = line.trim();
    let (inner, rest) = if let Some(body) = trimmed.strip_prefix("[[") {
        body.split_once("]]")?
    } else {
        trimmed.strip_prefix('[')?.split_once(']')?
    };
    let rest = rest.trim_start();
    if !(rest.is_empty() || rest.starts_with('#')) || inner.trim().is_empty() {
        return None;
    }
    Some(
        inner
            .split('.')
            .map(|segment| {
                segment
                    .trim()
                    .trim_matches('"')
                    .trim_matches('\'')
                    .to_owned()
            })
            .collect(),
    )
}

/// Profile names in draft order.
pub fn profile_names(draft: &str) -> Vec<String> {
    let mut names = Vec::new();
    for line in draft.lines() {
        if let Some(path) = header_path(line)
            && path.len() == 2
            && path[0] == "profiles"
            && !names.contains(&path[1])
        {
            names.push(path[1].clone());
        }
    }
    names
}

/// Byte range of the `[profiles.<name>]` table body (header excluded).
fn block_range(draft: &str, name: &str) -> Option<(usize, usize, usize)> {
    let mut offset = 0;
    let mut header: Option<(usize, usize)> = None;
    for line in draft.split_inclusive('\n') {
        if let Some(path) = header_path(line) {
            if let Some((start, body)) = header {
                return Some((start, body, offset));
            }
            if path.len() == 2 && path[0] == "profiles" && path[1] == name {
                header = Some((offset, offset + line.len()));
            }
        }
        offset += line.len();
    }
    header.map(|(start, body)| (start, body, draft.len()))
}

/// TOML basic string with the required escapes.
pub fn quote(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 2);
    out.push('"');
    for c in value.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\t' => out.push_str("\\t"),
            c if c.is_control() => out.push_str(&format!("\\u{:04X}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

fn key_of(line: &str) -> Option<&str> {
    let (key, _) = line.split_once('=')?;
    let key = key.trim();
    (!key.is_empty() && !key.starts_with('#')).then_some(key)
}

/// What the form may show for one key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Field {
    Absent,
    /// A plain string the form can round-trip exactly.
    Simple(String),
    /// Escapes, arrays, multi-line or other values: source view only.
    Complex,
}

/// Decode `"text"` (no escapes) or `'text'`, with only a comment after it.
fn simple_string(raw: &str) -> Option<String> {
    let raw = raw.trim();
    let quote = raw.chars().next().filter(|c| *c == '"' || *c == '\'')?;
    let body = &raw[1..];
    let close = body.find(quote)?;
    let value = &body[..close];
    let rest = body[close + 1..].trim_start();
    let plain = !value.contains('\\') && !value.contains(['\n', '\r']);
    (plain && (rest.is_empty() || rest.starts_with('#'))).then(|| value.to_owned())
}

/// The form state of `key` in profile `name`.
pub fn field(draft: &str, name: &str, key: &str) -> Field {
    let Some((_, body, end)) = block_range(draft, name) else {
        return Field::Absent;
    };
    draft[body..end]
        .lines()
        .find(|line| key_of(line) == Some(key))
        .map_or(Field::Absent, |line| {
            let raw = line.split_once('=').map_or("", |(_, value)| value);
            simple_string(raw).map_or(Field::Complex, Field::Simple)
        })
}

/// Set (or with `None`, remove) one `key = "value"` line in one profile.
pub fn set_field(
    draft: &str,
    name: &str,
    key: &str,
    value: Option<&str>,
) -> Result<String, String> {
    let (_, body, end) =
        block_range(draft, name).ok_or_else(|| format!("profile {name} is not in the draft"))?;
    let section = &draft[body..end];
    let mut lines: Vec<String> = section.split_inclusive('\n').map(str::to_owned).collect();
    let position = lines.iter().position(|line| key_of(line) == Some(key));
    let replacement = value.map(|value| format!("{key} = {}\n", quote(value)));
    match (position, replacement) {
        (Some(index), Some(line)) => lines[index] = line,
        (Some(index), None) => {
            lines.remove(index);
        }
        (None, Some(line)) => {
            // After the last key line, before trailing blank/comment lines.
            let insert_at = lines
                .iter()
                .rposition(|line| key_of(line).is_some())
                .map_or(0, |index| index + 1);
            if insert_at > 0 && !lines[insert_at - 1].ends_with('\n') {
                lines[insert_at - 1].push('\n');
            }
            lines.insert(insert_at, line);
        }
        (None, None) => return Ok(draft.to_owned()),
    }
    Ok(format!(
        "{}{}{}",
        &draft[..body],
        lines.concat(),
        &draft[end..]
    ))
}

/// Copy profile `from` (and its `[profiles.from.*]` subtables) as `to`.
pub fn duplicate(draft: &str, from: &str, to: &str) -> Result<String, String> {
    let to = to.trim();
    if to.is_empty()
        || !to
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    {
        return Err("a profile name uses letters, digits, hyphen and underscore".to_owned());
    }
    if profile_names(draft).iter().any(|name| name == to) {
        return Err(format!("profile {to} already exists in the draft"));
    }
    let mut copied = String::new();
    let mut inside = false;
    for line in draft.split_inclusive('\n') {
        if let Some(path) = header_path(line) {
            inside = path.len() >= 2 && path[0] == "profiles" && path[1] == from;
            if inside {
                let rest: Vec<&str> = path[2..].iter().map(String::as_str).collect();
                let header = if rest.is_empty() {
                    format!("[profiles.{to}]\n")
                } else {
                    format!("[profiles.{to}.{}]\n", rest.join("."))
                };
                copied.push_str(&header);
                continue;
            }
        }
        if inside {
            copied.push_str(line);
        }
    }
    if copied.is_empty() {
        return Err(format!("profile {from} is not in the draft"));
    }
    let mut out = draft.to_owned();
    if !out.is_empty() && !out.ends_with('\n') {
        out.push('\n');
    }
    if !out.is_empty() && !out.ends_with("\n\n") {
        out.push('\n');
    }
    out.push_str(copied.trim_end_matches('\n'));
    out.push('\n');
    Ok(out)
}
