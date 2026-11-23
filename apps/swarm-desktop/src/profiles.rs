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

use eliot_core::api::native_profile::{
    ANTIGRAVITY_MODES, CLAUDE_EFFORTS, CLAUDE_PERMISSION_MODES, COMMON_EFFORTS,
    CodexApprovalPolicy, CodexPermissions, MUSE_APPROVALS, MUSE_PERMISSIONS,
};

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
            effort(&COMMON_EFFORTS),
            FormField {
                key: "permissions",
                label: "Permissions",
                values: list(&CodexPermissions::ALL.map(CodexPermissions::as_str)),
            },
            FormField {
                key: "approval_policy",
                label: "Approval policy",
                values: list(&CodexApprovalPolicy::ALL.map(CodexApprovalPolicy::as_str)),
            },
        ],
        "claude" => vec![
            FormField {
                key: "effort",
                label: "Effort",
                values: list(&CLAUDE_EFFORTS),
            },
            FormField {
                key: "approval",
                label: "Permission mode",
                values: list(&CLAUDE_PERMISSION_MODES),
            },
        ],
        "muse" => vec![
            effort(&COMMON_EFFORTS),
            FormField {
                key: "approval",
                label: "Approval",
                values: list(&MUSE_APPROVALS),
            },
            FormField {
                key: "permissions",
                label: "Permissions",
                values: list(&MUSE_PERMISSIONS.map(CodexPermissions::as_str)),
            },
        ],
        "antigravity" => vec![FormField {
            key: "mode",
            label: "Mode",
            values: list(&ANTIGRAVITY_MODES),
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

/// A table header line: its dotted path, and whether it is an
/// array-of-tables entry (`[[a.b]]`) rather than a table (`[a.b]`).
struct Header {
    path: Vec<String>,
    array: bool,
}

impl Header {
    /// The header line for `path` with this header's bracket kind.
    fn line(&self, path: &[&str]) -> String {
        let joined = path.join(".");
        if self.array {
            format!("[[{joined}]]\n")
        } else {
            format!("[{joined}]\n")
        }
    }
}

/// Header of a line (`[a.b]` or `[[a.b]]`), `None` otherwise. Mirrors the
/// daemon's rule: only whitespace or a comment may follow the bracket.
fn header_path(line: &str) -> Option<Header> {
    let trimmed = line.trim();
    let (array, inner, rest) = if let Some(body) = trimmed.strip_prefix("[[") {
        let (inner, rest) = body.split_once("]]")?;
        (true, inner, rest)
    } else {
        let (inner, rest) = trimmed.strip_prefix('[')?.split_once(']')?;
        (false, inner, rest)
    };
    let rest = rest.trim_start();
    if !(rest.is_empty() || rest.starts_with('#')) || inner.trim().is_empty() {
        return None;
    }
    let path = inner
        .split('.')
        .map(|segment| {
            segment
                .trim()
                .trim_matches('"')
                .trim_matches('\'')
                .to_owned()
        })
        .collect();
    Some(Header { path, array })
}

/// Profile names in draft order.
pub fn profile_names(draft: &str) -> Vec<String> {
    let mut names = Vec::new();
    for line in draft.lines() {
        if let Some(header) = header_path(line)
            && header.path.len() == 2
            && header.path[0] == "profiles"
            && !names.contains(&header.path[1])
        {
            names.push(header.path[1].clone());
        }
    }
    names
}

/// Byte range of the `[profiles.<name>]` table body (header excluded).
fn block_range(draft: &str, name: &str) -> Option<(usize, usize, usize)> {
    let mut offset = 0;
    let mut header: Option<(usize, usize)> = None;
    for line in draft.split_inclusive('\n') {
        if let Some(parsed) = header_path(line) {
            if let Some((start, body)) = header {
                return Some((start, body, offset));
            }
            let path = &parsed.path;
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
    set_line(draft, name, key, value.map(quote).as_deref())
}

/// Set (or with `None`, remove) one `key = <raw TOML value>` line in one
/// profile.
fn set_line(draft: &str, name: &str, key: &str, raw: Option<&str>) -> Result<String, String> {
    let (_, body, end) =
        block_range(draft, name).ok_or_else(|| format!("profile {name} is not in the draft"))?;
    let section = &draft[body..end];
    let mut lines: Vec<String> = section.split_inclusive('\n').map(str::to_owned).collect();
    let position = lines.iter().position(|line| key_of(line) == Some(key));
    let replacement = raw.map(|raw| format!("{key} = {raw}\n"));
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

#[derive(serde::Deserialize)]
struct DuplicateDocument {
    #[serde(default)]
    profiles: std::collections::HashMap<String, DuplicateProfile>,
}

#[derive(serde::Deserialize)]
struct DuplicateProfile {
    #[serde(default)]
    cmd: Option<toml::Spanned<String>>,
}

/// Full source-line range of the profile's top-level `cmd`, if present.
/// TOML decoding handles quoted and escaped keys, while the value span
/// identifies every line of a multiline command string.
fn duplicate_command_range(
    draft: &str,
    from: &str,
) -> Result<Option<std::ops::Range<usize>>, String> {
    let document: DuplicateDocument = toml::from_str(draft).map_err(|_| {
        "The profile source has invalid TOML; correct it before duplicating.".to_owned()
    })?;
    let Some(command) = document
        .profiles
        .get(from)
        .and_then(|profile| profile.cmd.as_ref())
    else {
        return Ok(None);
    };
    let span = command.span();
    let start = draft[..span.start]
        .rfind('\n')
        .map_or(0, |newline| newline + 1);
    let end = draft[span.end..]
        .find('\n')
        .map_or(draft.len(), |newline| span.end + newline + 1);
    Ok(Some(start..end))
}

/// Copy profile `from` (and its `[profiles.from.*]` subtables) as `to`,
/// leaving the top-level `cmd` omitted so a copy uses the installed default.
pub fn duplicate(draft: &str, from: &str, to: &str) -> Result<String, String> {
    let to = to.trim();
    check_new_name(draft, to)?;
    let command_range = duplicate_command_range(draft, from)?;
    let mut copied = String::new();
    let mut inside = false;
    let mut offset = 0;
    for line in draft.split_inclusive('\n') {
        let line_start = offset;
        offset += line.len();
        if command_range
            .as_ref()
            .is_some_and(|range| range.contains(&line_start))
        {
            continue;
        }
        if let Some(header) = header_path(line) {
            let path = &header.path;
            inside = path.len() >= 2 && path[0] == "profiles" && path[1] == from;
            if inside {
                let mut renamed = vec!["profiles", to];
                renamed.extend(path[2..].iter().map(String::as_str));
                copied.push_str(&header.line(&renamed));
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

/// A profile's list field (`child_profiles = ["a", "b"]`) as the form
/// shows it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ListField {
    Absent,
    /// A one-line array of plain strings the form can round-trip.
    Items(Vec<String>),
    /// Multi-line arrays, escapes or other values: source view only.
    Complex,
}

/// Decode a one-line array of plain strings, with only a comment after it.
fn simple_list(raw: &str) -> Option<Vec<String>> {
    let body = raw.trim().strip_prefix('[')?;
    let (inner, rest) = body.split_once(']')?;
    let rest = rest.trim_start();
    if !(rest.is_empty() || rest.starts_with('#')) {
        return None;
    }
    inner
        .split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .map(|item| {
            let quote = item.chars().next().filter(|c| *c == '"' || *c == '\'')?;
            let value = item.strip_prefix(quote)?.strip_suffix(quote)?;
            (!value.contains(['\\', '"', '\''])).then(|| value.to_owned())
        })
        .collect()
}

/// The form state of list `key` in profile `name`.
pub fn list_field(draft: &str, name: &str, key: &str) -> ListField {
    let Some((_, body, end)) = block_range(draft, name) else {
        return ListField::Absent;
    };
    draft[body..end]
        .lines()
        .find(|line| key_of(line) == Some(key))
        .map_or(ListField::Absent, |line| {
            let raw = line.split_once('=').map_or("", |(_, value)| value);
            simple_list(raw).map_or(ListField::Complex, ListField::Items)
        })
}

/// Set (or with an empty list, remove) one `key = ["a", "b"]` line.
pub fn set_list_field(
    draft: &str,
    name: &str,
    key: &str,
    values: &[String],
) -> Result<String, String> {
    let raw = (!values.is_empty()).then(|| {
        let items: Vec<String> = values.iter().map(|value| quote(value)).collect();
        format!("[{}]", items.join(", "))
    });
    set_line(draft, name, key, raw.as_deref())
}

/// Check a new preset name: the daemon's profile-name alphabet, not yet
/// in the draft.
fn check_new_name(draft: &str, name: &str) -> Result<(), String> {
    if name.is_empty()
        || !name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    {
        return Err("A preset name uses letters, digits, hyphen and underscore.".to_owned());
    }
    if profile_names(draft).iter().any(|existing| existing == name) {
        return Err(format!("A preset named {name} already exists."));
    }
    Ok(())
}

/// Append a new `[profiles.<name>]` table for `harness`; its model and
/// other settings keep the harness defaults until chosen. Leave `cmd`
/// omitted so the daemon follows the harness's installed default; explicit
/// Owner command overrides remain in the source.
pub fn create(draft: &str, name: &str, harness: &str) -> Result<String, String> {
    let name = name.trim();
    check_new_name(draft, name)?;
    let mut out = draft.to_owned();
    if !out.is_empty() && !out.ends_with('\n') {
        out.push('\n');
    }
    if !out.is_empty() && !out.ends_with("\n\n") {
        out.push('\n');
    }
    out.push_str(&format!(
        "[profiles.{name}]\nharness = {}\n",
        quote(harness)
    ));
    Ok(out)
}

/// The effective `native_subagents` of a profile: its own value, else
/// `deny` (the daemon's default for every supported harness).
pub fn native_subagents_allowed(draft: &str, name: &str) -> bool {
    match field(draft, name, "native_subagents") {
        Field::Simple(value) => value.trim() == "allow",
        _ => false,
    }
}

/// A daemon refusal of one profile field
/// (`config.toml:3:1: profiles.<name>.<key>: <reason>`): the profile, the
/// key and the reason in plain words.
pub fn field_error(message: &str) -> Option<(String, String, String)> {
    let start = message.find("profiles.")? + "profiles.".len();
    let (path, reason) = message[start..].split_once(':')?;
    let (name, key) = path.split_once('.')?;
    let name = name.trim().trim_matches('"').to_owned();
    let key = key.trim();
    let reason = reason.trim();
    let words = match (key, reason) {
        ("child_profiles", reason) if reason.starts_with("unknown child profile ") => format!(
            "There is no preset named {}.",
            reason.trim_start_matches("unknown child profile ")
        ),
        (_, reason) => {
            let mut chars = reason.chars();
            let first = chars.next().map(|c| c.to_uppercase().collect::<String>());
            let mut words = first.unwrap_or_default() + chars.as_str();
            if !words.ends_with('.') {
                words.push('.');
            }
            words
        }
    };
    Some((name, key.to_owned(), words))
}

/// The field key a refusal of the New preset row carries.
pub const NEW_PRESET_ROW: &str = "new";
/// The field key a refusal of the Duplicate row carries.
pub const DUPLICATE_ROW: &str = "duplicate";
