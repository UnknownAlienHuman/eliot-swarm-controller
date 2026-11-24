//! Profile draft editing as source-preserving TOML operations.
//!
//! Models and profiles keeps one document draft: the `profiles` settings
//! section as `[profiles.*]` TOML text (see
//! [`crate::settings_model::profiles_toml`]). The form and the source view
//! edit that same text, so there is never a second model to reconcile.
//! `toml_edit` handles TOML structure while retaining source formatting and
//! comments. Validation and saving stay with the daemon (`settings.save`, dry
//! run first), which runs its own parser and launch resolution.

use eliot_core::api::native_profile::{
    ANTIGRAVITY_MODES, CLAUDE_EFFORTS, CLAUDE_PERMISSION_MODES, COMMON_EFFORTS,
    CodexApprovalPolicy, CodexPermissions, MUSE_APPROVALS, MUSE_PERMISSIONS, NativeSubagentsPolicy,
};
use eliot_daemon::operator::native_subagents_policy;
use toml_edit::visit_mut::{self, VisitMut};
use toml_edit::{Array, DocumentMut, Item, Table, TableLike, Value};

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

fn parse_document(draft: &str) -> Result<DocumentMut, String> {
    draft
        .parse()
        .map_err(|_| "The profile source has invalid TOML; correct it before editing.".to_owned())
}

fn profiles(document: &DocumentMut) -> Option<&dyn TableLike> {
    document.as_table().get("profiles")?.as_table_like()
}

fn profile<'a>(document: &'a DocumentMut, name: &str) -> Option<&'a dyn TableLike> {
    profiles(document)?.get(name)?.as_table_like()
}

fn profiles_mut(document: &mut DocumentMut) -> Result<&mut dyn TableLike, String> {
    let root = document.as_table_mut();
    if !root.contains_key("profiles") {
        let mut profiles = Table::new();
        profiles.set_implicit(true);
        root.insert("profiles", Item::Table(profiles));
    }
    root.get_mut("profiles")
        .and_then(Item::as_table_like_mut)
        .ok_or_else(|| "profiles is not a TOML table.".to_owned())
}

fn profile_mut<'a>(
    document: &'a mut DocumentMut,
    name: &str,
) -> Result<&'a mut dyn TableLike, String> {
    profiles_mut(document)?
        .get_mut(name)
        .and_then(Item::as_table_like_mut)
        .ok_or_else(|| format!("profile {name} is not in the draft"))
}

/// Profile names in draft order.
pub fn profile_names(draft: &str) -> Vec<String> {
    draft
        .parse::<DocumentMut>()
        .ok()
        .and_then(|document| {
            profiles(&document)
                .map(|profiles| profiles.iter().map(|(key, _)| key.to_owned()).collect())
        })
        .unwrap_or_default()
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

/// A single-line string whose original spelling is safe for a form field.
fn simple_string(value: &Value) -> Option<String> {
    let text = value.as_str()?;
    let Value::String(formatted) = value else {
        return None;
    };
    let repr = formatted.display_repr();
    let quote = repr
        .chars()
        .next()
        .filter(|quote| matches!(quote, '\'' | '"'))?;
    let body = repr.strip_prefix(quote)?.strip_suffix(quote)?;
    (body == text && !text.contains(['\n', '\r'])).then(|| text.to_owned())
}

/// The form state of key in profile name.
pub fn field(draft: &str, name: &str, key: &str) -> Field {
    let Ok(document) = draft.parse::<DocumentMut>() else {
        return Field::Complex;
    };
    profile(&document, name)
        .and_then(|profile| profile.get(key))
        .map_or(Field::Absent, |item| {
            item.as_value()
                .and_then(simple_string)
                .map_or(Field::Complex, Field::Simple)
        })
}

/// Set (or with None, remove) one string field in one profile.
pub fn set_field(
    draft: &str,
    name: &str,
    key: &str,
    value: Option<&str>,
) -> Result<String, String> {
    set_value(
        draft,
        name,
        key,
        value.map(|value| Value::from(value.to_owned())),
    )
}

fn set_value(
    draft: &str,
    name: &str,
    key: &str,
    replacement: Option<Value>,
) -> Result<String, String> {
    let mut document = parse_document(draft)?;
    let profile = profile_mut(&mut document, name)?;
    match replacement {
        Some(mut replacement) => {
            if let Some(item) = profile.get_mut(key) {
                if let Some(existing) = item.as_value() {
                    *replacement.decor_mut() = existing.decor().clone();
                }
                *item = Item::Value(replacement);
            } else {
                profile.insert(key, Item::Value(replacement));
            }
        }
        None if profile.get(key).is_some() => {
            profile.remove(key);
        }
        None => return Ok(draft.to_owned()),
    }
    Ok(document.to_string())
}

/// Copy profile from (and its nested tables) as to, omitting its
/// top-level cmd so the copy follows the installed default.
pub fn duplicate(draft: &str, from: &str, to: &str) -> Result<String, String> {
    let to = to.trim();
    let mut document: DocumentMut = draft.parse().map_err(|_| {
        "The profile source has invalid TOML; correct it before duplicating.".to_owned()
    })?;
    check_new_name(&document, to)?;

    let source = profiles(&document)
        .and_then(|profiles| profiles.get(from))
        .ok_or_else(|| format!("profile {from} is not in the draft"))?;
    let mut copied = source.clone();
    copied
        .as_table_like_mut()
        .ok_or_else(|| format!("profile {from} is not in the draft"))?
        .remove("cmd");
    AppendTables.visit_item_mut(&mut copied);

    profiles_mut(&mut document)?.insert(to, copied);
    Ok(document.to_string())
}

struct AppendTables;

impl VisitMut for AppendTables {
    fn visit_table_mut(&mut self, table: &mut Table) {
        // Cloned headers carry their source positions. Place the whole clone
        // after the original document in the editor's stable display order.
        table.set_position(Some(isize::MAX));
        visit_mut::visit_table_mut(self, table);
    }
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

/// A one-line array of plain strings the form can round-trip.
fn simple_list(value: &Value) -> Option<Vec<String>> {
    let array = value.as_array()?;
    if value.to_string().contains(['\n', '\r']) {
        return None;
    }
    array
        .iter()
        .map(|value| {
            let text = simple_string(value)?;
            (!text.contains(['\\', '"', '\''])).then_some(text)
        })
        .collect()
}

/// The form state of list key in profile name.
pub fn list_field(draft: &str, name: &str, key: &str) -> ListField {
    let Ok(document) = draft.parse::<DocumentMut>() else {
        return ListField::Complex;
    };
    profile(&document, name)
        .and_then(|profile| profile.get(key))
        .map_or(ListField::Absent, |item| {
            item.as_value()
                .and_then(simple_list)
                .map_or(ListField::Complex, ListField::Items)
        })
}

/// Set (or with an empty list, remove) one array field.
pub fn set_list_field(
    draft: &str,
    name: &str,
    key: &str,
    values: &[String],
) -> Result<String, String> {
    if values.is_empty() {
        return set_value(draft, name, key, None);
    }
    let mut array = Array::new();
    for value in values {
        array.push(value.clone());
    }
    set_value(draft, name, key, Some(Value::Array(array)))
}

/// Check a new preset name: the daemon's profile-name alphabet, not yet
/// in the draft.
fn check_new_name(document: &DocumentMut, name: &str) -> Result<(), String> {
    if name.is_empty()
        || !name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    {
        return Err("A preset name uses letters, digits, hyphen and underscore.".to_owned());
    }
    if profiles(document).is_some_and(|profiles| profiles.contains_key(name)) {
        return Err(format!("A preset named {name} already exists."));
    }
    Ok(())
}

/// Append a new profile table for harness; its model and other settings keep
/// the harness defaults until chosen. Leave cmd omitted so the daemon follows
/// the installed default; explicit Owner command overrides remain in source.
pub fn create(draft: &str, name: &str, harness: &str) -> Result<String, String> {
    let name = name.trim();
    let mut document = parse_document(draft)?;
    check_new_name(&document, name)?;

    let mut profile = Table::new();
    profile.insert("harness", Item::Value(Value::from(harness.to_owned())));
    profiles_mut(&mut document)?.insert(name, Item::Table(profile));
    Ok(document.to_string())
}

fn native_subagents_policy_for_profile(draft: &str, name: &str) -> Option<NativeSubagentsPolicy> {
    match field(draft, name, "harness") {
        Field::Simple(harness) => native_subagents_policy(&harness),
        Field::Absent | Field::Complex => None,
    }
}

/// The effective `native_subagents` value shown for a profile: its explicit
/// value, else the capability default shared with the daemon.
pub fn native_subagents_allowed(draft: &str, name: &str) -> bool {
    match field(draft, name, "native_subagents") {
        Field::Simple(value) if !value.trim().is_empty() => value.trim() == "allow",
        Field::Absent | Field::Simple(_) => native_subagents_policy_for_profile(draft, name)
            .is_some_and(|policy| policy.default_allowed),
        Field::Complex => false,
    }
}

/// Whether the profile's harness can enforce an explicit native-subagent deny.
pub fn native_subagents_can_deny(draft: &str, name: &str) -> Option<bool> {
    native_subagents_policy_for_profile(draft, name).map(|policy| policy.can_deny)
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
