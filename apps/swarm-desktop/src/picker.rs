//! The composer's model picker as data. Pattern (no code copied): OpenCode's
//! model dialog (one "Search models" field over every provider, Recent with
//! the current model checked, then models grouped by provider; ←/→ switch
//! the variant, Ctrl+F marks a favorite) and Antigravity's model menu (the
//! effort inline on each row, a check on the current one, badges). The rows
//! come from `models.catalog` (k76a), the `[desktop]` favorites and recents
//! and the saved presets (configured profiles). No I/O.

use std::collections::HashMap;

use eliot_core::api::{
    HarnessEntry, ModelsCatalogEntry, ModelsCatalogResponse, ProfileSummary, catalog_matches,
};

use crate::settings::ModelRef;

/// The harness groups, in the order the picker shows them.
pub const HARNESSES: [&str; 5] = ["codex", "claude", "opencode", "muse", "antigravity"];
/// `models.catalog` availability of a model under a provider hold.
const HELD: &str = eliot_core::api::harness::CATALOG_HELD;

/// A harness code in the owner's words.
pub fn harness_label(code: &str) -> &str {
    match code {
        "codex" => "Codex",
        "claude" => "Claude Code",
        "opencode" => "OpenCode",
        "muse" => "Muse",
        "antigravity" => "Antigravity",
        other => other,
    }
}

/// A catalogue provider in the owner's words.
pub fn provider_label(provider: &str) -> String {
    match provider {
        "openai" => "OpenAI".to_owned(),
        "anthropic" => "Anthropic".to_owned(),
        "meta" => "Meta".to_owned(),
        "opencode" => "OpenCode".to_owned(),
        "opencode-go" => "OpenCode Go".to_owned(),
        "openrouter" => "OpenRouter".to_owned(),
        "deepseek" => "DeepSeek".to_owned(),
        "gpt" => "GPT".to_owned(),
        other => {
            let mut chars = other.chars();
            chars.next().map_or_else(String::new, |first| {
                first.to_uppercase().chain(chars).collect()
            })
        }
    }
}

/// One harness, model (None: the harness's own default) and effort (None:
/// the model's default).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Pick {
    pub harness: String,
    pub model: Option<String>,
    pub effort: Option<String>,
}

impl Pick {
    /// What `[desktop] recent_models` keeps (a pick with a model).
    pub fn model_ref(&self) -> Option<ModelRef> {
        Some(ModelRef {
            harness: self.harness.clone(),
            model: self.model.clone()?,
            effort: self.effort.clone(),
        })
    }
}

/// What a new task launches.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum Choice {
    /// Swarm's routing picks at admission (`routing.preview` shows what).
    #[default]
    Auto,
    /// A saved preset: a configured profile.
    Preset(String),
    /// An Owner ad-hoc launch of one harness and model.
    Model(Pick),
}

impl Choice {
    /// The model chip's words: `Harness · Model · effort`.
    pub fn chip_label(&self, auto_profile: Option<&str>) -> String {
        match self {
            Self::Auto => match auto_profile {
                Some(profile) => format!("Auto · {profile}"),
                None => "Auto".to_owned(),
            },
            Self::Preset(name) => format!("Preset · {name}"),
            Self::Model(pick) => format!(
                "{} · {} · {}",
                harness_label(&pick.harness),
                pick.model.as_deref().map_or("default model", short_model),
                pick.effort.as_deref().unwrap_or("default effort")
            ),
        }
    }

    pub fn harness(&self) -> Option<&str> {
        match self {
            Self::Model(pick) => Some(&pick.harness),
            _ => None,
        }
    }
}

/// A model id without its `provider/` prefix (OpenCode ids), for display.
pub fn short_model(id: &str) -> &str {
    id.split_once('/').map_or(id, |(_, model)| model)
}

/// What a choosable row launches.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ItemKind {
    Auto,
    Preset(String),
    Model {
        harness: String,
        model: Option<String>,
    },
}

/// One choosable row.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Item {
    /// Unique per row: the highlight and the row's effort follow it.
    pub key: String,
    pub kind: ItemKind,
    pub title: String,
    /// Secondary text: the provider (and harness), or Auto's pick and why.
    pub detail: String,
    /// The effort shown inline: what choosing the row launches.
    pub effort: Option<String>,
    /// The efforts ←/→ step through.
    pub efforts: Vec<String>,
    /// The current choice.
    pub current: bool,
    /// The harness's default model.
    pub default: bool,
    pub favorite: bool,
    /// Why the row cannot be chosen (it is greyed), if so.
    pub disabled: Option<String>,
}

impl Item {
    /// The choice this row makes with its shown effort.
    pub fn choice(&self) -> Choice {
        self.choice_with(self.effort.clone())
    }

    pub fn choice_with(&self, effort: Option<String>) -> Choice {
        match &self.kind {
            ItemKind::Auto => Choice::Auto,
            ItemKind::Preset(name) => Choice::Preset(name.clone()),
            ItemKind::Model { harness, model } => Choice::Model(Pick {
                harness: harness.clone(),
                model: model.clone(),
                effort,
            }),
        }
    }

    /// The favorite this row marks (a model row with a model id).
    pub fn model_ref(&self) -> Option<ModelRef> {
        match &self.kind {
            ItemKind::Model {
                harness,
                model: Some(model),
            } => Some(ModelRef {
                harness: harness.clone(),
                model: model.clone(),
                effort: self.effort.clone(),
            }),
            _ => None,
        }
    }

    /// The effort one step left (`forward` false) or right, clamped at the
    /// ends; None when the row has no efforts.
    pub fn step_effort(&self, forward: bool) -> Option<String> {
        if self.efforts.is_empty() {
            return None;
        }
        let at = self
            .effort
            .as_deref()
            .and_then(|effort| self.efforts.iter().position(|known| known == effort));
        let next = match (at, forward) {
            (None, true) => 0,
            (None, false) => self.efforts.len() - 1,
            (Some(at), true) => (at + 1).min(self.efforts.len() - 1),
            (Some(at), false) => at.saturating_sub(1),
        };
        self.efforts.get(next).cloned()
    }
}

/// One row of the picker.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Row {
    /// A section title ("Favorites", "Codex") and its note (a version,
    /// "not installed").
    Heading {
        key: String,
        title: String,
        note: String,
    },
    /// A provider inside a harness (OpenCode's providers).
    Provider {
        key: String,
        title: String,
    },
    /// A line that is not a choice: an empty state or a reason.
    Note {
        key: String,
        text: String,
    },
    Choice(Item),
}

impl Row {
    #[cfg(test)]
    pub fn key(&self) -> &str {
        match self {
            Self::Heading { key, .. } | Self::Provider { key, .. } | Self::Note { key, .. } => key,
            Self::Choice(item) => &item.key,
        }
    }

    /// The row's item when it can be chosen.
    pub fn choosable(&self) -> Option<&Item> {
        match self {
            Self::Choice(item) if item.disabled.is_none() => Some(item),
            _ => None,
        }
    }
}

/// What the rows are built from.
pub struct Inputs<'a> {
    /// The catalogue read: not yet, failed, or the reply.
    pub catalog: Option<&'a Result<ModelsCatalogResponse, String>>,
    pub query: &'a str,
    pub favorites: &'a [ModelRef],
    pub recents: &'a [ModelRef],
    /// Presets the selected project allows.
    pub presets: &'a [&'a ProfileSummary],
    pub current: &'a Choice,
    /// Auto's secondary line: what Swarm picks now, and why.
    pub auto_detail: &'a str,
    /// Efforts chosen on rows with ←/→ (row key → effort).
    pub efforts: &'a HashMap<String, String>,
    /// Claude Code's closed effort vocabulary (it has no live catalogue).
    pub claude_efforts: &'a [&'a str],
}

/// The rows the picker shows for `inputs`, filtered by the query
/// (case-insensitive, across harnesses and providers).
pub fn rows(inputs: &Inputs) -> Vec<Row> {
    let query = inputs.query.trim();
    let mut rows = Vec::new();
    let reply = inputs.catalog.and_then(|read| read.as_ref().ok());

    if query.is_empty() || text_matches(&["auto", "swarm"], query) {
        rows.push(Row::Choice(Item {
            key: "auto".to_owned(),
            kind: ItemKind::Auto,
            title: "Auto".to_owned(),
            detail: inputs.auto_detail.to_owned(),
            effort: None,
            efforts: Vec::new(),
            current: *inputs.current == Choice::Auto,
            default: false,
            favorite: false,
            disabled: None,
        }));
    }

    for (title, prefix, list) in [
        ("Favorites", "fav", inputs.favorites),
        ("Recent", "recent", inputs.recents),
    ] {
        let items: Vec<Item> = list
            .iter()
            .map(|known| {
                let key = format!(
                    "{prefix}:{}:{}:{}",
                    known.harness,
                    known.model,
                    known.effort.as_deref().unwrap_or("")
                );
                remembered_item(inputs, reply, key, known)
            })
            .filter(|item| query.is_empty() || item_matches(item, reply, query))
            .collect();
        if !items.is_empty() {
            rows.push(heading(prefix, title, ""));
            rows.extend(items.into_iter().map(Row::Choice));
        }
    }

    match (inputs.catalog, reply) {
        (None, _) => rows.push(note("loading", "Reading the models…")),
        (Some(Err(error)), _) => rows.push(note("error", &format!("Models not read: {error}"))),
        (_, Some(reply)) => {
            for code in HARNESSES {
                harness_rows(inputs, reply, code, query, &mut rows);
            }
        }
        _ => {}
    }

    let presets: Vec<Item> = inputs
        .presets
        .iter()
        .filter(|preset| {
            query.is_empty()
                || text_matches(
                    &[
                        &preset.name,
                        &preset.harness,
                        harness_label(&preset.harness),
                        preset.model.as_deref().unwrap_or(""),
                        "preset",
                    ],
                    query,
                )
        })
        .map(|preset| Item {
            key: format!("preset:{}", preset.name),
            kind: ItemKind::Preset(preset.name.clone()),
            title: preset.name.clone(),
            detail: format!(
                "{} · {}",
                harness_label(&preset.harness),
                preset.model.as_deref().unwrap_or("default model")
            ),
            effort: preset.effort.clone(),
            efforts: Vec::new(),
            current: *inputs.current == Choice::Preset(preset.name.clone()),
            default: false,
            favorite: false,
            disabled: (!preset.launchable).then(|| {
                preset
                    .error
                    .clone()
                    .unwrap_or_else(|| "cannot start now".to_owned())
            }),
        })
        .collect();
    if !presets.is_empty() {
        rows.push(heading("presets", "Presets", ""));
        rows.extend(presets.into_iter().map(Row::Choice));
    }

    if !query.is_empty() && !rows.iter().any(|row| matches!(row, Row::Choice(_))) {
        rows.push(note(
            "no-match",
            &format!("No models match “{query}”. Clear the search to see every model."),
        ));
    }
    rows
}

/// One harness's group: its heading (once "not installed" when absent),
/// then its models (OpenCode under its providers).
fn harness_rows(
    inputs: &Inputs,
    reply: &ModelsCatalogResponse,
    code: &str,
    query: &str,
    rows: &mut Vec<Row>,
) {
    let label = harness_label(code);
    let entry = reply.harnesses.iter().find(|entry| entry.harness == code);
    let named = query.is_empty() || text_matches(&[code, label], query);
    let Some(entry) = entry.filter(|entry| entry.installed) else {
        if named {
            rows.push(heading(code, label, "not installed"));
        }
        return;
    };
    let version = version_note(entry);
    if code == "claude" {
        claude_rows(inputs, reply, entry, query, rows);
        return;
    }
    let models: Vec<&ModelsCatalogEntry> = reply
        .models
        .iter()
        .filter(|model| model.harness == code)
        .filter(|model| query.is_empty() || catalog_matches(model, query) || named)
        .collect();
    if models.is_empty() {
        if named {
            rows.push(heading(code, label, &version));
            rows.push(note(
                &format!("{code}:none"),
                "No models reported by this harness right now.",
            ));
        }
        return;
    }
    rows.push(heading(code, label, &version));
    let mut provider: Option<&str> = None;
    let mut ordered = models;
    if code == "opencode" {
        // Stable within a provider: the daemon sorts by id.
        ordered.sort_by(|left, right| left.provider.cmp(&right.provider));
    }
    for model in ordered {
        if code == "opencode" && provider != Some(model.provider.as_str()) {
            provider = Some(model.provider.as_str());
            rows.push(Row::Provider {
                key: format!("{code}:provider:{}", model.provider),
                title: provider_label(&model.provider),
            });
        }
        let key = format!("{code}:{}", model.id);
        rows.push(Row::Choice(catalog_item(inputs, key, model, None)));
    }
}

/// Claude Code catalogue rows: the frozen documented aliases and full model
/// ids from `models.catalog` (each marked `documented_alias`), plus the CLI
/// default and the model id typed in the search. There is no live native
/// inventory; the typed row still covers ids outside the frozen list.
fn claude_rows(
    inputs: &Inputs,
    reply: &ModelsCatalogResponse,
    entry: &HarnessEntry,
    query: &str,
    rows: &mut Vec<Row>,
) {
    let code = "claude";
    let label = harness_label(code);
    let efforts: Vec<String> = inputs
        .claude_efforts
        .iter()
        .map(|effort| (*effort).to_owned())
        .collect();
    let named = query.is_empty() || text_matches(&[code, label, "anthropic", "default"], query);
    let models: Vec<&ModelsCatalogEntry> = reply
        .models
        .iter()
        .filter(|model| model.harness == code)
        .filter(|model| query.is_empty() || catalog_matches(model, query))
        .collect();
    let typed = typed_model_id(query);
    if !named && models.is_empty() && typed.is_none() {
        return;
    }
    rows.push(heading(code, label, &version_note(entry)));
    for model in models {
        let key = format!("{code}:{}", model.id);
        rows.push(Row::Choice(catalog_item(inputs, key, model, None)));
    }
    let item = |key: String, model: Option<String>, title: String, detail: String| {
        let effort = row_effort(inputs, &key, code, model.as_deref(), None, None);
        Item {
            current: is_current(inputs.current, code, model.as_deref()),
            favorite: model
                .as_deref()
                .is_some_and(|model| is_favorite(inputs.favorites, code, model)),
            key,
            kind: ItemKind::Model {
                harness: code.to_owned(),
                model,
            },
            title,
            detail,
            effort,
            efforts: efforts.clone(),
            default: false,
            disabled: None,
        }
    };
    if named {
        rows.push(Row::Choice(item(
            "claude:default".to_owned(),
            None,
            "Default model".to_owned(),
            "Anthropic · the Claude Code CLI picks".to_owned(),
        )));
    }
    if let Some(typed) = typed {
        rows.push(Row::Choice(item(
            format!("claude:typed:{typed}"),
            Some(typed.to_owned()),
            typed.to_owned(),
            "Anthropic · use this model id".to_owned(),
        )));
    } else if query.is_empty() {
        rows.push(note(
            "claude:hint",
            "Type a model id to use one outside this list.",
        ));
    }
}

/// A search that reads as a Claude model id (one word, no spaces).
fn typed_model_id(query: &str) -> Option<&str> {
    let query = query.trim();
    (query.len() >= 3
        && query.len() <= 256
        && query.starts_with("claude")
        && !query.contains(char::is_whitespace)
        && !query.chars().any(char::is_control))
    .then_some(query)
}

/// A favorite or recent model as a row: matched against the catalogue
/// when its harness has one, greyed when it cannot start now.
fn remembered_item(
    inputs: &Inputs,
    reply: Option<&ModelsCatalogResponse>,
    key: String,
    known: &ModelRef,
) -> Item {
    let entry = reply.and_then(|reply| {
        reply
            .models
            .iter()
            .find(|model| model.harness == known.harness && model.id == known.model)
    });
    let installed = reply.map(|reply| {
        reply
            .harnesses
            .iter()
            .any(|entry| entry.harness == known.harness && entry.installed)
    });
    let mut item = match entry {
        Some(model) => catalog_item(inputs, key, model, known.effort.as_deref()),
        None => {
            let efforts: Vec<String> = if known.harness == "claude" {
                inputs
                    .claude_efforts
                    .iter()
                    .map(|effort| (*effort).to_owned())
                    .collect()
            } else {
                Vec::new()
            };
            Item {
                effort: row_effort(
                    inputs,
                    &key,
                    &known.harness,
                    Some(&known.model),
                    known.effort.as_deref(),
                    None,
                ),
                current: is_current(inputs.current, &known.harness, Some(&known.model)),
                favorite: is_favorite(inputs.favorites, &known.harness, &known.model),
                key,
                kind: ItemKind::Model {
                    harness: known.harness.clone(),
                    model: Some(known.model.clone()),
                },
                title: short_model(&known.model).to_owned(),
                detail: String::new(),
                efforts,
                default: false,
                disabled: None,
            }
        }
    };
    item.detail = format!(
        "{} · {}",
        harness_label(&known.harness),
        provider_label(&eliot_core::api::catalog_provider(
            &known.harness,
            &known.model
        ))
    );
    item.disabled = match installed {
        Some(false) => Some("not installed".to_owned()),
        // Claude Code has no catalogue to check against.
        Some(true) if entry.is_none() && known.harness != "claude" => {
            Some("not in the harness's catalogue now".to_owned())
        }
        _ => item.disabled.take(),
    };
    item
}

fn catalog_item(
    inputs: &Inputs,
    key: String,
    model: &ModelsCatalogEntry,
    remembered: Option<&str>,
) -> Item {
    let efforts = model.efforts.clone().unwrap_or_default();
    Item {
        effort: row_effort(
            inputs,
            &key,
            &model.harness,
            Some(&model.id),
            remembered,
            model.default_effort.as_deref(),
        ),
        current: is_current(inputs.current, &model.harness, Some(&model.id)),
        favorite: is_favorite(inputs.favorites, &model.harness, &model.id),
        kind: ItemKind::Model {
            harness: model.harness.clone(),
            model: Some(model.id.clone()),
        },
        title: if model.harness == "opencode" {
            short_model(&model.display_name).to_owned()
        } else {
            model.display_name.clone()
        },
        detail: provider_label(&model.provider),
        efforts,
        default: model.default,
        disabled: (model.availability == HELD)
            .then(|| "on hold: provider limit or outage".to_owned()),
        key,
    }
}

/// The effort a row shows: chosen on it with ←/→, else the current
/// choice's (for its model), else the remembered one, else the default.
fn row_effort(
    inputs: &Inputs,
    key: &str,
    harness: &str,
    model: Option<&str>,
    remembered: Option<&str>,
    default: Option<&str>,
) -> Option<String> {
    if let Some(chosen) = inputs.efforts.get(key) {
        return Some(chosen.clone());
    }
    if let Choice::Model(pick) = inputs.current
        && pick.harness == harness
        && pick.model.as_deref() == model
        && remembered.is_none()
        && pick.effort.is_some()
    {
        return pick.effort.clone();
    }
    remembered.or(default).map(str::to_owned)
}

fn is_current(current: &Choice, harness: &str, model: Option<&str>) -> bool {
    matches!(current, Choice::Model(pick) if pick.harness == harness && pick.model.as_deref() == model)
}

fn is_favorite(favorites: &[ModelRef], harness: &str, model: &str) -> bool {
    favorites
        .iter()
        .any(|known| known.harness == harness && known.model == model)
}

fn item_matches(item: &Item, reply: Option<&ModelsCatalogResponse>, query: &str) -> bool {
    if let (ItemKind::Model { harness, model }, Some(reply)) = (&item.kind, reply)
        && let Some(entry) = reply
            .models
            .iter()
            .find(|entry| &entry.harness == harness && Some(&entry.id) == model.as_ref())
        && catalog_matches(entry, query)
    {
        return true;
    }
    let harness = match &item.kind {
        ItemKind::Model { harness, .. } => harness.as_str(),
        _ => "",
    };
    text_matches(
        &[&item.title, &item.detail, harness, harness_label(harness)],
        query,
    )
}

/// Case-insensitive substring over any of `fields`.
fn text_matches(fields: &[&str], query: &str) -> bool {
    let query = query.to_lowercase();
    fields
        .iter()
        .any(|field| field.to_lowercase().contains(&query))
}

fn version_note(entry: &HarnessEntry) -> String {
    entry.version.clone().unwrap_or_default()
}

fn heading(key: &str, title: &str, note: &str) -> Row {
    Row::Heading {
        key: format!("heading:{key}"),
        title: title.to_owned(),
        note: note.to_owned(),
    }
}

fn note(key: &str, text: &str) -> Row {
    Row::Note {
        key: format!("note:{key}"),
        text: text.to_owned(),
    }
}

/// The highlight after moving `step` choosable rows from `from` (a row
/// key), clamped; the first choosable row when `from` is not shown.
pub fn moved(rows: &[Row], from: Option<&str>, step: isize) -> Option<String> {
    let keys: Vec<&str> = rows
        .iter()
        .filter_map(Row::choosable)
        .map(|item| item.key.as_str())
        .collect();
    let at = from.and_then(|from| keys.iter().position(|key| *key == from));
    let next = match at {
        None => 0,
        Some(at) => at
            .saturating_add_signed(step)
            .min(keys.len().saturating_sub(1)),
    };
    keys.get(next).map(|key| (*key).to_owned())
}

/// The row the highlight starts on: the current choice, else the first.
pub fn initial(rows: &[Row]) -> Option<String> {
    rows.iter()
        .filter_map(Row::choosable)
        .find(|item| item.current)
        .or_else(|| rows.iter().find_map(Row::choosable))
        .map(|item| item.key.clone())
}

/// The highlighted item, if it is shown and choosable.
pub fn find<'a>(rows: &'a [Row], key: Option<&str>) -> Option<&'a Item> {
    let key = key?;
    rows.iter()
        .filter_map(Row::choosable)
        .find(|item| item.key == key)
}

#[cfg(test)]
mod tests {
    use super::*;
    use eliot_core::api::harness::{CATALOG_AVAILABLE, HARNESS_READ_TTL_MS};

    fn harness(code: &str, installed: bool) -> HarnessEntry {
        HarnessEntry {
            harness: code.to_owned(),
            installed,
            cmd: None,
            version: installed.then(|| format!("{code} 1.0")),
            code: None,
            error: None,
            candidates: None,
        }
    }

    fn model(harness: &str, id: &str, efforts: &[&str]) -> ModelsCatalogEntry {
        ModelsCatalogEntry {
            harness: harness.to_owned(),
            id: id.to_owned(),
            display_name: id.to_owned(),
            provider: eliot_core::api::catalog_provider(harness, id),
            efforts: Some(efforts.iter().map(|e| (*e).to_owned()).collect()),
            default_effort: efforts.first().map(|e| (*e).to_owned()),
            default: false,
            availability: CATALOG_AVAILABLE.to_owned(),
            source: None,
        }
    }

    fn catalog() -> Result<ModelsCatalogResponse, String> {
        Ok(ModelsCatalogResponse {
            harnesses: vec![
                harness("codex", true),
                harness("claude", true),
                harness("opencode", true),
                harness("muse", false),
                harness("antigravity", true),
            ],
            models: vec![
                model("codex", "gpt-6-luna", &["low", "medium", "high"]),
                model("codex", "gpt-6.1-sol", &["low", "high"]),
                model("opencode", "opencode-go/gpt-5.6-luna", &[]),
                model("opencode", "openrouter/gpt-6-luna", &[]),
                model("antigravity", "gemini-4-hydrogen-high", &[]),
            ],
            next: None,
            fetched_at_ms: 1,
            ttl_ms: HARNESS_READ_TTL_MS,
        })
    }

    fn build(query: &str, current: &Choice, efforts: &HashMap<String, String>) -> Vec<Row> {
        let read = catalog();
        let recents = vec![ModelRef {
            harness: "codex".to_owned(),
            model: "gpt-6-luna".to_owned(),
            effort: Some("high".to_owned()),
        }];
        rows(&Inputs {
            catalog: Some(&read),
            query,
            favorites: &[],
            recents: &recents,
            presets: &[],
            current,
            auto_detail: "Swarm picks codex-luna",
            efforts,
            claude_efforts: &["low", "medium", "high", "xhigh", "max"],
        })
    }

    fn keys(rows: &[Row]) -> Vec<&str> {
        rows.iter().map(Row::key).collect()
    }

    #[test]
    fn groups_follow_auto_recent_then_harnesses_and_opencode_providers() {
        let rows = build("", &Choice::Auto, &HashMap::new());
        let keys = keys(&rows);
        let at = |key: &str| keys.iter().position(|k| *k == key).unwrap();
        assert_eq!(keys[0], "auto");
        assert!(at("heading:recent") < at("heading:codex"));
        assert!(at("heading:codex") < at("heading:claude"));
        assert!(at("heading:claude") < at("heading:opencode"));
        assert!(at("opencode:provider:opencode-go") < at("opencode:provider:openrouter"));
        // Muse is not installed: said once, no rows.
        assert_eq!(
            rows.iter()
                .filter(|row| matches!(row, Row::Heading { title, note, .. } if title == "Muse" && note == "not installed"))
                .count(),
            1
        );
        assert!(!keys.iter().any(|key| key.starts_with("muse:")));
        // OpenCode rows show the model without its provider prefix.
        let luna = find(&rows, Some("opencode:opencode-go/gpt-5.6-luna")).unwrap();
        assert_eq!(luna.title, "gpt-5.6-luna");
        assert_eq!(luna.detail, "OpenCode Go");
    }

    #[test]
    fn search_filters_across_harnesses_and_providers_case_insensitively() {
        let rows = build("LUNA", &Choice::Auto, &HashMap::new());
        let choices: Vec<&str> = rows
            .iter()
            .filter_map(Row::choosable)
            .map(|item| item.key.as_str())
            .collect();
        assert_eq!(
            choices,
            [
                "recent:codex:gpt-6-luna:high",
                "codex:gpt-6-luna",
                "opencode:opencode-go/gpt-5.6-luna",
                "opencode:openrouter/gpt-6-luna",
            ]
        );
        let none = build("nothing-like-this", &Choice::Auto, &HashMap::new());
        assert!(matches!(none.last(), Some(Row::Note { key, .. }) if key == "note:no-match"));
    }

    #[test]
    fn claude_offers_its_default_and_a_typed_model_id() {
        let rows = build("claude-opus-5-5", &Choice::Auto, &HashMap::new());
        let typed = find(&rows, Some("claude:typed:claude-opus-5-5")).unwrap();
        assert_eq!(
            typed.choice_with(Some("high".to_owned())),
            Choice::Model(Pick {
                harness: "claude".to_owned(),
                model: Some("claude-opus-5-5".to_owned()),
                effort: Some("high".to_owned()),
            })
        );
        assert_eq!(typed.efforts.len(), 5);
        let all = build("", &Choice::Auto, &HashMap::new());
        assert!(find(&all, Some("claude:default")).is_some());
    }

    #[test]
    fn claude_lists_its_documented_catalogue_entries() {
        let mut read = catalog();
        for id in ["haiku", "opusplan", "claude-haiku-5-5"] {
            let mut entry = model("claude", id, &["low", "medium", "high", "xhigh", "max"]);
            entry.source = Some("documented_alias".to_owned());
            read.as_mut().unwrap().models.push(entry);
        }
        let shown = rows(&Inputs {
            catalog: Some(&read),
            query: "",
            favorites: &[],
            recents: &[],
            presets: &[],
            current: &Choice::Auto,
            auto_detail: "Swarm picks codex-luna",
            efforts: &HashMap::new(),
            claude_efforts: &["low", "medium", "high", "xhigh", "max"],
        });
        assert!(find(&shown, Some("claude:default")).is_some());
        assert!(find(&shown, Some("claude:haiku")).is_some());
        assert!(find(&shown, Some("claude:opusplan")).is_some());
        assert!(find(&shown, Some("claude:claude-haiku-5-5")).is_some());
        let filtered = rows(&Inputs {
            catalog: Some(&read),
            query: "haiku",
            favorites: &[],
            recents: &[],
            presets: &[],
            current: &Choice::Auto,
            auto_detail: "Swarm picks codex-luna",
            efforts: &HashMap::new(),
            claude_efforts: &["low", "medium", "high", "xhigh", "max"],
        });
        assert!(find(&filtered, Some("claude:haiku")).is_some());
        assert!(find(&filtered, Some("claude:claude-haiku-5-5")).is_some());
        assert!(find(&filtered, Some("claude:opusplan")).is_none());
    }

    #[test]
    fn keyboard_moves_between_choosable_rows_and_steps_effort() {
        let current = Choice::Model(Pick {
            harness: "codex".to_owned(),
            model: Some("gpt-6.1-sol".to_owned()),
            effort: Some("high".to_owned()),
        });
        let rows = build("", &current, &HashMap::new());
        let start = initial(&rows);
        assert_eq!(start.as_deref(), Some("codex:gpt-6.1-sol"));
        let sol = find(&rows, start.as_deref()).unwrap();
        assert_eq!(sol.effort.as_deref(), Some("high"));
        assert!(sol.current);
        assert_eq!(sol.step_effort(false).as_deref(), Some("low"));
        assert_eq!(sol.step_effort(true).as_deref(), Some("high"));
        let up = moved(&rows, start.as_deref(), -1);
        assert_eq!(up.as_deref(), Some("codex:gpt-6-luna"));
        assert_eq!(moved(&rows, Some("auto"), -5).as_deref(), Some("auto"));
        // An effort chosen with the arrows sticks to its row.
        let efforts = HashMap::from([("codex:gpt-6-luna".to_owned(), "low".to_owned())]);
        let rows = build("", &current, &efforts);
        assert_eq!(
            find(&rows, Some("codex:gpt-6-luna"))
                .unwrap()
                .effort
                .as_deref(),
            Some("low")
        );
    }

    #[test]
    fn chip_reads_harness_model_and_effort() {
        let pick = Choice::Model(Pick {
            harness: "claude".to_owned(),
            model: Some("claude-opus-5-5".to_owned()),
            effort: Some("high".to_owned()),
        });
        assert_eq!(
            pick.chip_label(None),
            "Claude Code · claude-opus-5-5 · high"
        );
        assert_eq!(
            Choice::Auto.chip_label(Some("codex-luna")),
            "Auto · codex-luna"
        );
    }
}
