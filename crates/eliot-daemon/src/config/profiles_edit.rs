//! Profile-section safety and the `profile.list` summaries of the
//! `[profiles.*]` tables in `config.toml`.
//!
//! Owning contract: docs/spec/configuration.md "Profile section". Profiles
//! are resolved from the file at every launch admission, so a saved file is
//! what the next launch uses; running Agents keep their frozen snapshots.
//!
//! `settings.read` exposes the profile section only when `editability` finds no
//! reason to refuse: profile tables that mix in other sections, hold a known
//! secret value, or fail strict validation (including credential-bearing proxy
//! URLs) are never handed out as source. Other readable settings sections stay
//! available. Profile saves go through `settings.save`.

use std::path::Path;

use eliot_core::SecretPolicy;
use eliot_core::api::{MAX_LISTED_PROFILES, ProfileListResponse, ProfileSummary, REDACTION_MARKER};
use serde_json::Value;

use super::{core, parse_config, read_config_source, resolve_profile_in};

/// Lowercase hexadecimal SHA-256 of the file text.
pub(crate) fn revision_of(source: &str) -> String {
    eliot_core::api::inference::sha256_hex(source.as_bytes())
}

/// One table block: a header line and the lines up to the next header.
pub(super) struct Block<'a> {
    pub(super) text: &'a str,
    pub(super) section: Option<String>,
}

/// Table path of a header line, or `None` for any other line. A header is
/// `[path]` or `[[path]]` followed only by whitespace or a comment, so a
/// nested-array value line such as `["a", "b"],` is never taken for one.
/// Segments are bare keys or quoted keys (which may hold any characters).
fn header_path(line: &str) -> Option<Vec<String>> {
    let trimmed = line.trim();
    let (body, array) = match trimmed.strip_prefix("[[") {
        Some(body) => (body, true),
        None => (trimmed.strip_prefix('[')?, false),
    };
    let mut path = Vec::new();
    let mut chars = body.char_indices().peekable();
    loop {
        while chars.next_if(|(_, c)| *c == ' ' || *c == '\t').is_some() {}
        let segment = match chars.peek().map(|(_, c)| *c)? {
            quote @ ('"' | '\'') => {
                chars.next();
                let mut segment = String::new();
                loop {
                    let (_, c) = chars.next()?;
                    if c == quote {
                        break;
                    }
                    if c == '\\' && quote == '"' {
                        segment.push(chars.next()?.1);
                    } else {
                        segment.push(c);
                    }
                }
                segment
            }
            _ => {
                let mut segment = String::new();
                while let Some((_, c)) =
                    chars.next_if(|(_, c)| c.is_ascii_alphanumeric() || matches!(c, '_' | '-'))
                {
                    segment.push(c);
                }
                if segment.is_empty() {
                    return None;
                }
                segment
            }
        };
        path.push(segment);
        while chars.next_if(|(_, c)| *c == ' ' || *c == '\t').is_some() {}
        match chars.next()? {
            (_, '.') => continue,
            (index, ']') => {
                let rest = &body[index + 1..];
                let rest = if array { rest.strip_prefix(']')? } else { rest };
                let rest = rest.trim_start();
                return (rest.is_empty() || rest.starts_with('#')).then_some(path);
            }
            _ => return None,
        }
    }
}

pub(super) fn split_blocks(source: &str) -> Vec<Block<'_>> {
    let mut spans: Vec<(usize, usize, Option<String>)> = Vec::new();
    let mut start = 0;
    let mut section: Option<String> = None;
    // A leading UTF-8 BOM is not TOML content: the pinned parser accepts it,
    // but the header scan must not treat it as part of the first header. Keep
    // its bytes in a leading non-profile span so reads never hand it to the
    // editor and saves preserve it byte for byte.
    if let Some(rest) = source.strip_prefix('\u{FEFF}') {
        let bom = source.len() - rest.len();
        spans.push((0, bom, None));
        start = bom;
    }
    let mut offset = start;
    for line in source[start..].split_inclusive('\n') {
        if let Some(path) = header_path(line) {
            if offset > start {
                spans.push((start, offset, section.clone()));
            }
            start = offset;
            section = path.first().cloned();
        }
        offset += line.len();
    }
    if source.len() > start {
        spans.push((start, source.len(), section));
    }
    // Boundary comments and blank lines belong to neither section. Keep them
    // as separate spans so replacing either neighbor preserves their bytes.
    let mut blocks = Vec::new();
    for (index, (start, end, section)) in spans.iter().enumerate() {
        let mut split = *end;
        if section.is_some() && spans.get(index + 1).is_some_and(|next| section != &next.2) {
            for line in source[*start..*end].split_inclusive('\n').rev() {
                let trimmed = line.trim();
                if !(trimmed.is_empty() || trimmed.starts_with('#')) {
                    break;
                }
                split -= line.len();
            }
        }
        blocks.push(Block {
            text: &source[*start..split],
            section: section.clone(),
        });
        if split < *end {
            blocks.push(Block {
                text: &source[split..*end],
                section: None,
            });
        }
    }
    blocks
}

/// The bytes owned by one top-level section, retained in source order.
pub(super) fn section_source(source: &str, name: &str) -> Option<String> {
    let mut found = false;
    let mut text = String::new();
    for block in split_blocks(source) {
        if block.section.as_deref() == Some(name) {
            found = true;
            text.push_str(block.text);
        }
    }
    found.then_some(text)
}

/// The top-level section that owns a byte offset, when the offset falls
/// inside a table block.
pub(super) fn section_at_offset(source: &str, offset: usize) -> Option<String> {
    let mut start = 0;
    for block in split_blocks(source) {
        let end = start + block.text.len();
        if offset >= start && (offset < end || (offset == end && end == source.len())) {
            return block.section.clone();
        }
        start = end;
    }
    None
}

fn with_newline(text: &str) -> String {
    if text.is_empty() || text.ends_with('\n') {
        text.to_owned()
    } else {
        format!("{text}\n")
    }
}

/// The verbatim `[profiles.*]` blocks in file order.
fn profile_text(source: &str) -> String {
    split_blocks(source)
        .into_iter()
        .filter(|block| block.section.as_deref() == Some("profiles"))
        .map(|block| with_newline(block.text))
        .collect()
}

fn located(text: &str, offset: usize) -> String {
    let before = &text[..offset.min(text.len())];
    let line = before.matches('\n').count() + 1;
    let column = before.rsplit('\n').next().map_or(0, str::len) + 1;
    format!("line {line}, column {column}")
}

fn parse_table(text: &str, label: &str) -> core::Result<toml::Table> {
    toml::from_str::<toml::Table>(text).map_err(|error| {
        let at = error
            .span()
            .map(|span| format!(" at {}", located(text, span.start)))
            .unwrap_or_default();
        core::Error::new(
            "profile_error",
            format!("{label}: {}{at}", error.message().trim()),
        )
    })
}

/// A known secret value or the redaction marker, in the raw text or in any
/// decoded value of the parsed table (so TOML escapes cannot hide one).
fn has_secret(policy: &SecretPolicy, text: &str, parsed: Option<&toml::Table>) -> bool {
    text.contains(REDACTION_MARKER)
        || policy.contains_value(&Value::String(text.to_owned()))
        || parsed.is_some_and(|table| {
            serde_json::to_value(table).is_ok_and(|value| {
                policy.contains_value(&value) || value.to_string().contains(REDACTION_MARKER)
            })
        })
}

/// Why the profile sections of `source` may not be exposed, if so. `table`
/// and `config` are the generic and the strict parse of that same `source`
/// (`None` when the parse failed), so the reader parses the file once per
/// kind.
pub(super) fn editability(
    source: &str,
    table: Option<&toml::Table>,
    config: Option<&super::FileConfig>,
    policy: &SecretPolicy,
) -> Option<&'static str> {
    let Some(table) = table else {
        return Some("config.toml is not valid TOML; fix it in a text editor first");
    };
    let text = profile_text(source);
    let Ok(profiles) = parse_table(&text, "profiles") else {
        return Some("the [profiles.*] sections cannot be separated from the rest of the file");
    };
    if profiles.get("profiles") != table.get("profiles") || profiles.keys().any(|k| k != "profiles")
    {
        return Some("profiles are defined outside [profiles.*] tables; edit config.toml directly");
    }
    if has_secret(policy, &text, Some(&profiles)) {
        return Some(
            "the profile sections contain a known secret value; edit config.toml directly",
        );
    }
    // The TOML shape above is not enough: an unknown field or a
    // credential-bearing URL must never be handed to the editor as safe
    // source, even when no known secret is present. Launch-readiness alone
    // (a missing model, a dangling child reference) stays a save-time error:
    // it carries no credentials and the editor can repair it.
    match config {
        Some(config) if !has_unsafe_profile_or_proxy(config) => None,
        _ => Some("the profiles fail the daemon's strict validation; edit config.toml directly"),
    }
}

/// Keep unsafe provider configuration and credential-bearing proxy URLs out
/// of the profile editor.
fn has_unsafe_profile_or_proxy(config: &super::FileConfig) -> bool {
    config.profiles.values().any(|entry| {
        let entry = entry.get_ref();
        entry.local_provider.as_ref().is_some_and(|provider| {
            let pass_env: Vec<_> = entry
                .pass_env
                .iter()
                .flatten()
                .map(|name| name.trim().to_owned())
                .collect();
            provider.validate(&pass_env).is_err()
        })
    }) || config.proxies.iter().any(|(name, entry)| {
        let proxy = entry.get_ref();
        core::RouteBinding {
            name: name.clone(),
            endpoint: proxy.attach.clone(),
            readiness_path: proxy.health.clone(),
            provider_base_url: proxy.base_url_for.codex.clone(),
            client_key_env_name: None,
        }
        .validate()
        .is_err()
    })
}

/// `profile.list`: sanitized summaries plus launch-time resolvability.
pub(crate) fn list(data_root: &Path, policy: &SecretPolicy) -> core::Result<ProfileListResponse> {
    let Some(source) = read_config_source(data_root)? else {
        return Ok(ProfileListResponse {
            revision: None,
            valid: true,
            error: None,
            profiles: Vec::new(),
        });
    };
    let revision = Some(revision_of(&source));
    let config = match parse_config(&source) {
        Ok(config) => config,
        Err(error) => {
            // Parse diagnostics name the offending key, which can equal a
            // known secret, so they are redacted before entering the reply.
            return Ok(ProfileListResponse {
                revision,
                valid: false,
                error: Some(policy.redact_text(&error.to_string())),
                profiles: Vec::new(),
            });
        }
    };
    let mut names: Vec<&String> = config.profiles.keys().collect();
    names.sort();
    let profiles = names
        .into_iter()
        .take(MAX_LISTED_PROFILES)
        .filter_map(|name| {
            let entry = config.profiles[name].get_ref();
            let resolved = resolve_profile_in(&config, &source, name);
            let summary = ProfileSummary {
                name: name.clone(),
                harness: entry.harness.trim().to_owned(),
                model: entry.model.clone(),
                effort: entry.effort.clone(),
                permissions: entry.permissions.clone(),
                // The display policy is the harness-native resolved value, never the raw entry fallback: a launchable profile must not advertise a policy the frozen snapshot omits.
                approval_policy: resolved.as_ref().ok().and_then(|snapshot| {
                    match entry.harness.trim() {
                        "muse" => snapshot
                            .get("approval")
                            .and_then(Value::as_str)
                            .map(str::to_owned),
                        "claude" => snapshot
                            .get("permission_mode")
                            .and_then(Value::as_str)
                            .map(str::to_owned),
                        _ => snapshot
                            .get("approval_policy")
                            .and_then(Value::as_str)
                            .map(str::to_owned),
                    }
                }),
                control: entry.control.clone(),
                proxy: entry.proxy.clone(),
                account: entry.account.clone(),
                max_live: entry.max_live,
                child_profiles: entry.child_profiles.clone().unwrap_or_default(),
                launchable: resolved.is_ok(),
                error: resolved.err().map(|error| error.to_string()),
            };
            // A secret-bearing profile is omitted entirely: even the name and
            // harness are identifiers the picker would display, and resolution
            // errors name the profile, so no redacted placeholder can carry
            // them safely.
            match serde_json::to_value(&summary) {
                Ok(value) if !policy.contains_value(&value) => Some(summary),
                _ => None,
            }
        })
        .collect();
    Ok(ProfileListResponse {
        revision,
        valid: true,
        error: None,
        profiles,
    })
}
