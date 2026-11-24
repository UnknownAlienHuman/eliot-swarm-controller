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
use serde::Deserialize;
use serde_json::Value;
use toml::de::{DeTable, ValueDeserializer};
use toml_parser::{
    Source,
    parser::{Event, EventKind, parse_document},
};

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

/// Split at parsed table headers, including arrays of tables. Keep the
/// original source slices for writes, including the BOM and boundary trivia.
pub(super) fn split_blocks(source: &str) -> Vec<Block<'_>> {
    let input = Source::new(source);
    let mut spans = Vec::new();
    let mut start = 0;
    let mut section = None;
    let mut header_start = None;
    parse_document(
        &input.lex().into_vec(),
        &mut |event: Event| match event.kind() {
            EventKind::StdTableOpen | EventKind::ArrayTableOpen => {
                header_start = Some(event.span().start());
            }
            EventKind::SimpleKey => {
                if let Some(offset) = header_start.take()
                    && let Some(raw) = input.get(event)
                {
                    let mut key = String::new();
                    let mut error = None;
                    raw.decode_key(&mut key, &mut error);
                    if error.is_none() {
                        let line = source[..offset]
                            .rsplit_once('\n')
                            .map_or(0, |(before, _)| before.len() + 1);
                        let offset =
                            line.max(source.len() - source.trim_start_matches('\u{FEFF}').len());
                        if offset > start {
                            spans.push((start, offset, section.take()));
                        }
                        start = offset;
                        section = Some(key);
                    }
                }
            }
            EventKind::Whitespace => {}
            _ => header_start = None,
        },
        &mut (),
    );
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
    section_from_blocks(&split_blocks(source), name)
}

pub(super) fn section_from_blocks(blocks: &[Block<'_>], name: &str) -> Option<String> {
    let mut found = false;
    let mut text = String::new();
    for block in blocks {
        if block.section.as_deref() == Some(name) {
            found = true;
            text.push_str(block.text);
        }
    }
    found.then_some(text)
}

/// The top-level section that owns a byte offset, when the offset falls
/// inside a table block.
pub(super) fn section_at_offset<'a>(blocks: &'a [Block<'_>], offset: usize) -> Option<&'a str> {
    let mut start = 0;
    for (index, block) in blocks.iter().enumerate() {
        let end = start + block.text.len();
        if offset >= start && (offset < end || (offset == end && index + 1 == blocks.len())) {
            return block.section.as_deref();
        }
        start = end;
    }
    None
}

/// Prove that replacing the table blocks would account for all profiles.
/// Recovery must agree with the whole document's profile tree and must not
/// have assigned a syntax fault to a profile block.
pub(super) fn profile_source_problem(
    source: &str,
    document: &DeTable<'_>,
    has_syntax_error: bool,
    recover_profiles: impl FnOnce() -> Result<Option<toml::Value>, ()>,
) -> Option<&'static str> {
    let profiles = document.iter().find(|(key, _)| key.get_ref() == "profiles");
    if let Some((key, _)) = profiles {
        let before = &source[..key.span().start];
        if !before
            .rsplit('\n')
            .next()
            .unwrap_or("")
            .trim_start_matches('\u{FEFF}')
            .trim_start()
            .starts_with('[')
        {
            return Some(
                "profiles are defined outside [profiles.*] tables; edit config.toml directly",
            );
        }
    }
    if !has_syntax_error {
        return None;
    }
    let recovered =
        profiles.map(|(_, value)| toml::Value::deserialize(ValueDeserializer::from(value.clone())));
    if let Ok(isolated) = recover_profiles()
        && let Ok(recovered) = recovered.transpose()
        && recovered == isolated
    {
        return None;
    }
    Some(
        "the [profiles.*] sections cannot be separated from the rest of the file; edit config.toml directly",
    )
}

pub(super) fn profile_syntax_fault(blocks: &[Block<'_>], errors: &[toml::de::Error]) -> bool {
    errors.iter().any(|error| {
        error
            .span()
            .and_then(|span| section_at_offset(blocks, span.start))
            == Some("profiles")
    })
}

/// Why the profile settings cannot be safely exposed. The typed configuration
/// and readback values are decoded from the same parsed document.
pub(super) fn editability(
    raw_secret: bool,
    table: &toml::Table,
    config: Option<&super::FileConfig>,
    policy: &SecretPolicy,
) -> Option<&'static str> {
    let decoded_secret = table
        .get("profiles")
        .and_then(|profiles| serde_json::to_value(profiles).ok())
        .is_some_and(|value| {
            policy.contains_value(&value) || value.to_string().contains(REDACTION_MARKER)
        });
    if raw_secret || decoded_secret {
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
                    let field = super::ProfileCapabilities::of(entry.harness.trim())
                        .approval_field
                        .unwrap_or("approval_policy");
                    snapshot
                        .get(field)
                        .and_then(Value::as_str)
                        .map(str::to_owned)
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
