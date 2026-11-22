//! Static project command guards evaluated at native permission requests.

use eliot_core::{Error, Result};
use globset::GlobMatcher;
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub(crate) const SNAPSHOT_FIELD: &str = "_project_command_guards";
pub(crate) const REQUEST_FIELD: &str = eliot_core::COMMAND_GUARD_CONTEXT_FIELD;
pub(crate) const MAX_PROJECT_GUARDS: usize = 64;
pub(crate) const MAX_PATTERN_BYTES: usize = 1024;
pub(crate) const MAX_REFUSAL_BYTES: usize = 512;
pub(crate) const MAX_REQUEST_COMMANDS: usize = 8;
pub(crate) const MAX_COMMAND_BYTES: usize = 4096;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CommandGuardRequest {
    pub commands: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ProjectCommandGuard {
    pub pattern: String,
    pub refusal: String,
}

struct CompiledGuard {
    matcher: GlobMatcher,
    refusal: String,
}

pub(crate) struct CommandGuardPolicy {
    rules: Vec<CompiledGuard>,
}

impl CommandGuardPolicy {
    pub(crate) fn empty() -> Self {
        Self { rules: Vec::new() }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.rules.is_empty()
    }

    pub(crate) fn from_profile(profile: &Value) -> Result<Self> {
        let Some(value) = profile.get(SNAPSHOT_FIELD) else {
            return Ok(Self { rules: Vec::new() });
        };
        let entries: Vec<ProjectCommandGuard> =
            serde_json::from_value(value.clone()).map_err(|_| {
                Error::new(
                    "profile_error",
                    "frozen project command guards are malformed",
                )
            })?;
        if entries.len() > MAX_PROJECT_GUARDS {
            return Err(Error::new(
                "profile_error",
                "frozen project command guards exceed 64 entries",
            ));
        }
        let mut rules = Vec::with_capacity(entries.len());
        for entry in entries {
            let matcher = globset::GlobBuilder::new(&entry.pattern)
                .case_insensitive(cfg!(windows))
                .build()
                .map_err(|_| {
                    Error::new(
                        "profile_error",
                        "frozen project command guard pattern is invalid",
                    )
                })?
                .compile_matcher();
            rules.push(CompiledGuard {
                matcher,
                refusal: entry.refusal,
            });
        }
        Ok(Self { rules })
    }

    pub(crate) fn refusal(&self, commands: &[String]) -> Option<&str> {
        for command in commands {
            for rule in &self.rules {
                if rule.matcher.is_match(command)
                    || command_segments(command).any(|segment| rule.matcher.is_match(segment))
                {
                    return Some(rule.refusal.as_str());
                }
            }
        }
        None
    }
}

/// Match unquoted shell list separators without interpreting or executing
/// shell syntax. The verbatim request is also matched by the caller.
fn command_segments(command: &str) -> impl Iterator<Item = &str> {
    let mut start = 0;
    let mut quote = None;
    let mut escaped = false;
    command
        .char_indices()
        .chain(std::iter::once((command.len(), '\n')))
        .filter_map(move |(offset, ch)| {
            if offset != command.len() {
                if escaped {
                    escaped = false;
                    return None;
                }
                if matches!(ch, '\\' | '`') && quote != Some('\'') {
                    escaped = true;
                    return None;
                }
                if let Some(delimiter) = quote {
                    if ch == delimiter {
                        quote = None;
                    }
                    return None;
                }
                if matches!(ch, '\'' | '"') {
                    quote = Some(ch);
                    return None;
                }
                if !matches!(ch, ';' | '|' | '&' | '\n' | '\r') {
                    return None;
                }
            }
            let segment = command[start..offset].trim();
            start = offset + ch.len_utf8();
            (!segment.is_empty()).then_some(segment)
        })
}
