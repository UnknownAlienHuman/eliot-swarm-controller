//! Strict allowlist for OpenCode native `provider.<id>` fragments.
//!
//! One shared shape validator used at profile freeze (daemon) and before any
//! isolated-config write (OpenCode plugin: retained snapshots, serve spawn,
//! model readers). Callers add their own environment check on top: freeze
//! requires every `{env:NAME}` to name a frozen `pass_env` entry (never a
//! native-store selector), while the plugin requires every reference to
//! resolve to a non-empty runtime value.
//!
//! Verified against the installed OpenCode v2.2.7 native schema
//! (`@opencode/schema` `Config.Provider.Info`): a native `providers` entry
//! admits `package`, `settings`, `headers`, `body` and `models` (plus
//! nonsecret identity keys). The fragment allowlist below is deliberately
//! narrower — only `package`, `settings` with exactly `baseURL`/`apiKey`,
//! and `models`.
//!
//! Two deliberate deviations from loose naming, both verified in source:
//! - The provider-settings block is named `settings`, not `options`.
//!   `options` is the legacy V1 key (singular `provider` entries, migrated
//!   in `packages/core/src/config/normalize.ts`); under a native `providers`
//!   entry it decodes to nothing (`onExcessProperty: "ignore"`), so it is
//!   refused instead of silently dropped. The same holds for the legacy
//!   `provider` selector key, Bedrock-style `credentials`, and top-level
//!   `headers`/`body`/`env`: anything unadmitted fails closed before it can
//!   redirect the route or leak a literal credential to disk.
//! - The `models` subtree keeps its native capability tables (names, limits,
//!   variants, per-model overlays vary by endpoint), but every string inside
//!   it still passes the literal-credential scans: malformed `{env:}`
//!   placeholders, URLs with userinfo or credential query parameters, and
//!   credential-bearing keys holding anything but exactly `{env:NAME}` all
//!   refuse.
//!
//! All refusal messages are static: no endpoint, key, value or reference
//! name is ever echoed.

use serde_json::{Map, Value};

use crate::{Error, Result};

/// Maximum `{env:NAME}` reference length, matching profile `pass_env` names.
const MAX_ENV_NAME_LEN: usize = 256;

/// Maximum provider id length, matching the frozen profile bound.
const MAX_PROVIDER_ID_LEN: usize = 64;

/// Query parameter names that carry a credential. Compared case-insensitively
/// against the parameter name only (never values).
const CREDENTIAL_QUERY_KEYS: &[&str] = &[
    "key",
    "api_key",
    "apikey",
    "api-key",
    "api_secret",
    "token",
    "access_token",
    "accesstoken",
    "auth",
    "authorization",
    "bearer",
    "secret",
    "secret_key",
    "secretkey",
    "client_secret",
    "password",
    "passwd",
    "pwd",
    "session_token",
    "sessiontoken",
];

fn profile_error(message: &str) -> Error {
    Error::new("profile_error", message)
}

/// True for a valid `{env:NAME}` reference name: ASCII letters, digits and
/// underscores, starting with a letter or underscore.
pub fn valid_env_name(name: &str) -> bool {
    if name.is_empty() || name.len() > MAX_ENV_NAME_LEN {
        return false;
    }
    let mut chars = name.chars();
    chars
        .next()
        .is_some_and(|first| first.is_ascii_alphabetic() || first == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// An exact `{env:NAME}` reference: the whole trimmed value names one
/// environment entry. Literals, prefixed values and malformed placeholders
/// return `None`.
pub fn exact_env_ref(value: &str) -> Option<&str> {
    let trimmed = value.trim();
    if !(trimmed.starts_with("{env:") && trimmed.ends_with('}')) {
        return None;
    }
    let name = &trimmed[5..trimmed.len() - 1];
    valid_env_name(name).then_some(name)
}

/// True when any `{env:` occurrence in the value is malformed: unclosed,
/// empty or not a valid environment name. Well-formed references return
/// false; callers still check their own admission for those names.
pub fn has_malformed_env_ref(value: &str) -> bool {
    let mut index = 0;
    while let Some(start) = value[index..].find("{env:") {
        let base = index + start + 5;
        let Some(end_rel) = value[base..].find('}') else {
            return true;
        };
        if !valid_env_name(&value[base..base + end_rel]) {
            return true;
        }
        index = base + end_rel + 1;
    }
    false
}

/// Collect every well-formed `{env:NAME}` reference in a JSON fragment,
/// without duplicates. Malformed placeholders are ignored here; the shape
/// validator refuses them separately.
pub fn collect_env_refs(fragment: &Map<String, Value>, out: &mut Vec<String>) {
    fn walk(value: &Value, out: &mut Vec<String>) {
        match value {
            Value::String(text) => {
                let bytes = text.as_bytes();
                let mut index = 0;
                while index + 6 < text.len() {
                    if index + 5 <= text.len() && &bytes[index..index + 5] == b"{env:" {
                        if let Some(end) = text[index + 5..].find('}') {
                            let name = &text[index + 5..index + 5 + end];
                            if valid_env_name(name) && !out.iter().any(|known| known == name) {
                                out.push(name.to_owned());
                            }
                            index += 5 + end + 1;
                            continue;
                        }
                        break;
                    }
                    index += 1;
                }
            }
            Value::Array(items) => {
                for item in items {
                    walk(item, out);
                }
            }
            Value::Object(map) => {
                for item in map.values() {
                    walk(item, out);
                }
            }
            _ => {}
        }
    }
    walk(&Value::Object(fragment.clone()), out);
}

/// True when a URL-like value carries userinfo (`user@host` or
/// `user:password@host` in the authority). A literal credential in the URL
/// would reach disk, so it refuses the same as a literal `apiKey`.
pub fn url_has_userinfo(value: &str) -> bool {
    let Some(scheme_end) = value.find("://") else {
        return false;
    };
    let rest = &value[scheme_end + 3..];
    let authority_end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    rest[..authority_end].contains('@')
}

/// True when a URL-like value carries a credential-bearing query parameter
/// (`?api_key=...`, `?token=...`). Compared by parameter name only; values
/// are never inspected.
pub fn url_has_credential_query(value: &str) -> bool {
    let Some(query_start) = value.find('?') else {
        return false;
    };
    let query = &value[query_start + 1..];
    let query = query.split('#').next().unwrap_or(query);
    query.split('&').any(|pair| {
        let name = pair.split('=').next().unwrap_or(pair).trim();
        !name.is_empty()
            && CREDENTIAL_QUERY_KEYS
                .iter()
                .any(|key| name.eq_ignore_ascii_case(key))
    })
}

/// A credential-bearing key: `apiKey` in any spelling, or any value under a
/// headers table. Such values must be exactly one valid `{env:NAME}`
/// reference, never a literal.
fn is_credential_key(key: &str, parent_key: Option<&str>) -> bool {
    let key = key.trim().to_ascii_lowercase();
    if matches!(
        key.as_str(),
        "apikey" | "api_key" | "api-key" | "authorization"
    ) {
        return true;
    }
    if let Some(parent) = parent_key.map(|parent| parent.trim().to_ascii_lowercase())
        && matches!(parent.as_str(), "headers" | "header")
    {
        return true;
    }
    false
}

/// Strict shape validation for one frozen native `provider.<id>` fragment.
/// Per provider id only `package`, `settings` (exactly `baseURL`/`apiKey`)
/// and `models` are admitted; everything else — the legacy `options` and
/// `provider` keys, Bedrock-style `credentials`, arbitrary `headers`/`body`,
/// and any other key — refuses. Credential-bearing fields must be exactly
/// one valid `{env:NAME}`; URLs must carry no userinfo or credential query
/// parameters; every `{env:` occurrence must be well-formed.
pub fn validate_fragment_shape(fragment: &Map<String, Value>) -> Result<()> {
    for (id, entry) in fragment {
        let id = id.trim();
        if id.is_empty()
            || id.len() > MAX_PROVIDER_ID_LEN
            || id.chars().any(|c| c.is_control() || c.is_whitespace())
            || !id
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.' | '/'))
        {
            return Err(profile_error(
                "OpenCode provider fragment ids must be 1-64 characters",
            ));
        }
        let Value::Object(entry) = entry else {
            return Err(profile_error(
                "OpenCode provider fragment entries must be objects",
            ));
        };
        for key in entry.keys() {
            if !matches!(key.as_str(), "package" | "settings" | "models") {
                return Err(profile_error(
                    "OpenCode provider fragment admits only package, settings and models",
                ));
            }
        }
        if let Some(package) = entry.get("package") {
            let Value::String(package) = package else {
                return Err(profile_error(
                    "OpenCode provider fragment package must be a string",
                ));
            };
            if package.trim().is_empty()
                || package.chars().any(|c| c.is_control() || c.is_whitespace())
            {
                return Err(profile_error(
                    "OpenCode provider fragment package must be a non-empty string",
                ));
            }
        }
        if let Some(settings) = entry.get("settings") {
            let Value::Object(settings) = settings else {
                return Err(profile_error(
                    "OpenCode provider fragment settings must be a table",
                ));
            };
            if settings.is_empty() {
                return Err(profile_error(
                    "OpenCode provider fragment settings must name baseURL or apiKey",
                ));
            }
            for key in settings.keys() {
                if !matches!(key.as_str(), "baseURL" | "apiKey") {
                    return Err(profile_error(
                        "OpenCode provider fragment settings admits only baseURL and apiKey",
                    ));
                }
            }
            if let Some(base_url) = settings.get("baseURL") {
                let Value::String(base_url) = base_url else {
                    return Err(profile_error(
                        "OpenCode provider fragment baseURL must be a string",
                    ));
                };
                if base_url.trim().is_empty() {
                    return Err(profile_error(
                        "OpenCode provider fragment baseURL must be a non-empty string",
                    ));
                }
            }
            if let Some(api_key) = settings.get("apiKey") {
                let Value::String(api_key) = api_key else {
                    return Err(profile_error(
                        "OpenCode provider fragment credential fields must be a valid {env:NAME}",
                    ));
                };
                if exact_env_ref(api_key).is_none() {
                    return Err(profile_error(
                        "OpenCode provider fragment credential fields must be a valid {env:NAME}",
                    ));
                }
            }
        }
        if let Some(models) = entry.get("models") {
            let Value::Object(models) = models else {
                return Err(profile_error(
                    "OpenCode provider fragment models must be a table",
                ));
            };
            for model in models.values() {
                if !model.is_object() {
                    return Err(profile_error(
                        "OpenCode provider fragment model entries must be tables",
                    ));
                }
            }
        }
        validate_fragment_strings(&Value::Object(entry.clone()), None)?;
    }
    Ok(())
}

/// Literal-credential scan over every string in one provider entry:
/// malformed `{env:}` placeholders, URLs with userinfo or credential query
/// parameters, and credential-bearing keys holding anything but exactly
/// `{env:NAME}` all refuse. Messages stay static; no value is echoed.
fn validate_fragment_strings(value: &Value, parent_key: Option<&str>) -> Result<()> {
    match value {
        Value::String(text) => {
            if has_malformed_env_ref(text) {
                return Err(profile_error(
                    "OpenCode provider fragment contains a malformed {env:} reference",
                ));
            }
            if text.contains("://") && url_has_userinfo(text) {
                return Err(profile_error(
                    "OpenCode provider fragment URL must not contain credentials",
                ));
            }
            if text.contains('?') && url_has_credential_query(text) {
                return Err(profile_error(
                    "OpenCode provider fragment URL must not contain credentials",
                ));
            }
            Ok(())
        }
        Value::Array(items) => {
            for item in items {
                validate_fragment_strings(item, None)?;
            }
            Ok(())
        }
        Value::Object(map) => {
            for (key, item) in map {
                validate_fragment_strings(item, Some(key))?;
                if let Value::String(text) = item
                    && is_credential_key(key, parent_key)
                    && exact_env_ref(text).is_none()
                {
                    return Err(profile_error(
                        "OpenCode provider fragment credential fields must be a valid {env:NAME}",
                    ));
                }
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn fragment(value: Value) -> Map<String, Value> {
        value.as_object().expect("fragment object").clone()
    }

    fn minimal_entry() -> Value {
        json!({
            "package": "@opencode/ai/providers/openai-compatible",
            "settings": {
                "baseURL": "http://127.0.0.1:1234/v1",
                "apiKey": "{env:K173F_KEY}",
            },
            "models": {
                "test-model": { "name": "test-model", "tools": true },
            },
        })
    }

    #[test]
    fn minimal_fragment_passes_shape() {
        let map = fragment(json!({ "k173f-fixture": minimal_entry() }));
        validate_fragment_shape(&map).expect("minimal fragment");
        let mut refs = Vec::new();
        collect_env_refs(&map, &mut refs);
        assert_eq!(refs, vec!["K173F_KEY".to_owned()]);
    }

    #[test]
    fn options_and_provider_keys_refuse() {
        for entry in [
            json!({"options": {"baseURL": "http://127.0.0.1:1234/v1"}}),
            json!({"provider": "openai-compatible"}),
            json!({"credentials": {"secretAccessKey": "literal"}}),
            json!({"headers": {"X-Token": "{env:K}"}}),
            json!({"body": {"input": []}}),
            json!({"env": ["K"]}),
        ] {
            let map = fragment(json!({ "p": entry }));
            assert!(validate_fragment_shape(&map).is_err(), "refused: {entry}");
        }
    }

    #[test]
    fn bedrock_credentials_refuse() {
        let map = fragment(json!({
            "bedrock": {
                "package": "@opencode/ai/providers/amazon-bedrock",
                "settings": {
                    "region": "us-east-1",
                    "credentials": {
                        "accessKeyId": "AKIAEXAMPLE",
                        "secretAccessKey": "literal-secret",
                    },
                },
                "models": { "m": {} },
            },
        }));
        assert!(validate_fragment_shape(&map).is_err());
    }

    #[test]
    fn literal_and_malformed_credentials_refuse() {
        for settings in [
            json!({"apiKey": "sk-literal"}),
            json!({"apiKey": "prefix-{env:K}"}),
            json!({"apiKey": "{env:}}"}),
            json!({"apiKey": "{env:9bad}"}),
            json!({"apiKey": "{env:UNFINISHED"}),
            json!({"baseURL": "http://user:pass@host:1234/v1"}),
            json!({"baseURL": "http://host:1234/v1?api_key=secret"}),
            json!({"baseURL": "http://host:1234/v1?Token=secret"}),
        ] {
            let map = fragment(json!({
                "p": {
                    "package": "pkg",
                    "settings": settings,
                    "models": { "m": {} },
                },
            }));
            assert!(
                validate_fragment_shape(&map).is_err(),
                "refused: {settings}"
            );
        }
    }

    #[test]
    fn headers_anywhere_require_env_refs() {
        let map = fragment(json!({
            "p": {
                "package": "pkg",
                "settings": { "baseURL": "http://127.0.0.1:1234/v1" },
                "models": { "m": { "headers": { "X-Token": "literal" } } },
            },
        }));
        assert!(validate_fragment_shape(&map).is_err());
    }

    #[test]
    fn benign_query_strings_pass() {
        let map = fragment(json!({
            "p": {
                "package": "pkg",
                "settings": { "baseURL": "http://127.0.0.1:1234/v1?format=json" },
                "models": { "m": {} },
            },
        }));
        validate_fragment_shape(&map).expect("benign query passes");
    }

    #[test]
    fn exact_env_ref_accepts_only_bare_references() {
        assert_eq!(exact_env_ref("{env:KEY}"), Some("KEY"));
        assert_eq!(exact_env_ref("  {env:_K9}  "), Some("_K9"));
        assert!(exact_env_ref("sk-literal").is_none());
        assert!(exact_env_ref("prefix-{env:KEY}").is_none());
        assert!(exact_env_ref("{env:}").is_none());
        assert!(exact_env_ref("{env:9KEY}").is_none());
        assert!(exact_env_ref("{env:KEY").is_none());
    }
}
