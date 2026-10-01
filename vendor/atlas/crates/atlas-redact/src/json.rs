//! JSON-aware redaction: walk the value, scrub the leaves, keep the structure.
//!
//! Agent transcripts are JSON-shaped — a tool call's arguments, an imported
//! JSONL line, a message envelope. Running the flat layers over the raw text
//! works, but it destroys the envelope: a message id clears the entropy
//! threshold, a `content_hash` field is indistinguishable from a token, and the
//! result is a transcript that no longer parses and no longer renders.
//!
//! Walking instead means each leaf string is redacted in isolation, and the keys
//! that are structurally *never* secrets are skipped outright.

use serde_json::Value;

use crate::placeholder::is_real_secret_value;
use crate::{Category, RedactionCounts};

/// Redact every leaf string of a parsed JSON value in place.
pub(crate) fn redact_value(
    value: &mut Value,
    counts: &mut RedactionCounts,
    leaf: &dyn Fn(&str) -> (String, RedactionCounts),
) {
    walk(value, "", false, counts, leaf);
}

fn walk(
    value: &mut Value,
    key: &str,
    credential_context: bool,
    counts: &mut RedactionCounts,
    leaf: &dyn Fn(&str) -> (String, RedactionCounts),
) {
    match value {
        Value::Object(map) => {
            // Base64 image payloads are enormous, always high-entropy, and never
            // secrets. Scanning them is pure cost and redacting them corrupts
            // the attachment.
            if is_binary_payload(map) {
                return;
            }
            let credential_context = credential_context || is_credential_object(map);
            for (child_key, child) in map.iter_mut() {
                if is_structural_field(child_key) {
                    continue;
                }
                let child_key = child_key.clone();
                walk(child, &child_key, credential_context, counts, leaf);
            }
        }
        Value::Array(items) => {
            for item in items {
                walk(item, key, credential_context, counts, leaf);
            }
        }
        Value::String(text) => {
            // A link field is never entropy-scanned — a long path segment or a
            // signed-URL parameter is not a secret, and shredding it breaks the
            // link — but a credential *inside* one is still a leak, so the
            // URI/DSN password layers run on it in a link-preserving form.
            if is_link_field(&key.to_ascii_lowercase()) {
                let out = crate::redact_link(text);
                if out.changed() {
                    *text = out.text;
                    counts.merge(&out.counts);
                }
                return;
            }
            let (redacted, leaf_counts) = leaf(text);
            if &redacted != text {
                *text = redacted;
                counts.merge(&leaf_counts);
                return;
            }
            // The flat layers see `{"db_password": "hunter2"}` as a bare word —
            // the key that names it lives in the enclosing object, not in the
            // leaf. Redact on key evidence once the leaf itself came back clean.
            if is_credential_key(&key.to_ascii_lowercase(), credential_context)
                && is_real_secret_value(text)
            {
                *text = crate::PLACEHOLDER.to_string();
                counts.record(Category::CredentialValue);
            }
        }
        _ => {}
    }
}

/// Fields that are structurally never secrets, and whose destruction breaks the
/// consumer. Ids clear the entropy threshold constantly; paths are the single
/// most common string in a tool call and are the link rule's input.
fn is_structural_field(key: &str) -> bool {
    if key == "signature" {
        return true;
    }
    let lower = key.to_ascii_lowercase();
    if lower.ends_with("id") || lower.ends_with("ids") {
        return true;
    }
    matches!(
        lower.as_str(),
        "filepath" | "file_path" | "cwd" | "root" | "directory" | "dir" | "path"
    )
}

/// `url`/`uri` values are links: skipped by the entropy layer but still scanned
/// for embedded credentials — `{"url": "postgres://app:hunter2@db/x"}` carries
/// a live password, and skipping it wholesale is how that reaches disk.
fn is_link_field(key: &str) -> bool {
    matches!(key, "url" | "uri")
}

fn is_binary_payload(map: &serde_json::Map<String, Value>) -> bool {
    map.get("type")
        .and_then(Value::as_str)
        .map(|t| t.starts_with("image") || t == "base64")
        .unwrap_or(false)
}

/// An object carrying both a host and a user reads as a connection config, which
/// makes a bare `password` key inside it a credential rather than a word.
fn is_credential_object(map: &serde_json::Map<String, Value>) -> bool {
    let mut has_host = false;
    let mut has_user = false;
    for key in map.keys() {
        match normalize_key(key).as_str() {
            "host" | "hostname" | "server" | "addr" | "address" | "datasource" | "data_source" => {
                has_host = true
            }
            "user" | "username" | "userid" | "user_id" | "uid" => has_user = true,
            _ => {}
        }
    }
    has_host && has_user
}

fn is_credential_key(key: &str, credential_context: bool) -> bool {
    let normalized = normalize_key(key);
    // `password` names a credential wherever it appears; there is no innocent
    // reading of a string-valued `password` field. `pwd` (the shell's working
    // directory) and `secret` (could be anything) stay context-gated.
    if matches!(normalized.as_str(), "password" | "passwd" | "passphrase") {
        return true;
    }
    if credential_context && matches!(normalized.as_str(), "pwd" | "secret") {
        return true;
    }
    // Outside a credential-shaped object, require the key to name itself
    // unambiguously — a lone `secret` field could be anything.
    [
        "api_key",
        "apikey",
        "access_key",
        "secret_key",
        "private_key",
        "access_token",
        "refresh_token",
        "auth_token",
        "client_secret",
        "db_password",
        "database_password",
    ]
    .contains(&normalized.as_str())
}

/// Flattened-config exporters emit dotted keys (`db.password`,
/// `mysql.root.password`); treat those like underscored ones.
fn normalize_key(key: &str) -> String {
    key.trim()
        .to_ascii_lowercase()
        .replace(['-', ' ', '.'], "_")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::redact_json;

    #[test]
    fn structure_and_field_names_survive() {
        let input = r#"{"role":"user","content":"API_KEY=supersecretvalue123"}"#;
        let out = redact_json(input);
        let parsed: Value = serde_json::from_str(&out.text).expect("still valid JSON");
        assert_eq!(parsed["role"], "user");
        assert!(parsed["content"].as_str().unwrap().contains("[REDACTED]"));
    }

    #[test]
    fn id_fields_are_not_destroyed_by_the_entropy_layer() {
        let input = r#"{"message_id":"xJ3kQ9vB2mZ7pL5rT8wN4cF6yH1sD0gA","text":"hi"}"#;
        let out = redact_json(input);
        assert!(out.text.contains("xJ3kQ9vB2mZ7pL5rT8wN4cF6yH1sD0gA"));
    }

    #[test]
    fn a_path_field_survives() {
        let input = r#"{"file_path":"/Users/nafiz/Development/atlas/src/lib.rs"}"#;
        let out = redact_json(input);
        assert!(out.text.contains("/Users/nafiz/Development/atlas/src/lib.rs"));
    }

    #[test]
    fn a_bare_password_key_inside_a_connection_object_is_redacted() {
        let input = r#"{"host":"db.internal","user":"app","password":"hunter2"}"#;
        let out = redact_json(input);
        assert!(!out.text.contains("hunter2"), "{}", out.text);
        assert_eq!(out.counts.total(), 1);
    }

    #[test]
    fn a_named_credential_key_is_redacted_anywhere() {
        let input = r#"{"client_secret":"hunter2"}"#;
        let out = redact_json(input);
        assert!(!out.text.contains("hunter2"));
    }

    #[test]
    fn a_placeholder_value_under_a_credential_key_is_left_alone() {
        let input = r#"{"client_secret":"<your-secret>"}"#;
        let out = redact_json(input);
        assert!(out.text.contains("<your-secret>"));
        assert_eq!(out.counts.total(), 0);
    }

    #[test]
    fn a_bare_password_key_is_redacted_even_without_connection_context() {
        let input = r#"{"password":"hunter2"}"#;
        let out = redact_json(input);
        assert!(!out.text.contains("hunter2"), "{}", out.text);
        assert_eq!(out.counts.total(), 1);
    }

    #[test]
    fn a_url_field_keeps_its_link_but_loses_its_password() {
        let input = r#"{"url":"postgres://app:hunter2@db/x"}"#;
        let out = redact_json(input);
        assert_eq!(out.text, r#"{"url":"postgres://app:[REDACTED]@db/x"}"#);
        assert_eq!(out.counts.total(), 1);
    }

    #[test]
    fn a_password_query_param_in_a_uri_field_is_redacted_in_place() {
        let input = r#"{"uri":"mysql://db.internal/app?user=svc&password=hunter2"}"#;
        let out = redact_json(input);
        assert!(!out.text.contains("hunter2"), "{}", out.text);
        assert!(
            out.text.contains("mysql://db.internal/app?user=svc&password="),
            "link destroyed: {}",
            out.text
        );
    }

    #[test]
    fn a_plain_url_field_is_untouched_even_when_long_and_path_ish() {
        for input in [
            r#"{"url":"https://example.com/very/long/path"}"#,
            r#"{"url":"https://cdn.example.com/build/artifacts/2026-07-27/bundle.min.js?version=1720000000"}"#,
            r#"{"uri":"file:///Users/nafiz/Development/atlas/src/lib.rs"}"#,
        ] {
            let out = redact_json(input);
            assert_eq!(out.counts.total(), 0, "over-redacted: {}", out.text);
        }
    }

    #[test]
    fn a_documented_placeholder_password_in_a_url_field_is_left_alone() {
        let input = r#"{"url":"postgres://user:<password>@localhost/db"}"#;
        let out = redact_json(input);
        assert!(out.text.contains("<password>"));
        assert_eq!(out.counts.total(), 0);
    }

    #[test]
    fn a_base64_image_payload_is_skipped_wholesale() {
        let input = r#"{"type":"image","data":"iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJ"}"#;
        let out = redact_json(input);
        assert!(out.text.contains("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJ"));
    }
}
