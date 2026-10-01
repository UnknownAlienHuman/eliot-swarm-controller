//! Values that look like credentials but carry no secret.
//!
//! Over-redaction destroys a transcript exactly as thoroughly as under-redaction
//! leaks one, and it is the failure nobody notices — a timeline full of
//! `[REDACTED]` where the docs example used to be reads as a broken product
//! rather than a security incident. Every layer that redacts a *value* (rather
//! than a whole opaque token) runs it past this gate first.
//!
//! It is also what makes redaction idempotent: our own placeholder is the first
//! entry in the table, so a second pass over already-scrubbed content is a no-op.

use std::sync::OnceLock;

use regex::Regex;

/// Documentation placeholders and prior redactions, lowercased.
const PLACEHOLDER_VALUES: &[&str] = &[
    "redacted",
    "[redacted]",
    "<redacted>",
    "changeme",
    "change_me",
    "example",
    "placeholder",
    "your_password",
    "your-password",
    "your_db_password",
    "your_secret",
    "your_api_key",
    "secret_here",
    "todo",
    "none",
    "null",
    "unset",
    // Boolean literals: `"use_token": true` names a flag, not a credential.
    "true",
    "false",
];

/// The inside of a `<…>` doc placeholder: lowercase words joined by `-`/`_`.
/// Digits and mixed case are rejected so `<hunter2>` and `<RealPassword>` still
/// fall through to redaction.
fn bracketed_interior() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"^[a-z][a-z_-]*$").expect("static pattern"))
}

/// Is this a value we must leave alone?
pub(crate) fn is_placeholder(value: &str) -> bool {
    let trimmed = value.trim().trim_matches(['"', '\'']);
    if trimmed.is_empty() {
        return true;
    }
    if is_bracketed(trimmed) {
        return true;
    }
    let lower = trimmed.to_ascii_lowercase();
    // `${DB_PASSWORD}` / `$DB_PASSWORD` — shell or compose expansion, not a value.
    if lower.starts_with("${") && lower.ends_with('}') {
        return true;
    }
    if lower.starts_with('$') && lower.len() > 1 {
        return true;
    }
    // `{{ vault_password }}` — templating, not a value.
    if lower.starts_with("{{") && lower.ends_with("}}") {
        return true;
    }
    if PLACEHOLDER_VALUES.contains(&lower.as_str()) {
        return true;
    }
    is_mask_run(&lower)
}

/// `<password>`, `<your-db-password>`. The 5-char floor keeps `<a>` out.
fn is_bracketed(value: &str) -> bool {
    let bytes = value.as_bytes();
    if value.len() < 5 || bytes[0] != b'<' || bytes[value.len() - 1] != b'>' {
        return false;
    }
    bracketed_interior().is_match(&value[1..value.len() - 1])
}

/// `****`, `xxxx`, `....`, `----` — the masking runs used in docs and
/// screenshots. Three characters minimum so `x` and `**` are not treated as
/// masks, which would let a two-character real value through.
fn is_mask_run(value: &str) -> bool {
    if value.len() < 3 {
        return false;
    }
    let mut chars = value.chars();
    let first = chars.next().expect("non-empty");
    if !matches!(first, '*' | 'x' | '.' | '-') {
        return false;
    }
    chars.all(|c| c == first)
}

/// A value worth redacting: present, and not a placeholder.
pub(crate) fn is_real_secret_value(value: &str) -> bool {
    !value.trim().is_empty() && !is_placeholder(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn documentation_placeholders_are_recognised() {
        for value in [
            "<your-password-here>",
            "<password>",
            "changeme",
            "PLACEHOLDER",
            "${DB_PASSWORD}",
            "$DB_PASSWORD",
            "{{ vault_password }}",
            "****",
            "xxxxxxxx",
            "----",
            "",
            "   ",
        ] {
            assert!(is_placeholder(value), "expected placeholder: {value:?}");
        }
    }

    #[test]
    fn our_own_output_is_a_placeholder_so_a_second_pass_is_a_no_op() {
        assert!(is_placeholder("[REDACTED]"));
        assert!(is_placeholder("REDACTED"));
    }

    #[test]
    fn real_values_are_not_placeholders() {
        for value in [
            "hunter2",
            "<hunter2>",
            "<RealPassword>",
            "s3cr3t-value",
            "xxxy",
            "x",
        ] {
            assert!(!is_placeholder(value), "expected real value: {value:?}");
        }
    }

    #[test]
    fn quotes_around_a_placeholder_do_not_hide_it() {
        assert!(is_placeholder("\"changeme\""));
        assert!(is_placeholder("'<password>'"));
    }
}
