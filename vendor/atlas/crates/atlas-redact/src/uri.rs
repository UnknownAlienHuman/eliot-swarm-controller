//! Layer 4 — credentialed URIs.
//!
//! `postgres://user:hunter2@db.internal/app`, `redis://:pass@cache:6379/0`,
//! `https://token:x-oauth-basic@github.com/org/repo`. The password sits in
//! userinfo, is frequently a memorable low-entropy word, and belongs to no
//! vendor's key format — so the three layers above all pass over it.
//!
//! The whole URI is replaced rather than just the password. The host and
//! database name in an internal connection string are themselves infrastructure
//! detail, and splicing a placeholder into the middle produces a string that
//! still *looks* usable, which invites someone to try to use it.

use std::sync::OnceLock;

use regex::Regex;

use crate::placeholder::is_real_secret_value;
use crate::region::Region;
use crate::Category;

/// `scheme://userinfo:password@host…`, stopping at whitespace and the quoting
/// characters that bound a URI inside prose, JSON, or a shell command.
fn credentialed_uri() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(
            r#"(?i)\b[a-z][a-z0-9+.-]{1,31}://[^\s/?#@"'`<>:]*:([^\s/?#@"'`<>]+)@[^\s"'`<>]+"#,
        )
        .expect("static pattern")
    })
}

pub(crate) fn detect(input: &str) -> Vec<Region> {
    let mut regions = Vec::new();
    for captures in credentialed_uri().captures_iter(input) {
        let whole = captures.get(0).expect("group 0 always present");
        // `postgres://user:<password>@host` in a README is documentation, not a
        // leak, and redacting it makes the docs unreadable for no gain.
        let password = captures.get(1).map(|m| m.as_str()).unwrap_or_default();
        if !is_real_secret_value(password) {
            continue;
        }
        regions.push(Region::new(whole.start(), whole.end(), Category::CredentialedUri));
    }
    regions
}

/// Flag only the userinfo password of each credentialed URI, leaving the rest
/// of the link intact.
///
/// The whole-URI replacement above is right for prose, where a half-redacted
/// connection string invites reuse — but in a JSON `url` field the link *is*
/// the datum, and destroying it destroys the record. There the password is the
/// only part that must go.
pub(crate) fn detect_userinfo_passwords(input: &str) -> Vec<Region> {
    let mut regions = Vec::new();
    for captures in credentialed_uri().captures_iter(input) {
        let Some(password) = captures.get(1) else {
            continue;
        };
        if !is_real_secret_value(password.as_str()) {
            continue;
        }
        regions.push(Region::new(password.start(), password.end(), Category::CredentialedUri));
    }
    regions
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spans(input: &str) -> Vec<&str> {
        detect(input)
            .into_iter()
            .map(|r| &input[r.start..r.end])
            .collect()
    }

    #[test]
    fn a_postgres_url_with_a_password_is_flagged_whole() {
        assert_eq!(
            spans("db is postgres://app:hunter2@db.internal:5432/prod now"),
            vec!["postgres://app:hunter2@db.internal:5432/prod"]
        );
    }

    #[test]
    fn a_passwordless_url_is_left_alone() {
        assert!(spans("clone https://github.com/tryatlas/atlas.git").is_empty());
    }

    #[test]
    fn a_documented_placeholder_password_is_left_alone() {
        assert!(spans("postgres://user:<password>@localhost/db").is_empty());
        assert!(spans("redis://:${REDIS_PASSWORD}@cache:6379/0").is_empty());
    }

    #[test]
    fn an_empty_userinfo_password_form_is_still_caught() {
        assert_eq!(
            spans("redis://:s3cr3tpass@cache:6379/0"),
            vec!["redis://:s3cr3tpass@cache:6379/0"]
        );
    }
}
