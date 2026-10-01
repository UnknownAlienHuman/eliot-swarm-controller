//! Layer 3 — deterministic provider token prefixes.
//!
//! These exist because the two layers above them both miss the same class of
//! secret: a credential with a fixed, recognisable prefix and a body too short
//! or too narrow-alphabet to clear the entropy threshold, for which the vendored
//! corpus either has no rule or has a *composite* rule that only fires when
//! another marker (a project URL, say) appears in the same string. A key pasted
//! into a transcript on its own has no such company.
//!
//! Detection here is prefix + length only. It never consults entropy or the
//! surrounding key name, which is exactly what makes it the backstop.

use std::sync::OnceLock;

use regex::Regex;

use crate::region::Region;
use crate::Category;

/// Deterministic secret-token shapes, as `(name, pattern)`. Where a pattern has
/// a capture group, that group is the redacted span; otherwise the whole match is.
///
/// Anchoring is decided per prefix, and the two answers are both deliberate:
///
/// * A **long, distinctive** prefix (`sb_secret_`) is left unanchored. A word
///   boundary requires a non-word character before the prefix, so a token glued
///   to a preceding word character — `FOO_sb_secret_…`, or a JSON escape whose
///   trailing letter abuts the prefix — would slip through. Nothing else covers
///   these, so a miss means the raw key reaches storage, and the worst an
///   unanchored match can do is over-redact an identifier that happens to start
///   the same way.
/// * A **short** prefix (`sk-`) must have a boundary. Unanchored, `sk-` matches
///   inside `disk-management-controller-xyz`, and that class of false positive
///   would fire on ordinary code every day.
const PROVIDER_PATTERNS: &[(&str, &str)] = &[
    // Supabase secret API key — bypasses row-level security, server-side only.
    // The publishable key (`sb_publishable_`) is deliberately absent: it is
    // designed to ship in client code and redacting it is pure noise. Note this
    // only means *this* layer leaves it alone — a high-entropy publishable key
    // is still caught by the entropy layer, and that is accepted rather than
    // fixed. Suppressing a redaction by prefix is a mechanism whose failure mode
    // is a real key walking through, which is far worse than over-redacting a
    // public one.
    ("supabase-secret-key", r"sb_secret_[A-Za-z0-9_-]{20,}"),
    // Supabase personal access token (CLI / Management API).
    ("supabase-access-token", r"sbp_[a-z0-9_-]{20,}"),
    // Legacy `sk-` API keys (OpenAI's pre-`sk-proj-` format, and anything else
    // that adopted the convention). The vendored corpus only carries the modern
    // shapes, because upstream tracks what providers currently issue — but a key
    // minted years ago still works, and still turns up in a transcript. Its
    // alphabet is narrow enough that entropy misses it too, so without this rule
    // nothing catches it at all.
    ("legacy-sk-api-key", r"(?:^|[^A-Za-z0-9])(sk-[A-Za-z0-9_-]{16,})"),
    // GitHub personal access / OAuth / user / server / refresh tokens. The
    // corpus pins exact lengths per kind; this covers the same prefixes at any
    // length, which is what a truncated or future-length token needs.
    ("github-token", r"(?:^|[^A-Za-z0-9])(gh[pousr]_[A-Za-z0-9]{16,})"),
    // Slack bot / user / app / workspace tokens. The corpus rule requires the
    // two numeric id groups; a token quoted without them still leaks.
    ("slack-token", r"(?:^|[^A-Za-z0-9])(xox[baprse]-[A-Za-z0-9-]{10,})"),
];

fn patterns() -> &'static [(&'static str, Regex)] {
    static COMPILED: OnceLock<Vec<(&'static str, Regex)>> = OnceLock::new();
    COMPILED.get_or_init(|| {
        PROVIDER_PATTERNS
            .iter()
            .map(|(name, pattern)| (*name, Regex::new(pattern).expect("static pattern")))
            .collect()
    })
}

pub(crate) fn detect(input: &str) -> Vec<Region> {
    let mut regions = Vec::new();
    for (_, regex) in patterns() {
        for captures in regex.captures_iter(input) {
            // Group 1 where the pattern needed a leading boundary, so the
            // boundary character itself is not swallowed by the placeholder.
            let Some(token) = captures.get(1).or_else(|| captures.get(0)) else {
                continue;
            };
            regions.push(Region::new(token.start(), token.end(), Category::ProviderToken));
        }
    }
    regions
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Assembled at runtime so no complete token literal lives in the source —
    /// these are structurally valid by necessity, and a literal one trips
    /// secret scanning on every push.
    fn token(prefix: &str, body: &str) -> String {
        format!("{prefix}{body}")
    }

    #[test]
    fn a_supabase_secret_key_alone_in_a_string_is_found() {
        let input = format!("SUPABASE_KEY={}", token("sb_secret", "_QRSTUVWXYZabcdefghijklmnop"));
        assert_eq!(detect(&input).len(), 1);
    }

    #[test]
    fn a_token_glued_to_a_preceding_word_character_is_still_found() {
        let input = format!(r"line1\n{}", token("sb_secret", "_QRSTUVWXYZabcdefghijklmnop"));
        assert_eq!(detect(&input).len(), 1);
    }

    #[test]
    fn the_publishable_key_is_left_alone() {
        assert!(detect(&token("sb_publishable", "_QRSTUVWXYZabcdefghijklmnop")).is_empty());
    }

    #[test]
    fn a_short_identifier_sharing_the_prefix_is_not_a_token() {
        assert!(detect("sb_secret_short").is_empty());
    }

    #[test]
    fn a_legacy_sk_key_is_found_and_the_boundary_is_not_swallowed() {
        let secret = token("sk-", "ABCDEF0123456789ABCDEF");
        let input = format!("the value is {secret} ok");
        let regions = detect(&input);
        assert_eq!(regions.len(), 1);
        assert_eq!(&input[regions[0].start..regions[0].end], secret);
    }

    #[test]
    fn a_short_prefix_inside_an_ordinary_identifier_is_not_a_token() {
        // The reason short prefixes are anchored and long ones are not.
        assert!(detect("disk-management-controller-service").is_empty());
        assert!(detect("the risk-assessment-framework-v2 doc").is_empty());
    }

    #[test]
    fn github_and_slack_prefixes_are_found() {
        assert_eq!(detect(&token("ghp", "_z63FfkCzJr4i0B3JrTAwR4y9ojfljoQoaF1L")).len(), 1);
        assert_eq!(detect(&token("gho", "_z63FfkCzJr4i0B3JrTAwR4y9ojfljoQoaF1L")).len(), 1);
        assert_eq!(detect(&token("xoxp", "-2837465091-4839201756-NyjOq")).len(), 1);
    }
}
