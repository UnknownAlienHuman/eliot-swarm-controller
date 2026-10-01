//! Parity with the ad-hoc redactor this crate replaces.
//!
//! The app already carries three inconsistent redactors — `memory_delta.rs`
//! (pattern-based, tested, explicitly "no entropy scan in MVP"), the telemetry
//! one, and the auth one. This crate becomes the single source of truth for
//! *secret* redaction, and ATL-80's contract is that nothing `memory_delta`
//! caught may be lost in the swap.
//!
//! Its own test cases are ported verbatim below, plus a case per pattern in its
//! `looks_secret` table, so the parity claim is checked rather than asserted.

use atlas_redact::redact;

mod fixtures;

#[track_caller]
fn assert_scrubbed(input: &str, secret: &str) {
    let out = redact(input);
    assert!(
        !out.text.contains(secret),
        "memory_delta caught this and we do not\n  input:  {input}\n  output: {}",
        out.text
    );
}

// ── Ported verbatim from memory_delta.rs's own tests ────────────────────────

#[test]
fn secrets_are_redacted() {
    let secret = fixtures::legacy_sk_key();
    let out = redact(&format!("the key is {secret} and done"));
    assert!(out.text.contains("[REDACTED]"));
    assert!(!out.text.contains(&secret));
}

#[test]
fn assignment_secret_redacted() {
    assert!(redact("API_KEY=supersecretvalue123").text.contains("[REDACTED]"));
}

#[test]
fn ordinary_words_not_redacted() {
    assert_eq!(redact("the quick brown fox jumps").text, "the quick brown fox jumps");
}

// ── One case per prefix in memory_delta's `looks_secret` table ──────────────

#[test]
fn every_prefix_memory_delta_recognised_is_still_caught() {
    for secret in [
        fixtures::legacy_sk_key(),
        fixtures::github_pat(),
        fixtures::github_oauth_token(),
        fixtures::slack_bot_token(),
        fixtures::slack_user_token(),
        fixtures::aws_access_key_id(),
    ] {
        assert_scrubbed(&format!("the value is {secret} ok"), &secret);
    }
}

#[test]
fn every_assignment_key_memory_delta_recognised_is_still_caught() {
    // Its rule was: key containing secret/token/password/apikey/api_key with a
    // value of six characters or more.
    for input in [
        "CLIENT_SECRET=abcdef123456",
        "AUTH_TOKEN=abcdef123456",
        "DB_PASSWORD=abcdef123456",
        "APIKEY=abcdef123456",
        "API_KEY=abcdef123456",
    ] {
        assert_scrubbed(input, "abcdef123456");
    }
}

#[test]
fn the_long_opaque_blob_rule_is_still_covered() {
    // memory_delta redacted any 32+ character alphanumeric run mixing digits and
    // letters. The entropy layer is what carries that here.
    let secret = fixtures::opaque_high_entropy_token();
    assert_scrubbed(&format!("blob {secret} end"), &secret);
}

#[test]
fn a_bearer_header_is_still_caught() {
    let secret = fixtures::jwt();
    assert_scrubbed(&format!("Authorization: Bearer {secret}"), &secret);
}
