//! Table-driven coverage: one realistic case per layer, plus the provider
//! formats Atlas users actually paste into agent conversations.
//!
//! Assertions are on observable output — the secret is gone, the surrounding
//! text survives — never on which internal layer fired. Several of these tokens
//! are catchable by more than one layer, and pinning a case to a layer would
//! make the suite fail the first time the rule corpus is regenerated.

use atlas_redact::{redact, Category, PLACEHOLDER};

mod fixtures;

/// The secret is gone and the placeholder took its place.
#[track_caller]
fn assert_scrubbed(input: &str, secret: &str) {
    let out = redact(input);
    assert!(
        !out.text.contains(secret),
        "secret survived redaction\n  input:  {input}\n  output: {}",
        out.text
    );
    assert!(
        out.text.contains(PLACEHOLDER),
        "nothing was replaced\n  input:  {input}\n  output: {}",
        out.text
    );
    assert!(out.counts.total() > 0, "replacement was not counted");
}

// ── Layer 1: Shannon entropy ────────────────────────────────────────────────

#[test]
fn entropy_catches_a_random_key_no_rule_covers() {
    // Deliberately not any vendor's format: an in-house token, which is exactly
    // the case only the entropy layer can reach.
    let secret = fixtures::opaque_high_entropy_token();
    let secret = secret.as_str();
    assert_scrubbed(&format!("the internal key is {secret} for now"), secret);
    assert_eq!(
        redact(&format!("key {secret}")).counts.get(Category::Entropy),
        1
    );
}

// ── Layer 2: the vendored betterleaks corpus ────────────────────────────────
//
// Each of these has entropy *below* the 4.5 threshold, so they prove the corpus
// is doing work the entropy layer cannot.

#[test]
fn vendor_rules_catch_low_entropy_provider_formats() {
    for (name, context) in [
        ("aws access key id", fixtures::aws_access_key_id()),
        ("stripe live key", fixtures::stripe_live_key()),
        ("npm access token", fixtures::npm_token()),
    ] {
        let out = redact(&format!("export TOKEN {context} end"));
        assert!(
            !out.text.contains(&context),
            "{name} survived redaction: {}",
            out.text
        );
    }
}

#[test]
fn a_keyword_gated_vendor_rule_fires_when_its_vendor_is_named() {
    // Some corpus rules are shapes too generic to run unconditionally — a Twilio
    // key is `SK` plus 32 hex characters, which is also half the hashes in a
    // build log. Upstream gates them on the vendor name appearing nearby, and
    // this crate honours that gate rather than redacting every hex string.
    let secret = fixtures::twilio_api_key();
    let out = redact(&format!("TWILIO_API_KEY={secret}"));
    assert!(!out.text.contains(&secret), "gated rule did not fire: {}", out.text);

    let ungated = redact(&format!("the digest was {secret} before"));
    assert_eq!(
        ungated.text,
        format!("the digest was {secret} before"),
        "an ungated hex string should not be redacted"
    );
}

#[test]
fn vendor_rules_catch_the_formats_atlas_users_actually_have() {
    for (name, secret) in [
        ("anthropic api key", fixtures::anthropic_api_key()),
        ("github pat", fixtures::github_pat()),
        ("google api key", fixtures::google_api_key()),
        ("slack bot token", fixtures::slack_bot_token()),
        ("sendgrid api token", fixtures::sendgrid_token()),
    ] {
        let out = redact(&format!("the agent read {secret} from .env"));
        assert!(
            !out.text.contains(&secret),
            "{name} survived redaction: {}",
            out.text
        );
    }
}

// ── Layer 3: deterministic provider prefixes ────────────────────────────────

#[test]
fn provider_prefixes_catch_a_supabase_secret_no_other_layer_can_see() {
    // This is precisely the gap the layer exists for: the corpus's Supabase rule
    // is composite (it only fires when a matching project URL appears alongside),
    // and the body here is too repetitive to clear the entropy threshold. Both
    // other layers pass over it; without this one the raw key reaches storage.
    let secret = fixtures::supabase_secret_key_low_entropy();
    assert_scrubbed(&secret, &secret);
    assert_eq!(
        redact(&secret).counts.get(Category::ProviderToken),
        1,
        "expected the provider layer specifically"
    );
}

#[test]
fn a_high_entropy_supabase_secret_is_caught_too() {
    let secret = fixtures::supabase_secret_key();
    assert_scrubbed(&secret, &secret);
}

// ── Layer 4: credentialed URIs ──────────────────────────────────────────────

#[test]
fn credentialed_uris_are_replaced_whole() {
    let out = redact("connect to postgres://app:hunter2@db.internal:5432/prod first");
    assert!(!out.text.contains("hunter2"));
    assert!(!out.text.contains("db.internal"), "host leaked: {}", out.text);
    assert_eq!(out.text, format!("connect to {PLACEHOLDER} first"));
    assert_eq!(out.counts.get(Category::CredentialedUri), 1);
}

// ── Layer 5: database connection strings ────────────────────────────────────

#[test]
fn connection_strings_are_replaced_whole_in_every_shape() {
    for dsn in [
        "jdbc:postgresql://db:5432/app?user=svc&password=hunter2",
        "host=db.internal user=app password=hunter2 sslmode=require",
        "Server=db;User Id=app;Password=hunter2;Encrypt=true",
        "mysql://db.internal/app?user=svc&password=hunter2",
    ] {
        let out = redact(dsn);
        assert!(!out.text.contains("hunter2"), "password survived in {dsn}");
        assert_eq!(out.text, PLACEHOLDER, "expected a whole-unit replacement");
    }
}

// ── Layer 6: bounded credential key/value ───────────────────────────────────

#[test]
fn credential_assignments_keep_the_key_and_replace_the_value() {
    let out = redact("API_KEY=supersecretvalue123");
    assert_eq!(out.text, format!("API_KEY={PLACEHOLDER}"));
    assert_eq!(out.counts.get(Category::CredentialValue), 1);
}

#[test]
fn credential_keys_are_recognised_across_spellings() {
    for input in [
        "GITHUB_TOKEN=abcdefgh1234",
        "db_password=hunter2xyz",
        "x-api-key: abcdefgh1234",
        "authToken: abcdefgh1234",
        "client.secret=abcdefgh1234",
        "SUPABASE_SECRET_KEY=abcdefgh1234",
    ] {
        let out = redact(input);
        assert!(
            out.text.contains(PLACEHOLDER),
            "credential key not recognised in {input}"
        );
    }
}

// ── The composed pass ───────────────────────────────────────────────────────

#[test]
fn a_realistic_env_file_dump_is_scrubbed_but_still_readable() {
    let github_token = fixtures::github_pat();
    let input = format!(
        "DATABASE_URL=postgres://app:hunter2@db.internal:5432/prod\n\
         GITHUB_TOKEN={github_token}\n\
         LOG_LEVEL=debug\n\
         PORT=8080\n\
         PWD=/Users/nafiz/Development/atlas"
    );

    let out = redact(&input);
    assert!(!out.text.contains("hunter2"));
    assert!(!out.text.contains(&github_token));
    // The non-secrets are what make the transcript worth keeping.
    assert!(out.text.contains("LOG_LEVEL=debug"), "{}", out.text);
    assert!(out.text.contains("PORT=8080"), "{}", out.text);
    assert!(
        out.text.contains("PWD=/Users/nafiz/Development/atlas"),
        "{}",
        out.text
    );
    assert!(out.text.contains("DATABASE_URL="), "key name lost");
}

#[test]
fn a_credential_nested_inside_another_assignment_is_still_found() {
    // Regression: the scan used to consume the whole assignment including its
    // value, and regex iteration is non-overlapping — so an outer assignment
    // whose key is innocuous hid the credential sitting inside its value. Every
    // shape below leaked, and all of them are shapes agents record routinely.
    for input in [
        r#"out = f("API_KEY=hunter2xyz")"#,
        "x=API_KEY=hunter2xyz",
        "url=https://api.example.com/v1?api_key=hunter2xyz",
        "deploy --env=API_KEY=hunter2xyz",
        "ARGS=--token=hunter2xyz",
        r#"{"command":"export DB_PASSWORD=hunter2xyz"}"#,
    ] {
        assert_scrubbed(input, "hunter2xyz");
    }
}

#[test]
fn replacing_a_value_leaves_the_syntax_around_it_intact() {
    // The other half of the same bug: an unquoted value ends at whitespace, so
    // it swallowed the closing delimiters and left a corrupted line behind.
    for (input, expected) in [
        (
            r#"f("API_KEY=hunter2xyz")"#,
            format!(r#"f("API_KEY={PLACEHOLDER}")"#),
        ),
        (
            r#"{"k": "API_KEY=hunter2xyz"}"#,
            format!(r#"{{"k": "API_KEY={PLACEHOLDER}"}}"#),
        ),
        (
            "[API_KEY=hunter2xyz]",
            format!("[API_KEY={PLACEHOLDER}]"),
        ),
        (
            "call(token=hunter2xyz);",
            format!("call(token={PLACEHOLDER});"),
        ),
    ] {
        assert_eq!(redact(input).text, expected, "input: {input}");
    }
}
