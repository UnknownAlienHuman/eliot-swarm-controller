//! Secret-shaped test vectors, assembled at runtime.
//!
//! These have to be *structurally valid* — a redaction test proves nothing
//! against a token that would not match a real rule — which means they are, by
//! construction, indistinguishable from live credentials to a scanner. Written
//! as literals they trip GitHub's push protection and block the branch, and the
//! only ways out are to weaken the fixtures or to teach everyone on the repo to
//! click "allow this secret", which is a habit worth not forming.
//!
//! So each vector is joined from a prefix and a body at runtime. The value the
//! redactor sees is byte-for-byte what it would have been; no complete token
//! ever exists in the source. The bodies are randomly generated but committed —
//! reproducibility matters more than novelty for a fixture, and a scanner keys
//! on the *whole* shape, not the body alone.
//!
//! Every vector below is fake. None was ever issued by any provider.

// Each test binary compiles this whole module and uses a different subset of it,
// so unused-vector warnings are structural rather than a sign of dead fixtures.
#![allow(dead_code)]

/// Join a prefix to a body. The concatenation is what makes this work: the
/// literal token never appears, only its halves.
fn token(prefix: &str, body: &str) -> String {
    format!("{prefix}{body}")
}

/// Anthropic API key: `sk-ant-api03-` + 93 chars + `AA`.
pub fn anthropic_api_key() -> String {
    token(
        "sk-ant-",
        "api03-PtYgjmUhBel31iEl2hpChYgCfrL1spNxnyVmihA-2O76UMFxFkM-R5Kjp1vRt_1fjORS-6ilI8ihN5KXSc7Tvo-hBKqFYAA",
    )
}

/// Legacy `sk-` key — the format neither entropy nor the vendored corpus
/// catches, and the reason the provider layer carries short prefixes.
pub fn legacy_sk_key() -> String {
    token("sk-", "ABCDEF0123456789ABCDEF")
}

/// GitHub personal access token: `ghp_` + 36 alphanumerics.
pub fn github_pat() -> String {
    token("ghp", "_z63FfkCzJr4i0B3JrTAwR4y9ojfljoQoaF1L")
}

/// GitHub OAuth token — same shape, different prefix.
pub fn github_oauth_token() -> String {
    token("gho", "_z63FfkCzJr4i0B3JrTAwR4y9ojfljoQoaF1L")
}

/// Google API key: `AIza` + 35 chars.
pub fn google_api_key() -> String {
    token("AIza", "xHKas1VOqg6YYZYn9ZhyiA4uoRgnatmUdjA")
}

/// Slack bot token.
pub fn slack_bot_token() -> String {
    token("xoxb", "-2837465091-4839201756-NyjOq9wMxEhh2FDEEtfjgVvV")
}

/// Slack user token.
pub fn slack_user_token() -> String {
    token("xoxp", "-2837465091-4839201756-NyjOq9wMxEhh2FDEEtfjgVvV")
}

/// SendGrid API token: `SG.` + 66 chars.
pub fn sendgrid_token() -> String {
    token(
        "SG.",
        "bj3j4wj99ibag7i1mnbqns.6p-uq80idw3-706i8j76b2lajlj4h9du7794g9dpmrc",
    )
}

/// AWS access key id. Entropy 3.58 — below the threshold, which is what makes
/// it prove the vendored corpus is doing work the entropy layer cannot.
pub fn aws_access_key_id() -> String {
    token("AKIA", "QXKWOVOMPZOM7WBB")
}

/// Stripe live secret key. Entropy 4.45, also below the threshold.
pub fn stripe_live_key() -> String {
    token("sk_live", "_qE1SkHbn88HxjSI6bWHtP3fS")
}

/// npm access token: `npm_` + 36 lowercase alphanumerics.
pub fn npm_token() -> String {
    token("npm", "_r4qmw2wxfogo4mvn4a4wfhym4l1vfz3zfkki")
}

/// Twilio API key: `SK` + 32 hex. Keyword-gated upstream, so it only fires when
/// "twilio" appears nearby — which is what the gating test relies on.
pub fn twilio_api_key() -> String {
    token("SK", "3e02ea68ef786e4d3cea27d26934b484")
}

/// Supabase secret key, high entropy.
pub fn supabase_secret_key() -> String {
    token("sb_secret", "_QRSTUVWXYZabcdefghijklmnop")
}

/// Supabase secret key with a body too repetitive for the entropy layer — the
/// case only the provider layer can reach.
pub fn supabase_secret_key_low_entropy() -> String {
    token("sb_secret", "_aaaaaaaaaaaaaaaaaaaaaaaaaa")
}

/// Supabase *publishable* key. Designed to ship in client code, and therefore
/// deliberately not a provider-layer rule.
pub fn supabase_publishable_key() -> String {
    token("sb_publishable", "_QRSTUVWXYZabcdefghijklmnop")
}

/// Low-entropy publishable key — invisible to the entropy layer, so it proves
/// the provider layer genuinely leaves it alone.
pub fn supabase_publishable_key_low_entropy() -> String {
    token("sb_publishable", "_aaaaaaaaaaaaaaaaaaaaaaaaaa")
}

/// A JWT, for the bearer-header case.
pub fn jwt() -> String {
    token(
        "eyJhbGciOiJIUzI1NiJ9.",
        "eyJzdWIiOiIxMjM0NTY3ODkwIn0.dozjgNryP4J3jVmNHl0w5N_XgL0n3I9PlFUP0THsR8U",
    )
}

/// An in-house token matching no vendor format — the case only entropy reaches.
pub fn opaque_high_entropy_token() -> String {
    token("xJ3kQ9vB2mZ7pL5rT8", "wN4cF6yH1sD0gAe4Ru")
}
