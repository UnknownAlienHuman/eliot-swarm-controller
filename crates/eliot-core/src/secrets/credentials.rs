//! Credential shapes in native semantic text. All matching is local and the
//! replacement retains only the kind and original byte length.

use regex::{Captures, Match, Regex};
use std::{borrow::Cow, sync::LazyLock};

#[derive(Clone, Copy)]
enum Kind {
    OpenAi,
    Anthropic,
    OpenRouter,
    GitHub,
    Jwt,
    Bearer,
    QueryKey,
    QueryToken,
    FieldHex,
    FieldBase64,
}

impl Kind {
    fn label(self) -> &'static str {
        match self {
            Self::OpenAi => "openai",
            Self::Anthropic => "anthropic",
            Self::OpenRouter => "openrouter",
            Self::GitHub => "github",
            Self::Jwt => "jwt",
            Self::Bearer => "bearer",
            Self::QueryKey => "query_api_key",
            Self::QueryToken => "query_token",
            Self::FieldHex => "hex_field",
            Self::FieldBase64 => "base64_field",
        }
    }

    fn marker(self, bytes: usize) -> String {
        format!("[REDACTED:{}:{bytes}]", self.label())
    }
}

const PROVIDER: &str = concat!(
    r"(?P<anthropic>sk-ant-[A-Za-z0-9_-]{24,})|",
    r"(?P<openrouter>sk-or-v1-[0-9a-fA-F]{64})|",
    r"(?P<openai>sk-(?:(?:proj-|svcacct-|admin-)[A-Za-z0-9_-]{24,}|[A-Za-z0-9_]{24,}[A-Za-z0-9_-]*))|",
    r"(?P<github>gh[pousr]_[A-Za-z0-9]{36}|github_pat_[A-Za-z0-9_]{48,})|",
    r"(?P<jwt>eyJ[A-Za-z0-9_-]{5,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{12,})"
);
const TOKEN: &str = r"[A-Za-z0-9_~+/%=-][A-Za-z0-9_~+/%=.-]{14,}[A-Za-z0-9_~+/%=-]";
const FIELD: &str = concat!(
    "api[_-]?key|token|access[_-]?token|refresh[_-]?token|auth[_-]?token|",
    "bearer[_-]?token|dashboard[_-]?token|proxy[_-]?token|private[_-]?key|",
    "client[_-]?secret|secret[_-]?key|password"
);

static COMPLETE: LazyLock<Regex> = LazyLock::new(|| pattern(&format!(r"\A(?:{PROVIDER})\z")));
static TEXT: LazyLock<Regex> = LazyLock::new(|| {
    pattern(&format!(
        r#"\b(?:(?i:bearer)[ \t]+(?P<bearer>{TOKEN})|(?P<label>(?i:api[_-]?key|token))=(?P<query>{TOKEN})|(?i:{FIELD})["']?\s*[:=]\s*["']?(?P<field>{TOKEN})|{PROVIDER})"#,
    ))
});

fn pattern(value: &str) -> Regex {
    Regex::new(value).expect("credential pattern is valid")
}

fn provider(captures: &Captures<'_>) -> Option<Kind> {
    [
        ("anthropic", Kind::Anthropic),
        ("openrouter", Kind::OpenRouter),
        ("openai", Kind::OpenAi),
        ("github", Kind::GitHub),
        ("jwt", Kind::Jwt),
    ]
    .into_iter()
    .find_map(|(name, kind)| captures.name(name).map(|_| kind))
}

fn complete_provider(text: &str) -> Option<Kind> {
    COMPLETE.captures(text).as_ref().and_then(provider)
}

fn ordinary_identifier(text: &str) -> bool {
    let bytes = text.as_bytes();
    (bytes.len() == 40 && bytes.iter().all(u8::is_ascii_hexdigit))
        || (bytes.len() == 26
            && matches!(bytes[0], b'0'..=b'7')
            && bytes.iter().all(|byte| {
                matches!(byte.to_ascii_uppercase(), b'0'..=b'9' | b'A'..=b'H' | b'J'..=b'K' | b'M'..=b'N' | b'P'..=b'T' | b'V'..=b'Z')
            }))
}

fn field_kind(text: &str) -> Option<Kind> {
    if text.len() < 24 || ordinary_identifier(text) {
        return None;
    }
    let body = text.trim_end_matches('=');
    let hex = body.len() >= 32 && text.bytes().all(|byte| byte.is_ascii_hexdigit());
    let base64 = text.len() - body.len() <= 2
        && body
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'+' | b'/'));
    if !hex && !base64 {
        return None;
    }
    let mut counts = [0_usize; 128];
    for byte in body.bytes() {
        let byte = if hex { byte.to_ascii_lowercase() } else { byte };
        counts[usize::from(byte)] += 1;
    }
    let total = body.len() as f64;
    let entropy: f64 = counts
        .into_iter()
        .filter(|count| *count != 0)
        .map(|count| {
            let p = count as f64 / total;
            -p * p.log2()
        })
        .sum();
    match (hex, entropy) {
        (true, e) if e >= 3.1 => Some(Kind::FieldHex),
        (false, e) if e >= 3.55 => Some(Kind::FieldBase64),
        _ => None,
    }
}

fn labeled_token(text: &str) -> bool {
    if ordinary_identifier(text) {
        return false;
    }
    let lower = text.to_ascii_lowercase();
    if ["redacted", "your_", "example_", "placeholder"]
        .iter()
        .any(|prefix| lower.starts_with(prefix))
    {
        return false;
    }
    let mut seen = [false; 128];
    for byte in text.bytes() {
        seen[usize::from(byte)] = true;
    }
    seen.into_iter().filter(|value| *value).count() >= 9
}

fn matches(text: &str) -> impl Iterator<Item = (Match<'_>, Kind)> {
    TEXT.captures_iter(text).filter_map(|captures| {
        for label in ["bearer", "query", "field"] {
            if let Some(token) = captures.name(label) {
                let kind = complete_provider(token.as_str()).or_else(|| match label {
                    "field" => field_kind(token.as_str()),
                    "bearer" if labeled_token(token.as_str()) => Some(Kind::Bearer),
                    "query" if labeled_token(token.as_str()) => {
                        if captures["label"].eq_ignore_ascii_case("token") {
                            Some(Kind::QueryToken)
                        } else {
                            Some(Kind::QueryKey)
                        }
                    }
                    _ => None,
                });
                return kind.map(|kind| (token, kind));
            }
        }
        Some((captures.get(0)?, provider(&captures)?))
    })
}

pub(super) fn contains(text: &str) -> bool {
    matches(text).next().is_some()
}

pub(super) fn redact(text: &str) -> Cow<'_, str> {
    let mut matches = matches(text).peekable();
    if matches.peek().is_none() {
        return Cow::Borrowed(text);
    }
    let mut result = String::with_capacity(text.len());
    let mut end = 0;
    for (token, kind) in matches {
        result.push_str(&text[end..token.start()]);
        result.push_str(&kind.marker(token.len()));
        end = token.end();
    }
    result.push_str(&text[end..]);
    Cow::Owned(result)
}

pub(super) fn redact_field<'a>(text: &'a str, field: Option<&str>) -> Cow<'a, str> {
    if field.is_some_and(|name| FIELD_NAME.is_match(name))
        && let Some(kind) = complete_provider(text).or_else(|| field_kind(text))
    {
        return Cow::Owned(kind.marker(text.len()));
    }
    redact(text)
}

static FIELD_NAME: LazyLock<Regex> = LazyLock::new(|| pattern(&format!(r"\A(?i:{FIELD})\z")));
