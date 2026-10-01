//! Layer 5 — database connection strings, treated as one unit.
//!
//! Four shapes in the wild, none of which the earlier layers handle:
//!
//! * JDBC — `jdbc:postgresql://host/db?user=app&password=hunter2`
//! * Database URL with the password in the query — `mysql://host/db?password=…`
//! * Keyword DSN — `host=db user=app password=hunter2 sslmode=require`
//! * Semicolon form — `Server=db;User Id=app;Password=hunter2;`
//!
//! Each is redacted whole. A DSN with only its password blanked is still a
//! working map of internal infrastructure, and — more practically — the keyword
//! and semicolon forms have no reliable value boundary, so splicing into the
//! middle of one is how you produce a corrupted string that reads as valid.
//!
//! Every shape is gated on actually containing a non-placeholder password. The
//! keyword and semicolon patterns are loose by necessity (any `k=v k=v k=v` run
//! matches), so without that gate this layer would redact ordinary CLI flags.

use std::sync::OnceLock;

use regex::Regex;

use crate::placeholder::is_real_secret_value;
use crate::region::Region;
use crate::Category;

struct Shape {
    pattern: Regex,
    /// Extra evidence that the match really is a connection string, beyond
    /// "it contains a password assignment".
    corroborates: fn(&str) -> bool,
}

fn shapes() -> &'static [Shape] {
    static SHAPES: OnceLock<Vec<Shape>> = OnceLock::new();
    SHAPES.get_or_init(|| {
        vec![
            Shape {
                pattern: Regex::new(r#"(?i)\bjdbc:[^\s"'<>`]+"#).expect("static pattern"),
                corroborates: |candidate| candidate.to_ascii_lowercase().starts_with("jdbc:"),
            },
            Shape {
                pattern: Regex::new(
                    r#"(?i)\b(?:postgres(?:ql)?|mysql|mariadb|mongodb(?:\+srv)?|redis|sqlserver)://[^\s"'<>`]+"#,
                )
                .expect("static pattern"),
                // Userinfo passwords are the URI layer's job; here we only care
                // about the `?password=` query form.
                corroborates: |candidate| candidate.contains('?'),
            },
            Shape {
                pattern: Regex::new(
                    r#"(?i)\b[a-z_][a-z0-9_]*=(?:"[^"]*"|'[^']*'|[^\s"']+)(?:\s+[a-z_][a-z0-9_]*=(?:"[^"]*"|'[^']*'|[^\s"']+)){2,}"#,
                )
                .expect("static pattern"),
                corroborates: |candidate| {
                    keyword_host().is_match(candidate) && keyword_user().is_match(candidate)
                },
            },
            Shape {
                pattern: Regex::new(
                    r#"(?i)\b[a-z][a-z0-9 _-]*=(?:\{[^}]*\}|"[^"]*"|'[^']*'|[^=;"'\s]+)(?:;[a-z][a-z0-9 _-]*=(?:\{[^}]*\}|"[^"]*"|'[^']*'|[^=;"'\s]+)){2,}"#,
                )
                .expect("static pattern"),
                corroborates: |candidate| {
                    semicolon_server().is_match(candidate) && semicolon_user().is_match(candidate)
                },
            },
        ]
    })
}

fn keyword_host() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"(?i)(?:^|\s)host=").expect("static pattern"))
}

fn keyword_user() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"(?i)(?:^|\s)user=").expect("static pattern"))
}

fn semicolon_server() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r"(?i)(?:^|;)\s*(?:server|data source|datasource|addr|address)\s*=")
            .expect("static pattern")
    })
}

fn semicolon_user() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r"(?i)(?:^|;)\s*(?:user id|userid|user|uid)\s*=").expect("static pattern")
    })
}

/// `password=…` / `pwd=…` anywhere inside a candidate, in any of the separators
/// the four shapes use.
fn password_assignment() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r#"(?i)(?:^|[?&;\s])(?:password|pwd)=("[^"]*"|'[^']*'|[^&;\s"']+)"#)
            .expect("static pattern")
    })
}

pub(crate) fn detect(input: &str) -> Vec<Region> {
    // Every shape needs an `=`; bailing early keeps four regexes off ordinary prose.
    if !input.contains('=') {
        return Vec::new();
    }

    let mut regions = Vec::new();
    for shape in shapes() {
        for found in shape.pattern.find_iter(input) {
            let end = trim_trailing_punctuation(input, found.start(), found.end());
            if end <= found.start() {
                continue;
            }
            let candidate = &input[found.start()..end];
            if !(shape.corroborates)(candidate) {
                continue;
            }
            if !has_real_password(candidate) {
                continue;
            }
            regions.push(Region::new(found.start(), end, Category::ConnectionString));
        }
    }
    regions
}

/// A DSN at the end of a sentence picks up the full stop; a DSN in a list picks
/// up the comma. Neither belongs in the redacted span.
fn trim_trailing_punctuation(input: &str, start: usize, mut end: usize) -> usize {
    let bytes = input.as_bytes();
    while end > start {
        match bytes[end - 1] {
            b'.' | b',' | b';' | b':' | b'!' | b'?' | b')' | b']' => end -= 1,
            _ => return end,
        }
    }
    end
}

/// Flag only the values of `password=`/`pwd=` assignments, leaving whatever
/// carries them intact.
///
/// Used for JSON `url` fields, where the whole-unit replacement above would
/// destroy the link: `mysql://db/app?user=svc&password=hunter2` keeps its host
/// and path and loses only the password.
pub(crate) fn detect_password_values(input: &str) -> Vec<Region> {
    let mut regions = Vec::new();
    for captures in password_assignment().captures_iter(input) {
        let Some(value) = captures.get(1) else {
            continue;
        };
        if !is_real_secret_value(value.as_str()) {
            continue;
        }
        regions.push(Region::new(value.start(), value.end(), Category::ConnectionString));
    }
    regions
}

fn has_real_password(candidate: &str) -> bool {
    password_assignment().captures_iter(candidate).any(|captures| {
        captures
            .get(1)
            .map(|m| is_real_secret_value(m.as_str()))
            .unwrap_or(false)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Shapes overlap by design — a JDBC URL contains a database URL — so the
    /// meaningful assertion is on the merged result, not on raw spans.
    fn scrub(input: &str) -> String {
        crate::region::apply(input, detect(input), "[R]").0
    }

    fn spans(input: &str) -> Vec<&str> {
        detect(input)
            .into_iter()
            .map(|r| &input[r.start..r.end])
            .collect()
    }

    #[test]
    fn a_jdbc_url_with_a_password_is_flagged_whole() {
        assert_eq!(
            scrub("connect via jdbc:postgresql://db:5432/app?user=svc&password=hunter2."),
            "connect via [R]."
        );
    }

    #[test]
    fn nested_shape_matches_collapse_into_one_replacement() {
        // The JDBC and database-URL shapes both match here; the merge is what
        // stops that becoming two placeholders.
        assert!(spans("jdbc:postgresql://db/app?user=a&password=hunter2").len() > 1);
        assert_eq!(scrub("jdbc:postgresql://db/app?user=a&password=hunter2"), "[R]");
    }

    #[test]
    fn a_keyword_dsn_is_flagged_whole() {
        assert_eq!(
            spans("host=db.internal user=app password=hunter2 sslmode=require"),
            vec!["host=db.internal user=app password=hunter2 sslmode=require"]
        );
    }

    #[test]
    fn a_semicolon_connection_string_is_flagged_whole() {
        assert_eq!(
            spans("Server=db;User Id=app;Password=hunter2;Encrypt=true"),
            vec!["Server=db;User Id=app;Password=hunter2;Encrypt=true"]
        );
    }

    #[test]
    fn a_database_url_with_a_password_query_param_is_flagged() {
        assert_eq!(
            spans("mysql://db.internal/app?user=svc&password=hunter2"),
            vec!["mysql://db.internal/app?user=svc&password=hunter2"]
        );
    }

    #[test]
    fn a_keyword_run_that_is_not_a_dsn_is_left_alone() {
        // Three assignments, no host/user pair, no password: ordinary CLI flags.
        assert!(spans("cargo build --features=a --target=b --profile=c").is_empty());
    }

    #[test]
    fn a_dsn_with_a_placeholder_password_is_left_alone() {
        assert!(spans("host=localhost user=app password=<your-password> sslmode=disable").is_empty());
        assert!(spans("Server=db;User Id=app;Password=changeme;Encrypt=true").is_empty());
    }

    #[test]
    fn a_passwordless_dsn_is_left_alone() {
        assert!(spans("host=localhost user=app dbname=dev sslmode=disable").is_empty());
    }
}
