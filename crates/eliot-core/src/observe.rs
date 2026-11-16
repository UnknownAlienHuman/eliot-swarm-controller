//! Bounded observation signals: heavy child-process episodes, native session
//! growth and harness version/contract drift.
//!
//! Pure data and classification only: the daemon owns sampling and commit, the
//! Store owns persistence. Nothing here performs I/O. Classifiers retain at
//! most a program basename plus one subcommand token, so full argv, paths,
//! environment and extra args can never flow into persisted data.
//!
//! Donors and documents: Linux `/proc/<pid>/cmdline` follows `proc(5)`; the
//! Windows command-line probe follows Microsoft Learn
//! (`QueryFullProcessImageNameW`, `NtQueryInformationProcess`); harness
//! versions follow `docs/harnesses/*`.

use crate::{Error, Result, SessionUsage};
use serde_json::{Value, json};

/// Journal kind for one heavy child-process episode.
pub const ACTIVITY_HEAVY_KIND: &str = "activity.heavy";
/// Journal kind for a native-session size warning.
pub const SESSION_GROWTH_KIND: &str = "session.growth";
/// Journal kind for an installed-vs-verified harness mismatch.
pub const HARNESS_DRIFT_KIND: &str = "harness.drift";
/// Detail level for the heavy-episode record.
pub const OBSERVE_LEVEL_DEBUG: &str = "debug";
/// Warning level for owner-actionable growth and drift flags.
pub const OBSERVE_LEVEL_STANDARD: &str = "standard";

// TODO(k70/k68): route these levels through `[daemon] log_level` when the
// daemon gains one. Log-level ownership sits with k68 diagnostics work: no
// `[daemon] log_level` exists on this base, so every observe event commits
// with its level recorded in the payload and no filtering is applied.

/// Default heavy-episode thresholds; tuned from real data (see monitoring).
pub const DEFAULT_HEAVY_WALL_SECS: u64 = 30;
pub const DEFAULT_HEAVY_CPU_SECS: u64 = 60;
pub const DEFAULT_HEAVY_RSS_BYTES: u64 = 1024 * 1024 * 1024;
/// Default session-growth thresholds.
pub const DEFAULT_SESSION_GROWTH_RATIO: f64 = 0.8;
pub const DEFAULT_SESSION_FILE_BYTES: u64 = 64 * 1024 * 1024;
/// Longest retained program or token text; longer input fails closed.
pub const MAX_HEAVY_PROGRAM_BYTES: usize = 64;
pub const MAX_HEAVY_TOKEN_BYTES: usize = 32;

/// Thresholds for one heavy child-process episode. A process identity whose
/// wall time, consumed CPU or peak RSS crosses any one threshold closes as
/// one `activity.heavy` event.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HeavyThresholds {
    pub wall_secs: u64,
    pub cpu_secs: u64,
    pub rss_bytes: u64,
}

impl Default for HeavyThresholds {
    fn default() -> Self {
        Self {
            wall_secs: DEFAULT_HEAVY_WALL_SECS,
            cpu_secs: DEFAULT_HEAVY_CPU_SECS,
            rss_bytes: DEFAULT_HEAVY_RSS_BYTES,
        }
    }
}

impl HeavyThresholds {
    pub fn validate(&self) -> Result<()> {
        if self.wall_secs == 0 || self.cpu_secs == 0 || self.rss_bytes == 0 {
            return Err(Error::new("invalid", "heavy thresholds must be positive"));
        }
        Ok(())
    }
}

/// Thresholds for one native-session growth warning.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct SessionGrowthThresholds {
    /// Context occupancy that warns, in (0, 1].
    pub context_ratio: f64,
    /// Native session file size that warns, in bytes.
    pub file_bytes: u64,
}

impl Default for SessionGrowthThresholds {
    fn default() -> Self {
        Self {
            context_ratio: DEFAULT_SESSION_GROWTH_RATIO,
            file_bytes: DEFAULT_SESSION_FILE_BYTES,
        }
    }
}

impl SessionGrowthThresholds {
    pub fn validate(&self) -> Result<()> {
        if !(self.context_ratio > 0.0 && self.context_ratio <= 1.0) {
            return Err(Error::new(
                "invalid",
                "session growth context ratio must be within (0, 1]",
            ));
        }
        if self.file_bytes == 0 {
            return Err(Error::new(
                "invalid",
                "session growth file size must be positive",
            ));
        }
        Ok(())
    }
}
/// Programs whose argv[0] names a launcher rather than the work itself: the
/// category then comes from the launched script stem with the token after it,
/// so `node .../npm-cli.js install` reads as `npm install`.
const LAUNCHER_PROGRAMS: &[&str] = &[
    "node",
    "nodejs",
    "deno",
    "bun",
    "python",
    "python3",
    "uv",
    "sh",
    "bash",
    "dash",
    "cmd",
    "powershell",
    "pwsh",
];

/// Basename of a program path: text after the last `/` or `\`, lowercased
/// ASCII, with a trailing `.exe` stripped. Returns `None` when nothing
/// usable remains.
fn program_stem(argv0: &str) -> Option<String> {
    let base = argv0.rsplit(['/', '\\']).next().unwrap_or(argv0);
    let base = base.strip_suffix(".exe").unwrap_or(base);
    let base = base.strip_suffix(".EXE").unwrap_or(base);
    let stem = base.trim().to_ascii_lowercase();
    if stem.is_empty() || stem.len() > MAX_HEAVY_PROGRAM_BYTES {
        return None;
    }
    Some(stem)
}

/// A safe first subcommand token: a short bare word, never a flag, path,
/// extension-bearing name or empty string. Anything else fails closed to no
/// token so full argv, paths and flags stay out of persisted data.
fn safe_token(token: &str) -> Option<String> {
    if token.len() > MAX_HEAVY_TOKEN_BYTES {
        return None;
    }
    let mut chars = token.chars();
    let first = chars.next()?;
    if !first.is_ascii_alphanumeric() {
        return None;
    }
    if !token
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '+' | '-'))
    {
        return None;
    }
    Some(token.to_owned())
}

/// Classify one observed command into its bounded category: the program
/// basename plus at most one safe subcommand token (`cargo build`,
/// `npm install`, `git`). Launcher programs resolve through the script stem
/// (`node .../npm-cli.js install` becomes `npm install`). Never records full
/// argv, paths, env or args beyond that token.
pub fn classify_heavy_command(argv0: &str, args: &[&str]) -> String {
    let Some(mut program) = program_stem(argv0) else {
        return "unknown".to_owned();
    };
    let mut rest = args;
    if LAUNCHER_PROGRAMS.contains(&program.as_str()) {
        let Some(script) = rest.first() else {
            return program;
        };
        rest = &rest[1..];
        let Some(stem) = program_stem(script) else {
            return program;
        };
        // Script wrappers name their program: npm-cli.js becomes npm.
        let stem = stem.strip_suffix(".js").unwrap_or(&stem);
        let stem = stem.strip_suffix(".ts").unwrap_or(stem);
        let stem = stem.strip_suffix(".py").unwrap_or(stem);
        let stem = stem.strip_suffix(".sh").unwrap_or(stem);
        let stem = stem.strip_suffix(".ps1").unwrap_or(stem);
        program = stem.strip_suffix("-cli").unwrap_or(stem).to_owned();
        // A bare interpreter with no script stays the interpreter.
        if program == "unknown" {
            return "unknown".to_owned();
        }
    }
    match rest.first().and_then(|token| safe_token(token)) {
        Some(token) => format!("{program} {token}"),
        None => program,
    }
}

/// Split a Linux `/proc/<pid>/cmdline` body (NUL-separated argv, `proc(5)`)
/// into owned args. Invalid UTF-8 fails closed to no args.
pub fn split_unix_cmdline(body: &[u8]) -> Vec<String> {
    body.split(|byte| *byte == 0)
        .filter(|part| !part.is_empty())
        .map(|part| std::str::from_utf8(part).unwrap_or("").to_owned())
        .filter(|part| !part.is_empty())
        .collect()
}

/// Split a Windows command-line string with `CommandLineToArgvW`-compatible
/// rules (whitespace separates, double quotes group, backslash pairs quote
/// the quote): only the first two tokens ever matter downstream, but the
/// split itself is exact so classification never misreads quoting.
pub fn split_windows_command_line(cmdline: &str) -> Vec<String> {
    let mut args = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut chars = cmdline.chars().peekable();
    let mut has_content = false;
    while let Some(c) = chars.next() {
        match c {
            '\\' => {
                let mut slashes = 1;
                while chars.peek() == Some(&'\\') {
                    chars.next();
                    slashes += 1;
                }
                if chars.peek() == Some(&'"') {
                    current.push_str(&"\\".repeat(slashes / 2));
                    if slashes % 2 == 1 {
                        chars.next();
                        current.push('"');
                    }
                    has_content = true;
                } else {
                    current.push_str(&"\\".repeat(slashes));
                    has_content = true;
                }
            }
            '"' => {
                in_quotes = !in_quotes;
                has_content = true;
            }
            c if c.is_whitespace() && !in_quotes => {
                if has_content {
                    args.push(std::mem::take(&mut current));
                    has_content = false;
                }
            }
            c => {
                current.push(c);
                has_content = true;
            }
        }
    }
    if has_content {
        args.push(current);
    }
    args
}

/// How the episode category was observed. `cmdline` saw the program plus its
/// first token; `image` saw the program image only (token unobservable).
pub const CATEGORY_SOURCE_CMDLINE: &str = "cmdline";
pub const CATEGORY_SOURCE_IMAGE: &str = "image";

/// One closed heavy child-process episode: exactly one `activity.heavy`
/// event. CPU is the identity's own consumed CPU between its first and last
/// samples, RSS its peak, wall its observed lifetime. An unknown exit stays
/// unknown, never zero.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HeavyEpisode {
    pub category: String,
    pub category_source: String,
    pub started_at: i64,
    pub ended_at: i64,
    /// `observed_exit` when the closing sample carried `Exited`;
    /// `last_seen` when the identity went absent (end is the last sighting,
    /// never an inferred exit).
    pub end_source: String,
    pub duration_ms: i64,
    pub cpu_ms: i64,
    pub peak_rss_bytes: u64,
    pub exit_code: Option<i32>,
}

impl HeavyEpisode {
    pub fn validate(&self) -> Result<()> {
        if self.category.trim().is_empty()
            || self.category.len() > MAX_HEAVY_PROGRAM_BYTES + MAX_HEAVY_TOKEN_BYTES + 1
        {
            return Err(Error::new("invalid", "heavy episode category is invalid"));
        }
        if !matches!(self.category_source.as_str(), "cmdline" | "image") {
            return Err(Error::new(
                "invalid",
                "heavy episode category source is invalid",
            ));
        }
        if !matches!(self.end_source.as_str(), "observed_exit" | "last_seen") {
            return Err(Error::new("invalid", "heavy episode end source is invalid"));
        }
        if self.ended_at < self.started_at || self.duration_ms < 0 || self.cpu_ms < 0 {
            return Err(Error::new("invalid", "heavy episode times are invalid"));
        }
        Ok(())
    }

    /// True when any one threshold crosses.
    pub fn crosses(&self, thresholds: &HeavyThresholds) -> bool {
        u64::try_from(self.duration_ms).is_ok_and(|ms| ms >= thresholds.wall_secs * 1000)
            || u64::try_from(self.cpu_ms).is_ok_and(|ms| ms >= thresholds.cpu_secs * 1000)
            || self.peak_rss_bytes >= thresholds.rss_bytes
    }

    /// Bounded journal payload for `activity.heavy`.
    pub fn payload(&self) -> Value {
        json!({
            "category": self.category,
            "category_source": self.category_source,
            "started_at": self.started_at,
            "ended_at": self.ended_at,
            "end_source": self.end_source,
            "duration_ms": self.duration_ms,
            "cpu_ms": self.cpu_ms,
            "peak_rss_bytes": self.peak_rss_bytes,
            "exit_code": self.exit_code,
            "level": OBSERVE_LEVEL_DEBUG,
        })
    }
}
/// What crossed a session-growth threshold.
#[derive(Clone, Debug, PartialEq)]
pub enum SessionGrowthBasis {
    ContextRatio { used: i64, window: i64, ratio: f64 },
    SessionFileBytes { bytes: u64 },
}

/// One session-growth warning: exactly one `session.growth` event per basis
/// per Agent generation until the signal clears. The message names the owner
/// action; Eliot never compacts automatically.
#[derive(Clone, Debug, PartialEq)]
pub struct SessionGrowth {
    pub basis: SessionGrowthBasis,
    pub message: String,
    pub compact_hint: Option<String>,
}

impl SessionGrowth {
    pub fn basis_name(&self) -> &'static str {
        match self.basis {
            SessionGrowthBasis::ContextRatio { .. } => "context_ratio",
            SessionGrowthBasis::SessionFileBytes { .. } => "session_file_bytes",
        }
    }

    /// Bounded journal payload for `session.growth`.
    pub fn payload(&self) -> Value {
        let mut data = json!({
            "basis": self.basis_name(),
            "message": self.message,
            "level": OBSERVE_LEVEL_STANDARD,
        });
        match &self.basis {
            SessionGrowthBasis::ContextRatio {
                used,
                window,
                ratio,
            } => {
                data["context_used_tokens"] = json!(used);
                data["context_window_tokens"] = json!(window);
                data["ratio"] = json!(ratio);
            }
            SessionGrowthBasis::SessionFileBytes { bytes } => {
                data["session_bytes"] = json!(bytes);
            }
        }
        if let Some(hint) = &self.compact_hint {
            data["compact_hint"] = json!(hint);
        }
        data
    }
}

/// Native compact or summarize operation Eliot may name in a growth warning.
/// On this base no harness page documents an Eliot-exposed compact or
/// summarize operation (Muse lists compact as native-only, advertised after
/// implementation), so every harness maps to `None` and the warning keeps
/// the generic owner action. Revisit when a harness page documents one.
pub fn compact_hint_for_harness(_harness: &str) -> Option<String> {
    None
}

fn growth_message(basis: &str, detail: &str, compact_hint: Option<&str>) -> String {
    let action = compact_hint.unwrap_or(
        "narrow the task or restart the session; Eliot performs no automatic compaction",
    );
    format!("session growth ({basis}): {detail}; {action}")
}

/// Evaluate the retained session snapshot plus an optional read-only native
/// session size against thresholds. File bytes stay `None` where the harness
/// exposes no documented size source; absent context numbers never warn.
pub fn evaluate_session_growth(
    harness: &str,
    usage: Option<&SessionUsage>,
    session_bytes: Option<u64>,
    thresholds: &SessionGrowthThresholds,
) -> Option<SessionGrowth> {
    if let Some(bytes) = session_bytes
        && bytes >= thresholds.file_bytes
    {
        let hint = compact_hint_for_harness(harness);
        return Some(SessionGrowth {
            message: growth_message(
                "session_file_bytes",
                &format!("native session file is {bytes} bytes"),
                hint.as_deref(),
            ),
            compact_hint: hint,
            basis: SessionGrowthBasis::SessionFileBytes { bytes },
        });
    }
    if let Some(usage) = usage
        && let (Some(used), Some(window)) = (usage.context_used_tokens, usage.context_window_tokens)
        && window > 0
        && used >= 0
    {
        let ratio = used as f64 / window as f64;
        if ratio >= thresholds.context_ratio {
            let hint = compact_hint_for_harness(harness);
            return Some(SessionGrowth {
                message: growth_message(
                    "context_ratio",
                    &format!("context at {used}/{window} tokens"),
                    hint.as_deref(),
                ),
                compact_hint: hint,
                basis: SessionGrowthBasis::ContextRatio {
                    used,
                    window,
                    ratio,
                },
            });
        }
    }
    None
}

/// One harness the docs verify Eliot against: the installed version (and
/// native schema or protocol marker where one exists) plus the doc section
/// to re-verify when the installed surface differs. The docs link this
/// table; the code reads it. Single source: update here and in
/// `docs/harnesses/README.md` together.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct VerifiedHarness {
    pub harness: &'static str,
    pub verified_version: &'static str,
    pub verified_marker: &'static str,
    pub doc_section: &'static str,
}

pub const VERIFIED_HARNESSES: &[VerifiedHarness] = &[
    VerifiedHarness {
        harness: "codex",
        verified_version: "0.167.1",
        verified_marker: "app-server schema v2 (0.167.1)",
        doc_section: "docs/harnesses/codex.md#sources",
    },
    VerifiedHarness {
        harness: "claude",
        verified_version: "2.1.278",
        verified_marker: "stream-json records (2.1.278)",
        doc_section: "docs/harnesses/claude-code.md#sources-and-unresolved-facts",
    },
    VerifiedHarness {
        harness: "muse",
        verified_version: "1.4.0-R4161.1",
        verified_marker: "MSP v1 host 1.4.0-R4161.1",
        doc_section: "docs/harnesses/muse.md#usage-attribution",
    },
    VerifiedHarness {
        harness: "opencode",
        verified_version: "2.2.7",
        verified_marker: "native schema (2.2.7)",
        doc_section: "docs/harnesses/opencode.md#working-profile",
    },
    VerifiedHarness {
        harness: "antigravity",
        verified_version: "1.5.9",
        verified_marker: "ACP v1-7ec5185c.json, AGY 1.5.2",
        doc_section: "docs/harnesses/antigravity.md#native-identity-and-transport",
    },
];

/// Verified record for one harness code, if the table covers it.
pub fn verified_harness(harness: &str) -> Option<&'static VerifiedHarness> {
    VERIFIED_HARNESSES
        .iter()
        .find(|entry| entry.harness == harness)
}

/// One installed-vs-verified mismatch: exactly one `harness.drift` event per
/// harness and installed surface until the surface changes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HarnessDrift {
    pub harness: String,
    pub installed: Option<String>,
    pub verified: String,
    pub doc_section: String,
    pub message: String,
}

impl HarnessDrift {
    /// Bounded journal payload for `harness.drift`.
    pub fn payload(&self) -> Value {
        json!({
            "harness": self.harness,
            "installed": self.installed,
            "verified": self.verified,
            "doc_section": self.doc_section,
            "message": self.message,
            "level": OBSERVE_LEVEL_STANDARD,
        })
    }
}

/// Flag drift when the installed version (or, when both sides carry one, the
/// schema fingerprint) differs from the verified record. Exact trimmed
/// compare: build metadata counts, so `1.4.20` drifts against `1.4.0-R4161.1`.
/// Unknown harnesses and missing records never flag.
pub fn check_harness_drift(
    harness: &str,
    installed_version: Option<&str>,
    installed_fingerprint: Option<&str>,
) -> Option<HarnessDrift> {
    let verified = verified_harness(harness)?;
    let installed = installed_version.map(str::trim).filter(|v| !v.is_empty());
    let version_drift = installed.is_some_and(|v| v != verified.verified_version);
    let fingerprint_drift = match (installed_fingerprint, verified.verified_marker) {
        (Some(fingerprint), marker) if !marker.is_empty() => {
            !fingerprint.trim().is_empty() && fingerprint.trim() != marker
        }
        _ => false,
    };
    if !version_drift && !fingerprint_drift {
        return None;
    }
    let message = match installed {
        Some(version) => format!(
            "harness {harness} behaves as installed {version}, verified as {} ({}); re-verify {} before trusting new behavior; our bugs still read as our bugs",
            verified.verified_version, verified.verified_marker, verified.doc_section
        ),
        None => format!(
            "harness {harness} version is unknown, verified as {} ({}); re-verify {}",
            verified.verified_version, verified.verified_marker, verified.doc_section
        ),
    };
    Some(HarnessDrift {
        harness: harness.to_owned(),
        installed: installed.map(str::to_owned),
        verified: verified.verified_version.to_owned(),
        doc_section: verified.doc_section.to_owned(),
        message,
    })
}
#[cfg(test)]
mod observe_tests {
    use super::*;

    fn usage(used: Option<i64>, window: Option<i64>) -> SessionUsage {
        SessionUsage {
            context_used_tokens: used,
            context_window_tokens: window,
            cumulative_tokens: None,
            cumulative_cost: None,
            source: Some("test".to_owned()),
            observed_at: None,
        }
    }

    #[test]
    fn heavy_category_keeps_program_plus_one_token() {
        assert_eq!(classify_heavy_command("cargo", &["build"]), "cargo build");
        assert_eq!(
            classify_heavy_command("/usr/local/bin/cargo", &["build", "--manifest-path", "x"]),
            "cargo build"
        );
        assert_eq!(
            classify_heavy_command("C:\\Rust\\cargo.exe", &["test"]),
            "cargo test"
        );
        assert_eq!(classify_heavy_command("git", &[]), "git");
        assert_eq!(classify_heavy_command("git", &["fetch"]), "git fetch");
        assert_eq!(
            classify_heavy_command(
                "/opt/node/bin/node",
                &["/opt/node/lib/npm-cli.js", "install"]
            ),
            "npm install"
        );
        assert_eq!(classify_heavy_command("dotnet", &["build"]), "dotnet build");
    }

    #[test]
    fn heavy_category_rejects_flags_paths_and_empty() {
        assert_eq!(classify_heavy_command("cargo", &["--version"]), "cargo");
        assert_eq!(classify_heavy_command("cargo", &["/tmp/x"]), "cargo");
        assert_eq!(classify_heavy_command("rustc", &["foo.rs"]), "rustc");
        assert_eq!(classify_heavy_command("", &["build"]), "unknown");
        assert_eq!(classify_heavy_command("node", &[]), "node");
    }

    #[test]
    fn windows_split_respects_quotes() {
        assert_eq!(
            split_windows_command_line(r#""C:\Program Files\cargo.exe" build --locked"#),
            vec!["C:\\Program Files\\cargo.exe", "build", "--locked"]
        );
        assert_eq!(
            split_windows_command_line("git   fetch"),
            vec!["git", "fetch"]
        );
        assert!(split_windows_command_line("   ").is_empty());
    }

    #[test]
    fn unix_split_is_nul_separated() {
        assert_eq!(
            split_unix_cmdline(b"cargo\0build\0--locked\0"),
            vec!["cargo", "build", "--locked"]
        );
        assert!(split_unix_cmdline(b"\0\0").is_empty());
        assert!(split_unix_cmdline(&[0xff, 0x00]).is_empty());
    }

    #[test]
    fn episode_crosses_any_single_threshold() {
        let thresholds = HeavyThresholds::default();
        let base = HeavyEpisode {
            category: "cargo build".to_owned(),
            category_source: "cmdline".to_owned(),
            started_at: 1_000,
            ended_at: 31_000,
            end_source: "observed_exit".to_owned(),
            duration_ms: 30_000,
            cpu_ms: 1_000,
            peak_rss_bytes: 1,
            exit_code: Some(0),
        };
        assert!(base.crosses(&thresholds));
        let cpu = HeavyEpisode {
            duration_ms: 1_000,
            cpu_ms: 60_000,
            peak_rss_bytes: 1,
            ..base.clone()
        };
        assert!(cpu.crosses(&thresholds));
        let rss = HeavyEpisode {
            duration_ms: 1_000,
            cpu_ms: 1_000,
            peak_rss_bytes: 1024 * 1024 * 1024,
            ..base.clone()
        };
        assert!(rss.crosses(&thresholds));
        let small = HeavyEpisode {
            duration_ms: 1_000,
            cpu_ms: 1_000,
            peak_rss_bytes: 1,
            ..base.clone()
        };
        assert!(!small.crosses(&thresholds));
        assert!(base.validate().is_ok());
        assert!(small.payload()["level"].as_str() == Some("debug"));
    }

    #[test]
    fn thresholds_reject_nonpositive() {
        assert!(
            HeavyThresholds {
                wall_secs: 0,
                ..HeavyThresholds::default()
            }
            .validate()
            .is_err()
        );
        assert!(
            SessionGrowthThresholds {
                context_ratio: 0.0,
                ..SessionGrowthThresholds::default()
            }
            .validate()
            .is_err()
        );
        assert!(
            SessionGrowthThresholds {
                context_ratio: 1.5,
                ..SessionGrowthThresholds::default()
            }
            .validate()
            .is_err()
        );
    }

    #[test]
    fn growth_warns_on_ratio_and_bytes() {
        let thresholds = SessionGrowthThresholds::default();
        let warn = evaluate_session_growth(
            "codex",
            Some(&usage(Some(81), Some(100))),
            None,
            &thresholds,
        );
        assert!(warn.is_some());
        let warn = warn.expect("ratio over default warns");
        assert_eq!(warn.basis_name(), "context_ratio");
        assert!(warn.payload()["level"].as_str() == Some("standard"));
        assert!(
            evaluate_session_growth(
                "codex",
                Some(&usage(Some(79), Some(100))),
                None,
                &thresholds
            )
            .is_none()
        );
        assert!(evaluate_session_growth("codex", None, None, &thresholds).is_none());
        let file = evaluate_session_growth("codex", None, Some(64 * 1024 * 1024), &thresholds);
        assert!(file.is_some_and(|growth| growth.basis_name() == "session_file_bytes"));
        assert!(evaluate_session_growth("codex", None, Some(1024), &thresholds).is_none());
    }

    #[test]
    fn drift_flags_version_mismatch_only() {
        assert!(check_harness_drift("codex", Some("0.167.1"), None).is_none());
        let drift = check_harness_drift("muse", Some("1.4.20"), None).expect("1.4.20 drifts");
        assert_eq!(drift.verified, "1.4.0-R4161.1");
        assert!(drift.message.contains("docs/harnesses/muse.md"));
        assert!(check_harness_drift("unknown-harness", Some("9.9.9"), None).is_none());
        assert!(check_harness_drift("codex", None, None).is_none());
        assert!(drift.payload()["level"].as_str() == Some("standard"));
    }
}
