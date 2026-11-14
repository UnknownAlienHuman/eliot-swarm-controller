//! Read, validate and save the `[profiles.*]` sections of `config.toml`.
//!
//! Owning contract: docs/spec/configuration.md "Profile editing". Profiles
//! are resolved from the file at every launch admission, so a saved file is
//! what the next launch uses; running Agents keep their frozen snapshots.
//!
//! The editor sees the verbatim text of the `[profiles.*]` table blocks only.
//! A save splices the draft back in place of those blocks, leaves every other
//! byte of the file as it was, and then refuses unless:
//! * the draft holds only `[profiles.*]` tables (no other section can be
//!   written through this path);
//! * the spliced file parses to exactly the old non-profile sections and the
//!   draft's profiles;
//! * the daemon's own strict parser, launch-time profile resolution and
//!   project validation accept the result;
//! * the file still has the revision the draft was read from.
//!
//! The file is then replaced through a private temporary file and a rename.
//! An invalid draft is never written, so the file keeps its last valid
//! revision and the draft stays with the editor. No secret value or
//! redaction marker is returned or accepted.

use std::path::Path;
use std::sync::Mutex;

use eliot_core::SecretPolicy;
use eliot_core::api::{
    MAX_LISTED_PROFILES, ProfileListResponse, ProfileSummary, ProfilesReadResponse,
    ProfilesSaveRequest, ProfilesSaveResponse, REDACTION_MARKER,
};
use serde_json::Value;
use sha2::{Digest, Sha256};

use super::{
    MAX_CONFIG_BYTES, core, parse_config, project_registrations_in, read_config_source,
    resolve_profile_in,
};

/// Serializes saves within this daemon process.
static SAVE_LOCK: Mutex<()> = Mutex::new(());

/// Lowercase hexadecimal SHA-256 of the file text.
pub(crate) fn revision_of(source: &str) -> String {
    Sha256::digest(source.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// One table block: a header line and the lines up to the next header.
struct Block<'a> {
    text: &'a str,
    profile: bool,
}

/// Table path of a header line, or `None` for any other line. A header is
/// `[path]` or `[[path]]` followed only by whitespace or a comment, so a
/// nested-array value line such as `["a", "b"],` is never taken for one.
/// Segments are bare keys or quoted keys (which may hold any characters).
fn header_path(line: &str) -> Option<Vec<String>> {
    let trimmed = line.trim();
    let (body, array) = match trimmed.strip_prefix("[[") {
        Some(body) => (body, true),
        None => (trimmed.strip_prefix('[')?, false),
    };
    let mut path = Vec::new();
    let mut chars = body.char_indices().peekable();
    loop {
        while chars.next_if(|(_, c)| *c == ' ' || *c == '\t').is_some() {}
        let segment = match chars.peek().map(|(_, c)| *c)? {
            quote @ ('"' | '\'') => {
                chars.next();
                let mut segment = String::new();
                loop {
                    let (_, c) = chars.next()?;
                    if c == quote {
                        break;
                    }
                    if c == '\\' && quote == '"' {
                        segment.push(chars.next()?.1);
                    } else {
                        segment.push(c);
                    }
                }
                segment
            }
            _ => {
                let mut segment = String::new();
                while let Some((_, c)) =
                    chars.next_if(|(_, c)| c.is_ascii_alphanumeric() || matches!(c, '_' | '-'))
                {
                    segment.push(c);
                }
                if segment.is_empty() {
                    return None;
                }
                segment
            }
        };
        path.push(segment);
        while chars.next_if(|(_, c)| *c == ' ' || *c == '\t').is_some() {}
        match chars.next()? {
            (_, '.') => continue,
            (index, ']') => {
                let rest = &body[index + 1..];
                let rest = if array { rest.strip_prefix(']')? } else { rest };
                let rest = rest.trim_start();
                return (rest.is_empty() || rest.starts_with('#')).then_some(path);
            }
            _ => return None,
        }
    }
}

fn split_blocks(source: &str) -> Vec<Block<'_>> {
    let mut spans: Vec<(usize, usize, bool)> = Vec::new();
    let mut start = 0;
    let mut profile = false;
    // A leading UTF-8 BOM is not TOML content: the pinned parser accepts it,
    // but the header scan must not treat it as part of the first header. Keep
    // its bytes in a leading non-profile span so reads never hand it to the
    // editor and saves preserve it byte for byte.
    if let Some(rest) = source.strip_prefix('\u{FEFF}') {
        let bom = source.len() - rest.len();
        spans.push((0, bom, false));
        start = bom;
    }
    let mut offset = start;
    for line in source[start..].split_inclusive('\n') {
        if let Some(path) = header_path(line) {
            if offset > start {
                spans.push((start, offset, profile));
            }
            start = offset;
            profile = path.first().is_some_and(|first| first == "profiles");
        }
        offset += line.len();
    }
    if source.len() > start {
        spans.push((start, source.len(), profile));
    }
    // Comment and blank lines that end a profile block and precede another
    // section belong to that section: they are never handed to the editor,
    // so a save cannot drop them.
    for index in 1..spans.len() {
        let (block_start, block_end, is_profile) = spans[index - 1];
        if !is_profile || spans[index].2 {
            continue;
        }
        let mut split = block_end;
        for line in source[block_start..block_end].split_inclusive('\n').rev() {
            let trimmed = line.trim();
            if !(trimmed.is_empty() || trimmed.starts_with('#')) {
                break;
            }
            split -= line.len();
        }
        if split > block_start {
            spans[index - 1].1 = split;
            spans[index].0 = split;
        }
    }
    spans
        .into_iter()
        .map(|(start, end, profile)| Block {
            text: &source[start..end],
            profile,
        })
        .collect()
}

fn with_newline(text: &str) -> String {
    if text.is_empty() || text.ends_with('\n') {
        text.to_owned()
    } else {
        format!("{text}\n")
    }
}

/// The verbatim `[profiles.*]` blocks in file order.
fn profile_text(source: &str) -> String {
    split_blocks(source)
        .into_iter()
        .filter(|block| block.profile)
        .map(|block| with_newline(block.text))
        .collect()
}

/// Replace every profile block with the draft, at the first block's place
/// (or at the end when the file had none). Other text is kept byte for byte.
fn splice(source: &str, draft: &str) -> String {
    let draft = with_newline(draft);
    let mut out = String::with_capacity(source.len() + draft.len());
    let mut inserted = false;
    for block in split_blocks(source) {
        if block.profile {
            if !inserted {
                out.push_str(&draft);
                inserted = true;
            }
        } else {
            if !out.is_empty() && !out.ends_with('\n') {
                out.push('\n');
            }
            out.push_str(block.text);
        }
    }
    if !inserted && !draft.trim().is_empty() {
        if !out.is_empty() {
            if !out.ends_with('\n') {
                out.push('\n');
            }
            if !out.ends_with("\n\n") {
                out.push('\n');
            }
        }
        out.push_str(&draft);
    }
    out
}

fn located(text: &str, offset: usize) -> String {
    let before = &text[..offset.min(text.len())];
    let line = before.matches('\n').count() + 1;
    let column = before.rsplit('\n').next().map_or(0, str::len) + 1;
    format!("line {line}, column {column}")
}

fn parse_table(text: &str, label: &str) -> core::Result<toml::Table> {
    toml::from_str::<toml::Table>(text).map_err(|error| {
        let at = error
            .span()
            .map(|span| format!(" at {}", located(text, span.start)))
            .unwrap_or_default();
        core::Error::new(
            "profile_error",
            format!("{label}: {}{at}", error.message().trim()),
        )
    })
}

fn not_editable(revision: Option<String>, reason: &str) -> ProfilesReadResponse {
    ProfilesReadResponse {
        revision,
        source: None,
        editable: false,
        reason: Some(reason.to_owned()),
    }
}

/// A known secret value or the redaction marker, in the raw text or in any
/// decoded value of the parsed table (so TOML escapes cannot hide one).
fn has_secret(policy: &SecretPolicy, text: &str, parsed: Option<&toml::Table>) -> bool {
    text.contains(REDACTION_MARKER)
        || policy.contains_value(&Value::String(text.to_owned()))
        || parsed.is_some_and(|table| {
            serde_json::to_value(table).is_ok_and(|value| {
                policy.contains_value(&value) || value.to_string().contains(REDACTION_MARKER)
            })
        })
}

/// Why the profile blocks of `source` cannot be edited as text, if so.
fn editability(source: &str, policy: &SecretPolicy) -> Option<&'static str> {
    let Ok(table) = parse_table(source, "config.toml") else {
        return Some("config.toml is not valid TOML; fix it in a text editor first");
    };
    let text = profile_text(source);
    let Ok(profiles) = parse_table(&text, "profiles") else {
        return Some("the [profiles.*] sections cannot be separated from the rest of the file");
    };
    if profiles.get("profiles") != table.get("profiles") || profiles.keys().any(|k| k != "profiles")
    {
        return Some("profiles are defined outside [profiles.*] tables; edit config.toml directly");
    }
    if has_secret(policy, &text, Some(&profiles)) {
        return Some(
            "the profile sections contain a known secret value; edit config.toml directly",
        );
    }
    // The TOML shape above is not enough: an unknown field or a
    // credential-bearing URL must never be handed to the editor as safe
    // source, even when no known secret is present. Launch-readiness alone
    // (a missing model, a dangling child reference) stays a save-time error:
    // it carries no credentials and the editor can repair it.
    let config = match parse_config(source) {
        Ok(config) => config,
        Err(_) => {
            return Some(
                "the profiles fail the daemon's strict validation; edit config.toml directly",
            );
        }
    };
    if has_credentialed_url(&config, source) {
        return Some("the profiles fail the daemon's strict validation; edit config.toml directly");
    }
    None
}

/// Profiles or adopted proxies whose URL fields carry credentials (userinfo,
/// query, fragment) must never be handed to the editor as safe source.
fn has_credentialed_url(config: &super::FileConfig, source: &str) -> bool {
    config.profiles.values().any(|entry| {
        entry
            .get_ref()
            .provider_base_url
            .as_deref()
            .is_some_and(|raw| super::validate_provider_base_url("", raw, source, 0).is_err())
    }) || config.proxies.iter().any(|(name, entry)| {
        let proxy = entry.get_ref();
        core::RouteBinding {
            name: name.clone(),
            endpoint: proxy.attach.clone(),
            readiness_path: proxy.health.clone(),
            provider_base_url: proxy.base_url_for.codex.clone(),
            client_key_env_name: None,
        }
        .validate()
        .is_err()
    })
}

/// `config.profiles.read`: the `[profiles.*]` sections only, when editable.
pub(crate) fn read(data_root: &Path, policy: &SecretPolicy) -> core::Result<ProfilesReadResponse> {
    let Some(source) = read_config_source(data_root)? else {
        return Ok(ProfilesReadResponse {
            revision: None,
            source: Some(String::new()),
            editable: true,
            reason: None,
        });
    };
    let revision = Some(revision_of(&source));
    if let Some(reason) = editability(&source, policy) {
        return Ok(not_editable(revision, reason));
    }
    Ok(ProfilesReadResponse {
        revision,
        source: Some(profile_text(&source)),
        editable: true,
        reason: None,
    })
}

/// `profile.list`: sanitized summaries plus launch-time resolvability.
pub(crate) fn list(data_root: &Path, policy: &SecretPolicy) -> core::Result<ProfileListResponse> {
    let Some(source) = read_config_source(data_root)? else {
        return Ok(ProfileListResponse {
            revision: None,
            valid: true,
            error: None,
            profiles: Vec::new(),
        });
    };
    let revision = Some(revision_of(&source));
    let config = match parse_config(&source) {
        Ok(config) => config,
        Err(error) => {
            // Parse diagnostics name the offending key, which can equal a
            // known secret, so they are redacted before entering the reply.
            return Ok(ProfileListResponse {
                revision,
                valid: false,
                error: Some(policy.redact_text(&error.to_string())),
                profiles: Vec::new(),
            });
        }
    };
    let mut names: Vec<&String> = config.profiles.keys().collect();
    names.sort();
    let profiles = names
        .into_iter()
        .take(MAX_LISTED_PROFILES)
        .filter_map(|name| {
            let entry = config.profiles[name].get_ref();
            let resolved = resolve_profile_in(&config, &source, name);
            let summary = ProfileSummary {
                name: name.clone(),
                harness: entry.harness.trim().to_owned(),
                model: entry.model.clone(),
                effort: entry.effort.clone(),
                permissions: entry.permissions.clone(),
                approval_policy: entry
                    .approval_policy
                    .clone()
                    .or_else(|| entry.approval.clone()),
                control: entry.control.clone(),
                proxy: entry.proxy.clone(),
                account: entry.account.clone(),
                max_live: entry.max_live,
                child_profiles: entry.child_profiles.clone().unwrap_or_default(),
                launchable: resolved.is_ok(),
                error: resolved.err().map(|error| error.to_string()),
            };
            // A secret-bearing profile is omitted entirely: even the name and
            // harness are identifiers the picker would display, and resolution
            // errors name the profile, so no redacted placeholder can carry
            // them safely.
            match serde_json::to_value(&summary) {
                Ok(value) if !policy.contains_value(&value) => Some(summary),
                _ => None,
            }
        })
        .collect();
    Ok(ProfileListResponse {
        revision,
        valid: true,
        error: None,
        profiles,
    })
}

/// Check a draft holds only `[profiles.*]` tables; return its parsed table.
fn check_draft(draft: &str) -> core::Result<toml::Table> {
    let table = parse_table(draft, "draft")?;
    if table.keys().any(|key| key != "profiles") {
        return Err(core::Error::new(
            "profile_error",
            "draft: only [profiles.*] tables may be edited here",
        ));
    }
    for block in split_blocks(draft) {
        let content = block.text.lines().any(|line| {
            let line = line.trim();
            !line.is_empty() && !line.starts_with('#')
        });
        if !block.profile && content {
            return Err(core::Error::new(
                "profile_error",
                "draft: text outside [profiles.*] tables is not accepted",
            ));
        }
    }
    Ok(table)
}

/// The spliced file must differ from the old one only in its profiles.
fn verify_splice(original: &str, spliced: &str, draft: &toml::Table) -> core::Result<()> {
    let mut before = parse_table(original, "config.toml")?;
    let mut after = parse_table(spliced, "config.toml")?;
    let after_profiles = after.remove("profiles");
    before.remove("profiles");
    if before != after || after_profiles.as_ref() != draft.get("profiles") {
        return Err(core::Error::new(
            "profile_error",
            "the draft cannot be spliced without changing other sections; edit config.toml directly",
        ));
    }
    Ok(())
}

/// Validate a draft against the current file; return the spliced text and
/// the accepted profile names.
fn validated(original: &str, draft: &str) -> core::Result<(String, Vec<String>)> {
    // Editors may hand back either line ending; keep the file's own.
    let normalized = draft.replace("\r\n", "\n");
    let draft = if original.contains("\r\n") {
        normalized.replace('\n', "\r\n")
    } else {
        normalized
    };
    let draft = draft.as_str();
    let draft_table = check_draft(draft)?;
    let spliced = splice(original, draft);
    if spliced.len() > MAX_CONFIG_BYTES {
        return Err(core::Error::new(
            "profile_error",
            "config.toml: file would exceed the 128 KiB bound",
        ));
    }
    verify_splice(original, &spliced, &draft_table)?;
    let config = parse_config(&spliced)?;
    let mut names: Vec<String> = config.profiles.keys().cloned().collect();
    names.sort();
    for name in &names {
        resolve_profile_in(&config, &spliced, name)?;
    }
    project_registrations_in(&config, &spliced)?;
    Ok((spliced, names))
}

/// `config.profiles.save`: validate, then (unless a dry run) replace the
/// file through a private temporary file when its revision still matches.
pub(crate) fn save(
    data_root: &Path,
    request: &ProfilesSaveRequest,
    policy: &SecretPolicy,
) -> core::Result<ProfilesSaveResponse> {
    let _guard = SAVE_LOCK
        .lock()
        .map_err(|_| core::Error::new("internal", "profile save lock poisoned"))?;
    let current = read_config_source(data_root)?;
    let current_revision = current.as_deref().map(revision_of);
    if request.expected_revision != current_revision {
        return Err(core::Error::new(
            "conflict",
            "config.toml changed since this draft was read; reload it and reapply the edit",
        ));
    }
    let parsed_draft = toml::from_str::<toml::Table>(&request.source).ok();
    if has_secret(policy, &request.source, parsed_draft.as_ref()) {
        return Err(core::Error::new(
            "not_allowed",
            "the draft contains a known secret value or redaction marker",
        ));
    }
    let original = current.unwrap_or_default();
    if let Some(reason) = editability(&original, policy) {
        return Err(core::Error::new("profile_error", reason));
    }
    let (spliced, profiles) = validated(&original, &request.source)?;
    if request.dry_run {
        return Ok(ProfilesSaveResponse {
            saved: false,
            revision: current_revision,
            profiles,
        });
    }
    if spliced == original {
        // Nothing to write; an absent file stays absent.
        return Ok(ProfilesSaveResponse {
            saved: current_revision.is_some(),
            revision: current_revision,
            profiles,
        });
    }
    // Recheck right before the rename: an external edit since the first read
    // is a conflict. Another process can still write in the instant between
    // this read and the rename; that residual window is documented.
    if read_config_source(data_root)?.as_deref().map(revision_of) != current_revision {
        return Err(core::Error::new(
            "conflict",
            "config.toml changed while this draft was validated; reload it and reapply the edit",
        ));
    }
    crate::platform::data_dir::replace_private_file(data_root, "config.toml", spliced.as_bytes())?;
    Ok(ProfilesSaveResponse {
        saved: true,
        revision: Some(revision_of(&spliced)),
        profiles,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const FILE: &str = "# owner config\n[daemon]\nattention_suspicion_secs = 900\n\n[profiles.a]\nharness = \"codex\"\nmodel = \"m1\"\n\n# accounts follow\n[accounts.main]\nmax_live = 2\n\n[profiles.b]\nharness = \"codex\"\npass_env = [\n  \"CODEX_HOME\",\n]\n";

    #[test]
    fn profile_text_is_the_verbatim_profile_blocks() {
        let text = profile_text(FILE);
        assert!(text.starts_with("[profiles.a]\n"));
        assert!(
            text.contains(
                "[profiles.b]\nharness = \"codex\"\npass_env = [\n  \"CODEX_HOME\",\n]\n"
            )
        );
        assert!(!text.contains("[accounts.main]"));
        assert!(!text.contains("[daemon]"));
    }

    #[test]
    fn splice_keeps_other_sections_byte_for_byte() {
        let draft = "[profiles.a]\nharness = \"codex\"\nmodel = \"m2\"\n";
        let spliced = splice(FILE, draft);
        assert!(spliced.starts_with("# owner config\n[daemon]\nattention_suspicion_secs = 900\n\n[profiles.a]\nharness = \"codex\"\nmodel = \"m2\"\n"));
        assert!(spliced.contains("# accounts follow\n[accounts.main]\nmax_live = 2\n"));
        assert!(!spliced.contains("[profiles.b]"));
        let table = check_draft(draft).unwrap();
        verify_splice(FILE, &spliced, &table).unwrap();
    }

    #[test]
    fn drafts_cannot_write_other_sections() {
        assert!(check_draft("[accounts.x]\nmax_live = 1\n").is_err());
        assert!(check_draft("profiles.a.harness = \"codex\"\n").is_err());
        assert!(
            check_draft("[profiles.a]\nharness = \"codex\"\n[proxies.p]\nattach = \"x\"\n")
                .is_err()
        );
        assert!(check_draft("# only a comment\n").is_ok());
    }

    #[test]
    fn nested_array_lines_are_not_headers() {
        assert!(header_path("  [\"a\", \"b\"],").is_none());
        assert!(header_path("[profiles.a] # note").is_some());
        assert!(header_path("[[rule]]").is_some());
        assert_eq!(
            header_path("[ profiles . \"x\" ]").unwrap(),
            vec!["profiles", "x"]
        );
    }

    #[test]
    fn profiles_outside_tables_are_not_editable() {
        let policy = SecretPolicy::new(Vec::<String>::new());
        let dotted = "profiles.a.harness = \"codex\"\n[daemon]\nattention_suspicion_secs = 900\n";
        assert!(editability(dotted, &policy).is_some());
        assert!(editability(FILE, &policy).is_none());
        assert!(editability("[profiles.a\nbroken", &policy).is_some());
    }

    #[test]
    fn known_secret_values_withhold_the_source() {
        let policy = SecretPolicy::new(vec!["s3cr3t-token-value".to_owned()]);
        let file = "[profiles.a]\nharness = \"codex\"\nmodel = \"s3cr3t-token-value\"\n";
        assert!(editability(file, &policy).is_some());
        assert!(has_secret(&policy, "model = \"[REDACTED]\"", None));
        let escaped = "[profiles.a]\nharness = \"codex\"\nmodel = \"s3cr3t-token-v\\u0061lue\"\n";
        let table: toml::Table = toml::from_str(escaped).unwrap();
        assert!(
            has_secret(&policy, escaped, Some(&table)),
            "escaped secret is caught"
        );
    }

    #[test]
    fn draft_into_an_empty_or_profileless_file_appends() {
        let draft = "[profiles.n]\nharness = \"codex\"\n";
        assert_eq!(splice("", draft), draft);
        let spliced = splice("[daemon]\nattention_suspicion_secs = 900", draft);
        assert_eq!(
            spliced,
            "[daemon]\nattention_suspicion_secs = 900\n\n[profiles.n]\nharness = \"codex\"\n"
        );
    }

    #[test]
    fn launch_resolution_accepts_a_valid_edit_and_rejects_bad_ones() {
        let file = "[daemon]\nattention_suspicion_secs = 900\n\n[profiles.a]\nharness = \"codex\"\nmodel = \"m1\"\n";
        let (spliced, names) = validated(
            file,
            "[profiles.a]\nharness = \"codex\"\nmodel = \"m2\"\neffort = \"high\"\n",
        )
        .expect("valid edit");
        assert_eq!(names, vec!["a".to_owned()]);
        assert!(spliced.contains("model = \"m2\""));
        let unknown = validated(file, "[profiles.a]\nharness = \"codex\"\nmodle = \"m2\"\n");
        assert_eq!(unknown.unwrap_err().code, "profile_error");
        let dangling = validated(
            file,
            "[profiles.a]\nharness = \"codex\"\nmodel = \"m1\"\nchild_profiles = [\"missing\"]\n",
        );
        let dangling = dangling.unwrap_err();
        assert_eq!(dangling.code, "profile_error", "{}", dangling.message);
        assert!(
            dangling.message.contains("child_profiles"),
            "{}",
            dangling.message
        );
        let syntax = validated(file, "[profiles.a\nharness = ");
        assert!(syntax.unwrap_err().message.starts_with("draft:"));
    }

    #[test]
    fn crlf_files_keep_their_line_endings() {
        let file = "[daemon]\r\nattention_suspicion_secs = 900\r\n\r\n[profiles.a]\r\nharness = \"codex\"\r\nmodel = \"m1\"\r\n";
        let (spliced, _) =
            validated(file, "[profiles.a]\nharness = \"codex\"\nmodel = \"m2\"\n").expect("valid");
        assert_eq!(
            spliced,
            "[daemon]\r\nattention_suspicion_secs = 900\r\n\r\n[profiles.a]\r\nharness = \"codex\"\r\nmodel = \"m2\"\r\n"
        );
    }

    #[test]
    fn revision_is_lowercase_sha256_hex() {
        let revision = revision_of("x");
        assert_eq!(revision.len(), 64);
        assert!(
            revision
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        );
    }

    struct TestDataRoot(std::path::PathBuf);

    impl TestDataRoot {
        fn new(config: &str) -> Self {
            static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
            let id = NEXT.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let path =
                std::env::temp_dir().join(format!("eliot-qa-profiles-{}-{id}", std::process::id()));
            std::fs::create_dir_all(&path).expect("create profiles fixture root");
            std::fs::write(path.join("config.toml"), config).expect("write profiles fixture");
            Self(path)
        }
    }

    impl Drop for TestDataRoot {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn secret_bearing_profiles_are_omitted_from_list() {
        let policy = SecretPolicy::new(vec![
            "qa-secret-007-x".to_owned(),
            "qa-model-secret-1".to_owned(),
        ]);
        let root = TestDataRoot::new(
            "[profiles.clean]\nharness = \"codex\"\nmodel = \"m1\"\n\n[profiles.qa-secret-007-x]\nharness = \"codex\"\nmodel = \"m1\"\n\n[profiles.leaky-model]\nharness = \"codex\"\nmodel = \"qa-model-secret-1\"\n",
        );
        let reply = list(&root.0, &policy).expect("list profiles");
        assert!(reply.valid);
        let names: Vec<&str> = reply
            .profiles
            .iter()
            .map(|summary| summary.name.as_str())
            .collect();
        assert_eq!(names, vec!["clean"]);
        let encoded = serde_json::to_value(&reply).expect("encode list reply");
        assert!(!policy.contains_value(&encoded));
    }

    #[test]
    fn parse_diagnostics_are_redacted_in_list() {
        let policy = SecretPolicy::new(vec!["zz-top-secret-key".to_owned()]);
        let root =
            TestDataRoot::new("[profiles.a]\nharness = \"codex\"\nzz-top-secret-key = \"x\"\n");
        let reply = list(&root.0, &policy).expect("list profiles");
        assert!(!reply.valid);
        assert!(reply.profiles.is_empty());
        let error = reply.error.expect("parse error is reported");
        assert!(!error.contains("zz-top-secret-key"), "unredacted: {error}");
        assert!(error.contains(REDACTION_MARKER));
    }

    #[test]
    fn credential_bearing_invalid_profiles_are_not_editable() {
        let policy = SecretPolicy::new(Vec::<String>::new());
        let userinfo = "[profiles.local]\nharness = \"opencode\"\ncmd = \"C:/Tools/OpenCode/opencode.exe\"\nmodel = \"eliot-local/qwen3-8b\"\nprovider_base_url = \"http://user:password@localhost:1234/v1\"\nbackend = \"lmstudio\"\ncontext_limit = 131072\noutput_limit = 8192\n";
        assert!(editability(userinfo, &policy).is_some());
        let unknown = "[profiles.a]\nharness = \"codex\"\nmodel = \"m1\"\napi_key = \"hunter2\"\n";
        assert!(editability(unknown, &policy).is_some());
        assert!(editability(FILE, &policy).is_none());
    }

    #[test]
    fn bom_prefixed_profiles_edit_as_text_and_keep_the_bom() {
        let policy = SecretPolicy::new(Vec::<String>::new());
        let file = "\u{FEFF}[profiles.a]\nharness = \"codex\"\nmodel = \"m1\"\n";
        assert!(editability(file, &policy).is_none());
        let text = profile_text(file);
        assert!(!text.starts_with('\u{FEFF}'));
        assert!(text.starts_with("[profiles.a]\n"));
        let draft = "[profiles.a]\nharness = \"codex\"\nmodel = \"m2\"\n";
        let (spliced, _) = validated(file, draft).expect("BOM file accepts a valid edit");
        assert!(spliced.starts_with("\u{FEFF}[profiles.a]\nharness = \"codex\"\nmodel = \"m2\"\n"));
    }
}
