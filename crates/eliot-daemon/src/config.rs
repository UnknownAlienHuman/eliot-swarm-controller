//! Minimal global profile loader for the daemon data directory.
//!
//! Owning contract: docs/spec/configuration.md first profile and
//! docs/harnesses/codex.md first useful path. Only the single global
//! profiles map is implemented here. Later configuration layers (proxy
//! routes, extends, overlays, accounts, services, automation and project
//! overlays) are rejected with unsupported, never silently substituted.
//!
//! Returns a bounded nonsecret snapshot object with the keys harness, cmd,
//! model, effort, collaboration_mode, permissions, approval_policy, pass_env,
//! raw setup and normalized relative scratch directories.
//! pass_env carries admitted environment variable names only; secret values are never
//! resolved or copied into the snapshot. The parent daemon calls resolve_profile
//! before Store admission.
use eliot_core as core;
use serde::Deserialize;
use serde_json::{Value, json};
use std::collections::{HashMap, HashSet};
use std::io::Read;
use std::path::Path;
const MAX_CONFIG_BYTES: usize = 128 * 1024;
const MAX_PROFILE_NAME_LEN: usize = 128;
const MAX_PROFILES: usize = 256;
const MAX_MODEL_LEN: usize = 256;
const MAX_EFFORT_LEN: usize = 128;
const MAX_CMD_LEN: usize = 1024;
const MAX_ENV_NAME_LEN: usize = 256;
const MAX_PASS_ENV_ENTRIES: usize = 64;
const FAKE_PROFILE: &str = "fake";
const APPROVAL_POLICY_NEVER: &str = "never";
const APPROVAL_POLICY_ON_REQUEST: &str = "on-request";
const PERMISSION_READ_ONLY: &str = "read-only";
const PERMISSION_WORKSPACE_WRITE: &str = "workspace-write";
const PERMISSION_DANGER_FULL_ACCESS: &str = "danger-full-access";
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FileConfig {
    #[serde(default)]
    profiles: HashMap<String, toml::Spanned<ProfileEntry>>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ProfileEntry {
    harness: String,
    #[serde(default)]
    cmd: Option<String>,
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    effort: Option<String>,
    #[serde(default)]
    collaboration_mode: Option<String>,
    #[serde(default)]
    permissions: Option<String>,
    #[serde(default)]
    approval_policy: Option<String>,
    #[serde(default)]
    pass_env: Option<Vec<String>>,
    #[serde(default)]
    setup: Vec<crate::setup::SetupEntry>,
    #[serde(default = "default_scratch")]
    scratch: Vec<String>,
}
pub(crate) fn resolve_profile(data_root: &Path, name: &str) -> core::Result<Value> {
    validate_profile_name(name)?;
    let Some((config, source)) = read_config(data_root)? else {
        return if name == FAKE_PROFILE {
            Ok(fake_snapshot())
        } else {
            Err(core::Error::new(
                "profile_error",
                format!("config.toml: profiles.{name}: profile not found"),
            ))
        };
    };
    if config.profiles.len() > MAX_PROFILES {
        return Err(core::Error::new(
            "profile_error",
            "config.toml: profiles: profile count exceeds 256 entries",
        ));
    }
    for (profile_name, entry) in &config.profiles {
        if validate_profile_name(profile_name).is_err() {
            return Err(config_error_at(
                &source,
                entry.span().start,
                "profiles",
                "invalid profile name",
                "profile_error",
            ));
        }
    }
    if let Some(fake) = config.profiles.get(FAKE_PROFILE) {
        return Err(config_error_at(
            &source,
            fake.span().start,
            "profiles.fake",
            "builtin profile cannot be overridden",
            "profile_error",
        ));
    }
    if name == FAKE_PROFILE {
        return Ok(fake_snapshot());
    }
    let entry = config.profiles.get(name).ok_or_else(|| {
        core::Error::new(
            "profile_error",
            format!("config.toml: profiles.{name}: profile not found"),
        )
    })?;
    snapshot_from_entry(name, entry.get_ref(), entry.span().start, &source)
}
fn read_config(data_root: &Path) -> core::Result<Option<(FileConfig, String)>> {
    let file = match std::fs::File::open(data_root.join("config.toml")) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => {
            return Err(core::Error::new(
                "profile_error",
                "config.toml: cannot read configuration file",
            ));
        }
    };
    let mut reader = file.take((MAX_CONFIG_BYTES + 1) as u64);
    let mut bytes = Vec::with_capacity(MAX_CONFIG_BYTES + 1);
    reader.read_to_end(&mut bytes).map_err(|_| {
        core::Error::new(
            "profile_error",
            "config.toml: cannot read configuration file",
        )
    })?;
    if bytes.len() > MAX_CONFIG_BYTES {
        return Err(core::Error::new(
            "profile_error",
            "config.toml: file exceeds the 128 KiB bound",
        ));
    }
    let source = String::from_utf8(bytes)
        .map_err(|_| core::Error::new("profile_error", "config.toml: invalid UTF-8"))?;
    let config: FileConfig =
        toml::from_str(&source).map_err(|error| map_toml_error(&source, error))?;
    Ok(Some((config, source)))
}
fn fake_snapshot() -> Value {
    json!({
        "harness": "fake",
        "permissions": PERMISSION_WORKSPACE_WRITE,
        "approval_policy": APPROVAL_POLICY_NEVER,
        "pass_env": [],
        "scratch": default_scratch(),
    })
}
fn snapshot_from_entry(
    name: &str,
    entry: &ProfileEntry,
    table_offset: usize,
    source: &str,
) -> core::Result<Value> {
    let harness = entry.harness.trim();
    if harness != "codex" {
        return Err(profile_field_error(
            source,
            table_offset,
            name,
            "harness",
            if harness == FAKE_PROFILE {
                "builtin fake harness cannot be overridden"
            } else {
                "unsupported harness"
            },
            if harness == FAKE_PROFILE {
                "profile_error"
            } else {
                "unsupported"
            },
        ));
    }
    let model = entry
        .model
        .as_deref()
        .map(str::trim)
        .filter(|model| !model.is_empty())
        .ok_or_else(|| {
            profile_field_error(
                source,
                table_offset,
                name,
                "model",
                "required non-empty model is missing",
                "profile_error",
            )
        })?;
    validate_model(name, model, source, table_offset)?;
    let effort = entry.effort.as_deref().map(str::trim);
    if effort.is_some_and(|value| {
        value.is_empty() || value.len() > MAX_EFFORT_LEN || value.chars().any(char::is_control)
    }) {
        return Err(profile_field_error(
            source,
            table_offset,
            name,
            "effort",
            "must be non-empty, at most 128 characters and contain no controls",
            "profile_error",
        ));
    }
    let collaboration_mode = entry.collaboration_mode.as_deref().map(str::trim);
    if collaboration_mode.is_some_and(|mode| !matches!(mode, "default" | "plan")) {
        return Err(profile_field_error(
            source,
            table_offset,
            name,
            "collaboration_mode",
            "must be default or plan",
            "profile_error",
        ));
    }
    let permissions = entry
        .permissions
        .as_deref()
        .map(str::trim)
        .unwrap_or(PERMISSION_WORKSPACE_WRITE);
    if permissions != PERMISSION_READ_ONLY
        && permissions != PERMISSION_WORKSPACE_WRITE
        && permissions != PERMISSION_DANGER_FULL_ACCESS
    {
        return Err(profile_field_error(
            source,
            table_offset,
            name,
            "permissions",
            "unsupported permission mode",
            "unsupported",
        ));
    }
    let approval_policy = entry
        .approval_policy
        .as_deref()
        .map(str::trim)
        .unwrap_or(APPROVAL_POLICY_NEVER);
    if approval_policy != APPROVAL_POLICY_NEVER && approval_policy != APPROVAL_POLICY_ON_REQUEST {
        return Err(profile_field_error(
            source,
            table_offset,
            name,
            "approval_policy",
            "must be never or on-request",
            "profile_error",
        ));
    }
    let pass_env = match entry.pass_env.as_deref() {
        Some(names) => validate_pass_env(name, names, source, table_offset)?,
        None => Vec::new(),
    };
    let cmd = match entry.cmd.as_deref() {
        Some(cmd) => Some(validate_cmd(name, cmd, source, table_offset)?),
        None => None,
    };
    crate::setup::validate_entries(&entry.setup).map_err(|error| {
        profile_field_error(
            source,
            table_offset,
            name,
            "setup",
            &error.message,
            "profile_error",
        )
    })?;
    let scratch = normalize_scratch(&entry.scratch).map_err(|error| {
        profile_field_error(
            source,
            table_offset,
            name,
            "scratch",
            &error.message,
            "profile_error",
        )
    })?;
    let mut snapshot = serde_json::Map::new();
    snapshot.insert("harness".to_owned(), Value::String("codex".to_owned()));
    if let Some(cmd) = cmd {
        snapshot.insert("cmd".to_owned(), Value::String(cmd));
    }
    snapshot.insert("model".to_owned(), Value::String(model.to_owned()));
    if let Some(effort) = effort {
        snapshot.insert("effort".to_owned(), Value::String(effort.to_owned()));
    }
    if let Some(collaboration_mode) = collaboration_mode {
        snapshot.insert(
            "collaboration_mode".to_owned(),
            Value::String(collaboration_mode.to_owned()),
        );
    }
    snapshot.insert(
        "permissions".to_owned(),
        Value::String(permissions.to_owned()),
    );
    snapshot.insert(
        "approval_policy".to_owned(),
        Value::String(approval_policy.to_owned()),
    );
    snapshot.insert(
        "pass_env".to_owned(),
        Value::Array(pass_env.into_iter().map(Value::String).collect()),
    );
    snapshot.insert("scratch".to_owned(), json!(scratch));
    if !entry.setup.is_empty() {
        snapshot.insert(
            "setup".to_owned(),
            serde_json::to_value(&entry.setup)
                .map_err(|_| core::Error::new("profile_error", "setup could not be encoded"))?,
        );
    }
    Ok(Value::Object(snapshot))
}
fn default_scratch() -> Vec<String> {
    vec!["target/".to_owned(), "node_modules/".to_owned()]
}

fn normalize_scratch(paths: &[String]) -> core::Result<Vec<String>> {
    if paths.len() > 64 {
        return Err(core::Error::new(
            "profile_error",
            "scratch exceeds 64 directories",
        ));
    }
    let mut normalized = Vec::new();
    let mut seen = HashSet::new();
    for path in paths {
        let portable = path.replace('\\', "/");
        if portable.is_empty()
            || portable.len() > 4096
            || portable.starts_with('/')
            || portable.contains(':')
            || portable.chars().any(char::is_control)
        {
            return Err(core::Error::new(
                "profile_error",
                "scratch entries must be bounded relative directories",
            ));
        }
        let parts: Vec<_> = portable
            .split('/')
            .filter(|part| !part.is_empty())
            .collect();
        if parts.is_empty() || parts.iter().any(|part| matches!(*part, "." | "..")) {
            return Err(core::Error::new(
                "profile_error",
                "scratch entries cannot name the root or contain dot traversal",
            ));
        }
        let directory = format!("{}/", parts.join("/"));
        #[cfg(windows)]
        let key = directory.to_ascii_lowercase();
        #[cfg(not(windows))]
        let key = directory.clone();
        if seen.insert(key) {
            normalized.push(directory);
        }
    }
    Ok(normalized)
}

fn validate_profile_name(name: &str) -> core::Result<()> {
    if name.is_empty() || name.len() > MAX_PROFILE_NAME_LEN {
        return Err(core::Error::new(
            "profile_error",
            "profile name must be 1-128 characters",
        ));
    }
    let ok = name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_');
    if !ok {
        return Err(core::Error::new(
            "profile_error",
            "profile name may only contain letters, digits, hyphen and underscore",
        ));
    }
    Ok(())
}
fn validate_model(profile: &str, model: &str, source: &str, offset: usize) -> core::Result<()> {
    if model.len() > MAX_MODEL_LEN || model.chars().any(|c| c.is_whitespace() || c.is_control()) {
        return Err(profile_field_error(
            source,
            offset,
            profile,
            "model",
            "must be a single token of at most 256 characters",
            "profile_error",
        ));
    }
    Ok(())
}
fn validate_pass_env(
    profile: &str,
    names: &[String],
    source: &str,
    offset: usize,
) -> core::Result<Vec<String>> {
    if names.len() > MAX_PASS_ENV_ENTRIES {
        return Err(profile_field_error(
            source,
            offset,
            profile,
            "pass_env",
            "exceeds 64 entries",
            "profile_error",
        ));
    }
    let mut seen = HashSet::new();
    let mut out = Vec::with_capacity(names.len());
    for raw in names {
        let name = raw.trim();
        if name.is_empty() || name.len() > MAX_ENV_NAME_LEN {
            return Err(profile_field_error(
                source,
                offset,
                profile,
                "pass_env",
                "names must be 1-256 characters",
                "profile_error",
            ));
        }
        let mut chars = name.chars();
        let first_ok = chars
            .next()
            .is_some_and(|c| c.is_ascii_alphabetic() || c == '_');
        let rest_ok = chars.all(|c| c.is_ascii_alphanumeric() || c == '_');
        if !first_ok || !rest_ok {
            return Err(profile_field_error(
                source,
                offset,
                profile,
                "pass_env",
                "names must use letters, digits and underscore",
                "profile_error",
            ));
        }
        if name.to_ascii_uppercase().starts_with("ELIOT_AGENT_") {
            return Err(profile_field_error(
                source,
                offset,
                profile,
                "pass_env",
                "cannot override Eliot identity variables",
                "profile_error",
            ));
        }
        if seen.insert(name.to_owned()) {
            out.push(name.to_owned());
        }
    }
    Ok(out)
}
fn validate_cmd(profile: &str, cmd: &str, source: &str, offset: usize) -> core::Result<String> {
    let cmd = cmd.trim();
    if cmd.is_empty() || cmd.len() > MAX_CMD_LEN {
        return Err(profile_field_error(
            source,
            offset,
            profile,
            "cmd",
            "must be 1-1024 characters",
            "profile_error",
        ));
    }
    const BACKSLASH: char = '\\';
    let file = cmd.rsplit(['/', BACKSLASH]).next().unwrap_or(cmd);
    let lower = file.to_ascii_lowercase();
    let stem = lower.strip_suffix(".exe").unwrap_or(&lower);
    const SHELLS: &[&str] = &[
        "sh",
        "bash",
        "dash",
        "zsh",
        "fish",
        "cmd",
        "powershell",
        "pwsh",
    ];
    let has_path_separator = cmd.contains('/') || cmd.contains(BACKSLASH);
    if SHELLS.contains(&stem)
        || (cmd.chars().any(char::is_whitespace)
            && (!has_path_separator || file.chars().any(char::is_whitespace)))
        || cmd.chars().any(char::is_control)
    {
        return Err(profile_field_error(
            source,
            offset,
            profile,
            "cmd",
            "must name one executable path, not a shell command",
            "profile_error",
        ));
    }
    Ok(cmd.to_owned())
}
fn profile_field_error(
    source: &str,
    offset: usize,
    profile: &str,
    field: &str,
    class: &str,
    code: &str,
) -> core::Error {
    config_error_at(
        source,
        offset,
        &format!("profiles.{profile}.{field}"),
        class,
        code,
    )
}
fn config_error_at(source: &str, offset: usize, key: &str, class: &str, code: &str) -> core::Error {
    core::Error::new(
        code,
        format!("{}: {key}: {class}", source_location(source, Some(offset))),
    )
}
fn source_location(source: &str, offset: Option<usize>) -> String {
    let Some(offset) = offset else {
        return "config.toml:unknown location".to_owned();
    };
    let prefix = source.get(..offset.min(source.len())).unwrap_or("");
    let line = prefix.bytes().filter(|byte| *byte == b'\n').count() + 1;
    let column = prefix.rsplit('\n').next().unwrap_or("").chars().count() + 1;
    format!("config.toml:{line}:{column}")
}
fn map_toml_error(source: &str, error: toml::de::Error) -> core::Error {
    let span = error.span();
    let location = source_location(source, span.as_ref().map(|range| range.start));
    if let Some(field) = span
        .as_ref()
        .and_then(|span| decoded_key_at_span(source, span))
    {
        if is_unsupported_field(&field) {
            return core::Error::new(
                "unsupported",
                format!("{location}: {field}: unsupported configuration field"),
            );
        }
        return core::Error::new(
            "profile_error",
            format!("{location}: {field}: unknown configuration field"),
        );
    }
    let key = span
        .as_ref()
        .and_then(|range| key_at_offset(source, range.start))
        .unwrap_or("configuration");
    core::Error::new(
        "profile_error",
        format!("{location}: {key}: invalid TOML syntax or value"),
    )
}
fn decoded_key_at_span(source: &str, span: &std::ops::Range<usize>) -> Option<String> {
    let parsed = toml::de::DeTable::parse(source).ok()?;
    decoded_key_at_span_in_table(parsed.get_ref(), span)
}
fn decoded_key_at_span_in_table(
    table: &toml::de::DeTable<'_>,
    span: &std::ops::Range<usize>,
) -> Option<String> {
    for (key, value) in table.iter() {
        let key_span = key.span();
        if key_span.start == span.start && key_span.end == span.end {
            let decoded = key.get_ref().as_ref();
            return safe_config_key(decoded).then(|| decoded.to_owned());
        }
        if let Some(decoded) = decoded_key_at_span_in_value(value.get_ref(), span) {
            return Some(decoded);
        }
    }
    None
}
fn decoded_key_at_span_in_value(
    value: &toml::de::DeValue<'_>,
    span: &std::ops::Range<usize>,
) -> Option<String> {
    if let Some(table) = value.as_table()
        && let Some(decoded) = decoded_key_at_span_in_table(table, span)
    {
        return Some(decoded);
    }
    if let Some(array) = value.as_array() {
        for item in array.as_ref() {
            if let Some(decoded) = decoded_key_at_span_in_value(item.get_ref(), span) {
                return Some(decoded);
            }
        }
    }
    None
}
fn key_at_offset(source: &str, offset: usize) -> Option<&str> {
    let prefix = source.get(..offset.min(source.len()))?;
    for line in prefix.lines().rev().map(str::trim) {
        if line.starts_with('[') {
            break;
        }
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some((raw_key, _)) = line.split_once('=') {
            let key = raw_key
                .trim()
                .trim_matches(|character| character == '\'' || character == '"');
            if safe_config_key(key) {
                return Some(key);
            }
        }
    }
    None
}
fn safe_config_key(key: &str) -> bool {
    !key.is_empty()
        && key.len() <= MAX_PROFILE_NAME_LEN
        && key
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || "_.-".contains(character))
}
fn is_unsupported_field(field: &str) -> bool {
    const FIELDS: &[&str] = &[
        "proxy",
        "proxies",
        "route",
        "routes",
        "extends",
        "account",
        "accounts",
        "registry",
        "registries",
        "overlay",
        "overlays",
        "project_overlay",
        "project_overlays",
        "secret",
        "secrets",
        "secret_env",
        "secret_values",
        "api_key",
        "api_key_env",
        "token",
        "password",
        "daemon",
        "mail",
        "service",
        "services",
        "plugin",
        "plugins",
        "project",
        "projects",
        "worktree",
        "frozen",
        "rule",
        "automation",
        "inherit_env",
        "workspace_mode",
        "git_identity",
    ];
    FIELDS
        .iter()
        .any(|candidate| field.eq_ignore_ascii_case(candidate))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config_error(source: &str) -> core::Error {
        let error = match toml::from_str::<FileConfig>(source) {
            Ok(_) => panic!("test configuration unexpectedly deserialized"),
            Err(error) => error,
        };
        map_toml_error(source, error)
    }

    #[test]
    fn deferred_fields_use_decoded_keys_at_the_error_span() {
        let sources = [
            "[profiles.codex-luna]\nharness = 'codex'\n\"proxy\" = 'private-value'\n",
            r#"[profiles.codex-luna]
harness = 'codex'
"\u0070roxy" = 'private-value'
"#,
            "profiles.codex-luna.harness = 'codex'\nprofiles.codex-luna.proxy = 'private-value'\n",
            "[profiles.codex-luna]\nharness = 'codex'\n[[profiles.codex-luna.setup]]\nprogram = 'C:/tools/pwsh.exe'\nargs = []\ntimeout_ms = 120000\nextends = 'private-value'\n",
        ];

        for source in sources {
            let error = config_error(source);
            assert_eq!(error.code, "unsupported");
            assert!(error.message.contains("unsupported configuration field"));
            assert!(!error.message.contains("private-value"));
        }
    }

    #[test]
    fn ordinary_unknown_fields_remain_profile_errors() {
        let error = config_error(
            "[profiles.codex-luna]\nharness = 'codex'\nfuture_option = 'private-value'\n",
        );

        assert_eq!(error.code, "profile_error");
        assert!(
            error
                .message
                .contains("future_option: unknown configuration field")
        );
        assert!(!error.message.contains("private-value"));
    }

    #[test]
    fn missing_fields_and_values_containing_unknown_field_text_remain_profile_errors() {
        let missing = config_error("[profiles.codex-luna]\nmodel = 'gpt-6-luna'\n");
        assert_eq!(missing.code, "profile_error");

        let wrong_value = config_error(
            "[profiles.codex-luna]\nharness = 'codex'\npass_env = 'unknown field `proxy`'\n",
        );
        assert_eq!(wrong_value.code, "profile_error");
        assert!(!wrong_value.message.contains("unknown field `proxy`"));
        assert!(
            !wrong_value
                .message
                .contains("unsupported configuration field")
        );
    }
}
