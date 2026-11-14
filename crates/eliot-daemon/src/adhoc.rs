//! Ad-hoc Owner launch: freeze a profile-equivalent snapshot from built-in
//! per-harness defaults plus flags, with deterministic executable discovery.
//!
//! Owning contract: docs/spec/configuration.md (ad-hoc launch) and the five
//! harness pages. Profiles stay the durable explicit record; ad-hoc is the
//! Owner tool that freezes an equivalent snapshot. Broader authority stays
//! profile-only: this module only emits each harness's ordinary safe posture
//! and never widens it.
//!
//! Requested vs effective: an omitted `--model`/`--effort` is frozen as an
//! absent key (requested-absent). The plugin resolves the harness native
//! default at start and reports the effective value through its existing
//! readback; no substitution is ever made for an unavailable requested model.

use eliot_core as core;
use serde_json::{Map, Value, json};
use std::path::{Path, PathBuf};

pub(crate) const ADHOC_PROFILE_PREFIX: &str = "adhoc-";
pub(crate) const SUPPORTED_HARNESSES: &[&str] =
    &["codex", "claude", "muse", "opencode", "antigravity"];

pub(crate) fn adhoc_profile_name(harness: &str) -> String {
    format!("{ADHOC_PROFILE_PREFIX}{harness}")
}

fn split_provider_model(model: &str) -> Option<(&str, &str)> {
    // First-slash split like profiles: nested ids such as
    // `openrouter/stealth/crouton-alpha` are allowed; a `#variant` suffix,
    // empty segments and controls are not.
    let (provider, id) = model.split_once('/')?;
    if provider.is_empty()
        || id.is_empty()
        || id.starts_with('/')
        || id.ends_with('/')
        || model.contains('#')
        || model.contains(char::is_control)
        || model.chars().any(char::is_whitespace)
    {
        return None;
    }
    Some((provider, id))
}

pub(crate) fn validate_adhoc_harness(harness: &str) -> core::Result<String> {
    let trimmed = harness.trim();
    if SUPPORTED_HARNESSES.contains(&trimmed) {
        Ok(trimmed.to_owned())
    } else {
        Err(core::Error::new(
            "unsupported",
            format!("unsupported launch harness {harness}"),
        ))
    }
}

fn validate_adhoc_model(model: &str) -> core::Result<String> {
    let trimmed = model.trim();
    if trimmed.is_empty() || trimmed.len() > 256 || trimmed.chars().any(char::is_control) {
        return Err(core::Error::new(
            "profile_error",
            "ad-hoc model must be a non-empty string of at most 256 characters with no controls",
        ));
    }
    Ok(trimmed.to_owned())
}

fn validate_adhoc_effort(harness: &str, effort: &str) -> core::Result<String> {
    if !matches!(harness, "codex" | "muse") {
        return Err(core::Error::new(
            "unsupported",
            format!("the {harness} slice has no reasoning-effort mapping"),
        ));
    }
    let trimmed = effort.trim();
    if trimmed.is_empty() || trimmed.len() > 128 || trimmed.chars().any(char::is_control) {
        return Err(core::Error::new(
            "profile_error",
            "ad-hoc effort must be non-empty, at most 128 characters and contain no controls",
        ));
    }
    Ok(trimmed.to_owned())
}

/// Build and freeze an ad-hoc snapshot from per-harness safe defaults plus
/// the requested model/effort. `cmd` is the already-discovered absolute
/// native path for harnesses that require one, or `None` for Codex (which
/// launches `codex` by name). An omitted model/effort stays absent so the
/// plugin applies the harness native default and reports it separately.
pub(crate) fn adhoc_snapshot(
    harness: &str,
    model: Option<&str>,
    effort: Option<&str>,
    cmd: Option<String>,
) -> core::Result<Value> {
    let harness = validate_adhoc_harness(harness)?;
    let model = model
        .filter(|value| !value.trim().is_empty())
        .map(validate_adhoc_model)
        .transpose()?;
    let effort = effort
        .filter(|value| !value.trim().is_empty())
        .map(|value| validate_adhoc_effort(&harness, value))
        .transpose()?;
    if harness == "antigravity" && model.is_none() {
        return Err(core::Error::new(
            "profile_error",
            "ad-hoc antigravity launch requires an explicit model",
        ));
    }
    if harness == "opencode" && model.is_none() {
        // The OpenCode slice splits provider/model at the first slash; an
        // absent model leaves the configured native default to the plugin.
        // Auth selection stays profile-only until a default is defined.
    }
    if let Some(model) = model.as_deref()
        && harness == "opencode"
        && split_provider_model(model).is_none()
    {
        return Err(core::Error::new(
            "profile_error",
            "ad-hoc opencode model must be provider/model with non-empty slash segments and no variant suffix",
        ));
    }

    let profile_name = adhoc_profile_name(&harness);
    let mut snapshot = Map::new();
    snapshot.insert("harness".to_owned(), Value::String(harness.clone()));
    match harness.as_str() {
        "codex" => {
            if let Some(cmd) = cmd {
                let validated = validate_native_cmd(&profile_name, &cmd)?;
                snapshot.insert("cmd".to_owned(), Value::String(validated));
            }
            if let Some(model) = model {
                snapshot.insert("model".to_owned(), Value::String(model));
            }
            if let Some(effort) = effort {
                snapshot.insert("effort".to_owned(), Value::String(effort));
            }
            snapshot.insert(
                "permissions".to_owned(),
                Value::String("workspace-write".to_owned()),
            );
            snapshot.insert(
                "approval_policy".to_owned(),
                Value::String("never".to_owned()),
            );
        }
        "claude" => {
            let cmd = cmd.ok_or_else(|| {
                core::Error::new(
                    "profile_error",
                    "ad-hoc claude launch requires a discovered native cmd",
                )
            })?;
            let validated = validate_claude_cmd(&profile_name, &cmd)?;
            snapshot.insert("cmd".to_owned(), Value::String(validated));
            if let Some(model) = model {
                snapshot.insert("model".to_owned(), Value::String(model));
            }
            // No effort mapping; validated absent above.
            snapshot.insert(
                "permissions".to_owned(),
                Value::String("workspace-write".to_owned()),
            );
            snapshot.insert(
                "approval_policy".to_owned(),
                Value::String("never".to_owned()),
            );
        }
        "muse" => {
            let cmd = cmd.ok_or_else(|| {
                core::Error::new(
                    "profile_error",
                    "ad-hoc muse launch requires a discovered native cmd",
                )
            })?;
            let validated = validate_native_cmd(&profile_name, &cmd)?;
            snapshot.insert("cmd".to_owned(), Value::String(validated));
            if let Some(model) = model {
                snapshot.insert("model".to_owned(), Value::String(model));
            }
            if let Some(effort) = effort {
                snapshot.insert("effort".to_owned(), Value::String(effort));
            }
            snapshot.insert(
                "permissions".to_owned(),
                Value::String("workspace-write".to_owned()),
            );
            snapshot.insert(
                "approval_policy".to_owned(),
                Value::String("never".to_owned()),
            );
        }
        "opencode" => {
            let cmd = cmd.ok_or_else(|| {
                core::Error::new(
                    "profile_error",
                    "ad-hoc opencode launch requires a discovered native cmd",
                )
            })?;
            let validated = validate_native_cmd(&profile_name, &cmd)?;
            snapshot.insert("cmd".to_owned(), Value::String(validated));
            if let Some(model) = model {
                snapshot.insert("model".to_owned(), Value::String(model));
            }
        }
        "antigravity" => {
            let cmd = cmd.ok_or_else(|| {
                core::Error::new(
                    "profile_error",
                    "ad-hoc antigravity launch requires a discovered native cmd",
                )
            })?;
            let validated = validate_native_cmd(&profile_name, &cmd)?;
            snapshot.insert("cmd".to_owned(), Value::String(validated));
            if let Some(model) = model {
                snapshot.insert("model".to_owned(), Value::String(model));
            }
            snapshot.insert(
                "dangerously_skip_permissions".to_owned(),
                Value::Bool(false),
            );
        }
        _ => {
            return Err(core::Error::new(
                "unsupported",
                format!("unsupported launch harness {harness}"),
            ));
        }
    }
    // Safe defaults shared by every ad-hoc snapshot: inherit-only MCP,
    // no allow list, no child allow-list (children reuse the parent
    // snapshot only), empty pass_env, default scratch, and a profile-only
    // capacity scope. No setup, route, account, proxy, or elevated
    // permissions are ever frozen here.
    snapshot.insert("mcp".to_owned(), Value::String("inherit".to_owned()));
    snapshot.insert("mcp_allow".to_owned(), Value::Array(Vec::new()));
    snapshot.insert("child_profiles".to_owned(), Value::Array(Vec::new()));
    snapshot.insert("pass_env".to_owned(), Value::Array(Vec::new()));
    snapshot.insert("scratch".to_owned(), json!(["target/", "node_modules/"]));
    snapshot.insert(
        "capacity_scopes".to_owned(),
        json!([{"scope": format!("profile:{profile_name}")}]),
    );
    Ok(Value::Object(snapshot))
}

/// Ad-hoc cmd shape: one explicit executable path, never a shell command.
/// Mirrors the profile `cmd` rule without a config source for errors.
fn validate_native_cmd(profile: &str, cmd: &str) -> core::Result<String> {
    let cmd = cmd.trim();
    if cmd.is_empty() || cmd.len() > 1024 {
        return Err(core::Error::new(
            "profile_error",
            format!("ad-hoc {profile}: cmd must be 1-1024 characters"),
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
        return Err(core::Error::new(
            "profile_error",
            format!("ad-hoc {profile}: cmd must name one executable path, not a shell command"),
        ));
    }
    if is_script_wrapper(cmd) {
        return Err(core::Error::new(
            "profile_error",
            format!(
                "ad-hoc {profile}: cmd must name one native executable path, not a .cmd/.bat/.ps1/.js wrapper"
            ),
        ));
    }
    Ok(cmd.to_owned())
}

/// Claude cmd rule: absolute native path, never a bare name or wrapper.
/// A wrapper would orphan the real CLI on stop, when only the wrapper PID
/// is owned.
fn validate_claude_cmd(profile: &str, cmd: &str) -> core::Result<String> {
    let cmd = validate_native_cmd(profile, cmd)?;
    if !Path::new(&cmd).is_absolute() {
        return Err(core::Error::new(
            "profile_error",
            format!(
                "ad-hoc {profile}: cmd must be an absolute native executable path, not a bare command name"
            ),
        ));
    }
    if cfg!(windows) && !cmd.to_ascii_lowercase().ends_with(".exe") {
        return Err(core::Error::new(
            "profile_error",
            format!("ad-hoc {profile}: cmd must name the native .exe, not a script wrapper"),
        ));
    }
    Ok(cmd)
}

/// Returns true when the path names a script wrapper that must never be
/// frozen as a native executable.
pub(crate) fn is_script_wrapper(path: &str) -> bool {
    let lower = path.to_ascii_lowercase();
    lower.ends_with(".cmd")
        || lower.ends_with(".bat")
        || lower.ends_with(".ps1")
        || lower.ends_with(".js")
}

/// Resolve the installed native executable for a harness. Codex resolves by
/// name (`codex`) through the existing PATH lookup at spawn, so this returns
/// `None`. Every other harness returns an absolute native path, or a
/// deterministic `unsupported`/`profile_error` naming what was searched.
pub(crate) fn discover_cmd_for_harness(harness: &str) -> core::Result<Option<String>> {
    let harness = validate_adhoc_harness(harness)?;
    match harness.as_str() {
        "codex" => Ok(None),
        "claude" => discover_claude_cmd().map(Some),
        "muse" => discover_muse_cmd(Path::new(r"C:\Tools\MuseCode")).map(Some),
        "opencode" => discover_opencode_cmd().map(Some),
        "antigravity" => discover_antigravity_cmd().map(Some),
        _ => Err(core::Error::new(
            "unsupported",
            format!("unsupported launch harness {harness}"),
        )),
    }
}

fn path_search_dirs() -> Vec<PathBuf> {
    std::env::var_os("PATH").map_or_else(Vec::new, |paths| std::env::split_paths(&paths).collect())
}

fn find_on_path(file: &str) -> Option<PathBuf> {
    let target = file.to_ascii_lowercase();
    for dir in path_search_dirs() {
        let candidate = dir.join(file);
        if candidate.is_file()
            && candidate
                .file_name()
                .is_some_and(|name| name.to_string_lossy().to_ascii_lowercase() == target)
        {
            return Some(candidate);
        }
    }
    None
}

fn user_profile() -> Option<PathBuf> {
    std::env::var_os("USERPROFILE")
        .or_else(|| std::env::var_os("HOME"))
        .map(PathBuf::from)
}

/// Well-known data roots for one Windows shell folder, without probing the
/// filesystem: the owning environment variable first, then the
/// `USERPROFILE`-derived default. The daemon environment may strip
/// `APPDATA`/`LOCALAPPDATA` while the native installs still exist under the
/// profile, so discovery must consult the derived roots too.
fn data_roots(
    variable: Option<std::ffi::OsString>,
    home: Option<std::ffi::OsString>,
    leaf: &str,
) -> Vec<PathBuf> {
    let mut roots = Vec::new();
    if let Some(dir) = variable {
        roots.push(PathBuf::from(dir));
    }
    if let Some(home) = home {
        let derived = PathBuf::from(home).join("AppData").join(leaf);
        if !roots.contains(&derived) {
            roots.push(derived);
        }
    }
    roots
}

fn local_roots() -> Vec<PathBuf> {
    data_roots(
        std::env::var_os("LOCALAPPDATA").or_else(|| std::env::var_os("APPDATA")),
        user_profile().map(|path| path.into_os_string()),
        "Local",
    )
}

fn roaming_roots() -> Vec<PathBuf> {
    data_roots(
        std::env::var_os("APPDATA"),
        user_profile().map(|path| path.into_os_string()),
        "Roaming",
    )
}

fn sorted_child_dirs(parent: &Path) -> Vec<PathBuf> {
    let mut dirs = Vec::new();
    if let Ok(entries) = std::fs::read_dir(parent) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                dirs.push(path);
            }
        }
    }
    dirs.sort();
    dirs
}

/// Desktop-app bundles under one roaming root:
/// `<root>/Claude/claude-code/<version>/<hash>/claude.exe`.
fn claude_bundle_exes(roaming: &Path) -> Vec<PathBuf> {
    let mut exes = Vec::new();
    for version in sorted_child_dirs(&roaming.join("Claude").join("claude-code")) {
        for bundle in sorted_child_dirs(&version) {
            exes.push(bundle.join("claude.exe"));
        }
    }
    exes
}

/// Newest-first ordering key for `<exe> --version` output: the first
/// `major.minor.patch` numeric triple on the bounded first line
/// (`2.1.325 (Claude Code)`, `1.4.20-R8619.1`).
fn parse_cli_version(text: &str) -> Option<(u64, u64, u64)> {
    let first = text.lines().next().unwrap_or_default();
    for token in first.split(|c: char| !(c.is_ascii_alphanumeric() || c == '.' || c == '-')) {
        let core = token.split('-').next().unwrap_or(token);
        let mut parts = core.split('.');
        let triple = match (parts.next(), parts.next(), parts.next(), parts.next()) {
            (Some(major), Some(minor), Some(patch), None) => {
                match (major.parse(), minor.parse(), patch.parse()) {
                    (Ok(major), Ok(minor), Ok(patch)) => Some((major, minor, patch)),
                    _ => None,
                }
            }
            _ => None,
        };
        if triple.is_some() {
            return triple;
        }
    }
    None
}

/// Every discovered `claude.exe` install, newest first: `claude.exe` hits on
/// `PATH` (stable order), the standard user locations, then the desktop-app
/// bundles. Each entry carries its own bounded `--version`.
pub(crate) fn discover_claude_candidates() -> Vec<core::api::HarnessCandidate> {
    let mut paths: Vec<PathBuf> = Vec::new();
    let mut push = |path: PathBuf| {
        if !paths.contains(&path) {
            paths.push(path);
        }
    };
    for dir in path_search_dirs() {
        let exe = dir.join("claude.exe");
        if exe.is_file() {
            push(exe);
        }
    }
    if let Some(home) = user_profile() {
        push(home.join(".local").join("bin").join("claude.exe"));
    }
    for roaming in roaming_roots() {
        push(roaming.join("npm").join("claude.exe"));
    }
    for local in local_roots() {
        push(local.join("Programs").join("claude").join("claude.exe"));
    }
    for roaming in roaming_roots() {
        for exe in claude_bundle_exes(&roaming) {
            push(exe);
        }
    }
    /// One discovered install plus its parsed `--version` triple, if any.
    type VersionedCandidate = (core::api::HarnessCandidate, Option<(u64, u64, u64)>);
    let mut candidates: Vec<VersionedCandidate> = paths
        .into_iter()
        .filter(|path| path.is_file())
        .map(|path| {
            let cmd = path.to_string_lossy().into_owned();
            let version = native_version_string(&cmd);
            (
                core::api::HarnessCandidate {
                    cmd,
                    version: version.clone(),
                },
                version.as_deref().and_then(parse_cli_version),
            )
        })
        .filter(|(candidate, _)| {
            Path::new(&candidate.cmd).is_absolute() && !is_script_wrapper(&candidate.cmd)
        })
        .collect();
    // Stable sort: versioned newest-first, unversioned last in discovery
    // order. Ties keep the stable priority order above.
    candidates.sort_by(|left, right| match (&left.1, &right.1) {
        (Some(left), Some(right)) => right.cmp(left),
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (None, None) => std::cmp::Ordering::Equal,
    });
    candidates
        .into_iter()
        .map(|(candidate, _)| candidate)
        .collect()
}

fn claude_search_locations() -> Vec<String> {
    let mut searched = vec!["claude.exe on PATH".to_owned()];
    if let Some(home) = user_profile() {
        searched.push(
            home.join(".local")
                .join("bin")
                .join("claude.exe")
                .to_string_lossy()
                .into_owned(),
        );
    }
    for roaming in roaming_roots() {
        searched.push(
            roaming
                .join("npm")
                .join("claude.exe")
                .to_string_lossy()
                .into_owned(),
        );
    }
    for local in local_roots() {
        searched.push(
            local
                .join("Programs")
                .join("claude")
                .join("claude.exe")
                .to_string_lossy()
                .into_owned(),
        );
    }
    for roaming in roaming_roots() {
        searched.push(
            roaming
                .join("Claude")
                .join("claude-code")
                .join("<version>")
                .join("<hash>")
                .join("claude.exe")
                .to_string_lossy()
                .into_owned(),
        );
    }
    searched.dedup();
    searched
}

fn discover_claude_cmd() -> core::Result<String> {
    let candidates = discover_claude_candidates();
    if let Some(best) = candidates.first() {
        return Ok(best.cmd.clone());
    }
    Err(core::Error::new(
        "profile_error",
        format!(
            "claude native executable was not found; searched {}",
            claude_search_locations().join(", ")
        ),
    ))
}

/// Candidate native executables for an npm-installed OpenCode, in priority
/// order: the shim target
/// (`<shim-dir>/node_modules/@opencode/cli/bin/opencode.exe`) for every
/// discovered `opencode.cmd` shim, then each known package-local path under
/// every roaming root (the shim itself must never be frozen).
fn opencode_candidates(shim_dirs: &[PathBuf], roaming: &[PathBuf]) -> Vec<PathBuf> {
    const PACKAGE: &[&str] = &["node_modules", "@opencode", "cli"];
    fn package_bin(root: &Path, extra: &[&str]) -> PathBuf {
        let mut path = root.to_owned();
        for segment in PACKAGE.iter().chain(extra.iter()) {
            path = path.join(segment);
        }
        path.join("bin").join("opencode.exe")
    }
    let mut candidates = Vec::new();
    let mut push = |path: PathBuf| {
        if !candidates.contains(&path) {
            candidates.push(path);
        }
    };
    for dir in shim_dirs {
        push(package_bin(dir, &[]));
    }
    for root in roaming {
        let npm = root.join("npm");
        push(package_bin(&npm, &[]));
        for arch in ["cli-windows-x64", "cli-windows-x64-baseline"] {
            push(package_bin(&npm, &["node_modules", "@opencode", arch]));
        }
    }
    candidates
}

fn opencode_shim_dirs() -> Vec<PathBuf> {
    let mut dirs = Vec::new();
    let mut push = |dir: PathBuf| {
        if !dirs.contains(&dir) {
            dirs.push(dir);
        }
    };
    for dir in path_search_dirs() {
        if dir.join("opencode.cmd").is_file() {
            push(dir);
        }
    }
    for root in roaming_roots() {
        push(root.join("npm"));
    }
    dirs
}

fn discover_opencode_cmd() -> core::Result<String> {
    // Follow the npm shim to the native exe inside the package: the shim
    // itself (opencode.cmd) must never be frozen.
    let roaming = roaming_roots();
    let candidates = opencode_candidates(&opencode_shim_dirs(), &roaming);
    let mut searched: Vec<String> = candidates
        .iter()
        .map(|path| path.to_string_lossy().into_owned())
        .collect();
    for candidate in &candidates {
        if candidate.is_file() {
            let path = candidate.to_string_lossy().into_owned();
            if !is_script_wrapper(&path) {
                return Ok(path);
            }
        }
    }
    if let Some(found) = find_on_path("opencode.exe") {
        let path = found.to_string_lossy().into_owned();
        if !is_script_wrapper(&path) && Path::new(&path).is_absolute() {
            return Ok(path);
        }
        searched.push("opencode.exe on PATH (wrapper or non-absolute)".to_owned());
    } else {
        searched.push("opencode.exe on PATH".to_owned());
    }
    Err(core::Error::new(
        "unsupported",
        format!(
            "opencode native executable was not found; searched {}",
            searched.join(", ")
        ),
    ))
}

fn current_muse_version(launcher_dir: &Path) -> Option<String> {
    for name in ["muse-version", ".muse-version"] {
        let file = launcher_dir.join(name);
        if let Ok(text) = std::fs::read_to_string(&file) {
            let version = text.trim().to_owned();
            if !version.is_empty()
                && version.len() <= 64
                && !version.chars().any(char::is_control)
                && !version.contains(['/', '\\', ':', '*', '?', '"', '<', '>', '|'])
            {
                return Some(version);
            }
        }
    }
    None
}

fn discover_muse_cmd(launcher_dir: &Path) -> core::Result<String> {
    let version = current_muse_version(launcher_dir).ok_or_else(|| {
        core::Error::new(
            "profile_error",
            format!(
                "muse launcher version is unknown; searched {}",
                launcher_dir.join(".muse-version").to_string_lossy()
            ),
        )
    })?;
    // Never resolve `muse`/`muse.cmd` from PATH: that lands on the launcher
    // wrapper whose PID would orphan the real `muse serve` host on stop.
    let file_name = format!("muse-bin-{version}.exe");
    let native = launcher_dir.join(&file_name);
    if native.is_file() {
        return Ok(native.to_string_lossy().into_owned());
    }
    Err(core::Error::new(
        "profile_error",
        format!(
            "muse native executable for launcher version {version} was not found; searched {}",
            native.to_string_lossy()
        ),
    ))
}

fn discover_antigravity_cmd() -> core::Result<String> {
    let mut searched: Vec<String> = Vec::new();
    for local in local_roots() {
        let native = local
            .join("Programs")
            .join("Antigravity")
            .join("Antigravity.exe");
        searched.push(native.to_string_lossy().into_owned());
        if native.is_file() {
            return Ok(native.to_string_lossy().into_owned());
        }
    }
    // The harness page speaks ACP through `agy`; if only that binary is
    // present the Electron app path above cannot serve it.
    if find_on_path("agy.exe").is_some() || find_on_path("agy").is_some() {
        return Err(core::Error::new(
            "unsupported",
            "antigravity harness needs its ACP binary (agy); the installed Antigravity.exe app path does not serve ACP",
        ));
    }
    searched.push("agy.exe on PATH (ACP binary probe)".to_owned());
    Err(core::Error::new(
        "unsupported",
        format!(
            "antigravity native executable was not found; searched {}",
            searched.join(", ")
        ),
    ))
}

/// Read-only harness installation facts. No model turn and no inference:
/// only filesystem presence plus a bounded `--version` read.
pub(crate) fn harness_installation(harness: &str) -> Value {
    match discover_cmd_for_harness(harness) {
        Ok(Some(cmd)) => {
            let version = native_version(&cmd);
            json!({
                "harness": harness,
                "installed": true,
                "cmd": cmd,
                "version": version,
            })
        }
        Ok(None) => {
            // Codex resolves `codex` by name at spawn; report PATH presence.
            let installed = find_on_path("codex.exe")
                .or_else(|| find_on_path("codex"))
                .is_some();
            json!({
                "harness": harness,
                "installed": installed,
                "cmd": Value::Null,
                "version": if installed { native_version("codex") } else { Value::Null },
            })
        }
        Err(error) => json!({
            "harness": harness,
            "installed": false,
            "cmd": Value::Null,
            "version": Value::Null,
            "error": error.message,
            "code": error.code,
        }),
    }
}

/// Bound on one `<exe> --version` probe: a hung native executable must not
/// stall `harness.list` for every harness.
const VERSION_PROBE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// Bounded `<exe> --version` first line, or `None` when the probe fails.
/// A hung native executable must not stall `harness.list`: only the child
/// this probe started is ever killed on timeout.
fn native_version_string(program: &str) -> Option<String> {
    use std::io::Read;
    use std::process::{Command, Stdio};
    let Ok(mut child) = Command::new(program)
        .arg("--version")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
    else {
        return None;
    };
    let deadline = std::time::Instant::now() + VERSION_PROBE_TIMEOUT;
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break Some(status),
            Ok(None) if std::time::Instant::now() < deadline => {
                std::thread::sleep(std::time::Duration::from_millis(20));
            }
            // Timed out or unwaitable: kill only the child this probe started.
            _ => {
                let _ = child.kill();
                let _ = child.wait();
                break None;
            }
        }
    };
    let mut stdout = String::new();
    if let Some(mut pipe) = child.stdout.take() {
        let _ = pipe.read_to_string(&mut stdout);
    }
    match status {
        Some(status) if status.success() => {
            let text = stdout.lines().next().unwrap_or_default().trim().to_owned();
            if text.is_empty() || text.len() > 256 {
                None
            } else {
                Some(text)
            }
        }
        _ => None,
    }
}

fn native_version(program: &str) -> Value {
    native_version_string(program).map_or(Value::Null, Value::String)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn adhoc_snapshot_freezes_flags_plus_safe_defaults() {
        let snapshot = adhoc_snapshot("codex", Some("gpt-6-luna"), Some("medium"), None).unwrap();
        assert_eq!(snapshot["harness"], json!("codex"));
        assert_eq!(snapshot["model"], json!("gpt-6-luna"));
        assert_eq!(snapshot["effort"], json!("medium"));
        assert_eq!(snapshot["permissions"], json!("workspace-write"));
        assert_eq!(snapshot["approval_policy"], json!("never"));
        assert_eq!(snapshot["mcp"], json!("inherit"));
        assert_eq!(snapshot["mcp_allow"], json!([]));
        assert_eq!(snapshot["child_profiles"], json!([]));
        assert_eq!(snapshot["pass_env"], json!([]));
        assert!(snapshot.get("cmd").is_none());
        // Broader authority is never frozen ad-hoc.
        assert!(snapshot.get("proxy").is_none());
        assert!(snapshot.get("account").is_none());
        assert!(snapshot.get("route_binding").is_none());
        assert!(snapshot.get("setup").is_none());
    }

    #[test]
    fn adhoc_snapshot_omits_absent_model_and_effort_for_native_default() {
        let snapshot = adhoc_snapshot("codex", None, None, None).unwrap();
        assert!(snapshot.get("model").is_none());
        assert!(snapshot.get("effort").is_none());
        let snapshot = adhoc_snapshot(
            "muse",
            None,
            None,
            Some(r"C:\Tools\MuseCode\muse-bin-1.exe".to_owned()),
        )
        .unwrap();
        assert!(snapshot.get("model").is_none());
        assert!(snapshot.get("effort").is_none());
        // Claude omits both so the CLI default applies.
        let snapshot =
            adhoc_snapshot("claude", None, None, Some(r"C:\bin\claude.exe".to_owned())).unwrap();
        assert!(snapshot.get("model").is_none());
        assert!(snapshot.get("effort").is_none());
    }

    #[test]
    fn adhoc_snapshot_rejects_broader_effort_mappings() {
        for harness in ["claude", "opencode", "antigravity"] {
            let error = adhoc_snapshot(
                harness,
                Some("model"),
                Some("medium"),
                Some(r"C:\bin\native.exe".to_owned()),
            )
            .unwrap_err();
            assert_eq!(error.code, "unsupported", "{harness}");
        }
    }

    #[test]
    fn adhoc_snapshot_rejects_unknown_harness() {
        let error = adhoc_snapshot("fake", None, None, None).unwrap_err();
        assert_eq!(error.code, "unsupported");
    }

    #[test]
    fn adhoc_snapshot_requires_antigravity_model() {
        let error = adhoc_snapshot(
            "antigravity",
            None,
            None,
            Some(r"C:\bin\Antigravity.exe".to_owned()),
        )
        .unwrap_err();
        assert_eq!(error.code, "profile_error");
    }

    #[test]
    fn adhoc_snapshot_rejects_opencode_slashless_model() {
        let error = adhoc_snapshot(
            "opencode",
            Some("no-slash"),
            None,
            Some(r"C:\bin\opencode.exe".to_owned()),
        )
        .unwrap_err();
        assert_eq!(error.code, "profile_error");
    }

    #[test]
    fn wrapper_paths_are_never_native_executables() {
        for wrapper in [
            r"C:\bin\claude.cmd",
            r"C:\bin\tool.bat",
            r"C:\bin\tool.ps1",
            r"C:\bin\tool.js",
        ] {
            assert!(is_script_wrapper(wrapper), "{wrapper}");
        }
        assert!(!is_script_wrapper(r"C:\bin\claude.exe"));
        assert!(!is_script_wrapper(
            r"C:\Tools\MuseCode\muse-bin-1.4.20-R8619.1.exe"
        ));
        let error = adhoc_snapshot("claude", None, None, Some(r"C:\bin\claude.cmd".to_owned()))
            .unwrap_err();
        assert_eq!(error.code, "profile_error");
    }

    #[test]
    fn muse_discovery_chooses_the_launcher_marked_version() {
        let dir = std::env::temp_dir().join(format!(
            "eliot-adhoc-muse-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|elapsed| elapsed.as_nanos())
                .unwrap_or(0)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join(".muse-version"), "9.9.9-R1\n").unwrap();
        let native = dir.join("muse-bin-9.9.9-R1.exe");
        std::fs::write(&native, b"fake").unwrap();
        // A stale wrapper must not win over the marked native binary.
        std::fs::write(dir.join("muse.cmd"), b"@echo off").unwrap();
        let resolved = discover_muse_cmd(&dir).unwrap();
        assert_eq!(resolved, native.to_string_lossy());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn muse_discovery_without_a_marked_version_fails_honestly() {
        let dir = std::env::temp_dir().join(format!(
            "eliot-adhoc-muse-missing-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|elapsed| elapsed.as_nanos())
                .unwrap_or(0)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let error = discover_muse_cmd(&dir).unwrap_err();
        assert_eq!(error.code, "profile_error");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn adhoc_snapshot_freezes_an_explicit_cmd_verbatim() {
        // The `--cmd` override pins the native binary (e.g. the Muse 1.4.0
        // binary the launcher no longer marks current); discovery is
        // bypassed and the path is frozen exactly as given.
        let pinned = r"C:\Tools\MuseCode\muse-bin-1.4.0-R4161.1.exe";
        let snapshot = adhoc_snapshot("muse", None, None, Some(pinned.to_owned())).unwrap();
        assert_eq!(snapshot["cmd"], json!(pinned));
        let error = adhoc_snapshot(
            "muse",
            None,
            None,
            Some(r"C:\Tools\MuseCode\muse.cmd".to_owned()),
        )
        .unwrap_err();
        assert_eq!(error.code, "profile_error");
    }

    #[test]
    fn data_roots_fall_back_to_the_profile_derived_default() {
        use std::ffi::OsString;
        // A stripped daemon environment keeps no APPDATA, yet the installs
        // still exist under the profile: the derived root must be consulted.
        let roots = data_roots(None, Some(OsString::from(r"C:\Users\qa")), "Roaming");
        assert_eq!(roots, vec![PathBuf::from(r"C:\Users\qa\AppData\Roaming")]);
        let roots = data_roots(
            Some(OsString::from(r"D:\Custom\AppData\Roaming")),
            Some(OsString::from(r"C:\Users\qa")),
            "Roaming",
        );
        assert_eq!(
            roots,
            vec![
                PathBuf::from(r"D:\Custom\AppData\Roaming"),
                PathBuf::from(r"C:\Users\qa\AppData\Roaming"),
            ]
        );
        // No double entry when the variable already names the default.
        let roots = data_roots(
            Some(OsString::from(r"C:\Users\qa\AppData\Roaming")),
            Some(OsString::from(r"C:\Users\qa")),
            "Roaming",
        );
        assert_eq!(roots, vec![PathBuf::from(r"C:\Users\qa\AppData\Roaming")]);
    }

    #[test]
    fn opencode_candidates_follow_the_shim_target_first() {
        let shim_dir = PathBuf::from(r"C:\pkg\npm");
        let roaming = PathBuf::from(r"C:\Users\qa\AppData\Roaming");
        let candidates = opencode_candidates(
            std::slice::from_ref(&shim_dir),
            std::slice::from_ref(&roaming),
        );
        // The shim target is independent of the deep arch layout: the shim
        // at `<shim-dir>/opencode.cmd` delegates to
        // `<shim-dir>/node_modules/@opencode/cli/bin/opencode.exe`.
        assert_eq!(
            candidates.first().expect("shim target first"),
            &shim_dir
                .join("node_modules")
                .join("@opencode")
                .join("cli")
                .join("bin")
                .join("opencode.exe")
        );
        // The known package-local paths follow under every roaming root.
        assert!(
            candidates.contains(
                &roaming
                    .join("npm")
                    .join("node_modules")
                    .join("@opencode")
                    .join("cli")
                    .join("node_modules")
                    .join("@opencode")
                    .join("cli-windows-x64")
                    .join("bin")
                    .join("opencode.exe")
            )
        );
        assert!(
            candidates.contains(
                &roaming
                    .join("npm")
                    .join("node_modules")
                    .join("@opencode")
                    .join("cli")
                    .join("node_modules")
                    .join("@opencode")
                    .join("cli-windows-x64-baseline")
                    .join("bin")
                    .join("opencode.exe")
            )
        );
    }

    #[test]
    fn parse_cli_version_reads_the_leading_triple() {
        assert_eq!(
            parse_cli_version("2.1.325 (Claude Code)"),
            Some((2, 1, 325))
        );
        assert_eq!(
            parse_cli_version("Muse Code 1.4.20 (1.4.20-R8619.1)"),
            Some((1, 4, 20))
        );
        assert_eq!(parse_cli_version("not a version"), None);
        assert_eq!(parse_cli_version(""), None);
    }

    #[test]
    fn claude_bundle_exes_enumerate_the_versioned_layout() {
        let dir = std::env::temp_dir().join(format!(
            "eliot-adhoc-claude-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|elapsed| elapsed.as_nanos())
                .unwrap_or(0)
        ));
        let first = dir
            .join("Claude")
            .join("claude-code")
            .join("2.1.321")
            .join("d020ce1b5d30");
        let second = dir
            .join("Claude")
            .join("claude-code")
            .join("2.1.325")
            .join("806e0a45e6f5");
        std::fs::create_dir_all(&first).unwrap();
        std::fs::create_dir_all(&second).unwrap();
        let exes = claude_bundle_exes(&dir);
        assert_eq!(
            exes,
            vec![first.join("claude.exe"), second.join("claude.exe"),]
        );
        std::fs::remove_dir_all(&dir).ok();
    }
}
