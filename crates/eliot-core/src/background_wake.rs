//! Pure types, classifiers, and formatting for automatic background-process wakes.
//!
//! Nothing here performs I/O or OS queries. Monitoring owns custody sampling
//! and process-tree reads; Store owns durable wake state; Router owns arming
//! and delivery.

use crate::observe::split_windows_command_line;
use serde_json::Value;

pub const DEFAULT_BACKGROUND_WAKE_MAX_WAIT_SECS: i64 = 1_800;
pub const MAX_BACKGROUND_WAKE_MAX_WAIT_SECS: i64 = 86_400;
pub const BACKGROUND_WAKE_NOTE_KIND: &str = "background_process_wake";
pub const BACKGROUND_WAKE_PRINCIPAL: &str = "swarm_note:background_process_wake";
pub const MAX_BACKGROUND_WAKE_COMMAND_BYTES: usize = 512;
pub const MAX_BACKGROUND_WAKE_OUTPUT_TAIL_BYTES: usize = 1_024;
pub const BACKGROUND_SERVER_TOPIC: &str = "background.server";
pub const HARNESS_HELPER_COMMANDS_FIELD: &str = "harness_helpers";
pub const MAX_HARNESS_HELPER_COMMANDS: usize = 16;
pub const MAX_HARNESS_HELPER_ARGV: usize = 16;
pub const MAX_HARNESS_HELPER_TOKEN_BYTES: usize = 512;

const SWARM_INFRA_SUBCOMMANDS: &[&str] = &["mcp", "hook", "plugin", "daemon"];

const HARNESS_OWNED_STEMS: &[&str] = &[
    "agy",
    "antigravity",
    "claude",
    "codex",
    "opencode",
    "muse",
    "language_server",
    "language-server",
    "rust-analyzer",
    "typescript-language-server",
    "pyright-langserver",
    "basedpyright-langserver",
    "clangd",
    "gopls",
    "zls",
    "omnisharp",
    "jdtls",
    "pylsp",
    "lua-language-server",
    "crashpad_handler",
];

fn executable_stem(raw: &str) -> Option<String> {
    let trimmed = raw.trim().trim_matches(|ch| ch == '"' || ch == '\'');
    let base = trimmed.rsplit(['/', '\\']).next().unwrap_or(trimmed);
    let base = base
        .strip_suffix(".exe")
        .or_else(|| base.strip_suffix(".EXE"))
        .or_else(|| base.strip_suffix(".cmd"))
        .or_else(|| base.strip_suffix(".CMD"))
        .or_else(|| base.strip_suffix(".bat"))
        .or_else(|| base.strip_suffix(".BAT"))
        .unwrap_or(base);
    let stem = base.trim().to_ascii_lowercase();
    (!stem.is_empty()).then_some(stem)
}

fn owned_tokens(
    image_path: Option<&str>,
    argv: &[String],
    command_line: Option<&str>,
) -> Vec<String> {
    if !argv.is_empty() {
        return argv.to_vec();
    }
    if let Some(cmd) = command_line {
        let parsed = split_windows_command_line(cmd);
        if !parsed.is_empty() {
            return parsed;
        }
    }
    image_path
        .map(|path| vec![path.to_owned()])
        .unwrap_or_default()
}

fn is_swarm_infra_tokens(tokens: &[String]) -> bool {
    let Some(first) = tokens.first() else {
        return false;
    };
    let Some(stem) = executable_stem(first) else {
        return false;
    };
    if !matches!(
        stem.as_str(),
        "swarm" | "eliot" | "eliot-daemon" | "eliot-cli"
    ) {
        return false;
    }
    if matches!(stem.as_str(), "eliot-daemon") {
        return true;
    }
    let mut index = 1;
    while let Some(token) = tokens.get(index) {
        let lower = token.to_ascii_lowercase();
        if matches!(lower.as_str(), "--data-dir" | "--id" | "--config") {
            index += 2;
            continue;
        }
        if lower.starts_with('-') {
            index += 1;
            continue;
        }
        return SWARM_INFRA_SUBCOMMANDS.contains(&lower.as_str());
    }
    false
}

/// Extract configured MCP server program stems/names from a frozen profile snapshot.
pub fn extract_configured_mcp_programs(profile_snapshot: &Value) -> Vec<String> {
    let mut programs = Vec::new();
    let Some(root) = profile_snapshot.as_object() else {
        return programs;
    };
    for key in ["mcp_servers", "mcpServers", "servers"] {
        let Some(entries) = root.get(key).and_then(Value::as_object) else {
            continue;
        };
        for server in entries.values() {
            if let Some(command) = server.get("command").and_then(Value::as_str)
                && let Some(stem) = executable_stem(command)
            {
                programs.push(stem);
            }
            if let Some(args) = server.get("args").and_then(Value::as_array) {
                for arg in args.iter().filter_map(Value::as_str) {
                    if let Some(stem) = executable_stem(arg)
                        && (stem.contains("mcp") || arg.contains(['/', '\\']))
                    {
                        programs.push(stem);
                    }
                }
            }
        }
    }
    programs.sort();
    programs.dedup();
    programs
}

/// Whether `harness` attaches the native shell command string (`data.command`)
/// to command `tool.started` events.
pub fn harness_emits_tool_commands(harness: &str) -> bool {
    matches!(harness, "codex" | "claude" | "opencode")
}

/// Extract plugin-reported harness helper argv prefixes from `Agent.effective`.
/// Any entry whose length exceeds `MAX_HARNESS_HELPER_ARGV` or whose elements are
/// invalid is dropped as a whole rather than truncated to a broader prefix.
pub fn extract_harness_helper_commands(effective: Option<&Value>) -> Vec<Vec<String>> {
    let Some(entries) = effective
        .and_then(|value| value.get(HARNESS_HELPER_COMMANDS_FIELD))
        .and_then(Value::as_array)
    else {
        return Vec::new();
    };
    entries
        .iter()
        .take(MAX_HARNESS_HELPER_COMMANDS)
        .filter_map(|entry| {
            let argv = entry.as_array()?;
            if argv.is_empty() || argv.len() > MAX_HARNESS_HELPER_ARGV {
                return None;
            }
            let mut tokens = Vec::with_capacity(argv.len());
            for item in argv {
                let token = item.as_str().map(str::trim)?;
                if token.is_empty()
                    || token.len() > MAX_HARNESS_HELPER_TOKEN_BYTES
                    || token.chars().any(char::is_control)
                {
                    return None;
                }
                tokens.push(token.to_owned());
            }
            Some(tokens)
        })
        .collect()
}

fn has_path_sep(raw: &str) -> bool {
    raw.contains(['/', '\\'])
}

fn normalize_helper_path(raw: &str) -> String {
    let trimmed = raw.trim().trim_matches(|ch| ch == '"' || ch == '\'');
    let stripped = trimmed
        .strip_prefix("\\\\?\\")
        .or_else(|| trimmed.strip_prefix("//?/"))
        .unwrap_or(trimmed);
    stripped.replace('\\', "/").to_ascii_lowercase()
}

fn normalize_helper_executable_path(raw: &str) -> String {
    let normalized = normalize_helper_path(raw);
    for ext in [".exe", ".cmd", ".bat", ".com"] {
        if let Some(stem) = normalized.strip_suffix(ext)
            && !stem.is_empty()
        {
            return stem.to_owned();
        }
    }
    normalized
}

fn helper_prog_matches(helper_prog: &str, proc_prog: &str) -> bool {
    if has_path_sep(helper_prog) && has_path_sep(proc_prog) {
        normalize_helper_executable_path(helper_prog) == normalize_helper_executable_path(proc_prog)
    } else {
        executable_stem(helper_prog)
            .is_some_and(|stem| executable_stem(proc_prog).as_deref() == Some(stem.as_str()))
    }
}

fn matches_harness_helper(
    image_path: Option<&str>,
    tokens: &[String],
    harness_helpers: &[Vec<String>],
) -> bool {
    harness_helpers.iter().any(|helper| {
        let Some(helper_prog) = helper.first() else {
            return false;
        };
        let prog_matches = image_path.is_some_and(|path| helper_prog_matches(helper_prog, path))
            || tokens
                .first()
                .is_some_and(|prog| helper_prog_matches(helper_prog, prog));
        if !prog_matches {
            return false;
        }
        if helper.len() > 1 {
            if tokens.len() < helper.len() {
                return false;
            }
            for (expected, actual) in helper.iter().zip(tokens.iter()).skip(1) {
                let arg_matches = actual == expected
                    || ((has_path_sep(expected) || has_path_sep(actual))
                        && normalize_helper_path(actual) == normalize_helper_path(expected));
                if !arg_matches {
                    return false;
                }
            }
        }
        true
    })
}

/// Whether the process belongs to the harness runtime, console host plumbing,
/// LSP, telemetry helper, plugin-reported harness helper, or Swarm/configured
/// MCP infrastructure.
pub fn is_harness_owned_process(
    image_path: Option<&str>,
    argv: &[String],
    command_line: Option<&str>,
    configured_mcp_programs: &[String],
    harness_helpers: &[Vec<String>],
) -> bool {
    let tokens = owned_tokens(image_path, argv, command_line);
    if is_swarm_infra_tokens(&tokens)
        || matches_harness_helper(image_path, &tokens, harness_helpers)
    {
        return true;
    }
    let stem = image_path
        .and_then(executable_stem)
        .or_else(|| tokens.first().and_then(|token| executable_stem(token)));
    if let Some(stem) = stem.as_deref() {
        if crate::CONSOLE_HOST_PROGRAMS.contains(&stem)
            || matches!(stem, "winpty-agent" | "conpty")
            || HARNESS_OWNED_STEMS.contains(&stem)
            || stem.starts_with("language_server_")
            || stem.starts_with("language-server-")
        {
            return true;
        }
        if stem == "esbuild" && tokens.iter().any(|token| token.starts_with("--service=")) {
            return true;
        }
        if configured_mcp_programs
            .iter()
            .any(|entry| !entry.is_empty() && entry.eq_ignore_ascii_case(stem))
        {
            return true;
        }
    }
    let mut saw_mcp = false;
    let mut saw_stdio = false;
    for token in &tokens {
        let lower = token.to_ascii_lowercase();
        if lower.contains("mcp-server")
            || lower.contains("mcp_server")
            || lower.contains("@modelcontextprotocol/")
            || lower.contains("eliot-mcp")
            || lower.contains("codebase-memory-mcp")
            || lower.contains("mcpls")
        {
            return true;
        }
        if lower == "mcp" {
            saw_mcp = true;
        }
        if matches!(lower.as_str(), "--stdio" | "stdio") {
            saw_stdio = true;
        }
        if !configured_mcp_programs.is_empty()
            && let Some(token_stem) = executable_stem(token)
            && configured_mcp_programs
                .iter()
                .any(|entry| !entry.is_empty() && entry.eq_ignore_ascii_case(&token_stem))
        {
            return true;
        }
    }
    saw_mcp && saw_stdio
}

const SECRET_MASK: &str = "***";
const SECRET_QUOTES: &[char] = &['"', '\'', '`'];
const SECRET_FLAG_WORDS: &[&str] = &[
    "password",
    "passwd",
    "token",
    "secret",
    "api-key",
    "api_key",
    "apikey",
    "private-key",
];
const SECRET_NAME_WORDS: &[&str] = &["TOKEN", "SECRET", "PASSWORD", "PASSWD", "API_KEY", "APIKEY"];

/// Mask common secret shapes in a command line or output tail: the value of a
/// `--password`, `--token`, `--secret` or `--api-key` style flag (`--flag v` or
/// `--flag=v`), an `Authorization:` header value, a `Bearer` credential, and
/// `NAME=value` where NAME contains TOKEN, SECRET, PASSWORD or API_KEY. This is
/// shape-based and best effort; it runs after literal known-secret redaction.
pub fn redact_secret_shapes(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut mask_next = false;
    let mut rest = text;
    while !rest.is_empty() {
        let space = rest.len() - rest.trim_start().len();
        out.push_str(&rest[..space]);
        rest = &rest[space..];
        let end = rest.find(char::is_whitespace).unwrap_or(rest.len());
        if end == 0 {
            break;
        }
        let token = &rest[..end];
        rest = &rest[end..];
        let core = token.trim_matches(SECRET_QUOTES).to_ascii_lowercase();
        if mask_next {
            if matches!(core.as_str(), "bearer" | "basic" | "token" | "digest") {
                out.push_str(token);
                continue;
            }
            out.push_str(&mask_value(token, 0));
            mask_next = false;
            continue;
        }
        if let Some(flag) = core.strip_prefix('-') {
            let flag = flag.trim_start_matches('-');
            match flag.split_once('=') {
                Some((name, value)) if is_secret_flag(name) && !value.is_empty() => {
                    let at = token.find('=').map_or(token.len(), |index| index + 1);
                    out.push_str(&mask_value(token, at));
                }
                None if is_secret_flag(flag) => {
                    out.push_str(token);
                    mask_next = true;
                }
                _ => out.push_str(token),
            }
            continue;
        }
        if let Some(value) = core.strip_prefix("authorization:") {
            if value.is_empty() || matches!(value, "bearer" | "basic" | "token" | "digest") {
                out.push_str(token);
                mask_next = true;
            } else {
                let at = token.find(':').map_or(token.len(), |index| index + 1);
                out.push_str(&mask_value(token, at));
            }
            continue;
        }
        if core == "bearer" {
            out.push_str(token);
            mask_next = true;
            continue;
        }
        let trimmed = token.trim_start_matches(SECRET_QUOTES);
        if let Some((name, value)) = trimmed.split_once('=')
            && !name.is_empty()
            && !value.trim_matches(SECRET_QUOTES).is_empty()
            && name
                .chars()
                .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
            && SECRET_NAME_WORDS
                .iter()
                .any(|word| name.to_ascii_uppercase().contains(word))
        {
            let at = token.find('=').map_or(token.len(), |index| index + 1);
            out.push_str(&mask_value(token, at));
            continue;
        }
        out.push_str(token);
    }
    out
}

fn is_secret_flag(name: &str) -> bool {
    SECRET_FLAG_WORDS.iter().any(|word| name.contains(word))
}

/// Keep `token[..at]` and surrounding quotes; replace the value with the mask.
fn mask_value(token: &str, at: usize) -> String {
    let (prefix, value) = token.split_at(at.min(token.len()));
    let lead = value.len() - value.trim_start_matches(SECRET_QUOTES).len();
    let trail = value.len() - value.trim_end_matches(SECRET_QUOTES).len();
    if lead + trail >= value.len() {
        return token.to_owned();
    }
    format!(
        "{prefix}{}{SECRET_MASK}{}",
        &value[..lead],
        &value[value.len() - trail..]
    )
}

/// Format the body of the `#888` Swarm note delivered for an automatic background wake.
pub fn format_background_wake_note_body(
    outcome_kind: &str,
    pid: u32,
    command_line: &str,
    command_truncated: bool,
    exit_code: Option<i32>,
    output_tail: Option<&str>,
    max_wait_secs: i64,
) -> String {
    let suffix = if command_truncated {
        " [truncated]"
    } else {
        ""
    };
    if outcome_kind == "expired" {
        return format!(
            "Background process is still running after {max_wait_secs}s (pid {pid}).\n\
             Command: {command_line}{suffix}\n\
             Swarm stopped waiting automatically."
        );
    }
    let code_str = exit_code
        .map(|code| code.to_string())
        .unwrap_or_else(|| "unknown".to_owned());
    let mut body = format!(
        "Background process exited (pid {pid}, exit code {code_str}).\n\
         Command: {command_line}{suffix}"
    );
    if let Some(tail) = output_tail.map(str::trim).filter(|tail| !tail.is_empty()) {
        body.push_str("\nOutput tail:\n");
        body.push_str(tail);
    }
    body
}
