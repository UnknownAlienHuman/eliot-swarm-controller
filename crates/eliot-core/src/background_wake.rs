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

/// Whether the process belongs to the harness runtime, console host plumbing,
/// LSP, telemetry helper, or Swarm/configured MCP infrastructure.
pub fn is_harness_owned_process(
    image_path: Option<&str>,
    argv: &[String],
    command_line: Option<&str>,
    configured_mcp_programs: &[String],
) -> bool {
    let tokens = owned_tokens(image_path, argv, command_line);
    if is_swarm_infra_tokens(&tokens) {
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
