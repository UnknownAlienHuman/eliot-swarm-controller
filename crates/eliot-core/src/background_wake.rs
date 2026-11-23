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
/// Stored in place of a tail when several tool windows contain the process start.
pub const BACKGROUND_WAKE_AMBIGUOUS_TAIL: &str =
    "(omitted: several tool calls overlapped this process start)";
pub const BACKGROUND_SERVER_TOPIC: &str = "background.server";
/// `tool.started.native_wake_scope`: the harness wakes itself for the call's
/// own task only. A process that outlives the task gets a Swarm wake.
pub const NATIVE_WAKE_SCOPE_TOOL_PROCESS: &str = "tool_process";
/// `tool.started.native_wake_scope`: the harness's own wake covers the call's
/// whole process tree, so Swarm adds no wake for any process it started.
pub const NATIVE_WAKE_SCOPE_PROCESS_TREE: &str = "process_tree";

const SWARM_INFRA_SUBCOMMANDS: &[&str] = &["mcp", "hook", "plugin", "daemon"];

const HARNESS_OWNED_STEMS: &[&str] = &[
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

/// Programs that run other programs: their name says nothing about the job.
const GENERIC_LAUNCHER_STEMS: &[&str] = &[
    "bash",
    "bun",
    "bunx",
    "cargo",
    "cmd",
    "dash",
    "deno",
    "docker",
    "dotnet",
    "env",
    "go",
    "java",
    "node",
    "npm",
    "npx",
    "perl",
    "php",
    "pip",
    "pipx",
    "pnpm",
    "pnpx",
    "podman",
    "powershell",
    "pwsh",
    "py",
    "python",
    "python3",
    "ruby",
    "sh",
    "uv",
    "uvx",
    "yarn",
    "zsh",
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

/// One MCP server from a frozen profile: its program stem and exact arguments.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConfiguredMcpServer {
    program: String,
    args: Vec<String>,
}

impl ConfiguredMcpServer {
    /// Whether a process runs this server. A generic launcher rewrites its own
    /// argv (`npx` runs `node`), so its server is identified by the configured
    /// arguments alone, and only when they name more than flags. Any other
    /// program must match too.
    fn matches(&self, program: Option<&str>, tokens: &[String]) -> bool {
        if GENERIC_LAUNCHER_STEMS.contains(&self.program.as_str()) {
            if self.args.iter().all(|arg| arg.starts_with('-')) {
                return false;
            }
        } else if program != Some(self.program.as_str()) {
            return false;
        }
        self.args.is_empty()
            || tokens.windows(self.args.len()).any(|window| {
                window.iter().zip(&self.args).all(|(token, arg)| {
                    token
                        .trim_matches(|ch| ch == '"' || ch == '\'')
                        .eq_ignore_ascii_case(arg)
                })
            })
    }
}

/// Extract configured MCP servers from a frozen profile snapshot.
pub fn extract_configured_mcp_servers(profile_snapshot: &Value) -> Vec<ConfiguredMcpServer> {
    let mut servers = Vec::new();
    let Some(root) = profile_snapshot.as_object() else {
        return servers;
    };
    for key in ["mcp_servers", "mcpServers", "servers"] {
        let Some(entries) = root.get(key).and_then(Value::as_object) else {
            continue;
        };
        for server in entries.values() {
            let Some(program) = server
                .get("command")
                .and_then(Value::as_str)
                .and_then(executable_stem)
            else {
                continue;
            };
            let args = server
                .get("args")
                .and_then(Value::as_array)
                .map(|args| {
                    args.iter()
                        .filter_map(Value::as_str)
                        .map(str::to_owned)
                        .collect()
                })
                .unwrap_or_default();
            servers.push(ConfiguredMcpServer { program, args });
        }
    }
    servers
}

/// Whether the process belongs to the harness runtime, console host plumbing,
/// LSP, telemetry helper, or Swarm/configured MCP infrastructure.
pub fn is_harness_owned_process(
    image_path: Option<&str>,
    argv: &[String],
    command_line: Option<&str>,
    configured_mcp_servers: &[ConfiguredMcpServer],
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
            || crate::api::HarnessCapabilities::is_harness_executable_stem(stem)
            || HARNESS_OWNED_STEMS.contains(&stem)
            || stem.starts_with("language_server_")
            || stem.starts_with("language-server-")
        {
            return true;
        }
        if stem == "esbuild" && tokens.iter().any(|token| token.starts_with("--service=")) {
            return true;
        }
    }
    if configured_mcp_servers
        .iter()
        .any(|server| server.matches(stem.as_deref(), &tokens))
    {
        return true;
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
        "Background process exited (pid {pid}, raw process exit code {code_str}).\n\
         Command: {command_line}{suffix}"
    );
    if let Some(tail) = output_tail.map(str::trim).filter(|tail| !tail.is_empty()) {
        body.push_str("\nOutput tail:\n");
        body.push_str(tail);
    }
    body
}
