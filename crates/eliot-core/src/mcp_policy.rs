//! Frozen worker MCP inheritance policy shared by the daemon config loader
//! and the harness plugins. Pure validation; no I/O.
//!
//! Owning contract: `docs/spec/configuration.md` worker MCP policy. A profile
//! selects `minimal` (the default: only Eliot's Agent-scoped server plus an
//! explicit allow list) or `inherit` (explicit opt-in: the worker keeps the
//! owner's native MCP surface plus Eliot's Agent-scoped server). The daemon
//! freezes the resolved policy with the profile snapshot; each harness plugin
//! enforces and reports it. Owner-side Swarm MCP entries are reserved in
//! every policy: a worker that talks to the owner's daemon with the owner's
//! identity is an authority leak, so Swarm names can never be allow-listed.

use crate::{Error, Result};

/// Keep the owner's native MCP surface plus Eliot's Agent-scoped server.
pub const MCP_POLICY_INHERIT: &str = "inherit";
/// Only Eliot's Agent-scoped server plus the explicit allow list.
pub const MCP_POLICY_MINIMAL: &str = "minimal";
/// Bound matching the daemon's other per-profile entry lists.
pub const MAX_MCP_ALLOW_ENTRIES: usize = 64;
/// Bound matching the daemon's other per-profile name fields.
pub const MAX_MCP_SERVER_NAME_LEN: usize = 128;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum McpPolicy {
    Inherit,
    Minimal,
}

impl McpPolicy {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Inherit => MCP_POLICY_INHERIT,
            Self::Minimal => MCP_POLICY_MINIMAL,
        }
    }

    /// `None` (unset profile field) means the default `minimal`. Any other
    /// unknown value is rejected at config load. Frozen snapshots without
    /// the key keep their recorded meaning (see `parse_snapshot`); only live
    /// profile parsing defaults here.
    pub fn parse(raw: Option<&str>) -> Result<Self> {
        match raw.map(str::trim) {
            None => Ok(Self::Minimal),
            Some(MCP_POLICY_INHERIT) => Ok(Self::Inherit),
            Some(MCP_POLICY_MINIMAL) => Ok(Self::Minimal),
            Some(_) => Err(Error::new(
                "profile_error",
                "mcp must be inherit or minimal",
            )),
        }
    }

    /// Read the frozen snapshot fields back. Snapshots predating the policy
    /// (no `mcp` key) keep their recorded meaning, `inherit`; malformed
    /// frozen values fail instead of silently inheriting. New freezes always
    /// record the key explicitly, so this legacy read never mints `minimal`.
    pub fn parse_snapshot(profile: &serde_json::Map<String, serde_json::Value>) -> Result<Self> {
        match profile.get("mcp") {
            None | Some(serde_json::Value::Null) => Ok(Self::Inherit),
            Some(serde_json::Value::String(raw)) => Self::parse(Some(raw)),
            Some(_) => Err(Error::new(
                "profile_error",
                "frozen profile mcp policy must be inherit or minimal",
            )),
        }
    }
}

/// Validate an explicit allow list of native MCP server names. Trims each
/// entry; rejects empty, overlong, control-bearing and duplicated names.
/// The returned names keep file order for deterministic snapshots.
pub fn validate_allow_list(raw: &[String]) -> Result<Vec<String>> {
    if raw.len() > MAX_MCP_ALLOW_ENTRIES {
        return Err(Error::new("profile_error", "mcp_allow exceeds 64 entries"));
    }
    let mut names = Vec::with_capacity(raw.len());
    for entry in raw {
        let name = entry.trim();
        if name.is_empty()
            || name.len() > MAX_MCP_SERVER_NAME_LEN
            || name.chars().any(char::is_control)
        {
            return Err(Error::new(
                "profile_error",
                "mcp_allow names must be non-empty, at most 128 bytes and contain no controls",
            ));
        }
        if is_swarm_reserved_server_name(name) {
            return Err(Error::new(
                "profile_error",
                "mcp_allow cannot include a Swarm MCP server",
            ));
        }
        if names.iter().any(|kept| *kept == name) {
            return Err(Error::new(
                "profile_error",
                "mcp_allow contains a duplicate server name",
            ));
        }
        names.push(name.to_owned());
    }
    Ok(names)
}

/// Owner-side Swarm MCP server names that must never reach a worker in any
/// policy. The exact owner entries (`eliot_swarm`, `eliot-swarm`) carry the
/// owner's daemon identity; the prefixed session names (`eliot_swarm_*`,
/// `eliot-swarm-*`) are minted per worker session and are always present
/// without being allowed, so reserving the whole prefix cannot starve the
/// Agent-scoped server.
pub const SWARM_RESERVED_SERVER_NAMES: &[&str] = &["eliot_swarm", "eliot-swarm"];

/// Whether `name` names a Swarm MCP server: the exact owner entries or the
/// per-session prefix. Reserved names are never valid `mcp_allow` entries.
pub fn is_swarm_reserved_server_name(name: &str) -> bool {
    name == "eliot_swarm"
        || name == "eliot-swarm"
        || name.starts_with("eliot_swarm_")
        || name.starts_with("eliot-swarm-")
}

/// Read the frozen `mcp_allow` snapshot field back. Absent means empty.
pub fn parse_snapshot_allow_list(
    profile: &serde_json::Map<String, serde_json::Value>,
) -> Result<Vec<String>> {
    match profile.get("mcp_allow") {
        None | Some(serde_json::Value::Null) => Ok(Vec::new()),
        Some(serde_json::Value::Array(values)) => {
            let mut raw = Vec::with_capacity(values.len());
            for value in values {
                match value.as_str() {
                    Some(name) => raw.push(name.to_owned()),
                    None => {
                        return Err(Error::new(
                            "profile_error",
                            "frozen profile mcp_allow must be an array of strings",
                        ));
                    }
                }
            }
            validate_allow_list(&raw)
        }
        Some(_) => Err(Error::new(
            "profile_error",
            "frozen profile mcp_allow must be an array of strings",
        )),
    }
}

/// Owner Swarm MCP server identity: a transport shape, not an entry name.
/// A renamed owner entry (`swarm`, `owner-tools`, ...) is still the owner's
/// daemon identity when its transport matches this rule; the Agent-scoped
/// server Eliot generates is the only Swarm server a worker ever gets.
///
/// Stdio matches Swarm and `mcp` in the command or arguments unless the Agent
/// flag is present. Shell text naming Swarm and missing argument readback
/// cannot prove Agent scope.
///
/// HTTP matches the configured cloud host or a recognizable Swarm host when
/// the snapshot lacks the hostname.
///
/// Owning contract: `docs/spec/configuration.md` worker MCP policy. Every
/// harness plugin applies this rule in every policy, regardless of the
/// configured entry name or any allow list.
pub fn is_owner_swarm_server(
    command: Option<&str>,
    args: Option<&[String]>,
    url: Option<&str>,
    controller_executable: Option<&str>,
    cloud_mcp_hostname: Option<&str>,
) -> bool {
    is_owner_swarm_stdio(command, args, controller_executable)
        || is_owner_swarm_http(url, cloud_mcp_hostname)
}

/// Whether a stdio server entry carries the owner Swarm identity: a Swarm
/// executable command (file name or controller path) selecting `mcp`
/// without `--require-agent`.
pub fn is_owner_swarm_stdio(
    command: Option<&str>,
    args: Option<&[String]>,
    controller_executable: Option<&str>,
) -> bool {
    let mut swarm = false;
    let mut shell = false;
    let mut mcp = false;
    let mut require_agent = false;
    for text in command
        .into_iter()
        .chain(args.into_iter().flatten().map(String::as_str))
    {
        swarm |= is_swarm_executable_token(text, controller_executable);
        shell |= is_shell_command(text);
        for word in command_words(text) {
            swarm |= is_swarm_executable_token(word, controller_executable);
            shell |= is_shell_command(word);
            mcp |= word == "mcp";
            require_agent |= word == "--require-agent";
        }
    }
    swarm && (shell || ((mcp || args.is_none()) && !require_agent))
}

/// Match the configured cloud host. With no hostname, reject hosts with a
/// Swarm service label, including the owner endpoint documented in the vision.
pub fn is_owner_swarm_http(url: Option<&str>, cloud_mcp_hostname: Option<&str>) -> bool {
    let Some(url) = url else {
        return false;
    };
    if let Some(hostname) = cloud_mcp_hostname.filter(|host| !host.trim().is_empty()) {
        return url_host_equals(url, hostname);
    }
    url::Url::parse(url.trim()).ok().is_some_and(|parsed| {
        parsed.host_str().is_some_and(|host| {
            host.split('.')
                .any(|label| matches!(label, "swarm" | "eliot-swarm" | "eliot_swarm"))
        })
    })
}

/// Whether `command` names a Swarm executable: file name `swarm` (with or
/// without `.exe`, any case) or equality with the active controller binary
/// path (covers a renamed controller binary). Empty commands never match.
pub fn is_swarm_executable_command(command: &str, controller_executable: Option<&str>) -> bool {
    is_swarm_executable_token(command, controller_executable)
        || command_words(command).any(|word| is_swarm_executable_token(word, controller_executable))
}

fn is_swarm_executable_token(command: &str, controller_executable: Option<&str>) -> bool {
    if command.trim().is_empty() {
        return false;
    }
    let file = command_file_name(command);
    if file.eq_ignore_ascii_case("swarm") || file.eq_ignore_ascii_case("swarm.exe") {
        return true;
    }
    match controller_executable {
        Some(controller) if !controller.trim().is_empty() => {
            normalize_executable_path(command) == normalize_executable_path(controller)
        }
        _ => false,
    }
}

// Scan literal tokens in argv and shell text without interpreting shell
// expansions. A backslash can be a Windows path separator.
fn command_words(text: &str) -> impl Iterator<Item = &str> {
    text.split(|c: char| {
        c.is_whitespace()
            || matches!(
                c,
                '\'' | '"' | '`' | '&' | '|' | ';' | '(' | ')' | '<' | '>'
            )
    })
    .filter(|word| !word.is_empty())
}

fn is_shell_command(text: &str) -> bool {
    let name = command_file_name(text).to_ascii_lowercase();
    matches!(
        name.strip_suffix(".exe").unwrap_or(&name),
        "cmd" | "powershell" | "pwsh" | "sh" | "bash" | "dash" | "zsh" | "ksh" | "ash" | "fish"
    )
}

/// Whether `url` points at `hostname`: the parsed URL host equals the
/// configured cloud hostname ignoring case. Ports, userinfo and paths do
/// not affect the comparison; an unparseable URL is not a match.
pub fn url_host_equals(url: &str, hostname: &str) -> bool {
    if hostname.trim().is_empty() {
        return false;
    }
    let Ok(parsed) = url::Url::parse(url.trim()) else {
        return false;
    };
    parsed.host_str().is_some_and(|host| {
        host.trim_end_matches('.')
            .eq_ignore_ascii_case(hostname.trim().trim_end_matches('.'))
    })
}

/// File name of a command path: the text after the last `/` or `\`, with
/// one layer of surrounding quotes and whitespace removed.
fn command_file_name(command: &str) -> &str {
    let command = clean_command_text(command);
    command
        .rfind(['/', '\\'])
        .map(|index| &command[index + 1..])
        .unwrap_or(command)
}

/// Normalize an executable path for equality: trim, strip one layer of
/// surrounding quotes and the Windows verbatim `\\?\` prefix; compare
/// case-insensitively with unified separators on Windows, exactly
/// elsewhere.
fn normalize_executable_path(path: &str) -> String {
    let path = clean_command_text(path);
    let path = path.strip_prefix(r"\\?\").unwrap_or(path);
    if cfg!(windows) {
        path.replace('/', "\\").to_lowercase()
    } else {
        path.to_owned()
    }
}

/// Trim whitespace and strip one layer of surrounding single/double quotes.
fn clean_command_text(command: &str) -> &str {
    let command = command.trim();
    let bytes = command.as_bytes();
    if bytes.len() >= 2 {
        let (first, last) = (bytes[0], bytes[bytes.len() - 1]);
        if (first == b'"' && last == b'"') || (first == b'\'' && last == b'\'') {
            return command[1..command.len() - 1].trim();
        }
    }
    command
}

/// Read the frozen `mcp_cloud_hostname` snapshot field back. Absent, null,
/// non-string or blank means the daemon has no cloud MCP hostname
/// configured (or the snapshot predates it). The identity rule then checks
/// recognizable Swarm host labels. A present value is trimmed.
pub fn parse_snapshot_cloud_hostname(
    profile: &serde_json::Map<String, serde_json::Value>,
) -> Option<String> {
    match profile.get("mcp_cloud_hostname") {
        Some(serde_json::Value::String(hostname)) if !hostname.trim().is_empty() => {
            Some(hostname.trim().to_owned())
        }
        _ => None,
    }
}

/// Strip JSONC comments (`//` line and `/* */` block comments) outside
/// string literals, so owner/project config files with comments still scan
/// for owner-identity entries. String escapes are honored; newlines inside
/// comments are preserved; an unterminated block comment drops the
/// remainder. Never fails.
pub fn strip_jsonc_comments(text: &str) -> String {
    #[derive(Clone, Copy, PartialEq, Eq)]
    enum State {
        Code,
        LineComment,
        BlockComment,
    }
    let mut out = String::with_capacity(text.len());
    let mut state = State::Code;
    let mut in_string = false;
    let mut escaped = false;
    let mut chars = text.chars().peekable();
    while let Some(current) = chars.next() {
        match state {
            State::LineComment => {
                if current == '\n' {
                    state = State::Code;
                    out.push(current);
                }
            }
            State::BlockComment => {
                if current == '\n' {
                    out.push(current);
                } else if current == '*' && chars.peek() == Some(&'/') {
                    chars.next();
                    state = State::Code;
                }
            }
            State::Code => {
                if in_string {
                    out.push(current);
                    if escaped {
                        escaped = false;
                    } else if current == '\\' {
                        escaped = true;
                    } else if current == '"' {
                        in_string = false;
                    }
                } else if current == '"' {
                    in_string = true;
                    out.push(current);
                } else if current == '/' {
                    match chars.peek() {
                        Some('/') => {
                            chars.next();
                            state = State::LineComment;
                        }
                        Some('*') => {
                            chars.next();
                            state = State::BlockComment;
                        }
                        _ => out.push(current),
                    }
                } else {
                    out.push(current);
                }
            }
        }
    }
    out
}

/// Drop JSONC trailing commas (a comma followed only by whitespace up to a
/// `}` or `]`) outside string literals. Applied after [`strip_jsonc_comments`];
/// string escapes are honored. Never fails.
pub fn strip_jsonc_trailing_commas(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut in_string = false;
    let mut escaped = false;
    let chars: Vec<char> = text.chars().collect();
    let mut index = 0;
    while index < chars.len() {
        let current = chars[index];
        if in_string {
            out.push(current);
            if escaped {
                escaped = false;
            } else if current == '\\' {
                escaped = true;
            } else if current == '"' {
                in_string = false;
            }
            index += 1;
            continue;
        }
        if current == '"' {
            in_string = true;
            out.push(current);
            index += 1;
            continue;
        }
        if current == ',' {
            let mut ahead = index + 1;
            while ahead < chars.len() && chars[ahead].is_whitespace() {
                ahead += 1;
            }
            if ahead < chars.len() && (chars[ahead] == '}' || chars[ahead] == ']') {
                index += 1;
                continue;
            }
        }
        out.push(current);
        index += 1;
    }
    out
}
