use std::{
    ffi::OsString,
    fs,
    path::{Path, PathBuf},
};

use eliot_core::{Error, Result};
use eliot_plugin::native_cli::{
    WORKER_SWARM_MCP_SERVER_NAME, require_agent_mcp_root, worker_swarm_mcp_args,
};
use serde_json::json;

pub(crate) const MCP_TOKEN_ENV: &str = "ELIOT_CLAUDE_MCP_TOKEN";

/// One private Claude CLI config file for an owned native session.
pub(crate) struct ClaudeMcpConfigFile {
    path: PathBuf,
}

impl ClaudeMcpConfigFile {
    /// Resolve the exact Agent-bound Eliot data root from the launch
    /// environment. The MCP server itself still enforces this identity with
    /// `--require-agent`; this preflight never falls back to Owner auth.
    pub(crate) fn require_agent_data_root(
        environment: &[(OsString, OsString)],
        agent_id: &str,
        generation: i64,
    ) -> Result<PathBuf> {
        require_agent_mcp_root(
            environment,
            agent_id,
            generation,
            "Claude MCP requires the matching inherited Agent generation identity",
            "Claude MCP requires the explicit Eliot data root",
        )
    }

    /// Write public selectors and a reference Claude expands from its own
    /// environment. The generation credential never enters this file.
    pub(crate) fn create(
        instance_dir: &Path,
        swarm_binary: &Path,
        data_root: &Path,
        environment: &[(OsString, OsString)],
        agent_mcp_url: Option<&str>,
    ) -> Result<Self> {
        if !instance_dir.is_absolute() || !instance_dir.is_dir() {
            return Err(Error::new(
                "not_allowed",
                "Claude MCP config requires the private Agent instance directory",
            ));
        }
        if !swarm_binary.is_absolute()
            || !swarm_binary.is_file()
            || !data_root.is_absolute()
            || !data_root.is_dir()
        {
            return Err(Error::new(
                "platform_io",
                "Claude MCP requires the current Eliot executable and data root",
            ));
        }
        let command = swarm_binary
            .to_str()
            .ok_or_else(|| Error::new("invalid", "Claude MCP executable path is not UTF-8"))?;
        let data_dir = data_root
            .to_str()
            .ok_or_else(|| Error::new("invalid", "Claude MCP data-root path is not UTF-8"))?;
        let token_reference = format!("${{{MCP_TOKEN_ENV}}}");
        let environment_json = environment
            .iter()
            .map(|(name, value)| {
                (
                    name.to_string_lossy().into_owned(),
                    if name.eq_ignore_ascii_case("ELIOT_AGENT_TOKEN") {
                        json!(token_reference)
                    } else {
                        json!(value.to_string_lossy())
                    },
                )
            })
            .collect::<serde_json::Map<_, _>>();
        let server = if let Some(url) = agent_mcp_url {
            validate_agent_mcp_url(url)?;
            let find = |name: &str| {
                environment
                    .iter()
                    .find(|(key, _)| key.to_string_lossy().eq_ignore_ascii_case(name))
                    .map(|(_, value)| value.to_string_lossy().into_owned())
            };
            let agent_id = find("ELIOT_AGENT_ID").ok_or_else(|| {
                Error::new("not_allowed", "Claude Agent MCP identity is incomplete")
            })?;
            let generation = find("ELIOT_AGENT_GENERATION").ok_or_else(|| {
                Error::new("not_allowed", "Claude Agent MCP identity is incomplete")
            })?;
            json!({"type":"http", "url":url, "headers":{
                "x-eliot-agent-id":agent_id,
                "x-eliot-agent-generation":generation,
                "Authorization":format!("Bearer {token_reference}")
            }})
        } else {
            json!({"command":command,"args":worker_swarm_mcp_args(data_dir),"env":environment_json})
        };
        let config = json!({"mcpServers": {WORKER_SWARM_MCP_SERVER_NAME: server}});
        let contents = serde_json::to_vec(&config)
            .map_err(|_| Error::new("internal", "Claude MCP config could not be encoded"))?;
        let path = instance_dir.join("claude-mcp-config.json");
        eliot_plugin::install::write_new_file(
            &path,
            &contents,
            || Error::new("platform_io", "could not create Claude MCP config"),
            || Error::new("platform_io", "could not write Claude MCP config"),
        )?;
        Ok(Self { path })
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }
}

fn validate_agent_mcp_url(url: &str) -> Result<()> {
    let port = url
        .strip_prefix("http://127.0.0.1:")
        .and_then(|url| url.strip_suffix("/mcp"))
        .filter(|port| port.bytes().all(|byte| byte.is_ascii_digit()))
        .and_then(|port| port.parse::<u16>().ok());
    if port.is_none_or(|port| port == 0) {
        return Err(Error::new(
            "not_allowed",
            "Claude Agent MCP bridge URL must target 127.0.0.1 with an explicit port and /mcp path",
        ));
    }
    Ok(())
}

impl Drop for ClaudeMcpConfigFile {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}
