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

    /// Write the public executable/data-root arguments and the exact scoped
    /// Agent identity for this private MCP child. The file is generation-local
    /// and removed with the native session.
    pub(crate) fn create(
        instance_dir: &Path,
        swarm_binary: &Path,
        data_root: &Path,
        environment: &[(OsString, OsString)],
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
        let environment = environment
            .iter()
            .map(|(name, value)| {
                (
                    name.to_string_lossy().into_owned(),
                    serde_json::Value::String(value.to_string_lossy().into_owned()),
                )
            })
            .collect::<serde_json::Map<_, _>>();
        let config = json!({
            "mcpServers": {
                WORKER_SWARM_MCP_SERVER_NAME: {
                    "command": command,
                    "args": worker_swarm_mcp_args(data_dir),
                    "env": environment,
                }
            }
        });
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

impl Drop for ClaudeMcpConfigFile {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}
