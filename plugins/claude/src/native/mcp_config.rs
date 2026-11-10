use std::{
    ffi::{OsStr, OsString},
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

use eliot_core::{Error, Result};
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
        let expected_generation = generation.to_string();
        let matches_agent = unique_environment_value(environment, "ELIOT_AGENT_ID")
            .and_then(OsStr::to_str)
            == Some(agent_id);
        let matches_generation = unique_environment_value(environment, "ELIOT_AGENT_GENERATION")
            .and_then(OsStr::to_str)
            == Some(expected_generation.as_str());
        let has_token = matches!(
            unique_environment_value(environment, "ELIOT_AGENT_TOKEN"),
            Some(token) if !token.is_empty()
        );

        if agent_id.trim().is_empty()
            || generation <= 0
            || !matches_agent
            || !matches_generation
            || !has_token
        {
            return Err(Error::new(
                "not_allowed",
                "Claude MCP requires the matching inherited Agent generation identity",
            ));
        }

        unique_environment_value(environment, "ELIOT_HOME")
            .and_then(OsStr::to_str)
            .map(PathBuf::from)
            .filter(|path| path.is_absolute() && path.is_dir())
            .ok_or_else(|| {
                Error::new(
                    "not_allowed",
                    "Claude MCP requires the explicit Eliot data root",
                )
            })
    }

    /// Write only public executable/data-root arguments. Agent authentication
    /// remains in the inherited environment and is never copied into this file.
    pub(crate) fn create(
        instance_dir: &Path,
        swarm_binary: &Path,
        data_root: &Path,
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
        let config = json!({
            "mcpServers": {
                "eliot-swarm": {
                    "command": command,
                    "args": ["--data-dir", data_dir, "mcp", "--require-agent"]
                }
            }
        });
        let contents = serde_json::to_vec(&config)
            .map_err(|_| Error::new("internal", "Claude MCP config could not be encoded"))?;
        let path = instance_dir.join("claude-mcp-config.json");
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .map_err(|_| Error::new("platform_io", "could not create Claude MCP config"))?;
        let guard = Self { path };
        let write_result = file.write_all(&contents).and_then(|()| file.sync_all());
        drop(file);
        if write_result.is_err() {
            drop(guard);
            return Err(Error::new(
                "platform_io",
                "could not write Claude MCP config",
            ));
        }
        Ok(guard)
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

fn unique_environment_value<'a>(
    environment: &'a [(OsString, OsString)],
    name: &str,
) -> Option<&'a OsStr> {
    let mut matches = environment
        .iter()
        .filter(|(key, _)| key.to_string_lossy().eq_ignore_ascii_case(name))
        .map(|(_, value)| value.as_os_str());
    let value = matches.next()?;
    matches.next().is_none().then_some(value)
}
