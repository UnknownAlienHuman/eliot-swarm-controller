use std::{
    fs,
    io::ErrorKind,
    path::{Path, PathBuf},
};

use eliot_core::{Error, Result};
use eliot_plugin::native_cli::ANTIGRAVITY_MCP_CONFIG_FILE;

use crate::{profile::Plan, session::without_verbatim_prefix};

pub(crate) struct OwnedConfig(PathBuf);

impl Drop for OwnedConfig {
    fn drop(&mut self) {
        let _ = fs::remove_file(self.0.join("config").join(ANTIGRAVITY_MCP_CONFIG_FILE));
        let _ = fs::remove_dir(self.0.join("config"));
        // Native conversations/logs survive for exact session/load on resume.
        let _ = fs::remove_dir(&self.0);
    }
}

/// Install the daemon's prepared file in the native `--gemini-dir` layout.
/// The plugin owns its lifetime through native exit.
pub(crate) fn install(plan: &Plan, instance: &Path) -> Result<Option<OwnedConfig>> {
    let Some(root) = &plan.mcp_root else {
        return Ok(None);
    };
    let instance = without_verbatim_prefix(&fs::canonicalize(instance).map_err(|_| {
        Error::new(
            "invalid",
            "Antigravity private instance directory is unavailable",
        )
    })?);
    let workspace = without_verbatim_prefix(
        &fs::canonicalize(&plan.launch.cwd)
            .map_err(|_| Error::new("profile_error", "Antigravity workspace is unavailable"))?,
    );
    if root.parent() != instance.parent()
        || root.starts_with(&workspace)
        || workspace.starts_with(root)
    {
        return Err(Error::new(
            "invalid",
            "Antigravity MCP root is outside its private parent",
        ));
    }
    match fs::create_dir(root) {
        Ok(()) => (),
        Err(error) if error.kind() == ErrorKind::AlreadyExists => {
            check_root(root)?;
            if plan.resume().is_none() {
                // This fresh launch owns the Agent's private slot and has no
                // retained session to load. Never follow a redirected root.
                fs::remove_dir_all(root).map_err(|_| {
                    Error::new("io_error", "Could not clear stale Antigravity MCP root")
                })?;
                fs::create_dir(root).map_err(|_| {
                    Error::new("io_error", "Could not recreate owned Antigravity MCP root")
                })?;
            }
        }
        Err(_) => {
            return Err(Error::new(
                "io_error",
                "Could not create owned Antigravity MCP root",
            ));
        }
    }
    check_root(root)?;
    let config_dir = root.join("config");
    fs::create_dir_all(&config_dir).map_err(|_| {
        Error::new(
            "io_error",
            "Could not create owned Antigravity MCP config directory",
        )
    })?;
    if fs::canonicalize(&config_dir)
        .ok()
        .map(|path| without_verbatim_prefix(&path))
        != Some(without_verbatim_prefix(&config_dir))
    {
        return Err(Error::new(
            "invalid",
            "Antigravity MCP config directory was redirected",
        ));
    }
    fs::rename(
        instance.join(ANTIGRAVITY_MCP_CONFIG_FILE),
        config_dir.join(ANTIGRAVITY_MCP_CONFIG_FILE),
    )
    .inspect_err(|_| {
        let _ = fs::remove_dir(&config_dir);
        let _ = fs::remove_dir(root);
    })
    .map_err(|_| {
        Error::new(
            "io_error",
            "Could not install the prepared Antigravity MCP config",
        )
    })?;
    Ok(Some(OwnedConfig(root.clone())))
}

fn check_root(root: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(root)
        .map_err(|_| Error::new("invalid", "Antigravity MCP root is unavailable"))?;
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x400;
        if metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
            return Err(Error::new("invalid", "Antigravity MCP root was redirected"));
        }
    }
    if !metadata.is_dir()
        || fs::canonicalize(root)
            .ok()
            .map(|path| without_verbatim_prefix(&path))
            != Some(without_verbatim_prefix(root))
    {
        return Err(Error::new("invalid", "Antigravity MCP root was redirected"));
    }
    Ok(())
}
