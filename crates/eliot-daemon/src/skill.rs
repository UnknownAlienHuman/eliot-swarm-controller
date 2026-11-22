//! Materialize the bundled `eliot-swarm` Agent skill under the data directory.
//!
//! Runs once per daemon startup, after the data-directory lock is held: the
//! canonical skill files plus the Claude Code plugin wrapper are rewritten
//! atomically with current-user-only permissions, so every harness plugin
//! reads the same bytes through its own per-session mechanism. A failure
//! fails startup: a daemon that cannot publish the skill must not serve
//! Agents that depend on it.

use std::path::Path;

use eliot_core::{Error, Result};
use eliot_plugin::skill;

use crate::platform::ForeignDataDirAce;
use crate::platform::data_dir::{
    ensure_private_root, repair_existing_managed_dir_acl, replace_file_atomically,
};

const CLAUDE_PLUGIN_DIR: &str = ".claude-plugin";
const CLAUDE_PLUGIN_FILE: &str = "plugin.json";

/// Claude Code plugin manifest for the materialized skill root. The root
/// doubles as the plugin directory: `skills/eliot-swarm/` beside it is the
/// plugin's skill inventory. Version tracks this daemon build.
fn claude_plugin_manifest() -> Result<String> {
    serde_json::to_string_pretty(&serde_json::json!({
        "name": skill::SKILL_NAME,
        "version": env!("CARGO_PKG_VERSION"),
        "description": "Eliot Swarm coordination: identity, mail, claims, shared variables, children and handoff.",
        "skills": ["./skills/eliot-swarm"],
    }))
    .map_err(|_| Error::new("internal", "skill plugin manifest could not be encoded"))
}

/// Publish the bundled skill files under `data_dir` with a private ACL and
/// atomic replace. Idempotent: every startup rewrites the same bytes.
pub(crate) fn materialize(data_dir: &Path) -> Result<Vec<ForeignDataDirAce>> {
    let root = skill::skill_root(data_dir);
    let mut removed = repair_existing_managed_dir_acl(&root)?;
    ensure_private_root(&root)?;
    let nested = skill::skill_dir(data_dir);
    removed.extend(repair_existing_managed_dir_acl(&nested)?);
    ensure_private_root(&nested)?;
    replace_file_atomically(
        &nested,
        skill::SKILL_FILE,
        skill::SKILL_BODY.as_bytes(),
        true,
    )?;
    replace_file_atomically(
        &nested,
        skill::SKILL_REFERENCE_FILE,
        skill::SKILL_REFERENCE.as_bytes(),
        true,
    )?;
    let plugin_dir = root.join(CLAUDE_PLUGIN_DIR);
    removed.extend(repair_existing_managed_dir_acl(&plugin_dir)?);
    ensure_private_root(&plugin_dir)?;
    let manifest = claude_plugin_manifest()?;
    replace_file_atomically(&plugin_dir, CLAUDE_PLUGIN_FILE, manifest.as_bytes(), true)?;
    Ok(removed)
}
