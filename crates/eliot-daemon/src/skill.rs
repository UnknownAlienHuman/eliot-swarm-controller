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

use crate::platform::data_dir::{ensure_private_root, replace_file_atomically};

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
pub(crate) fn materialize(data_dir: &Path) -> Result<()> {
    let root = skill::skill_root(data_dir);
    ensure_private_root(&root)?;
    let nested = skill::skill_dir(data_dir);
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
    ensure_private_root(&plugin_dir)?;
    let manifest = claude_plugin_manifest()?;
    replace_file_atomically(&plugin_dir, CLAUDE_PLUGIN_FILE, manifest.as_bytes(), true)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use eliot_plugin::skill as shared;
    use std::path::PathBuf;

    fn scratch_root(tag: &str) -> PathBuf {
        std::env::temp_dir().join(format!("eliot-skill-{tag}-{}", ulid::Ulid::new()))
    }

    #[test]
    fn materialize_writes_the_embedded_skill_atomically() {
        let root = scratch_root("write");
        materialize(&root).unwrap();
        assert_eq!(
            std::fs::read_to_string(shared::skill_body_path(&root)).unwrap(),
            shared::SKILL_BODY
        );
        assert_eq!(
            std::fs::read_to_string(shared::skill_reference_path(&root)).unwrap(),
            shared::SKILL_REFERENCE
        );
        // Idempotent: a second startup rewrites the same bytes.
        materialize(&root).unwrap();
        assert_eq!(
            std::fs::read_to_string(shared::skill_body_path(&root)).unwrap(),
            shared::SKILL_BODY
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn claude_manifest_names_the_bundled_skill() {
        let manifest = claude_plugin_manifest().unwrap();
        let value: serde_json::Value = serde_json::from_str(&manifest).unwrap();
        assert_eq!(value["name"], serde_json::json!(skill::SKILL_NAME));
        assert!(value["version"].as_str().is_some_and(|v| !v.is_empty()));
        let skills = value["skills"].as_array().cloned().unwrap_or_default();
        assert!(skills.iter().any(|s| s == "./skills/eliot-swarm"));
    }
}
