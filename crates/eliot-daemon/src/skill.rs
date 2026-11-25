//! Materialize the bundled `eliot-swarm` Agent skill under the data directory.
//!
//! Runs once per daemon startup, after the data-directory lock is held: the
//! canonical skill files and declared native wrappers are rewritten
//! atomically with current-user-only permissions, so every harness plugin
//! reads the same bytes through its own per-session mechanism. A failure
//! fails startup: a daemon that cannot publish the skill must not serve
//! Agents that depend on it.

use std::path::Path;

use eliot_core::Result;
use eliot_plugin::skill;

use crate::platform::ForeignDataDirAce;
use crate::platform::data_dir::{
    ensure_private_root, repair_existing_managed_dir_acl, replace_file_atomically,
};

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
    for wrapper in eliot_core::api::HarnessCapabilities::declarations()
        .iter()
        .filter_map(|(_, caps)| caps.skill_wrapper)
    {
        let directory = root.join(wrapper.directory);
        removed.extend(repair_existing_managed_dir_acl(&directory)?);
        ensure_private_root(&directory)?;
        let manifest = (wrapper.manifest)()?;
        replace_file_atomically(&directory, wrapper.filename, manifest.as_bytes(), true)?;
    }
    Ok(removed)
}
