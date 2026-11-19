//! Bundled `eliot-swarm` Agent skill: one repo source, native per-harness delivery.
//!
//! The canonical text lives in `skills/eliot-swarm/` at the repository root
//! and is embedded here with `include_str!`, so every binary carries the same
//! bytes. The daemon materializes those bytes under the data directory at
//! startup (private ACL, atomic replace); each harness plugin then exposes
//! the materialized files through that harness's own per-session mechanism
//! (plugin directory, skill roots, isolated config) or, where the harness
//! offers none, through the launch-time first-turn system context. No skill
//! file is ever written into a user checkout or a global directory.

use std::path::{Path, PathBuf};

use eliot_core::{Error, Result};

use crate::bounded_file::{BoundedFileError, read_regular_file};

/// Skill directory name, shared by every harness mechanism.
pub const SKILL_NAME: &str = "eliot-swarm";
/// Agent Skills entry file inside one skill directory.
pub const SKILL_FILE: &str = "SKILL.md";
/// The single allowed companion file inside one skill directory.
pub const SKILL_REFERENCE_FILE: &str = "reference.md";
/// Canonical skill body, embedded from the repository source.
pub const SKILL_BODY: &str = include_str!("../../../skills/eliot-swarm/SKILL.md");
/// Canonical skill reference, embedded from the repository source.
pub const SKILL_REFERENCE: &str = include_str!("../../../skills/eliot-swarm/reference.md");
/// Upper bound for reading back a daemon-materialized skill file. The
/// bundled text is a few KiB; anything larger is not this skill.
pub const MAX_SKILL_FILE_BYTES: usize = 64 * 1024;

/// `<data-dir>/skills`: the daemon-materialized root. It doubles as a
/// Claude Code plugin directory (`.claude-plugin/plugin.json` beside
/// `skills/`), so one tree serves every harness without duplication.
pub fn skill_root(data_dir: &Path) -> PathBuf {
    data_dir.join("skills")
}

/// `<data-dir>/skills/skills/eliot-swarm`: the canonical skill directory
/// (Agent Skills layout) that Codex extra roots, OpenCode copies and the
/// Antigravity staging area all read from.
pub fn skill_dir(data_dir: &Path) -> PathBuf {
    skill_root(data_dir).join("skills").join(SKILL_NAME)
}

/// Path of the canonical `SKILL.md` under a data directory.
pub fn skill_body_path(data_dir: &Path) -> PathBuf {
    skill_dir(data_dir).join(SKILL_FILE)
}

/// Path of the canonical `reference.md` under a data directory.
pub fn skill_reference_path(data_dir: &Path) -> PathBuf {
    skill_dir(data_dir).join(SKILL_REFERENCE_FILE)
}

/// Read back one daemon-materialized skill file, bounded and UTF-8. A
/// missing or oversized file is reported; callers skip native delivery
/// rather than failing the session.
pub fn read_skill_file(path: &Path) -> Result<String> {
    let bytes = read_regular_file(path, MAX_SKILL_FILE_BYTES).map_err(|error| match error {
        BoundedFileError::Io(_) => Error::new(
            "unavailable",
            "daemon-materialized skill file is unavailable",
        ),
        BoundedFileError::TooLarge => Error::new(
            "invalid",
            "daemon-materialized skill file exceeds its bound",
        ),
    })?;
    String::from_utf8(bytes)
        .map_err(|_| Error::new("invalid", "daemon-materialized skill file is not UTF-8"))
}
