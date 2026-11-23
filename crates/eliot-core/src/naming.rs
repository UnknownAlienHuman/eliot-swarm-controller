//! One naming standard for managed worktrees (k178).
//!
//! A prepared worktree uses branch `<project>/<agent>/work` and lives at
//! `<data_dir>/worktrees/<project>/<agent>-<task-slug>`:
//!
//! * `project` is the registration's logical name as a slug (a folder slug
//!   for unregistered checkouts);
//! * `agent` is the admitted Agent ID (`aN`), unique per launch;
//! * `task` is an ASCII slug of the launch prompt's first line, bounded; it
//!   keeps worktree paths readable but does not name the branch.
//!
//! An explicitly requested branch name always wins over the standard. The
//! standard is never derived from caller command-id syntax: uniqueness comes
//! from the admitted Agent ID, which no caller supplies.

/// Maximum slug length for the project component.
pub const MAX_PROJECT_SLUG_LEN: usize = 64;
/// Maximum slug length for the task component.
pub const MAX_TASK_SLUG_LEN: usize = 48;
/// Fallback slug when the source text carries no usable characters.
pub const FALLBACK_SLUG: &str = "task";

/// Whether a slug would be an unusable path component on Windows: a
/// reserved device name (`con`, `prn`, `aux`, `nul`, `com1`-`com9`,
/// `lpt1`-`lpt9`), matched case-insensitively on the whole slug. Slugs
/// carry no dots or colons, so the stem is the whole value.
pub fn is_windows_device_slug(slug: &str) -> bool {
    let upper = slug.to_ascii_uppercase();
    matches!(upper.as_str(), "CON" | "PRN" | "AUX" | "NUL")
        || (upper.len() == 4
            && (upper.starts_with("COM") || upper.starts_with("LPT"))
            && upper.as_bytes()[3].is_ascii_digit())
}

/// Lowercase ASCII slug: every non `[a-z0-9]` run becomes one `-`, trimmed
/// and bounded to `max_len`. Returns [`FALLBACK_SLUG`] when nothing remains.
/// A slug that would be a Windows device name gains an `-x` suffix so the
/// generated worktree directory stays launchable.
fn slug_with_len(value: &str, max_len: usize) -> String {
    let mut slug = String::new();
    let mut dash = false;
    for byte in value.bytes().map(|byte| byte.to_ascii_lowercase()) {
        if byte.is_ascii_alphanumeric() {
            if slug.len() >= max_len {
                break;
            }
            slug.push(byte as char);
            dash = false;
        } else if !slug.is_empty() && !dash {
            if slug.len() >= max_len {
                break;
            }
            slug.push('-');
            dash = true;
        }
    }
    while slug.ends_with('-') {
        slug.pop();
    }
    if slug.is_empty() {
        slug.push_str(FALLBACK_SLUG);
    }
    if is_windows_device_slug(&slug) {
        if slug.len() + 2 > max_len {
            slug.truncate(max_len - 2);
            while slug.ends_with('-') {
                slug.pop();
            }
        }
        slug.push_str("-x");
    }
    slug
}

/// The project component of the standard: the registration's logical name
/// as a slug.
pub fn project_slug(name: &str) -> String {
    slug_with_len(name, MAX_PROJECT_SLUG_LEN)
}

/// The task component of the standard: an ASCII slug of the launch prompt's
/// first line, bounded. Empty or unusable prompts read `task`.
pub fn task_slug(prompt: &str) -> String {
    let first = prompt.lines().next().unwrap_or_default();
    let first: String = first.chars().take(200).collect();
    slug_with_len(first.trim(), MAX_TASK_SLUG_LEN)
}

/// The standard branch: `<project>/<agent>/work`.
pub fn standard_branch(project: &str, agent: &str) -> String {
    format!(
        "{}/{}/work",
        project_slug(project),
        slug_with_len(agent, MAX_PROJECT_SLUG_LEN),
    )
}

/// The standard worktree leaf: `<agent>-<task>`.
pub fn standard_leaf(agent: &str, task: &str) -> String {
    format!(
        "{agent}-{task}",
        agent = slug_with_len(agent, MAX_PROJECT_SLUG_LEN),
        task = slug_with_len(task, MAX_TASK_SLUG_LEN),
    )
}
