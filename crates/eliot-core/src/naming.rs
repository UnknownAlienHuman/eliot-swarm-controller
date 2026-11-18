//! One naming standard for managed worktrees (k178).
//!
//! A prepared worktree is named `<project>/<agent>-<task-slug>` and lives at
//! `<data_dir>/worktrees/<project>/<agent>-<task-slug>`:
//!
//! * `project` is the registration's logical name as a slug (a folder slug
//!   for unregistered checkouts);
//! * `agent` is the admitted Agent ID (`aN`), unique per launch;
//! * `task` is an ASCII slug of the launch prompt's first line, bounded.
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

/// Lowercase ASCII slug: every non `[a-z0-9]` run becomes one `-`, trimmed
/// and bounded to `max_len`. Returns [`FALLBACK_SLUG`] when nothing remains.
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

/// The standard branch: `<project>/<agent>-<task>`.
pub fn standard_branch(project: &str, agent: &str, task: &str) -> String {
    format!(
        "{}/{agent}-{task}",
        project_slug(project),
        agent = slug_with_len(agent, MAX_PROJECT_SLUG_LEN),
        task = slug_with_len(task, MAX_TASK_SLUG_LEN),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slugs_are_lowercase_ascii_and_bounded() {
        assert_eq!(project_slug("Atlas"), "atlas");
        assert_eq!(project_slug("my  Cool_Project!"), "my-cool-project");
        assert_eq!(project_slug(""), "task");
        assert_eq!(project_slug("---"), "task");
        assert_eq!(task_slug("Fix login BUG!!!"), "fix-login-bug");
        assert_eq!(task_slug(""), "task");
        assert_eq!(task_slug("   \nsecond"), "task");
    }

    #[test]
    fn task_slug_uses_only_the_first_line_and_stays_bounded() {
        assert_eq!(task_slug("First line here\nsecond line"), "first-line-here");
        let long = "w".repeat(400);
        assert!(task_slug(&long).len() <= MAX_TASK_SLUG_LEN);
        let long_project = "p".repeat(400);
        assert!(project_slug(&long_project).len() <= MAX_PROJECT_SLUG_LEN);
    }

    #[test]
    fn standard_names_follow_project_agent_task() {
        assert_eq!(
            standard_branch("Atlas", "a12", "Fix login bug"),
            "atlas/a12-fix-login-bug"
        );
        assert_eq!(standard_leaf("a12", "Fix login bug"), "a12-fix-login-bug");
    }

    #[test]
    fn standard_names_never_carry_caller_syntax() {
        let branch = standard_branch("App", "a1", "01ARZ3NDEKTSV4RRFFQ69G5FAV");
        assert_eq!(branch, "app/a1-01arz3ndektsv4rrffq69g5fav");
        assert!(branch.is_ascii());
        assert!(!branch.contains(|c: char| c.is_uppercase()));
    }
}
