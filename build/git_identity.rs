//! Emit a commit identifier for the CLI and daemon crates.
//!
//! Git is queried at compile time. The worktree's actual HEAD and ref files
//! are registered with Cargo so moving a branch invalidates the embedded id,
//! including when Git resolves the worktree through an ancestor marker.

use std::{
    env, fs,
    path::{Path, PathBuf},
    process::{Command, Output},
};

pub fn emit() {
    let manifest_dir = env::var_os("CARGO_MANIFEST_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| env::current_dir().unwrap_or_default());
    let workspace_root = find_workspace_root(&manifest_dir).unwrap_or(manifest_dir);
    let git_sha = read_git_sha(&workspace_root);
    let git_dirty = git_sha
        .as_ref()
        .and_then(|_| read_git_dirty(&workspace_root));
    let package_version = env::var("CARGO_PKG_VERSION").unwrap_or_else(|_| "unknown".to_owned());
    let display_version = match git_sha.as_deref() {
        Some(sha) => {
            let dirty_marker = match git_dirty {
                Some(false) => "",
                Some(true) | None => "-dirty",
            };
            format!("{package_version}+g{}{dirty_marker}", &sha[..8])
        }
        None => format!("{package_version}+git-unknown"),
    };

    // An empty value keeps a host environment variable from supplying a
    // plausible but unrelated revision when building a source archive.
    println!(
        "cargo:rustc-env=ELIOT_BUILD_GIT_SHA={}",
        git_sha.as_deref().unwrap_or_default()
    );
    println!("cargo:rustc-env=ELIOT_BUILD_VERSION={display_version}");
}

fn find_workspace_root(manifest_dir: &Path) -> Option<PathBuf> {
    manifest_dir.ancestors().find_map(|directory| {
        let manifest = fs::read_to_string(directory.join("Cargo.toml")).ok()?;
        manifest
            .lines()
            .any(|line| line.trim() == "[workspace]")
            .then(|| directory.to_path_buf())
    })
}

fn read_git_sha(workspace_root: &Path) -> Option<String> {
    watch_git_markers(workspace_root);
    let top = git_output(workspace_root, &["rev-parse", "--show-toplevel"])?;
    let git_root = PathBuf::from(output_text(&top)?);
    if !same_directory(workspace_root, &git_root) {
        // A source archive nested in some unrelated checkout must not inherit
        // the enclosing repository's commit id.
        return None;
    }

    watch_git_inputs(workspace_root);
    watch_worktree_inputs(workspace_root);

    let output = git_output(workspace_root, &["rev-parse", "--verify", "HEAD"])?;
    let sha = output_text(&output)?;
    valid_sha(&sha).then(|| sha.to_ascii_lowercase())
}

fn watch_git_inputs(workspace_root: &Path) {
    let Some(git_dir) = git_output(workspace_root, &["rev-parse", "--absolute-git-dir"])
        .and_then(|output| output_text(&output))
        .map(PathBuf::from)
    else {
        return;
    };
    watch(&git_dir.join("HEAD"));
    watch(&git_dir.join("commondir"));
    if let Some(path) = git_output(workspace_root, &["rev-parse", "--git-path", "index"])
        .and_then(|output| output_text(&output))
    {
        watch(&absolute_from(workspace_root, Path::new(&path)));
    }

    let common_dir = git_output(workspace_root, &["rev-parse", "--git-common-dir"])
        .and_then(|output| output_text(&output))
        .map(|path| absolute_from(workspace_root, Path::new(&path)))
        .unwrap_or_else(|| git_dir.clone());
    watch(&common_dir.join("packed-refs"));

    if let Some(reference) = git_output(workspace_root, &["symbolic-ref", "--quiet", "HEAD"])
        .and_then(|output| output_text(&output))
        .filter(|reference| reference.starts_with("refs/"))
        && let Some(path) = git_output(workspace_root, &["rev-parse", "--git-path", &reference])
            .and_then(|output| output_text(&output))
    {
        let path = absolute_from(workspace_root, Path::new(&path));
        watch(&path);
        if !path.exists()
            && let Some(parent) = path.parent()
        {
            watch(parent);
        }
    }
}

fn read_git_dirty(workspace_root: &Path) -> Option<bool> {
    let output = git_output(
        workspace_root,
        &["status", "--porcelain=v1", "--untracked-files=no"],
    )?;
    Some(!output.stdout.is_empty())
}

fn watch_worktree_inputs(workspace_root: &Path) {
    let Some(files) = git_output(workspace_root, &["ls-files", "-z"]) else {
        return;
    };
    for relative in nul_paths(&files) {
        watch(&workspace_root.join(relative));
    }
}

fn nul_paths(output: &Output) -> Vec<PathBuf> {
    output
        .stdout
        .split(|byte| *byte == 0)
        .filter(|path| !path.is_empty())
        .filter_map(|path| std::str::from_utf8(path).ok().map(PathBuf::from))
        .collect()
}

fn watch_git_markers(workspace_root: &Path) {
    // Some managed worktrees keep their `.git` pointer outside the Cargo
    // workspace. Track existing ancestor markers too, without assuming a
    // particular worktree layout. The workspace marker is tracked when
    // missing so a later checkout of a source archive invalidates the id.
    for directory in workspace_root.ancestors() {
        let marker = directory.join(".git");
        if marker.is_file() || (directory == workspace_root && !marker.exists()) {
            watch(&marker);
        }
    }
}

fn git_output(directory: &Path, arguments: &[&str]) -> Option<Output> {
    let output = Command::new("git")
        .args(arguments)
        // Avoid Git's optional index refresh writes while calculating identity.
        .env("GIT_OPTIONAL_LOCKS", "0")
        .current_dir(directory)
        .output()
        .ok()?;
    output.status.success().then_some(output)
}

fn output_text(output: &Output) -> Option<String> {
    let text = std::str::from_utf8(&output.stdout).ok()?.trim();
    (!text.is_empty()).then(|| text.to_owned())
}

fn absolute_from(base: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        base.join(path)
    }
}

fn watch(path: &Path) {
    println!("cargo:rerun-if-changed={}", path.display());
}

fn same_directory(left: &Path, right: &Path) -> bool {
    let left = fs::canonicalize(left).unwrap_or_else(|_| left.to_path_buf());
    let right = fs::canonicalize(right).unwrap_or_else(|_| right.to_path_buf());
    #[cfg(windows)]
    {
        left.to_string_lossy()
            .eq_ignore_ascii_case(&right.to_string_lossy())
    }
    #[cfg(not(windows))]
    {
        left == right
    }
}

fn valid_sha(sha: &str) -> bool {
    matches!(sha.len(), 40 | 64) && sha.bytes().all(|byte| byte.is_ascii_hexdigit())
}
