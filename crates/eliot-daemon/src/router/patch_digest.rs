//! A short digest of an Agent's patch that focuses an independent review:
//! changed files and packages, new dependencies, public-interface lines,
//! checker and lint configuration changes, and added error suppressions.
//! It is an attention filter from a plain line scan, never a quality score.

use super::{Principal, Router, hierarchy};
use crate::git_monitor::{GitProbe, PatchRead};
use eliot_core::{
    Error, Result,
    api::{AgentPatchDigestRequest, AgentPatchDigestResponse, PatchDigestFile},
};
use serde_json::Value;
use std::{collections::BTreeSet, path::Path};

/// Most entries one digest list carries; `truncated` says when one was cut.
const MAX_ENTRIES: usize = 100;
/// Longest quoted line in a digest entry.
const MAX_LINE_CHARS: usize = 160;
/// Package manifests that mark a package directory.
const MANIFESTS: &[&str] = &["Cargo.toml", "package.json", "pyproject.toml", "go.mod"];
/// Checker, lint, toolchain and CI configuration, by file name.
const CHECKER_FILES: &[&str] = &[
    "clippy.toml",
    ".clippy.toml",
    "rustfmt.toml",
    ".rustfmt.toml",
    "rust-toolchain",
    "rust-toolchain.toml",
    "deny.toml",
    ".editorconfig",
    "setup.cfg",
    "tox.ini",
    ".pre-commit-config.yaml",
    ".golangci.yml",
    ".golangci.yaml",
];
/// Checker configuration by file-name prefix (eslint, prettier, tsconfig).
const CHECKER_PREFIXES: &[&str] = &[".eslintrc", "eslint.config.", ".prettierrc", "tsconfig"];
/// Markers of an added error or lint suppression.
const SUPPRESSIONS: &[&str] = &[
    "#[allow(",
    "#![allow(",
    "#[expect(",
    "#![expect(",
    "eslint-disable",
    "@ts-ignore",
    "@ts-expect-error",
    "# noqa",
    "# type: ignore",
    "//nolint",
    "@SuppressWarnings",
];

impl Router {
    pub(super) async fn agent_patch_digest(
        &self,
        params: &Value,
        principal: &Principal,
    ) -> Result<Value> {
        let request: AgentPatchDigestRequest =
            eliot_core::api::discovery::decode_params(params.clone())?;
        request.validate()?;
        let (agent, base) = {
            let store = self.inner.store.lock().await;
            hierarchy::authorize_agent_read(&store, principal, &request.id, request.generation)?;
            let agent = store.get_agent(&request.id)?.ok_or_else(|| {
                Error::new("not_found", format!("Agent {} was not found", request.id))
            })?;
            let base = match agent.workspace.as_deref() {
                Some(workspace) => store
                    .get_workspace_by_path(workspace)?
                    .map(|row| row.base_oid),
                None => None,
            };
            (agent, base)
        };
        if request
            .generation
            .is_some_and(|generation| generation != agent.generation)
        {
            return Err(Error::new(
                "stale_generation",
                "the patch digest requires the current Agent generation",
            ));
        }
        let workspace = agent.workspace.as_deref().ok_or_else(|| {
            Error::new("invalid", "the patch digest requires an admitted workspace")
        })?;
        let read = GitProbe::new()?
            .patch_read(workspace, base.as_deref())
            .await?;
        let root = read.root.clone();
        let digest = tokio::task::spawn_blocking(move || digest(&root, &read))
            .await
            .map_err(|_| Error::new("internal", "patch digest task failed"))?;
        serde_json::to_value(AgentPatchDigestResponse {
            agent: agent.id,
            generation: agent.generation,
            ..digest
        })
        .map_err(|_| Error::new("internal", "patch digest could not be encoded"))
    }
}

/// Classify one patch read. File reads only resolve packages and the
/// sections of changed manifests.
fn digest(root: &Path, read: &PatchRead) -> AgentPatchDigestResponse {
    let mut out = AgentPatchDigestResponse {
        agent: String::new(),
        generation: 0,
        base: read.base.clone(),
        head: read.head.clone(),
        complete: read.complete,
        files: Vec::new(),
        packages: Vec::new(),
        new_dependencies: Vec::new(),
        public_interface: Vec::new(),
        checker_changes: Vec::new(),
        suppressions: Vec::new(),
        truncated: false,
    };
    let mut files: Vec<PatchDigestFile> = Vec::new();
    let mut current: Option<String> = None;
    let mut new_line = 0_usize;
    let mut sections: Vec<Option<String>> = Vec::new();
    for line in read.diff.lines() {
        if let Some(rest) = line.strip_prefix("diff --git ") {
            let path = rest
                .rsplit_once(" b/")
                .map(|(_, path)| unquote(path))
                .unwrap_or_else(|| unquote(rest));
            sections = manifest_sections(root, &path);
            files.push(PatchDigestFile {
                path: path.clone(),
                change: "modified".to_owned(),
            });
            current = Some(path);
            continue;
        }
        let Some(path) = current.as_deref() else {
            continue;
        };
        if line.starts_with("new file mode") {
            if let Some(file) = files.last_mut() {
                file.change = "added".to_owned();
            }
        } else if line.starts_with("deleted file mode") {
            if let Some(file) = files.last_mut() {
                file.change = "deleted".to_owned();
            }
        } else if let Some(hunk) = line.strip_prefix("@@ ") {
            new_line = hunk
                .split_whitespace()
                .find_map(|part| part.strip_prefix('+'))
                .and_then(|range| range.split(',').next())
                .and_then(|start| start.parse().ok())
                .unwrap_or(0);
        } else if line.starts_with("+++") || line.starts_with("---") {
        } else if let Some(added) = line.strip_prefix('+') {
            classify(
                &mut out,
                path,
                '+',
                added,
                sections.get(new_line).cloned().flatten(),
            );
            new_line += 1;
        } else if let Some(removed) = line.strip_prefix('-') {
            classify(&mut out, path, '-', removed, None);
        }
    }
    for path in &read.untracked {
        files.push(PatchDigestFile {
            path: path.clone(),
            change: "untracked".to_owned(),
        });
    }
    let mut packages = BTreeSet::new();
    for file in &files {
        packages.insert(package_of(root, &file.path));
        if is_checker_file(&file.path) {
            push(
                &mut out.checker_changes,
                &mut out.truncated,
                file.path.clone(),
            );
        }
    }
    out.packages = packages.into_iter().collect();
    out.truncated |= files.len() > MAX_ENTRIES || out.packages.len() > MAX_ENTRIES;
    files.truncate(MAX_ENTRIES);
    out.packages.truncate(MAX_ENTRIES);
    out.files = files;
    out
}

/// Record one changed line in the lists it belongs to.
fn classify(
    out: &mut AgentPatchDigestResponse,
    path: &str,
    sign: char,
    text: &str,
    section: Option<String>,
) {
    let entry = || format!("{sign} {path}: {}", clip(text.trim()));
    if sign == '+' && SUPPRESSIONS.iter().any(|marker| text.contains(marker)) {
        push(&mut out.suppressions, &mut out.truncated, entry());
    }
    if is_public_item(path, text) {
        push(&mut out.public_interface, &mut out.truncated, entry());
    }
    if sign != '+' {
        return;
    }
    let file = file_name(path);
    let dependency = match file {
        "Cargo.toml"
            if section
                .as_deref()
                .is_some_and(|name| name.contains("dependencies")) =>
        {
            key_of(text)
        }
        "package.json" => text
            .trim()
            .strip_prefix('"')
            .and_then(|rest| rest.split_once("\":"))
            .filter(|(_, value)| value.trim().starts_with('"'))
            .map(|(name, _)| name.to_owned()),
        "Cargo.lock" => text
            .trim()
            .strip_prefix("name = \"")
            .and_then(|rest| rest.strip_suffix('"'))
            .map(ToOwned::to_owned),
        _ => None,
    };
    if let Some(name) = dependency {
        push(
            &mut out.new_dependencies,
            &mut out.truncated,
            format!("{path}: {name}"),
        );
    }
    if file == "Cargo.toml" && text.trim().starts_with("[lints") {
        push(
            &mut out.checker_changes,
            &mut out.truncated,
            format!("{path}: {}", clip(text.trim())),
        );
    }
}

/// A line declaring a public Rust, TypeScript/JavaScript or Go item.
fn is_public_item(path: &str, text: &str) -> bool {
    let line = text.trim_start();
    if path.ends_with(".rs") {
        return line.strip_prefix("pub ").is_some_and(|rest| {
            [
                "fn ",
                "async fn ",
                "struct ",
                "enum ",
                "trait ",
                "mod ",
                "const ",
                "static ",
                "type ",
                "use ",
                "unsafe fn ",
            ]
            .iter()
            .any(|item| rest.starts_with(item))
        });
    }
    if [".ts", ".tsx", ".js", ".mjs"]
        .iter()
        .any(|ext| path.ends_with(ext))
    {
        return line.starts_with("export ");
    }
    if path.ends_with(".go") {
        return ["func ", "type "].iter().any(|item| {
            text.strip_prefix(item)
                .and_then(|rest| rest.trim_start_matches('(').chars().next())
                .is_some_and(char::is_uppercase)
        });
    }
    false
}

fn is_checker_file(path: &str) -> bool {
    let name = file_name(path);
    path.starts_with(".github/workflows/")
        || path == ".cargo/config.toml"
        || CHECKER_FILES.contains(&name)
        || CHECKER_PREFIXES
            .iter()
            .any(|prefix| name.starts_with(prefix))
}

/// For a changed Cargo manifest, the TOML table each current line sits in
/// (index = 1-based line number); empty for any other file.
fn manifest_sections(root: &Path, path: &str) -> Vec<Option<String>> {
    if file_name(path) != "Cargo.toml" {
        return Vec::new();
    }
    let Ok(text) = std::fs::read_to_string(root.join(path)) else {
        return Vec::new();
    };
    let mut sections = vec![None];
    let mut section = None;
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with('[') {
            section = Some(trimmed.trim_matches(|c| c == '[' || c == ']').to_owned());
        }
        sections.push(section.clone());
    }
    sections
}

/// The nearest directory holding a package manifest, or `.` for the root.
fn package_of(root: &Path, path: &str) -> String {
    let mut parts: Vec<&str> = path.split('/').collect();
    parts.pop();
    while !parts.is_empty() {
        let dir = parts.join("/");
        if MANIFESTS
            .iter()
            .any(|manifest| root.join(&dir).join(manifest).is_file())
        {
            return dir;
        }
        parts.pop();
    }
    ".".to_owned()
}

fn key_of(text: &str) -> Option<String> {
    let (key, _) = text.split_once('=')?;
    let key = key.trim().trim_matches('"');
    (!key.is_empty()
        && key
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.')))
    .then(|| key.to_owned())
}

fn file_name(path: &str) -> &str {
    path.rsplit('/').next().unwrap_or(path)
}

fn unquote(path: &str) -> String {
    path.trim_matches('"').to_owned()
}

fn clip(text: &str) -> String {
    if text.chars().count() <= MAX_LINE_CHARS {
        return text.to_owned();
    }
    let mut clipped: String = text.chars().take(MAX_LINE_CHARS).collect();
    clipped.push('…');
    clipped
}

fn push(list: &mut Vec<String>, truncated: &mut bool, entry: String) {
    if list.len() < MAX_ENTRIES {
        list.push(entry);
    } else {
        *truncated = true;
    }
}
