//! A short digest of an Agent's patch that focuses an independent review:
//! changed files and packages, new dependencies, public-interface lines,
//! checker and lint configuration changes, and added error suppressions.
//! It is an attention filter from a plain line scan, never a quality score.

use super::{Principal, Router, hierarchy, principal_name};
use crate::git_monitor::{GitProbe, PatchRead};
use eliot_core::{
    Error, Event, Result, SubmitCommand,
    api::{
        AgentPatchDigestRequest, AgentPatchDigestResponse, ClaimListRequest, ClaimView,
        MAX_PATCH_DIGEST_OUTSIDE_CLAIMS, PatchDigestFile, ProjectRegistrationView,
        advisory_claim_matches, is_git_oid,
    },
    limits::MAX_EVENT_BYTES,
};
use serde_json::Value;
use std::{collections::BTreeSet, path::Path};

/// Most entries one digest list carries; `truncated` says when one was cut.
const MAX_ENTRIES: usize = 100;
/// Maximum prompt space reserved for the patch digest and its framing.
const MAX_REVIEW_PATCH_DIGEST_BYTES: usize = 12 * 1024;
/// Longest quoted line in a digest entry.
const MAX_LINE_CHARS: usize = 160;
/// Most bytes of untracked file text one digest scans.
const MAX_UNTRACKED_BYTES: u64 = 1024 * 1024;
/// Largest manifest read to resolve tables.
const MAX_MANIFEST_BYTES: u64 = 1024 * 1024;
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
        let (digest, _) = self
            .build_agent_patch_digest(
                &request.id,
                request.generation,
                None,
                None,
                false,
                principal,
            )
            .await?;
        let mut digest = serde_json::to_value(digest)
            .map_err(|_| Error::new("internal", "patch digest could not be encoded"))?;
        self.secret_policy_snapshot().redact_values(&mut digest);
        Ok(digest)
    }

    /// Attach the existing patch digest to one rule launch, after binding it
    /// to the event's exact Agent generation, workspace, base and fingerprint.
    /// The content is placed only in the review command text, never in the event.
    pub(crate) async fn attach_rule_patch_digest(
        &self,
        rule_principal: &str,
        project: Option<&ProjectRegistrationView>,
        event: &Event,
        request: &mut SubmitCommand,
    ) -> Result<bool> {
        if request.verb != "launch" {
            return Err(Error::new(
                "invalid",
                "patch digest attachment requires a launch action",
            ));
        }
        let principal = match project {
            None if rule_principal == "owner" => Principal::Owner,
            Some(project) if rule_principal == format!("project:{}", project.id) => {
                Principal::ProjectRule {
                    registration: Box::new(project.clone()),
                }
            }
            _ => {
                return Err(Error::new(
                    "not_allowed",
                    "patch digest attachment does not match the rule authority",
                ));
            }
        };
        if let Some(project) = project {
            self.verify_project_rule_submission_authority(std::slice::from_ref(request), project)
                .await?;
        }
        if !matches!(
            event.kind.as_str(),
            "turn.completed" | "project.worktree.check.passed"
        ) || (event.kind == "turn.completed" && event.data["project_check"] != "off")
        {
            return Ok(false);
        }
        let (Some(agent), Some(generation), Some(workspace), Some(base), Some(fingerprint)) = (
            event.agent.as_deref(),
            event.generation,
            event
                .subject
                .as_ref()
                .and_then(|subject| subject.workspace.as_deref()),
            event.data.pointer("/worktree/base").and_then(Value::as_str),
            event
                .data
                .pointer("/worktree/fingerprint")
                .and_then(Value::as_str),
        ) else {
            return Ok(false);
        };
        if !is_git_oid(base) {
            return Ok(false);
        }
        let (digest, observed_fingerprint) = match self
            .build_agent_patch_digest(
                agent,
                Some(generation),
                Some(workspace),
                Some(base),
                true,
                &principal,
            )
            .await
        {
            Ok(snapshot) => snapshot,
            Err(error) if matches!(error.code.as_str(), "stale_generation" | "stale_workspace") => {
                return Ok(false);
            }
            Err(error) => return Err(error),
        };
        if observed_fingerprint.as_deref() != Some(fingerprint)
            || digest.base.as_deref() != Some(base)
        {
            return Ok(false);
        }
        let text = request
            .args
            .get("text")
            .and_then(Value::as_str)
            .ok_or_else(|| Error::new("invalid", "patch digest attachment requires launch text"))?;
        let mut digest = serde_json::to_value(digest)
            .map_err(|_| Error::new("internal", "patch digest prompt could not be encoded"))?;
        self.secret_policy_snapshot().redact_values(&mut digest);
        request.args["text"] =
            Value::String(append_patch_digest_prompt(text, digest, fingerprint)?);
        Ok(true)
    }

    async fn build_agent_patch_digest(
        &self,
        id: &str,
        generation: Option<i64>,
        expected_workspace: Option<&str>,
        expected_base: Option<&str>,
        include_fingerprint: bool,
        principal: &Principal,
    ) -> Result<(AgentPatchDigestResponse, Option<String>)> {
        let (agent, workspace, base) = {
            let store = self.inner.store.lock().await;
            let agent = load_patch_agent(&store, principal, id, generation, include_fingerprint)?;
            if generation.is_some_and(|generation| generation != agent.generation) {
                return Err(Error::new(
                    "stale_generation",
                    "the patch digest requires the current Agent generation",
                ));
            }
            let workspace = agent
                .workspace
                .as_deref()
                .ok_or_else(|| {
                    Error::new("invalid", "the patch digest requires an admitted workspace")
                })?
                .to_owned();
            if expected_workspace.is_some_and(|expected| expected != workspace)
                || expected_base.is_some_and(|expected| agent.base_oid.as_deref() != Some(expected))
            {
                return Err(Error::new(
                    "stale_workspace",
                    "the patch digest Agent workspace or base differs from the event snapshot",
                ));
            }
            let base = match expected_base {
                Some(base) => Some(base.to_owned()),
                None => store
                    .get_workspace_by_path(&workspace)?
                    .map(|row| row.base_oid),
            };
            (agent, workspace, base)
        };
        let probe = GitProbe::new()?;
        let read = if include_fingerprint {
            probe
                .patch_read_with_fingerprint(&workspace, base.as_deref())
                .await?
        } else {
            probe.patch_read(&workspace, base.as_deref()).await?
        };
        let fingerprint = read.fingerprint.clone();
        let (claims, attributed_paths, attributed_truncated, exclusively_bound) = {
            let store = self.inner.store.lock().await;
            let current = load_patch_agent(
                &store,
                principal,
                &agent.id,
                Some(agent.generation),
                include_fingerprint,
            )?;
            if current.generation != agent.generation {
                return Err(Error::new(
                    "stale_generation",
                    "the patch digest requires the current Agent generation",
                ));
            }
            if expected_workspace
                .is_some_and(|expected| current.workspace.as_deref() != Some(expected))
                || expected_base
                    .is_some_and(|expected| current.base_oid.as_deref() != Some(expected))
            {
                return Err(Error::new(
                    "stale_workspace",
                    "the patch digest Agent workspace or base changed during the snapshot",
                ));
            }
            let claims = store
                .claim_list(
                    &principal_name(principal),
                    &ClaimListRequest {
                        project: None,
                        agent: Some(agent.id.clone()),
                    },
                    super::now_ms(),
                )
                .ok()
                .filter(|claims| !claims.truncated)
                .map(|claims| claims.claims);
            let exclusively_bound = !current.state.is_terminal()
                && current.workspace.as_deref().is_some_and(|workspace| {
                    store
                        .workspace_owner_counts()
                        .ok()
                        .and_then(|counts| counts.get(workspace).copied())
                        == Some(1)
                });
            let attributed = if exclusively_bound {
                None
            } else {
                store
                    .list_agent_edits(
                        &agent.id,
                        agent.generation,
                        None,
                        MAX_PATCH_DIGEST_OUTSIDE_CLAIMS + 1,
                    )
                    .ok()
                    .map(|edits| {
                        let mut paths = BTreeSet::new();
                        let mut truncated = edits.len() > MAX_PATCH_DIGEST_OUTSIDE_CLAIMS;
                        for edit in edits.into_iter().take(MAX_PATCH_DIGEST_OUTSIDE_CLAIMS) {
                            if edit.workspace != workspace
                                || edit.path.len() > eliot_core::api::MAX_PATCH_DIGEST_PATH_BYTES
                            {
                                truncated = true;
                                continue;
                            }
                            paths.insert(edit.path);
                        }
                        (paths.into_iter().collect::<Vec<_>>(), truncated)
                    })
            };
            let (attributed_paths, attributed_truncated) =
                attributed.map_or((None, false), |(paths, truncated)| (Some(paths), truncated));
            (
                claims,
                attributed_paths,
                attributed_truncated,
                exclusively_bound,
            )
        };
        let outside_claims_declared = claims.as_ref().map(|claims| !claims.is_empty());
        let root = read.root.clone();
        let digest = tokio::task::spawn_blocking(move || {
            digest(
                &root,
                &read,
                claims.as_deref().filter(|claims| !claims.is_empty()),
                exclusively_bound,
                attributed_paths.as_deref(),
                attributed_truncated,
            )
        })
        .await
        .map_err(|_| Error::new("internal", "patch digest task failed"))?;
        Ok((
            AgentPatchDigestResponse {
                agent: agent.id,
                generation: agent.generation,
                outside_claims_declared,
                ..digest
            },
            fingerprint,
        ))
    }
}

fn load_patch_agent(
    store: &eliot_store::Store,
    principal: &Principal,
    id: &str,
    generation: Option<i64>,
    guarded_review: bool,
) -> Result<eliot_core::Agent> {
    if guarded_review && let Principal::ProjectRule { registration } = principal {
        return store
            .get_agent(id)?
            .filter(|agent| agent.project == registration.id)
            .ok_or_else(|| {
                Error::new(
                    "not_allowed",
                    "review author is outside the registered project",
                )
            });
    }
    hierarchy::authorize_agent_read(store, principal, id, generation)?;
    store
        .get_agent(id)?
        .ok_or_else(|| Error::new("not_found", format!("Agent {id} was not found")))
}

fn append_patch_digest_prompt(text: &str, mut value: Value, fingerprint: &str) -> Result<String> {
    let heading = format!(
        "\n\nBounded `agent.patch_digest` for author snapshot fingerprint {fingerprint}; this is an attention filter, not a quality score:\n"
    );
    let attachment_budget =
        MAX_REVIEW_PATCH_DIGEST_BYTES.min(MAX_EVENT_BYTES.saturating_sub(text.len()));
    loop {
        let encoded = serde_json::to_string(&value)
            .map_err(|_| Error::new("internal", "patch digest prompt could not be encoded"))?
            .replace('`', "\\u0060");
        let fenced = eliot_core::fence_untrusted_text(&encoded);
        let attachment = format!("{heading}{fenced}");
        if attachment.len() <= attachment_budget {
            let mut combined = String::with_capacity(text.len() + attachment.len());
            combined.push_str(text);
            combined.push_str(&attachment);
            return Ok(combined);
        }
        if !drop_digest_prompt_entries(&mut value) {
            return Err(Error::new(
                "frame_too_large",
                "review text leaves no room for the bounded patch digest",
            ));
        }
        value["truncated"] = Value::Bool(true);
        value["prompt_truncated"] = Value::Bool(true);
    }
}

fn drop_digest_prompt_entries(value: &mut Value) -> bool {
    for field in [
        "outside_claims",
        "suppressions",
        "checker_changes",
        "public_interface",
        "new_dependencies",
        "packages",
        "files",
    ] {
        if let Some(entries) = value.get_mut(field).and_then(Value::as_array_mut)
            && !entries.is_empty()
        {
            let remove = (entries.len() / 2).max(1);
            entries.truncate(entries.len() - remove);
            if field == "outside_claims" {
                value["outside_claims_complete"] = Value::Bool(false);
            }
            return true;
        }
    }
    false
}

/// Classify one patch read. File reads only resolve packages and the
/// sections of changed manifests.
fn digest(
    root: &Path,
    read: &PatchRead,
    claims: Option<&[ClaimView]>,
    exclusively_bound: bool,
    attributed_paths: Option<&[String]>,
    attributed_truncated: bool,
) -> AgentPatchDigestResponse {
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
        outside_claims: Vec::new(),
        outside_claims_declared: None,
        outside_claims_complete: false,
        truncated: false,
    };
    let mut files: Vec<PatchDigestFile> = Vec::new();
    let mut current: Option<String> = None;
    let mut new_line = 0_usize;
    let mut in_hunk = false;
    let mut sections: Vec<Option<String>> = Vec::new();
    // Manifest keys a hunk removed: an entry removed and re-added (a version
    // bump, a trailing comma) is not a new dependency.
    let mut removed_keys: BTreeSet<String> = BTreeSet::new();
    for line in read.diff.lines() {
        if let Some(rest) = line.strip_prefix("diff --git ") {
            in_hunk = false;
            let Some(path) = header_path(rest) else {
                current = None;
                continue;
            };
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
            in_hunk = true;
            new_line = hunk
                .split_whitespace()
                .find_map(|part| part.strip_prefix('+'))
                .and_then(|range| range.split(',').next())
                .and_then(|start| start.parse().ok())
                .unwrap_or(0);
        } else if !in_hunk {
            // File header lines (`---`/`+++`, modes, index) before the first hunk.
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
            if let Some(key) = manifest_key(path, removed) {
                removed_keys.insert(format!("{path}: {key}"));
            }
            classify(&mut out, path, '-', removed, None);
        }
    }
    // Untracked files are new in full: their text counts as added lines,
    // within a total read bound; past it the digest says it is incomplete.
    let mut untracked_bytes = 0_u64;
    for path in &read.untracked {
        files.push(PatchDigestFile {
            path: path.clone(),
            change: "untracked".to_owned(),
        });
        match read_text(
            root,
            path,
            MAX_UNTRACKED_BYTES - untracked_bytes.min(MAX_UNTRACKED_BYTES),
        ) {
            Some(Ok(text)) => {
                untracked_bytes += text.len() as u64;
                let sections = manifest_sections(root, path);
                for (index, line) in text.lines().enumerate() {
                    classify(
                        &mut out,
                        path,
                        '+',
                        line,
                        sections.get(index + 1).cloned().flatten(),
                    );
                }
            }
            Some(Err(())) => out.complete = false,
            None => {}
        }
    }
    out.new_dependencies
        .retain(|entry| !removed_keys.contains(entry));
    if let Some(claims) = claims {
        let candidate_paths: BTreeSet<String> = if exclusively_bound {
            files.iter().map(|file| file.path.clone()).collect()
        } else {
            attributed_paths
                .unwrap_or_default()
                .iter()
                .cloned()
                .collect()
        };
        let outside: Vec<String> = candidate_paths
            .into_iter()
            .filter(|path| {
                claims
                    .iter()
                    .all(|claim| !advisory_claim_matches(&claim.glob, path))
            })
            .collect();
        out.outside_claims_complete = exclusively_bound
            && read.complete
            && !attributed_truncated
            && outside.len() <= MAX_PATCH_DIGEST_OUTSIDE_CLAIMS;
        if attributed_truncated || outside.len() > MAX_PATCH_DIGEST_OUTSIDE_CLAIMS {
            out.truncated = true;
        }
        out.outside_claims = outside
            .into_iter()
            .take(MAX_PATCH_DIGEST_OUTSIDE_CLAIMS)
            .collect();
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
        // `[dependencies.serde]` names the dependency in the table header;
        // the keys inside such a table are its fields, not dependencies.
        "Cargo.toml" if text.trim_start().starts_with('[') => text
            .trim()
            .trim_matches(|c| c == '[' || c == ']')
            .split_once("dependencies.")
            .map(|(_, name)| name.trim_matches('"').to_owned()),
        "Cargo.toml"
            if section.as_deref().is_some_and(|name| {
                name == "dependencies"
                    || name.ends_with("-dependencies")
                    || name.ends_with(".dependencies")
            }) =>
        {
            key_of(text)
        }
        "package.json"
            if section
                .as_deref()
                .is_some_and(|name| name.ends_with("ependencies")) =>
        {
            text.trim()
                .strip_prefix('"')
                .and_then(|rest| rest.split_once("\":"))
                .filter(|(_, value)| value.trim().starts_with('"'))
                .map(|(name, _)| name.to_owned())
        }
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
    let name = file_name(path);
    if name != "Cargo.toml" && name != "package.json" {
        return Vec::new();
    }
    let Some(Ok(text)) = read_text(root, path, MAX_MANIFEST_BYTES) else {
        return Vec::new();
    };
    let mut sections = vec![None];
    if name == "Cargo.toml" {
        let mut section = None;
        for line in text.lines() {
            let trimmed = line.trim();
            if trimmed.starts_with('[') {
                section = Some(trimmed.trim_matches(|c| c == '[' || c == ']').to_owned());
            }
            sections.push(section.clone());
        }
        return sections;
    }
    // package.json: the innermost object key, by brace depth.
    let mut stack: Vec<Option<String>> = Vec::new();
    for line in text.lines() {
        sections.push(stack.last().cloned().flatten());
        let trimmed = line.trim();
        let key = trimmed
            .strip_prefix('"')
            .and_then(|rest| rest.split_once("\":"))
            .map(|(key, _)| key.to_owned());
        for c in trimmed.chars() {
            match c {
                '{' => stack.push(key.clone()),
                '}' => {
                    stack.pop();
                }
                _ => {}
            }
        }
    }
    sections
}

/// Text of a regular, non-link file under `root` of at most `max` bytes;
/// `None` when absent or binary, `Some(Err)` when over the bound.
fn read_text(root: &Path, path: &str, max: u64) -> Option<std::result::Result<String, ()>> {
    let full = root.join(path);
    let meta = std::fs::symlink_metadata(&full).ok()?;
    if !meta.is_file() {
        return None;
    }
    if meta.len() > max {
        return Some(Err(()));
    }
    let bytes = std::fs::read(&full).ok()?;
    if bytes.contains(&0) {
        return None;
    }
    String::from_utf8(bytes).ok().map(Ok)
}

/// The path of a `diff --git a/P b/P` header. Without renames both sides
/// name the same path, so an unquoted header splits exactly in the middle
/// even when the path contains spaces or " b/"; a quoted header is
/// C-unquoted.
fn header_path(rest: &str) -> Option<String> {
    if rest.starts_with('"') {
        let (first, tail) = c_unquote(rest)?;
        let (second, _) = c_unquote(tail.trim_start())?;
        let path = second.strip_prefix("b/")?;
        return (first.strip_prefix("a/") == Some(path)).then(|| path.to_owned());
    }
    let half = rest.len().checked_sub(5)? / 2;
    let path = rest.get(2..2 + half)?;
    (rest.starts_with("a/") && rest.get(2 + half..)? == format!(" b/{path}"))
        .then(|| path.to_owned())
}

/// Decode one C-quoted Git path at the start of `text`; returns it and the
/// rest after the closing quote.
fn c_unquote(text: &str) -> Option<(String, &str)> {
    let body = text.strip_prefix('"')?;
    let mut bytes = Vec::new();
    let mut chars = body.char_indices();
    while let Some((index, c)) = chars.next() {
        match c {
            '"' => return Some((String::from_utf8(bytes).ok()?, &body[index + 1..])),
            '\\' => {
                let (_, escaped) = chars.next()?;
                match escaped {
                    'n' => bytes.push(b'\n'),
                    't' => bytes.push(b'\t'),
                    '"' | '\\' => bytes.push(escaped as u8),
                    '0'..='7' => {
                        let mut value = escaped.to_digit(8)?;
                        for _ in 0..2 {
                            value = value * 8 + chars.next()?.1.to_digit(8)?;
                        }
                        bytes.push(u8::try_from(value).ok()?);
                    }
                    _ => return None,
                }
            }
            _ => {
                let mut buffer = [0; 4];
                bytes.extend_from_slice(c.encode_utf8(&mut buffer).as_bytes());
            }
        }
    }
    None
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

/// The entry key of a Cargo or npm manifest line, whatever its table.
fn manifest_key(path: &str, text: &str) -> Option<String> {
    match file_name(path) {
        "Cargo.toml" => key_of(text),
        "package.json" => text
            .trim()
            .strip_prefix('"')
            .and_then(|rest| rest.split_once("\":"))
            .map(|(key, _)| key.to_owned()),
        _ => None,
    }
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
