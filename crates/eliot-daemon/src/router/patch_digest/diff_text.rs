use super::load_patch_agent;
use crate::router::{Principal, Router};
use crate::{git_monitor::GitProbe, platform::script_file::Directory, workspace_git::DiffCheckout};
use eliot_core::{
    Error, Result, SecretPolicy,
    api::{
        AgentDiffRequest, AgentDiffResponse, GitDiffFile, GitDiffSource, GitView,
        MAX_DIFF_TEXT_BYTES, MAX_DIFF_TEXT_FILES, MAX_DIFF_TEXT_HUNKS, discovery::decode_params,
        valid_diff_path,
    },
};
use eliot_plugin::edit_path::CanonicalEditRoot;
use serde_json::Value;
use std::{collections::BTreeSet, fmt::Write as _, io::Read, path::Path};

impl Router {
    pub(in crate::router) async fn agent_diff(
        &self,
        params: &Value,
        principal: &Principal,
    ) -> Result<Value> {
        let request: AgentDiffRequest = decode_params(params.clone())?;
        request.validate()?;
        let (agent, workspace, prepared, turn) = {
            let store = self.inner.store.lock().await;
            let agent =
                load_patch_agent(&store, principal, &request.id, request.generation, false)?;
            if request
                .generation
                .is_some_and(|generation| generation != agent.generation)
            {
                return Err(Error::new(
                    "stale_generation",
                    "diff requires the current Agent generation",
                ));
            }
            let workspace = agent.workspace.clone().ok_or_else(|| {
                Error::new("unavailable", "diff requires an admitted Git workspace")
            })?;
            let prepared = store.get_workspace_by_path(&workspace)?;
            let turn = if request.source == GitDiffSource::LastTurn {
                Some(
                    store
                        .latest_diff_turn(&agent.id, agent.generation)?
                        .ok_or_else(|| {
                            Error::new(
                                "not_found",
                                "no completed turn is available for this generation",
                            )
                        })?,
                )
            } else {
                None
            };
            (agent, workspace, prepared, turn)
        };
        let git = self.inner.monitoring.git_view(&agent).await.ok();
        let (paths, turn_id, turn_partial) = match turn {
            Some((turn, paths, mut partial)) => {
                let root = CanonicalEditRoot::new(&workspace)?;
                let paths = paths
                    .into_iter()
                    .filter_map(|path| match root.normalize(&path) {
                        Ok(path) if valid_diff_path(&path) => Some(path),
                        _ => {
                            partial = true;
                            None
                        }
                    })
                    .collect();
                (Some(paths), Some(turn), partial)
            }
            None => (None, None, false),
        };
        let expected_common = prepared.as_ref().map(|row| row.repo_common_dir.as_str());
        let checkout = DiffCheckout::open(&workspace, expected_common)?;
        let head = checkout.head()?;
        let base = match request.source {
            GitDiffSource::Uncommitted => head.clone(),
            _ => prepared
                .as_ref()
                .map_or_else(|| head.clone(), |row| row.base_oid.clone()),
        };
        if !eliot_core::api::is_git_oid(&base) {
            return Err(Error::new(
                "unavailable",
                "the workspace diff base is invalid",
            ));
        }
        let policy = self.secret_policy_snapshot();
        let mut diff = PatchBuilder {
            files: Vec::new(),
            bytes: MAX_DIFF_TEXT_BYTES,
            hunks: MAX_DIFF_TEXT_HUNKS,
            truncated: turn_partial,
            selection: request.path.as_deref(),
            turn_paths: paths,
            policy: policy.as_ref(),
        };
        if diff
            .turn_paths
            .as_ref()
            .is_none_or(|paths| !paths.is_empty())
        {
            let mut pathspec = Vec::new();
            if let Some(path) = request.path.as_deref() {
                pathspec.push(path);
                if let Some(GitView::Data(observation)) = git.as_ref()
                    && observation.head.as_deref() == Some(&head)
                    && let Some(entries) = observation.dirty.as_ref()
                    && let Some(companion) = entries.iter().find_map(|entry| {
                        let original = entry.original_path.as_deref()?;
                        if entry.path == path {
                            Some(original)
                        } else if original == path {
                            Some(entry.path.as_str())
                        } else {
                            None
                        }
                    })
                    && companion != path
                    && valid_diff_path(companion)
                {
                    pathspec.push(companion);
                }
            } else if let Some(paths) = &diff.turn_paths {
                diff.truncated |= paths.len() > MAX_DIFF_TEXT_FILES;
                pathspec.extend(paths.iter().take(MAX_DIFF_TEXT_FILES).map(String::as_str));
            }
            let (bytes, truncated) = GitProbe::new()?
                .diff_output(&checkout, &base, &pathspec)
                .await?;
            diff.read_tracked(&bytes, truncated)?;
            if let Some(GitView::Data(git)) = git
                && git.available
                && git.complete == Some(true)
                && git.head.as_deref() == Some(&head)
            {
                for entry in git.dirty.unwrap_or_default() {
                    if entry.status == "??"
                        && diff.matches(&entry.path, None)
                        && !diff.files.iter().any(|file| file.path == entry.path)
                    {
                        diff.read_untracked(&checkout.root.path, &entry.path);
                    }
                }
            } else if diff.needs_untracked_snapshot() {
                diff.truncated = true;
            }
        }
        let current_checkout = DiffCheckout::open(&workspace, expected_common)?;
        if !checkout.same_identity(&current_checkout) || current_checkout.head()? != head {
            return Err(Error::new(
                "stale_workspace",
                "Git checkout or HEAD changed during diff; refresh",
            ));
        }
        {
            let store = self.inner.store.lock().await;
            let current =
                load_patch_agent(&store, principal, &agent.id, Some(agent.generation), false)?;
            if current.generation != agent.generation
                || current.workspace != agent.workspace
                || store.get_workspace_by_path(&workspace)? != prepared
            {
                return Err(Error::new(
                    "stale_workspace",
                    "Agent workspace changed during diff; refresh",
                ));
            }
        }
        let response = AgentDiffResponse {
            agent: agent.id,
            generation: agent.generation,
            source: request.source,
            path: request.path.clone(),
            base,
            head,
            turn_id,
            files: diff.files,
            truncated: diff.truncated,
        };
        request.validate_response(&response)?;
        serde_json::to_value(response)
            .map_err(|_| Error::new("internal", "diff could not be encoded"))
    }
}

struct PatchBuilder<'a> {
    files: Vec<GitDiffFile>,
    bytes: usize,
    hunks: usize,
    truncated: bool,
    selection: Option<&'a str>,
    turn_paths: Option<BTreeSet<String>>,
    policy: &'a SecretPolicy,
}

impl PatchBuilder<'_> {
    fn needs_untracked_snapshot(&self) -> bool {
        let missing = |path: &str| {
            !self
                .files
                .iter()
                .any(|file| file.path == path || file.original_path.as_deref() == Some(path))
        };
        if let Some(path) = self.selection {
            return missing(path);
        }
        self.turn_paths
            .as_ref()
            .is_none_or(|paths| paths.iter().any(|path| missing(path)))
    }

    fn matches(&self, path: &str, original: Option<&str>) -> bool {
        self.selection
            .is_none_or(|chosen| chosen == path || original == Some(chosen))
            && self.turn_paths.as_ref().is_none_or(|paths| {
                paths.contains(path) || original.is_some_and(|path| paths.contains(path))
            })
    }

    fn read_tracked(&mut self, bytes: &[u8], truncated: bool) -> Result<()> {
        self.truncated |= truncated;
        if bytes.is_empty() {
            return Ok(());
        }
        let invalid = || Error::new("unavailable", "Git diff returned inconsistent file records");
        let Some(split) = bytes.windows(2).position(|pair| pair == [0, 0]) else {
            return if truncated { Ok(()) } else { Err(invalid()) };
        };
        let mut raw = bytes[..split].split(|byte| *byte == 0);
        let patch = String::from_utf8_lossy(&bytes[split + 2..]);
        let mut starts = patch
            .match_indices("diff --git ")
            .filter_map(|(at, _)| (at == 0 || patch.as_bytes()[at - 1] == b'\n').then_some(at))
            .peekable();
        while let Some(metadata) = raw.next() {
            let mut fields = std::str::from_utf8(metadata)
                .map_err(|_| invalid())?
                .split_ascii_whitespace();
            let (Some(old), Some(new), Some(status)) =
                (fields.next(), fields.next(), fields.nth(2))
            else {
                return Err(invalid());
            };
            let original = raw.next().ok_or_else(invalid)?;
            let renamed = matches!(status.as_bytes().first(), Some(b'R' | b'C'));
            let path = if renamed {
                raw.next().ok_or_else(invalid)?
            } else {
                original
            };
            let Some(start) = starts.next() else {
                if truncated {
                    break;
                }
                return Err(invalid());
            };
            let end = starts.peek().copied().unwrap_or(patch.len());
            let (Ok(path), Ok(original)) =
                (std::str::from_utf8(path), std::str::from_utf8(original))
            else {
                self.truncated = true;
                continue;
            };
            let original = renamed.then_some(original);
            if !self.matches(path, original) {
                continue;
            }
            let text = &patch[start..end];
            let binary = text.lines().any(|line| line.starts_with("Binary files "));
            let file = GitDiffFile {
                path: path.to_owned(),
                original_path: original.map(ToOwned::to_owned),
                old_mode: old.trim_start_matches(':').to_owned(),
                new_mode: new.to_owned(),
                binary,
                text: String::new(),
                hunks: 0,
                truncated: truncated && end == patch.len(),
            };
            self.push(file, text);
        }
        if starts.next().is_some() {
            return Err(invalid());
        }
        Ok(())
    }

    fn read_untracked(&mut self, root: &Path, path: &str) {
        if self.files.len() == MAX_DIFF_TEXT_FILES || self.bytes == 0 || !valid_diff_path(path) {
            self.truncated = true;
            return;
        }
        let content = (|| -> Result<_> {
            let (dir, name) = Directory::open(root)?.descend(path)?;
            let file = dir.file(&name)?;
            let mode = untracked_mode(&file);
            let mut bytes = Vec::new();
            file.take((MAX_DIFF_TEXT_BYTES + 1) as u64)
                .read_to_end(&mut bytes)
                .map_err(|_| Error::new("unavailable", "cannot read untracked diff file"))?;
            Ok((bytes, mode))
        })();
        let Ok((bytes, mode)) = content else {
            self.truncated = true;
            return;
        };
        let binary = bytes.iter().take(8_000).any(|byte| *byte == 0);
        let truncated = !binary && bytes.len() > MAX_DIFF_TEXT_BYTES;
        let mut text = format!("diff --git a/{path} b/{path}\nnew file mode {mode}\n");
        if binary {
            let _ = writeln!(text, "Binary files /dev/null and b/{path} differ");
        } else if !bytes.is_empty() {
            let content = String::from_utf8_lossy(&bytes);
            let lines = content.split_inclusive('\n');
            let _ = writeln!(
                text,
                "--- /dev/null\n+++ b/{path}\n@@ -0,0 +1,{} @@",
                lines.clone().count()
            );
            for line in lines {
                text.push('+');
                text.push_str(line);
            }
            if !content.ends_with('\n') && !truncated {
                text.push_str("\n\\ No newline at end of file\n");
            }
        }
        self.push(
            GitDiffFile {
                path: path.to_owned(),
                original_path: None,
                old_mode: "000000".to_owned(),
                new_mode: mode.to_owned(),
                binary,
                text: String::new(),
                hunks: 0,
                truncated,
            },
            &text,
        );
    }

    fn push(&mut self, mut file: GitDiffFile, text: &str) {
        if self.files.len() == MAX_DIFF_TEXT_FILES
            || self.bytes == 0
            || !valid_diff_path(&file.path)
            || self.policy.contains_text(&file.path)
            || file
                .original_path
                .as_deref()
                .is_some_and(|path| !valid_diff_path(path) || self.policy.contains_text(path))
        {
            self.truncated = true;
            return;
        }
        let (safe, _) = self.policy.redact_prefix(text.as_bytes(), !file.truncated);
        let safe = String::from_utf8_lossy(&safe);
        let mut end = 0;
        for line in safe.split_inclusive('\n') {
            let starts_hunk = line.starts_with("@@ ") || line.starts_with("@@@ ");
            if starts_hunk && self.hunks == file.hunks {
                break;
            }
            if end + line.len() > self.bytes {
                if !starts_hunk {
                    end = safe.floor_char_boundary(self.bytes);
                }
                break;
            }
            file.hunks += usize::from(starts_hunk);
            end += line.len();
        }
        file.truncated |= end < safe.len();
        self.truncated |= file.truncated;
        self.bytes -= end;
        self.hunks -= file.hunks;
        file.text = safe[..end].to_owned();
        self.files.push(file);
    }
}

fn untracked_mode(file: &std::fs::File) -> &'static str {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if file
            .metadata()
            .is_ok_and(|metadata| metadata.permissions().mode() & 0o100 != 0)
        {
            return "100755";
        }
    }
    let _ = file;
    "100644"
}
