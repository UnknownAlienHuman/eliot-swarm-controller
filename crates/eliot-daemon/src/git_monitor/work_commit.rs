use super::*;
use eliot_store::{MissingWork, WorkCommitRequest};

impl GitProbe {
    pub(crate) async fn verify_work_commit(
        &self,
        work: &MissingWork,
        request: &WorkCommitRequest,
    ) -> Result<String> {
        if !is_git_oid(&request.commit)
            || !["refs/heads/", "refs/remotes/"]
                .iter()
                .any(|prefix| request.retained_ref.starts_with(prefix))
        {
            return Err(Error::new(
                "invalid",
                "commit must be a full Git object ID and retained_ref a full branch ref",
            ));
        }
        let (_, common) = crate::workspace_git::WorkspaceGit::new()?
            .repository_identity(&work.repo)
            .await?;
        if Path::new(&common) != Path::new(&work.common_dir) {
            return Err(Error::new(
                "source_integrity",
                "repository identity differs from the removed worktree record",
            ));
        }
        let root = Path::new(&common);
        let binding = GitBinding {
            git_dir: root,
            common_dir: root,
        };
        let deadline = Instant::now() + PROBE_TIME;
        let commit_selector = format!("{}^{{commit}}", request.commit);
        let commit = self
            .project_git_metadata(
                root,
                &[
                    "rev-parse",
                    "--verify",
                    "--end-of-options",
                    &commit_selector,
                ],
                Some(binding),
                deadline,
            )
            .await?;
        if commit.code != 0
            || !std::str::from_utf8(&commit.stdout)
                .is_ok_and(|oid| oid.trim().eq_ignore_ascii_case(&request.commit))
        {
            return Err(Error::new("unproven", "object ID does not name a commit"));
        }
        let tip = self
            .project_git_metadata(
                root,
                &["show-ref", "--verify", "--hash", &request.retained_ref],
                Some(binding),
                deadline,
            )
            .await?;
        let tip = std::str::from_utf8(&tip.stdout)
            .ok()
            .map(str::trim)
            .filter(|oid| tip.code == 0 && is_git_oid(oid))
            .ok_or_else(|| Error::new("unproven", "retained Git ref could not be read"))?;
        let ancestor = self
            .project_git_metadata(
                root,
                &["merge-base", "--is-ancestor", &request.commit, tip],
                Some(binding),
                deadline,
            )
            .await?;
        if ancestor.code != 0 {
            return Err(Error::new(
                "unproven",
                "commit is not reachable from the retained ref",
            ));
        }
        let overrides = self
            .safe_overrides_for(root, deadline, Some(binding))
            .await?;
        let mut args = vec!["ls-tree", "-z", "--full-tree", &request.commit, "--"];
        for entry in &work.dirty {
            git_path(&entry.path)?;
            args.push(&entry.path);
        }
        let tree = self
            .run_scoped(root, &args, &overrides, &[], deadline, Some(binding))
            .await?;
        if tree.code != 0 {
            return Err(unavailable("could not read commit paths"));
        }
        let entries = nul_records(&tree.stdout)?
            .into_iter()
            .map(|record| {
                record
                    .split_once('\t')
                    .map(|(object, path)| (path, object))
                    .ok_or_else(|| unavailable("invalid Git tree entry"))
            })
            .collect::<Result<BTreeMap<_, _>>>()?;
        let mut digest = Sha256::new();
        let mut total_bytes = 0;
        for entry in &work.dirty {
            hash_dirty_entry(&mut digest, entry);
            let object = entries.get(entry.path.as_str());
            if entry.status == ".D" && object.is_none() {
                hash_digest_part(&mut digest, b"missing");
                continue;
            }
            let oid = object
                .and_then(|meta| {
                    meta.strip_prefix("100644 blob ")
                        .or_else(|| meta.strip_prefix("100755 blob "))
                })
                .filter(|oid| is_git_oid(oid))
                .ok_or_else(content_mismatch)?;
            let blob = self
                .run_scoped(
                    root,
                    &["cat-file", "blob", oid],
                    &overrides,
                    &[],
                    deadline,
                    Some(binding),
                )
                .await?;
            if blob.code != 0 {
                return Err(unavailable("could not read Git blob"));
            }
            let size = blob.stdout.len() as u64;
            total_bytes += size;
            if size > WORK_UNTRACKED_MAX_BYTES_PER_FILE
                || total_bytes > WORK_UNTRACKED_MAX_TOTAL_BYTES
            {
                return Err(unavailable("commit paths exceed the observation bound"));
            }
            hash_dirty_file(&mut digest, &blob.stdout);
        }
        if hex_digest(digest.finalize().as_slice()) != work.digest {
            return Err(content_mismatch());
        }
        Ok(tip.to_owned())
    }
}

fn content_mismatch() -> Error {
    Error::new(
        "unproven",
        "commit does not reproduce the recorded dirty-content digest",
    )
}
