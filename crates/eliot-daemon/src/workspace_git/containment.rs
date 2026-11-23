//! Fresh-upstream proof for explicit remote branch deletion.

use crate::{
    git_actions::{CleanupTransportOp, run_cleanup_transport},
    git_monitor::{Captured, GitProbe},
};
use eliot_core::{Error, Result, api::is_git_oid};
use std::{collections::BTreeMap, path::Path, time::Duration};
use tokio::time::Instant;

const CONTENT_TIME: Duration = Duration::from_secs(6);

pub(crate) struct BranchClassification {
    pub(crate) remote: String,
    pub(crate) url: String,
    pub(crate) tip: String,
}

async fn prove_containment(
    probe: &GitProbe,
    repo: &Path,
    tip: &str,
    default_tip: &str,
    deadline: Instant,
) -> Result<()> {
    let deadline = deadline.min(Instant::now() + CONTENT_TIME);
    if repo
        .join("info/grafts")
        .try_exists()
        .map_err(|_| merge_check_failed())?
    {
        return Err(merge_check_failed());
    }
    match proof_command(
        probe,
        repo,
        &["merge-base", "--is-ancestor", tip, default_tip],
        deadline,
    )
    .await?
    .code
    {
        0 => return Ok(()),
        1 => {}
        _ => return Err(merge_check_failed()),
    }

    let shallow = proof_command(
        probe,
        repo,
        &["rev-parse", "--is-shallow-repository"],
        deadline,
    )
    .await?;
    if shallow.code != 0 || !matches!(shallow.stdout.as_slice(), b"false\n" | b"false\r\n") {
        return Err(merge_check_failed());
    }
    let base = proof_command(
        probe,
        repo,
        &["merge-base", "--all", tip, default_tip],
        deadline,
    )
    .await?;
    match base.code {
        0 => {}
        1 => return Err(content_not_at_tip()),
        _ => return Err(merge_check_failed()),
    }
    let base = std::str::from_utf8(&base.stdout)
        .ok()
        .map(str::trim)
        .filter(|oid| is_git_oid(oid))
        .ok_or_else(merge_check_failed)?;

    let delta = tree_delta(probe, repo, base, tip, deadline).await?;
    let expected = raw_entries(&delta)?;
    if !expected.is_empty()
        && delta_matches(probe, repo, base, default_tip, &expected, deadline).await?
    {
        return Ok(());
    }
    Err(content_not_at_tip())
}

async fn delta_matches(
    probe: &GitProbe,
    repo: &Path,
    base: &str,
    commit: &str,
    expected: &BTreeMap<&[u8], &[u8]>,
    deadline: Instant,
) -> Result<bool> {
    let delta = tree_delta(probe, repo, base, commit, deadline).await?;
    let actual = raw_entries(&delta)?;
    Ok(expected
        .iter()
        .all(|(path, entry)| actual.get(path) == Some(entry)))
}

// Compare the whole branch's net delta at one upstream commit. Raw entries
// include modes and full object IDs; no attributes, merge drivers or rename
// heuristics participate, and other upstream paths may differ.
async fn tree_delta(
    probe: &GitProbe,
    repo: &Path,
    base: &str,
    tip: &str,
    deadline: Instant,
) -> Result<Vec<u8>> {
    let result = proof_command(
        probe,
        repo,
        &[
            "diff-tree",
            "--raw",
            "--no-commit-id",
            "--no-abbrev",
            "--no-renames",
            "--no-ext-diff",
            "--no-textconv",
            "--ignore-submodules=none",
            "-r",
            "-z",
            base,
            tip,
            "--",
        ],
        deadline,
    )
    .await?;
    if result.code != 0 {
        return Err(merge_check_failed());
    }
    Ok(result.stdout)
}

fn raw_entries(bytes: &[u8]) -> Result<BTreeMap<&[u8], &[u8]>> {
    if bytes.is_empty() {
        return Ok(BTreeMap::new());
    }
    let mut fields = bytes
        .strip_suffix(b"\0")
        .ok_or_else(merge_check_failed)?
        .split(|byte| *byte == 0);
    let mut entries = BTreeMap::new();
    while let Some(entry) = fields.next() {
        let path = fields.next().ok_or_else(merge_check_failed)?;
        if !entry.starts_with(b":") || path.is_empty() || entries.insert(path, entry).is_some() {
            return Err(merge_check_failed());
        }
    }
    Ok(entries)
}

async fn proof_command(
    probe: &GitProbe,
    repo: &Path,
    args: &[&str],
    deadline: Instant,
) -> Result<Captured> {
    probe
        .workspace_command(repo, args, deadline)
        .await
        .map_err(|_| {
            if Instant::now() >= deadline {
                Error::new("merge_scan_deadline", "upstream content scan timed out")
            } else {
                merge_check_failed()
            }
        })
}

fn merge_check_failed() -> Error {
    Error::new(
        "merge_check_failed",
        "exact upstream containment is unavailable",
    )
}

fn content_not_at_tip() -> Error {
    Error::new(
        "content_not_at_tip",
        "branch changes are absent from the current upstream default tip",
    )
}

/// Preserve the upstream safety gate for explicit remote deletion.
pub(crate) async fn classify_branch(
    data_root: &Path,
    checkout: &str,
    common: &Path,
    branch: &str,
    deadline: Instant,
) -> Result<BranchClassification> {
    if !crate::git_actions::valid_cleanup_branch(branch) {
        return Err(Error::new(
            "invalid_branch",
            "branch name is not admissible",
        ));
    }
    let repo = common;
    let probe = GitProbe::new()?;
    let branch_ref = format!("refs/heads/{branch}");
    let remote = branch_upstream_remote(&probe, repo, &branch_ref, deadline).await?;
    let url = branch_remote_url(&probe, repo, &remote, deadline).await?;
    let default_tip =
        fetch_default_tip(&probe, data_root, checkout, repo, &remote, &url, deadline).await?;
    let tip = branch_rev_parse(&probe, repo, &branch_ref, "branch_absent", deadline).await?;
    let upstream =
        branch_upstream_tip(&probe, repo, &branch_ref, &remote, branch, deadline).await?;
    let unpushed = match upstream.as_deref() {
        Some(upstream) => {
            let count = probe
                .workspace_command(
                    repo,
                    &["rev-list", "--max-count=1", &format!("{upstream}..{tip}")],
                    deadline,
                )
                .await
                .map_err(|_| {
                    Error::new("branch_read_failed", "branch ahead check is unavailable")
                })?;
            if count.code != 0 {
                return Err(Error::new(
                    "branch_read_failed",
                    "branch ahead check did not complete",
                ));
            }
            !count.stdout.is_empty()
        }
        None => false,
    };
    if unpushed {
        return Err(Error::new(
            "ahead",
            "branch carries commits its upstream tracking ref lacks",
        ));
    }
    prove_containment(&probe, repo, &tip, &default_tip, deadline).await?;
    let remote_heads = run_cleanup_transport(
        data_root,
        checkout,
        &remote,
        &url,
        Some(branch),
        CleanupTransportOp::ListHeads,
        deadline,
    )
    .await
    .map_err(|_| Error::new("remote_check_failed", "branch remote check is unavailable"))?;
    if remote_heads.interrupted || !remote_heads.quiescent || remote_heads.exit_code != Some(0) {
        return Err(Error::new(
            "remote_check_failed",
            "branch remote check did not complete",
        ));
    }
    if upstream.is_none() && !remote_heads.stdout.is_empty() {
        return Err(Error::new(
            "remote_check_failed",
            "remote branch exists without a freshly resolved tracking tip",
        ));
    }
    Ok(BranchClassification { remote, url, tip })
}

async fn fetch_default_tip(
    probe: &GitProbe,
    data_root: &Path,
    checkout: &str,
    repo: &Path,
    remote: &str,
    url: &str,
    deadline: Instant,
) -> Result<String> {
    // A fresh fetch through the same isolated transport as the approved
    // fetch action: private view, isolated HOME and config, no
    // credentials, only file/HTTPS/HTTP/git. It updates remote-tracking
    // refs but never deletes a branch; deletion happens only at apply.
    // No classification is trusted without this fetch completing.
    let fetch = run_cleanup_transport(
        data_root,
        checkout,
        remote,
        url,
        None,
        CleanupTransportOp::FetchPrune,
        deadline,
    )
    .await
    .map_err(|error| match error.code.as_str() {
        "unsupported" => Error::new(
            "remote_transport_unsupported",
            "upstream transport is not file, HTTPS, HTTP or git",
        ),
        "invalid_remote" | "invalid" => {
            Error::new("invalid_remote", "upstream remote is not admissible")
        }
        _ => Error::new(
            "fetch_failed",
            format!("branch upstream fetch failed: {}", error.message),
        ),
    })?;
    if fetch.interrupted || !fetch.quiescent || fetch.exit_code != Some(0) {
        return Err(Error::new(
            "fetch_failed",
            "branch upstream fetch did not complete",
        ));
    }
    let (default_name, advertised_tip) =
        branch_default(data_root, checkout, remote, url, deadline).await?;
    let default_ref = format!("refs/remotes/{remote}/{default_name}");
    let default_tip =
        branch_rev_parse(probe, repo, &default_ref, "no_default_branch", deadline).await?;
    if default_tip != advertised_tip {
        return Err(Error::new(
            "no_default_branch",
            "upstream HEAD changed since the fetch",
        ));
    }
    Ok(default_tip)
}

async fn branch_upstream_remote(
    probe: &crate::git_monitor::GitProbe,
    repo: &Path,
    branch_ref: &str,
    deadline: Instant,
) -> Result<String> {
    let remote = probe
        .workspace_command(
            repo,
            &[
                "for-each-ref",
                "--format=%(upstream:remotename)",
                branch_ref,
            ],
            deadline,
        )
        .await
        .map_err(|_| Error::new("branch_read_failed", "branch upstream is unreadable"))?;
    if remote.code != 0 {
        return Err(Error::new(
            "branch_read_failed",
            "branch upstream is unreadable",
        ));
    }
    let name = String::from_utf8(remote.stdout)
        .map_err(|_| Error::new("branch_read_failed", "branch upstream is not Unicode"))?;
    let name = name.trim();
    if !name.is_empty() {
        return Ok(name.to_owned());
    }
    let origin = probe
        .workspace_command(repo, &["remote", "get-url", "origin"], deadline)
        .await
        .map_err(|_| Error::new("no_upstream_remote", "branch has no upstream remote"))?;
    if origin.code != 0 {
        return Err(Error::new(
            "no_upstream_remote",
            "branch has no upstream remote",
        ));
    }
    Ok("origin".to_owned())
}

/// Read the exact remote URL from local repository config. The URL is
/// validated as an endpoint by the isolated transport before any network
/// use; only file/HTTPS/HTTP/git transports are admitted.
async fn branch_remote_url(
    probe: &crate::git_monitor::GitProbe,
    repo: &Path,
    remote: &str,
    deadline: Instant,
) -> Result<String> {
    let key = format!("remote.{remote}.url");
    let configured = probe
        .workspace_command(repo, &["config", "--get", &key], deadline)
        .await
        .map_err(|_| Error::new("no_upstream_remote", "branch remote URL is unreadable"))?;
    if configured.code != 0 {
        return Err(Error::new(
            "no_upstream_remote",
            "branch has no configured upstream remote",
        ));
    }
    let url = String::from_utf8(configured.stdout)
        .map_err(|_| Error::new("no_upstream_remote", "branch remote URL is not Unicode"))?;
    let url = url.trim().to_owned();
    if url.is_empty() || url.len() > 4096 || url.chars().any(char::is_control) {
        return Err(Error::new(
            "no_upstream_remote",
            "branch remote URL is not admissible",
        ));
    }
    Ok(url)
}

async fn branch_default(
    data_root: &Path,
    checkout: &str,
    remote: &str,
    url: &str,
    deadline: Instant,
) -> Result<(String, String)> {
    // The upstream HEAD symref is read fresh from the remote through the
    // isolated transport; a stale local remote-HEAD pointer alone never
    // decides the default branch.
    if let Ok(symref) = run_cleanup_transport(
        data_root,
        checkout,
        remote,
        url,
        None,
        CleanupTransportOp::ListSymrefHead,
        deadline,
    )
    .await
        && symref.exit_code == Some(0)
        && symref.quiescent
        && !symref.interrupted
    {
        let output = String::from_utf8_lossy(&symref.stdout);
        let head = output
            .lines()
            .find_map(|line| line.strip_suffix("\tHEAD").filter(|oid| is_git_oid(oid)));
        for line in output.lines() {
            if let Some(head) = head
                && let Some(rest) = line.strip_prefix("ref: ")
                && rest.split_whitespace().nth(1) == Some("HEAD")
                && let Some(name) = rest
                    .split_whitespace()
                    .next()
                    .and_then(|reference| reference.strip_prefix("refs/heads/"))
                && crate::git_actions::valid_cleanup_branch(name)
            {
                return Ok((name.to_owned(), head.to_owned()));
            }
        }
    }
    Err(Error::new(
        "no_default_branch",
        "upstream default branch is unreadable",
    ))
}

pub(crate) async fn branch_rev_parse(
    probe: &crate::git_monitor::GitProbe,
    repo: &Path,
    reference: &str,
    missing_code: &'static str,
    deadline: Instant,
) -> Result<String> {
    let parsed = probe
        .workspace_command(repo, &["rev-parse", "--verify", reference], deadline)
        .await
        .map_err(|_| Error::new("branch_read_failed", "branch tip is unreadable"))?;
    if parsed.code != 0 {
        return Err(Error::new(missing_code, "branch reference is absent"));
    }
    let oid = String::from_utf8(parsed.stdout)
        .map_err(|_| Error::new("branch_read_failed", "branch tip is not Unicode"))?;
    let oid = oid.trim().to_owned();
    if !eliot_core::api::is_git_oid(&oid) {
        return Err(Error::new(
            "branch_read_failed",
            "branch tip is not an object id",
        ));
    }
    Ok(oid)
}

async fn branch_upstream_tip(
    probe: &crate::git_monitor::GitProbe,
    repo: &Path,
    branch_ref: &str,
    remote: &str,
    branch: &str,
    deadline: Instant,
) -> Result<Option<String>> {
    let upstream = probe
        .workspace_command(
            repo,
            &["for-each-ref", "--format=%(upstream)", branch_ref],
            deadline,
        )
        .await
        .map_err(|_| Error::new("branch_read_failed", "branch upstream is unreadable"))?;
    if upstream.code != 0 {
        return Err(Error::new(
            "branch_read_failed",
            "branch upstream is unreadable",
        ));
    }
    let name = std::str::from_utf8(&upstream.stdout)
        .map_err(|_| Error::new("branch_read_failed", "branch upstream is not Unicode"))?;
    let name = name.trim();
    let same_name = format!("refs/remotes/{remote}/{branch}");
    // A configured ref can disappear during the fresh prune. In that case
    // still check the remote branch with the same name as the local branch.
    for reference in [name, same_name.as_str()]
        .into_iter()
        .filter(|name| !name.is_empty())
    {
        match branch_rev_parse(probe, repo, reference, "branch_absent", deadline).await {
            Ok(oid) => return Ok(Some(oid)),
            Err(error) if error.code == "branch_absent" => {}
            Err(error) => return Err(error),
        }
    }
    Ok(None)
}
