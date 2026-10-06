//! Read-only, exact-head GitHub pull request observations.

use crate::{
    error::{Error, Result},
    github::{
        client::{GhCli, PullRequestReadback, RepositoryRef},
        work_pool,
    },
    model,
};
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct PullRequestObservation {
    pub item_kind: &'static str,
    pub external_id: i64,
    pub number: i64,
    pub state: String,
    pub title: String,
    pub body: Option<String>,
    pub draft: bool,
    pub merged: bool,
    pub head_sha: String,
    pub base_sha: String,
    pub merge_commit_sha: Option<String>,
    pub source_revision: String,
    pub updated_at: String,
    pub html_url: String,
}

pub async fn read_pull_request(
    cli: &GhCli,
    repository: &RepositoryRef,
    expected_repository_id: i64,
    number: i64,
) -> Result<PullRequestObservation> {
    let repository_readback = cli.repository(repository).await?;
    if repository_readback.id != expected_repository_id {
        return Err(Error::new(
            "GITHUB_REPOSITORY_IDENTITY_CHANGED",
            "the configured GitHub path no longer resolves to the registered repository ID",
        ));
    }
    let pull = cli.pull_request(repository, number).await?;
    let digest_input = serde_json::json!({
        "pull_request_id": pull.id,
        "number": pull.number,
        "title": pull.title,
        "body": pull.body,
        "head_sha": pull.head.sha,
        "base_sha": pull.base.sha
    });
    Ok(project_pull(
        pull,
        model::digest(model::canonical(&digest_input)?.as_bytes()),
    ))
}

fn project_pull(pull: PullRequestReadback, content_digest: String) -> PullRequestObservation {
    PullRequestObservation {
        item_kind: "pull_request",
        external_id: pull.id,
        number: pull.number,
        state: pull.state,
        title: pull.title,
        body: pull.body,
        draft: pull.draft,
        merged: pull.merged,
        head_sha: pull.head.sha,
        base_sha: pull.base.sha,
        merge_commit_sha: pull.merge_commit_sha,
        source_revision: format!("sha256:{content_digest}"),
        updated_at: pull.updated_at,
        html_url: pull.html_url,
    }
}

pub fn task_origin_key(host: &str, repository_id: i64, pull_request_id: i64) -> String {
    work_pool::task_origin_key_for_pull(host, repository_id, pull_request_id)
}
