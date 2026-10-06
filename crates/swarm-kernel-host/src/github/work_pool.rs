//! Deterministic GitHub Issue identity and Task projection helpers.

use crate::{
    error::Result,
    github::client::IssueReadback,
    model::{self, Requirement, SourceIndexStatus, TaskSourceIndexEntry, TaskSpec},
};
use serde_json::json;

pub const TASK_PHASE: &str = "implementation";

/// `origin_key` is the existing Task uniqueness boundary. Repository names
/// and issue numbers are mutable/presentational; the repository and item IDs
/// remain stable across rename and transfer operations.
pub fn task_origin_key(host: &str, repository_id: i64, issue_id: i64) -> String {
    format!("github:{host}:{repository_id}:issue:{issue_id}")
}

/// Separate identity namespace for read-only pull request observations.
pub fn task_origin_key_for_pull(host: &str, repository_id: i64, pull_id: i64) -> String {
    format!("github:{host}:{repository_id}:pull:{pull_id}")
}

pub fn source_revision(issue: &IssueReadback) -> Result<String> {
    let content = json!({
        "title": issue.title,
        "body": issue.body.as_deref().unwrap_or("")
    });
    let digest = model::digest(model::canonical(&content)?.as_bytes());
    Ok(format!("sha256:{digest}"))
}

/// Deduplicate a normalized state fact without using `updated_at` as a
/// revision or compare-and-swap token. A label-only or timestamp-only change
/// therefore cannot revise a Task or create a new intake fact.
pub fn fact_digest(issue: &IssueReadback) -> Result<String> {
    let fact = json!({
        "issue_id": issue.id,
        "number": issue.number,
        "title": issue.title,
        "body": issue.body,
        "state": issue.state,
        "comments_count": issue.comments,
        "html_url": issue.html_url
    });
    Ok(model::digest(model::canonical(&fact)?.as_bytes()))
}

pub fn transition_event_key(
    source_id: &str,
    issue_id: i64,
    previous_event_key: Option<&str>,
    next_fact_digest: &str,
) -> Result<String> {
    let transition = json!({
        "source_id": source_id,
        "issue_id": issue_id,
        // Chain transitions, rather than just hashing the adjacent state
        // digests. That keeps A→B→A→B as three distinct observations while
        // an identical poll of B remains deduplicated by its current digest.
        "previous_event_key": previous_event_key,
        "next_fact_digest": next_fact_digest
    });
    Ok(model::digest(model::canonical(&transition)?.as_bytes()))
}

pub fn task_spec(host: &str, repository_id: i64, issue: &IssueReadback) -> Result<TaskSpec> {
    let revision = source_revision(issue)?;
    let base_ref = format!("github://{host}/{repository_id}/issue/{}/body", issue.id);
    let mut source_index = Vec::with_capacity(2);
    match issue.body.as_deref().filter(|body| !body.is_empty()) {
        Some(body) => source_index.push(TaskSourceIndexEntry {
            source_ref: base_ref,
            revision: Some(revision),
            content_sha256: Some(model::digest(body.as_bytes())),
            text: Some(body.to_owned()),
            status: SourceIndexStatus::Selected,
            gap_reason: None,
        }),
        None => source_index.push(TaskSourceIndexEntry {
            source_ref: base_ref,
            revision: Some(revision),
            content_sha256: None,
            text: None,
            status: SourceIndexStatus::Gap,
            gap_reason: Some("issue_body_empty".to_owned()),
        }),
    }
    if issue.comments > 0 {
        source_index.push(TaskSourceIndexEntry {
            source_ref: format!(
                "github://{host}/{repository_id}/issue/{}/comments",
                issue.id
            ),
            revision: None,
            content_sha256: None,
            text: None,
            status: SourceIndexStatus::Gap,
            gap_reason: Some("issue_comments_not_selected".to_owned()),
        });
    }

    let spec = TaskSpec {
        acceptance: None,
        objective: issue.title.clone(),
        phase: TASK_PHASE.to_owned(),
        requirements: vec![Requirement {
            id: "github_issue_title".to_owned(),
            statement: issue.title.clone(),
        }],
        dependencies: Vec::new(),
        scope: None,
        source_refs: Vec::new(),
        owner_policy_id: None,
        source_index,
        baseline_candidate_ref: None,
    };
    spec.validate()?;
    Ok(spec)
}

pub fn task_spec_digest(spec: &TaskSpec) -> Result<String> {
    Ok(model::digest(model::canonical(&json!(spec))?.as_bytes()))
}
