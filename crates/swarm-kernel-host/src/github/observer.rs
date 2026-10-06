//! Readback and normalization for one explicitly configured GitHub source.

use crate::{
    error::{Error, Result},
    github::client::{
        GhCli, IssueReadback, MAX_ISSUE_PAGES_PER_POLL, RepositoryReadback, RepositoryRef,
    },
    model,
};
use std::collections::BTreeMap;

#[derive(Debug, Clone)]
pub struct IssueSnapshot {
    pub repository_id: i64,
    pub starting_page: u32,
    pub next_page: Option<u32>,
    pub pages_read: u32,
    pub reached_end: bool,
    pub observed_at_ms: i64,
    pub issues: Vec<IssueReadback>,
}

pub async fn verify_repository(
    cli: &GhCli,
    repository: &RepositoryRef,
) -> Result<RepositoryReadback> {
    cli.repository(repository).await
}

/// Read a bounded page window outside the Store transaction. The stored
/// immutable repository ID is checked on every poll so a redirect or a
/// changed owner/name cannot silently bind the source to another repository.
pub async fn read_issue_snapshot(
    cli: &GhCli,
    repository: &RepositoryRef,
    expected_repository_id: i64,
    starting_page: u32,
) -> Result<IssueSnapshot> {
    if expected_repository_id <= 0 || starting_page == 0 {
        return Err(Error::invalid(
            "GitHub source identity and page cursor must be positive",
        ));
    }
    let repository_readback = cli.repository(repository).await?;
    if repository_readback.id != expected_repository_id {
        return Err(Error::new(
            "GITHUB_REPOSITORY_IDENTITY_CHANGED",
            "the configured GitHub path no longer resolves to the registered repository ID",
        ));
    }

    let mut page = starting_page;
    let mut pages_read = 0;
    let mut reached_end = false;
    let mut next_page = Some(starting_page);
    let mut unique_items = BTreeMap::<i64, IssueReadback>::new();
    while pages_read < MAX_ISSUE_PAGES_PER_POLL {
        let result = cli.issue_page(repository, page).await?;
        pages_read += 1;
        for issue in result.items {
            if let Some(existing) = unique_items.get(&issue.id) {
                if !same_source_state(existing, &issue) {
                    return Err(Error::new(
                        "GITHUB_PAGE_SNAPSHOT_UNSTABLE",
                        "a live issue changed between pages; the source page was not committed",
                    ));
                }
            } else {
                unique_items.insert(issue.id, issue);
            }
        }
        if result.reached_end {
            reached_end = true;
            next_page = None;
            break;
        }
        page = result.next_page.ok_or_else(|| {
            Error::new(
                "GITHUB_PAGE_CURSOR_INVALID",
                "a full GitHub issue page omitted its next page cursor",
            )
        })?;
        next_page = Some(page);
    }

    Ok(IssueSnapshot {
        repository_id: expected_repository_id,
        starting_page,
        next_page,
        pages_read,
        reached_end,
        observed_at_ms: model::now_ms()?,
        issues: unique_items.into_values().collect(),
    })
}

fn same_source_state(left: &IssueReadback, right: &IssueReadback) -> bool {
    left.id == right.id
        && left.number == right.number
        && left.title == right.title
        && left.body == right.body
        && left.state == right.state
        && left.comments == right.comments
        && left.html_url == right.html_url
}
