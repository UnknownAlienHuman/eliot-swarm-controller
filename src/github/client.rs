//! Bounded GitHub API access through the installed `gh` CLI.

use crate::error::{Error, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
#[cfg(windows)]
use std::os::windows::process::CommandExt;
use std::{future::Future, pin::Pin};
use std::{process::Stdio, time::Duration};
use tokio::{io::AsyncReadExt, process::Command, time::timeout};

pub const ISSUE_PAGE_SIZE: u32 = 50;
pub const MAX_ISSUE_PAGES_PER_POLL: u32 = 8;
const MAX_RESPONSE_BYTES: usize = 4 * 1024 * 1024;
const MAX_TEXT_FIELD_BYTES: usize = 256 * 1024;
const GH_TIMEOUT: Duration = Duration::from_secs(45);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepositoryRef {
    pub host: String,
    pub owner: String,
    pub name: String,
}

impl RepositoryRef {
    pub fn new(host: &str, owner: &str, name: &str) -> Result<Self> {
        let host = validate_host(host)?;
        let owner = validate_path_segment(owner, "repository owner")?;
        let name = validate_path_segment(name, "repository name")?;
        Ok(Self { host, owner, name })
    }

    pub fn repo_path(&self) -> String {
        format!("/repos/{}/{}", self.owner, self.name)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct RepositoryReadback {
    pub id: i64,
    pub full_name: String,
    pub html_url: String,
}

#[derive(Debug, Clone, Deserialize)]
struct IssueLabelEntry {
    name: String,
}

#[derive(Debug, Clone, Deserialize)]
struct IssueLabelResponse {
    id: i64,
    number: i64,
    labels: Vec<IssueLabelEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IssueLabelSnapshot {
    pub id: i64,
    pub number: i64,
    pub labels: Vec<String>,
}

/// Narrow transport contract used by the durable Store effect and its local
/// HTTP fixture. Production calls continue through the installed `gh`
/// account, so Store code never loads or persists a GitHub credential.
pub trait GitHubLabelApi: Send + Sync {
    fn repository<'a>(
        &'a self,
        repository: &'a RepositoryRef,
    ) -> Pin<Box<dyn Future<Output = Result<RepositoryReadback>> + Send + 'a>>;

    fn issue_labels<'a>(
        &'a self,
        repository: &'a RepositoryRef,
        number: i64,
    ) -> Pin<Box<dyn Future<Output = Result<IssueLabelSnapshot>> + Send + 'a>>;

    fn set_label<'a>(
        &'a self,
        repository: &'a RepositoryRef,
        number: i64,
        label: &'a str,
        present: bool,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct IssueReadback {
    pub id: i64,
    pub number: i64,
    pub title: String,
    #[serde(default)]
    pub body: Option<String>,
    pub state: String,
    pub updated_at: String,
    #[serde(default)]
    pub comments: i64,
    pub html_url: String,
    /// The repository Issues endpoint also returns pull requests. They are
    /// identified by this field and remain distinct from Issue work items.
    #[serde(default)]
    pub pull_request: Option<Value>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PullRequestReadback {
    pub id: i64,
    pub number: i64,
    pub state: String,
    pub title: String,
    #[serde(default)]
    pub body: Option<String>,
    pub updated_at: String,
    pub html_url: String,
    pub draft: bool,
    pub merged: bool,
    #[serde(default)]
    pub merge_commit_sha: Option<String>,
    pub head: PullRequestRefReadback,
    pub base: PullRequestRefReadback,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PullRequestRefReadback {
    pub sha: String,
    #[serde(default, rename = "ref")]
    pub ref_name: Option<String>,
}

#[derive(Debug, Clone)]
pub struct IssuePage {
    pub page: u32,
    pub items: Vec<IssueReadback>,
    pub next_page: Option<u32>,
    /// True only when the API returned fewer than a full page. This is a
    /// pagination boundary, not an atomic repository snapshot guarantee.
    pub reached_end: bool,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct GhCli;

impl GhCli {
    pub async fn repository(&self, repository: &RepositoryRef) -> Result<RepositoryReadback> {
        let endpoint = repository.repo_path();
        let value = self.api_json(repository, &endpoint, &[]).await?;
        let readback: RepositoryReadback = serde_json::from_value(value).map_err(|_| {
            Error::new(
                "GITHUB_RESPONSE_INVALID",
                "repository readback did not match the required identity fields",
            )
        })?;
        if readback.id <= 0
            || readback.full_name.trim().is_empty()
            || readback.html_url.trim().is_empty()
        {
            return Err(Error::new(
                "GITHUB_RESPONSE_INVALID",
                "repository readback omitted its immutable identity",
            ));
        }
        Ok(readback)
    }

    pub async fn issue_page(&self, repository: &RepositoryRef, page: u32) -> Result<IssuePage> {
        if !(1..=i32::MAX as u32).contains(&page) {
            return Err(Error::invalid("GitHub issue page must be positive"));
        }
        let endpoint = format!("{}/issues", repository.repo_path());
        let arguments = [
            ("state", "all".to_owned()),
            ("sort", "updated".to_owned()),
            ("direction", "desc".to_owned()),
            ("per_page", ISSUE_PAGE_SIZE.to_string()),
            ("page", page.to_string()),
        ];
        let value = self.api_json(repository, &endpoint, &arguments).await?;
        let raw_items: Vec<Value> = serde_json::from_value(value).map_err(|_| {
            Error::new(
                "GITHUB_RESPONSE_INVALID",
                "issue list readback was not an array",
            )
        })?;
        if raw_items.len() > ISSUE_PAGE_SIZE as usize {
            return Err(Error::new(
                "GITHUB_RESPONSE_INVALID",
                "issue list exceeded the requested page size",
            ));
        }
        let raw_page_len = raw_items.len();
        let mut items = Vec::with_capacity(raw_items.len());
        for raw in raw_items {
            // PRs are represented by the Issues endpoint too. Their Task
            // identity and lifecycle belong to the separately typed PR path.
            if raw
                .get("pull_request")
                .is_some_and(|pull_request| !pull_request.is_null())
            {
                continue;
            }
            let item: IssueReadback = serde_json::from_value(raw).map_err(|_| {
                Error::new(
                    "GITHUB_RESPONSE_INVALID",
                    "issue readback omitted a required identity or state field",
                )
            })?;
            validate_issue(&item)?;
            items.push(item);
        }
        let reached_end = raw_page_len < ISSUE_PAGE_SIZE as usize;
        let next_page = if reached_end {
            None
        } else {
            Some(
                page.checked_add(1)
                    .ok_or_else(|| Error::invalid("GitHub issue page number overflowed"))?,
            )
        };
        Ok(IssuePage {
            page,
            items,
            next_page,
            reached_end,
        })
    }

    pub async fn pull_request(
        &self,
        repository: &RepositoryRef,
        number: i64,
    ) -> Result<PullRequestReadback> {
        if number <= 0 {
            return Err(Error::invalid("pull request number must be positive"));
        }
        let endpoint = format!("{}/pulls/{number}", repository.repo_path());
        let value = self.api_json(repository, &endpoint, &[]).await?;
        let readback: PullRequestReadback = serde_json::from_value(value).map_err(|_| {
            Error::new(
                "GITHUB_RESPONSE_INVALID",
                "pull request readback omitted a required identity or head field",
            )
        })?;
        if readback.id <= 0
            || readback.number != number
            || !matches!(readback.state.as_str(), "open" | "closed")
            || !valid_object_id(&readback.head.sha)
            || !valid_object_id(&readback.base.sha)
        {
            return Err(Error::new(
                "GITHUB_RESPONSE_INVALID",
                "pull request readback contains an invalid identity or Git object ID",
            ));
        }
        Ok(readback)
    }

    async fn issue_labels(
        &self,
        repository: &RepositoryRef,
        number: i64,
    ) -> Result<IssueLabelSnapshot> {
        if number <= 0 {
            return Err(Error::invalid("issue number must be positive"));
        }
        let endpoint = format!("{}/issues/{number}", repository.repo_path());
        let value = self.api_json(repository, &endpoint, &[]).await?;
        let issue: IssueLabelResponse = serde_json::from_value(value).map_err(|_| {
            Error::new(
                "GITHUB_RESPONSE_INVALID",
                "issue readback omitted its immutable ID, number, or labels",
            )
        })?;
        validate_managed_label_snapshot(issue.id, issue.number, &issue.labels)?;
        Ok(IssueLabelSnapshot {
            id: issue.id,
            number: issue.number,
            labels: issue.labels.into_iter().map(|entry| entry.name).collect(),
        })
    }

    async fn set_label(
        &self,
        repository: &RepositoryRef,
        number: i64,
        label: &str,
        present: bool,
    ) -> Result<()> {
        validate_managed_label_name(label)?;
        if number <= 0 {
            return Err(Error::invalid("issue number must be positive"));
        }
        let endpoint = if present {
            format!("{}/issues/{number}/labels", repository.repo_path())
        } else {
            format!("{}/issues/{number}/labels/{label}", repository.repo_path())
        };
        let mut arguments = vec![
            "api".to_owned(),
            endpoint,
            "--hostname".to_owned(),
            repository.host.clone(),
            "--method".to_owned(),
            if present { "POST" } else { "DELETE" }.to_owned(),
        ];
        if present {
            arguments.push("--raw-field".to_owned());
            arguments.push(format!("labels[]={label}"));
        }
        // This is one explicit effect invocation. Rust never retries it.
        run_gh_api(&arguments).await?;
        Ok(())
    }

    async fn api_json(
        &self,
        repository: &RepositoryRef,
        endpoint: &str,
        fields: &[(&str, String)],
    ) -> Result<Value> {
        let mut arguments = vec![
            "api".to_owned(),
            endpoint.to_owned(),
            "--hostname".to_owned(),
            repository.host.clone(),
            "--method".to_owned(),
            "GET".to_owned(),
        ];
        for (name, value) in fields {
            arguments.push("--raw-field".to_owned());
            arguments.push(format!("{name}={value}"));
        }
        let output = run_gh_api(&arguments).await?;
        serde_json::from_slice(&output).map_err(|_| {
            Error::new(
                "GITHUB_RESPONSE_INVALID",
                "GitHub CLI returned a non-JSON response",
            )
        })
    }
}

impl GitHubLabelApi for GhCli {
    fn repository<'a>(
        &'a self,
        repository: &'a RepositoryRef,
    ) -> Pin<Box<dyn Future<Output = Result<RepositoryReadback>> + Send + 'a>> {
        Box::pin(GhCli::repository(self, repository))
    }

    fn issue_labels<'a>(
        &'a self,
        repository: &'a RepositoryRef,
        number: i64,
    ) -> Pin<Box<dyn Future<Output = Result<IssueLabelSnapshot>> + Send + 'a>> {
        Box::pin(GhCli::issue_labels(self, repository, number))
    }

    fn set_label<'a>(
        &'a self,
        repository: &'a RepositoryRef,
        number: i64,
        label: &'a str,
        present: bool,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(GhCli::set_label(self, repository, number, label, present))
    }
}

fn validate_managed_label_snapshot(
    issue_id: i64,
    number: i64,
    labels: &[IssueLabelEntry],
) -> Result<()> {
    if issue_id <= 0 || number <= 0 || labels.len() > 256 {
        return Err(Error::new(
            "GITHUB_RESPONSE_INVALID",
            "issue label readback exceeded identity or collection bounds",
        ));
    }
    let mut seen = std::collections::BTreeSet::new();
    for label in labels {
        if label.name.is_empty()
            || label.name.len() > 100
            || label.name.chars().any(char::is_control)
            || !seen.insert(label.name.as_str())
        {
            return Err(Error::new(
                "GITHUB_RESPONSE_INVALID",
                "issue label readback contained an invalid or duplicate name",
            ));
        }
    }
    Ok(())
}

fn validate_managed_label_name(label: &str) -> Result<()> {
    if !(10..=50).contains(&label.len())
        || !label.starts_with("eliot-")
        || !label
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        || label.ends_with('-')
    {
        return Err(Error::invalid(
            "label is outside the managed eliot-* namespace",
        ));
    }
    Ok(())
}

async fn run_gh_api(arguments: &[String]) -> Result<Vec<u8>> {
    let mut command = Command::new("gh");
    command
        .args(arguments)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    #[cfg(windows)]
    command.as_std_mut().creation_flags(0x08000000);

    let mut child = command.spawn().map_err(|_| {
        Error::new(
            "GITHUB_CLI_UNAVAILABLE",
            "the installed GitHub CLI could not be started",
        )
    })?;
    let result = timeout(GH_TIMEOUT, async {
        let mut stdout = child.stdout.take().ok_or_else(|| {
            Error::new(
                "GITHUB_CLI_FAILED",
                "the GitHub CLI response pipe was unavailable",
            )
        })?;
        let mut output = Vec::new();
        let mut chunk = [0u8; 16 * 1024];
        loop {
            let read = stdout.read(&mut chunk).await.map_err(|_| {
                Error::new(
                    "GITHUB_CLI_FAILED",
                    "the GitHub CLI response could not be read",
                )
            })?;
            if read == 0 {
                break;
            }
            if output.len().saturating_add(read) > MAX_RESPONSE_BYTES {
                let _ = child.start_kill();
                let _ = child.wait().await;
                return Err(Error::new(
                    "GITHUB_RESPONSE_TOO_LARGE",
                    "the GitHub API response exceeded the bounded page size",
                ));
            }
            output.extend_from_slice(&chunk[..read]);
        }
        let status = child.wait().await.map_err(|_| {
            Error::new(
                "GITHUB_CLI_FAILED",
                "the GitHub CLI process did not exit cleanly",
            )
        })?;
        if !status.success() {
            return Err(Error::new(
                "GITHUB_CLI_FAILED",
                format!(
                    "the GitHub CLI read failed with exit code {}",
                    status.code().unwrap_or(-1)
                ),
            ));
        }
        Ok(output)
    })
    .await;
    match result {
        Ok(result) => result,
        Err(_) => {
            let _ = child.start_kill();
            let _ = child.wait().await;
            Err(Error::new(
                "GITHUB_CLI_TIMEOUT",
                "the bounded GitHub CLI read exceeded its time limit",
            ))
        }
    }
}

fn validate_host(host: &str) -> Result<String> {
    let normalized = host.trim().trim_end_matches('.').to_ascii_lowercase();
    if normalized.is_empty()
        || normalized.len() > 253
        || normalized
            .split('.')
            .any(|label| label.is_empty() || label.len() > 63)
        || !normalized
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-'))
    {
        return Err(Error::invalid("GitHub host must be a DNS hostname"));
    }
    Ok(normalized)
}

fn validate_path_segment(value: &str, label: &str) -> Result<String> {
    let value = value.trim();
    if value.is_empty()
        || value.len() > 100
        || value == "."
        || value == ".."
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        return Err(Error::invalid(format!("{label} is invalid")));
    }
    Ok(value.to_owned())
}

fn validate_issue(issue: &IssueReadback) -> Result<()> {
    if issue.id <= 0
        || issue.number <= 0
        || issue.title.trim().is_empty()
        || issue.title.len() > MAX_TEXT_FIELD_BYTES
        || issue
            .body
            .as_ref()
            .is_some_and(|body| body.len() > MAX_TEXT_FIELD_BYTES)
        || !matches!(issue.state.as_str(), "open" | "closed")
        || issue.updated_at.trim().is_empty()
        || issue.updated_at.len() > 128
        || issue.comments < 0
        || issue.html_url.trim().is_empty()
        || issue.html_url.len() > 2048
        || issue.html_url.chars().any(char::is_control)
    {
        return Err(Error::new(
            "GITHUB_RESPONSE_INVALID",
            "issue readback exceeded field bounds or omitted required source facts",
        ));
    }
    Ok(())
}

fn valid_object_id(value: &str) -> bool {
    matches!(value.len(), 40 | 64) && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}
