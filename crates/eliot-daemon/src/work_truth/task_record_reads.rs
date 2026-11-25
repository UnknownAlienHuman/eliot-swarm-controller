//! Bounded reads of allow-listed sections from pinned Git task commits.
use super::*;
use eliot_core::api::{TASK_RECORD_TEXT_PAGE_BYTES, TaskGetRequest, inference::sha256_hex};

const MAX_TASK_SECTION_BYTES: usize = 1024 * 1024;

pub(crate) async fn read_task_record_section(
    data_root: &Path,
    task_ref: &str,
    request: &TaskGetRequest,
    policy: &SecretPolicy,
) -> Result<(String, Option<usize>, String, String)> {
    let section = &request.section;
    let offset = request.offset.unwrap_or(0);
    let git = GitProbe::new()?;
    let root = crate::work_truth_repo::existing(data_root).await?;
    let deadline = Instant::now() + GIT_TIME;
    let _read_lane = crate::work_truth_repo::read_lane(deadline).await?;
    let commit = task_commit(&git, &root, task_ref, deadline)
        .await
        .ok_or_else(|| Error::new("unavailable", "bound Git task record is unreadable"))?;
    if request
        .revision
        .as_ref()
        .is_some_and(|expected| expected != &commit)
    {
        return Err(Error::new(
            "stale_revision",
            "Git task record revision changed",
        ));
    }
    let spec = format!("{commit}:{section}");
    let size = git
        .workspace_command(&root, &["cat-file", "-s", &spec], deadline)
        .await?;
    if size.code != 0 {
        return Err(Error::new(
            "unavailable",
            format!("saved task entry {section} is unreadable"),
        ));
    }
    let size = std::str::from_utf8(&size.stdout)
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .ok_or_else(|| Error::new("source_integrity", "saved task entry size is invalid"))?;
    if size > MAX_TASK_SECTION_BYTES {
        return Err(Error::new(
            "frame_too_large",
            "saved task section exceeds the read limit",
        ));
    }
    let output = git
        .workspace_command(&root, &["cat-file", "blob", &spec], deadline)
        .await?;
    if output.code != 0 {
        return Err(Error::new(
            "unavailable",
            format!("saved task entry {section} is unreadable"),
        ));
    }
    let raw = String::from_utf8(output.stdout)
        .map_err(|_| Error::new("source_integrity", "saved task entry is not valid UTF-8"))?;
    if task_commit(&git, &root, task_ref, deadline)
        .await
        .as_deref()
        != Some(commit.as_str())
    {
        return Err(Error::new(
            "stale_revision",
            "Git task record moved during read",
        ));
    }
    let text = redact_record(&raw, policy);
    if text.len() > MAX_TASK_SECTION_BYTES {
        return Err(Error::new(
            "frame_too_large",
            "rendered task section exceeds the read limit",
        ));
    }
    let digest = sha256_hex(text.as_bytes());
    if request
        .content_digest
        .as_ref()
        .is_some_and(|expected| expected != &digest)
    {
        return Err(Error::new(
            "stale_revision",
            "rendered task section changed",
        ));
    }
    if offset > text.len() || !text.is_char_boundary(offset) {
        return Err(Error::invalid_field(
            "params.offset",
            "UTF-8 text page offset",
        ));
    }
    let mut end = offset
        .saturating_add(TASK_RECORD_TEXT_PAGE_BYTES)
        .min(text.len());
    while end > offset && !text.is_char_boundary(end) {
        end -= 1;
    }
    let next = (end < text.len()).then_some(end);
    Ok((text[offset..end].to_owned(), next, commit, digest))
}
