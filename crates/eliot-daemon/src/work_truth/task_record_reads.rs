//! Bounded reads of allow-listed sections from pinned Git task commits.
use super::*;
use eliot_core::api::{
    MAX_TASK_SOURCES, TASK_RECORD_TEXT_PAGE_BYTES, TaskGetRequest, TaskSnapshotManifest,
    TaskSnapshotOrigin, TaskSnapshotSource, inference::sha256_hex, is_git_oid, task_source_index,
};

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
    let text = if section == "manifest" || task_source_index(section).is_some() {
        let files = task_inputs::entries(&git, &root, &commit, deadline).await?;
        let mut sources = Vec::new();
        if let Some((_, kind, tree, _)) = files.iter().find(|(_, _, _, name)| name == "sources") {
            if kind != "tree" || !is_git_oid(tree) {
                return Err(Error::new(
                    "source_integrity",
                    "saved task sources are not a Git tree",
                ));
            }
            sources = task_inputs::entries(&git, &root, tree, deadline).await?;
            if sources.len() > MAX_TASK_SOURCES
                || sources.iter().any(|(mode, kind, oid, name)| {
                    mode != "100644"
                        || kind != "blob"
                        || !is_git_oid(oid)
                        || name.is_empty()
                        || name.chars().any(char::is_control)
                })
            {
                return Err(Error::new(
                    "source_integrity",
                    "saved task sources exceed their inventory bounds",
                ));
            }
        }
        if let Some(index) = task_source_index(section) {
            let (_, _, oid, _) = sources.get(index).ok_or_else(|| {
                Error::new("not_found", "saved task source is not in this revision")
            })?;
            redact_record(&read_blob(&git, &root, oid, deadline).await?, policy)
        } else {
            let inputs = task_inputs::read_inputs(&git, &root, &commit, &files, deadline).await?;
            let referenced = if files.iter().any(|(_, _, _, name)| name == "card.json") {
                let card: Value = serde_json::from_str(
                    &read_blob(&git, &root, &format!("{commit}:card.json"), deadline).await?,
                )
                .map_err(|_| Error::new("source_integrity", "saved task card is malformed"))?;
                card.get("referenced_files")
                    .and_then(Value::as_array)
                    .map(|files| {
                        files
                            .iter()
                            .filter(|file| {
                                file["captured"]
                                    .as_str()
                                    .and_then(|path| path.strip_prefix("sources/"))
                                    .is_none_or(|saved| {
                                        !sources.iter().any(|(_, _, _, name)| name == saved)
                                    })
                            })
                            .count()
                    })
            } else {
                None
            };
            let omitted = inputs
                .omitted
                .values()
                .copied()
                .fold(
                    inputs.compacted.map_or(0, |omissions| omissions.count),
                    usize::saturating_add,
                )
                .max(inputs.compacted_omissions);
            let manifest = TaskSnapshotManifest {
                correction_history_incomplete: inputs.history_incomplete
                    || inputs.original.is_none()
                    || omitted > 0,
                correction_history_unresolved: inputs.history_unresolved,
                corrections_retained: inputs.corrections.len(),
                corrections_omitted_at_least: omitted,
                task_saved: files
                    .iter()
                    .any(|(_, kind, _, name)| kind == "blob" && name == "task.md"),
                instructions_saved: files
                    .iter()
                    .any(|(_, kind, _, name)| kind == "blob" && name == "instructions.json"),
                original: inputs.original.map(|origin| TaskSnapshotOrigin {
                    command_id: origin.command_id,
                    agent: origin.source,
                    generation: origin.generation,
                    principal: origin.principal,
                    verb: origin.verb,
                    state: origin.state,
                }),
                references_without_saved_content: referenced,
                sources: sources
                    .iter()
                    .enumerate()
                    .map(|(index, (_, _, _, name))| TaskSnapshotSource {
                        section: format!("source:{index}"),
                        name: name.clone(),
                    })
                    .collect(),
            };
            let mut rendered = serde_json::to_value(&manifest)
                .map_err(|_| Error::new("internal", "task manifest encoding failed"))?;
            redact_record_value(&mut rendered, policy);
            rendered.to_string()
        }
    } else {
        let spec = format!("{commit}:{section}");
        redact_record(&read_blob(&git, &root, &spec, deadline).await?, policy)
    };
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

async fn read_blob(git: &GitProbe, root: &Path, spec: &str, deadline: Instant) -> Result<String> {
    let size = git
        .workspace_command(root, &["cat-file", "-s", spec], deadline)
        .await?;
    if size.code != 0 {
        return Err(Error::new("unavailable", "saved task entry is unreadable"));
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
        .workspace_command(root, &["cat-file", "blob", spec], deadline)
        .await?;
    if output.code != 0 || output.stdout.len() != size {
        return Err(Error::new("unavailable", "saved task entry is unreadable"));
    }
    String::from_utf8(output.stdout)
        .map_err(|_| Error::new("source_integrity", "saved task entry is not valid UTF-8"))
}
