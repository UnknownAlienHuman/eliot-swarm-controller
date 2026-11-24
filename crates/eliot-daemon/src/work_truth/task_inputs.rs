use super::*;
use eliot_store::{
    BriefFacts, CompactedTaskOmissions, MAX_HANDOFF_TASK_BYTES, TaskInstruction,
    TaskOmissionObservation,
};

const INSTRUCTIONS: &str = "instructions.json";
const MAX_INSTRUCTIONS_BYTES: usize = 896 * 1024;
const MAX_OMISSION_SOURCES: usize = 32;

/// Leave room for the fixed RPC envelope and the bounded H2 facts, even
/// when those facts need JSON escaping.
const MAX_ENCODED_TASK_BYTES: usize = eliot_core::limits::MAX_FRAME_BYTES - 128 * 1024;

#[derive(Default)]
struct TaskBudget {
    text_bytes: usize,
    escaped_bytes: usize,
}

impl TaskBudget {
    fn reserve(&mut self, text: &str) -> Result<()> {
        self.text_bytes = self.text_bytes.saturating_add(text.len());
        if self.text_bytes > MAX_HANDOFF_TASK_BYTES {
            return Err(task_too_large());
        }
        let encoded = serde_json::to_vec(text)
            .map_err(|_| Error::new("internal", "cannot encode handoff task text"))?;
        self.escaped_bytes = self.escaped_bytes.saturating_add(encoded.len() - 2);
        if self.escaped_bytes > MAX_ENCODED_TASK_BYTES {
            return Err(task_too_large());
        }
        Ok(())
    }
}

async fn task_blob(
    git: &GitProbe,
    root: &Path,
    commit: &str,
    path: &str,
    deadline: Instant,
) -> Result<String> {
    let spec = format!("{commit}:{path}");
    let output = git
        .workspace_command(root, &["cat-file", "blob", &spec], deadline)
        .await?;
    if output.code != 0 {
        return Err(Error::new(
            "unavailable",
            format!("saved task entry {path} is unreadable"),
        ));
    }
    String::from_utf8(output.stdout)
        .map_err(|_| Error::new("source_integrity", "saved task entry is not valid UTF-8"))
}

async fn entries(
    git: &GitProbe,
    root: &Path,
    tree: &str,
    deadline: Instant,
) -> Result<Vec<(String, String, String, String)>> {
    let output = git
        .workspace_command(root, &["ls-tree", "-z", tree], deadline)
        .await?;
    if output.code != 0 {
        return Err(Error::new("unavailable", "saved task tree is unreadable"));
    }
    let output = String::from_utf8(output.stdout)
        .map_err(|_| Error::new("source_integrity", "saved task tree is not valid UTF-8"))?;
    output
        .split_terminator('\0')
        .map(|entry| {
            let invalid = || Error::new("source_integrity", "saved task tree entry is malformed");
            let (meta, name) = entry.split_once('\t').ok_or_else(invalid)?;
            let mut fields = meta.split(' ');
            let mode = fields.next().ok_or_else(invalid)?;
            let kind = fields.next().ok_or_else(invalid)?;
            let oid = fields.next().ok_or_else(invalid)?;
            Ok((
                mode.to_owned(),
                kind.to_owned(),
                oid.to_owned(),
                name.to_owned(),
            ))
        })
        .collect()
}

pub(crate) async fn read_handoff_task(
    data_root: &Path,
    task_ref: &str,
    task_command: String,
    policy: &SecretPolicy,
) -> Result<BriefFacts> {
    let git = GitProbe::new()?;
    let root = crate::work_truth_repo::existing(data_root).await?;
    let _read_lane = crate::work_truth_repo::write_lane().await;
    let deadline = Instant::now() + GIT_TIME;
    let commit = task_commit(&git, &root, task_ref, deadline)
        .await
        .ok_or_else(|| Error::new("unavailable", "bound Git task record is unreadable"))?;
    let files = entries(&git, &root, &commit, deadline).await?;
    let task_text =
        policy.redact_text(&task_blob(&git, &root, &commit, "task.md", deadline).await?);
    let mut budget = TaskBudget::default();
    budget.reserve(&task_text)?;
    let task_inputs = read_inputs(&git, &root, &commit, &files, deadline).await?;
    let mut note = String::new();
    let mut source_count = 0;
    if let Some((_, _, tree, _)) = files.iter().find(|entry| entry.3 == "sources") {
        let sources = entries(&git, &root, tree, deadline).await?;
        source_count = sources.len();
        note = format!(
            "\n\nSaved task sources from the Git record ({source_count} complete, quoted data):\n"
        );
        budget.reserve(&note)?;
        for (_, _, _, name) in sources {
            let path = format!("sources/{name}");
            let text = policy.redact_text(&task_blob(&git, &root, &commit, &path, deadline).await?);
            let heading = format!("\nSaved source {}\n", policy.redact_text(&name));
            let quoted = eliot_core::fence_untrusted_text(&text);
            budget.reserve(&heading)?;
            budget.reserve(&quoted)?;
            budget.reserve("\n")?;
            note.push_str(&heading);
            note.push_str(&quoted);
            note.push('\n');
        }
    }
    Ok(BriefFacts {
        task_command: Some(task_command),
        task_ref: Some(task_ref.to_owned()),
        task_note: Some(note),
        saved_sources: Some(source_count),
        task_text: Some(task_text),
        task_inputs,
        ..BriefFacts::default()
    })
}

fn task_too_large() -> Error {
    Error::new(
        "frame_too_large",
        "the mandatory Git task and saved sources exceed the handoff input budget",
    )
}

async fn read_inputs(
    git: &GitProbe,
    root: &Path,
    commit: &str,
    files: &[(String, String, String, String)],
    deadline: Instant,
) -> Result<TaskInputs> {
    if !files.iter().any(|entry| entry.3 == INSTRUCTIONS) {
        let card = task_blob(git, root, commit, "card.json", deadline).await?;
        let card: Value = serde_json::from_str(&card)
            .map_err(|_| Error::new("source_integrity", "legacy task card is malformed"))?;
        let original = match (
            card["task_command"].as_str(),
            card["agent"].as_str(),
            card["generation"].as_i64(),
        ) {
            (Some(id), Some(source), Some(generation)) => Some(TaskInstruction {
                command_id: id.to_owned(),
                source: source.to_owned(),
                generation,
                principal: "unverified".to_owned(),
                verb: "send".to_owned(),
                state: eliot_core::CommandState::Unknown,
                admission_order: None,
                text: String::new(),
            }),
            _ => None,
        };
        return Ok(TaskInputs {
            original,
            history_incomplete: true,
            ..TaskInputs::default()
        });
    }
    let text = task_blob(git, root, commit, INSTRUCTIONS, deadline).await?;
    serde_json::from_str(&text)
        .map_err(|_| Error::new("source_integrity", "saved task instructions are malformed"))
}

pub(crate) async fn record_task_inputs(
    data_root: &Path,
    task_ref: &str,
    original: Option<TaskInstruction>,
    corrections: Vec<TaskInstruction>,
    omitted: BTreeMap<String, TaskOmissionObservation>,
    history_unresolved: bool,
) -> Result<TaskInputs> {
    let git = GitProbe::new()?;
    let root = crate::work_truth_repo::existing(data_root).await?;
    let _write_lane = crate::work_truth_repo::write_lane().await;
    let deadline = Instant::now() + GIT_TIME;
    let commit = task_commit(&git, &root, task_ref, deadline)
        .await
        .ok_or_else(|| Error::new("unavailable", "bound Git task record is unreadable"))?;
    let mut files = entries(&git, &root, &commit, deadline).await?;
    let mut inputs = read_inputs(&git, &root, &commit, &files, deadline).await?;
    let previous = inputs.clone();
    inputs.history_unresolved |= history_unresolved;
    if let Some(original) = original
        && inputs
            .original
            .as_ref()
            .is_none_or(|current| current.command_id == original.command_id)
    {
        inputs.original = Some(original);
    }
    for (source, observation) in omitted {
        if source_is_compacted(&inputs, &source) {
            continue;
        }
        let retained = inputs
            .corrections
            .iter()
            .filter(|entry| {
                instruction_source(entry) == source
                    && entry
                        .admission_order
                        .is_none_or(|order| order <= observation.through)
            })
            .count();
        // Store scans are overlapping observations. Exclude any older
        // corrections still saved in Git before raising the lower bound.
        let count = inputs.omitted.entry(source.clone()).or_default();
        *count = (*count).max(observation.count.saturating_sub(retained));
        advance_omission_bound(&mut inputs, source, observation.through);
    }
    for correction in corrections {
        if let Some(old) = inputs
            .corrections
            .iter_mut()
            .find(|old| old.command_id == correction.command_id)
        {
            old.state = correction.state;
            old.admission_order = old.admission_order.or(correction.admission_order);
        } else if !correction.admission_order.is_some_and(|order| {
            inputs
                .omitted_through
                .get(&instruction_source(&correction))
                .is_some_and(|through| order <= *through)
        }) {
            // An exact correction recovered from an older source may already
            // be represented in a compacted aggregate without individual IDs.
            if source_is_compacted(&inputs, &instruction_source(&correction))
                && let Some(compacted) = &mut inputs.compacted
            {
                compacted.count = compacted.count.saturating_sub(1);
            }
            inputs.compacted_omissions = inputs.compacted_omissions.saturating_sub(1);
            inputs.corrections.push(correction);
        }
    }
    compact_omission_sources(&mut inputs);
    if previous == inputs {
        return Ok(inputs);
    }
    let mut encoded = encode_task_inputs(&inputs)?;
    while encoded.len() > MAX_INSTRUCTIONS_BYTES {
        if inputs.corrections.is_empty() {
            return Err(Error::new(
                "frame_too_large",
                "Git task instruction metadata exceeds its bound",
            ));
        }
        let removed = inputs.corrections.remove(0);
        let source = instruction_source(&removed);
        if source_is_compacted(&inputs, &source)
            && let Some(compacted) = &mut inputs.compacted
        {
            compacted.count = compacted.count.saturating_add(1);
        } else {
            let count = inputs.omitted.entry(source.clone()).or_default();
            *count = count.saturating_add(1);
            if let Some(order) = removed.admission_order {
                advance_omission_bound(&mut inputs, source, order);
            }
        }
        compact_omission_sources(&mut inputs);
        encoded = encode_task_inputs(&inputs)?;
    }
    let blob = write_blob(&git, &root, &encoded, deadline).await?;
    files.retain(|entry| entry.3 != INSTRUCTIONS);
    files.push((
        "100644".to_owned(),
        "blob".to_owned(),
        blob,
        INSTRUCTIONS.to_owned(),
    ));
    let tree = write_tree(&git, &root, &files, deadline).await?;
    publish_task(&git, &root, task_ref, &tree, &commit, deadline).await?;
    Ok(inputs)
}

fn compact_omission_sources(inputs: &mut TaskInputs) {
    // Agent IDs grow monotonically along a handoff. Keep the largest source
    // identities to preserve deduplication for the current and recent sources.
    while inputs.omitted.len() > MAX_OMISSION_SOURCES {
        let source = inputs
            .omitted
            .keys()
            .min_by_key(|key| source_order(key))
            .cloned()
            .expect("omission map exceeds its nonzero bound");
        inputs.omitted_through.remove(&source);
        if let Some(omitted) = inputs.omitted.remove(&source) {
            if let Some(through) = source_order(&source) {
                let count = inputs
                    .compacted
                    .map_or(omitted, |prior| prior.count.saturating_add(omitted));
                inputs.compacted = Some(CompactedTaskOmissions { through, count });
            } else {
                inputs.compacted_omissions = inputs.compacted_omissions.max(omitted);
            }
        }
    }
}

fn instruction_source(input: &TaskInstruction) -> String {
    format!("{}/{}", input.source, input.generation)
}

fn source_order(key: &str) -> Option<(i64, i64)> {
    let (agent, generation) = key.split_once('/')?;
    Some((
        eliot_core::parse_agent_id(agent).ok()?,
        generation.parse().ok()?,
    ))
}

fn source_is_compacted(inputs: &TaskInputs, key: &str) -> bool {
    // An older generation can return through a handoff to an existing
    // Agent. Its count may already be in the compacted total.
    inputs
        .compacted
        .zip(source_order(key))
        .is_some_and(|(compacted, source)| source <= compacted.through)
}

fn advance_omission_bound(inputs: &mut TaskInputs, source: String, order: (i64, i64)) {
    let through = inputs.omitted_through.entry(source).or_insert(order);
    *through = (*through).max(order);
}
