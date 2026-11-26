use super::*;
use eliot_core::GitPathStatus;
use serde_json::json;

const METHOD: &str = "workspace.verify_work_commit";

#[derive(Deserialize, Serialize)]
pub struct WorkCommitRequest {
    pub id: String,
    pub agent: String,
    pub generation: i64,
    pub observation_seq: i64,
    pub retained_ref: String,
    pub commit: String,
}

#[derive(Serialize)]
pub struct WorkCommitProof {
    pub observation_seq: i64,
    pub head: String,
    pub dirty_digest: String,
    pub retained_ref: String,
    pub ref_tip: String,
    pub commit: String,
}

#[derive(Deserialize)]
struct Observation {
    head: String,
    dirty_digest: Option<String>,
    dirty: Vec<GitPathStatus>,
}

pub(crate) struct MissingWorkObservation {
    pub seq: i64,
    pub dirty: Vec<GitPathStatus>,
    head: String,
    digest: Option<String>,
}

impl MissingWorkObservation {
    pub fn supports_reconciliation(&self) -> bool {
        self.digest.is_some()
            && !self.dirty.is_empty()
            && self.dirty.iter().all(|entry| {
                entry.original_path.is_none() && matches!(entry.status.as_str(), ".M" | ".D" | "??")
            })
    }
}

pub struct MissingWork {
    pub repo: String,
    pub common_dir: String,
    pub head: String,
    pub dirty: Vec<GitPathStatus>,
    pub digest: String,
}

pub(crate) fn recorded_missing_work(
    db: &Connection,
    agent: i64,
    generation: i64,
    workspace: &str,
    outcome: &Value,
) -> Result<Option<MissingWorkObservation>> {
    let marker = &outcome["last_available_git_observation"];
    let Some(observed_at) = marker["observed_at"].as_i64() else {
        return Ok(None);
    };
    let event: Option<(i64, String)> = db
        .query_row(
            "SELECT seq, data FROM events INDEXED BY project_overview_git
             WHERE kind = 'git.changed' AND agent = ?1 AND generation = ?2
               AND ts = ?3 AND source = 'git'
               AND json_extract(data, '$.workspace') = ?4
               AND json_extract(data, '$.complete') = 1
             ORDER BY seq DESC LIMIT 1",
            params![agent, generation, observed_at, workspace],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(sql_error)?;
    let Some((seq, data)) = event else {
        return Ok(None);
    };
    let Ok(observation) = serde_json::from_str::<Observation>(&data) else {
        return Ok(None);
    };
    if marker["head"].as_str() != Some(&observation.head)
        || marker["dirty_digest"].as_str() != observation.dirty_digest.as_deref()
        || marker["dirty_path_count"].as_u64() != Some(observation.dirty.len() as u64)
    {
        return Ok(None);
    }
    Ok(Some(MissingWorkObservation {
        seq,
        dirty: observation.dirty,
        head: observation.head,
        digest: observation.dirty_digest,
    }))
}

impl Store {
    pub fn work_commit_receipt(&self, request: &WorkCommitRequest) -> Result<Option<Value>> {
        work_commit_receipt(&self.connection, request)
    }

    pub fn missing_work(
        &self,
        request: &WorkCommitRequest,
        item: &OwnerInboxItemRef,
        now: i64,
    ) -> Result<MissingWork> {
        missing_work(&self.connection, request, item, now)
    }

    pub fn settle_work_commit(
        &mut self,
        request: &WorkCommitRequest,
        item: &OwnerInboxItemRef,
        proof: &WorkCommitProof,
        now: i64,
    ) -> Result<Value> {
        self.ensure_writer_healthy()?;
        let result = (|| {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            if let Some(receipt) = work_commit_receipt(&tx, request)? {
                return Ok(receipt);
            }
            if command_binding(&tx, &request.id)?.is_some() || digest_id_bound(&tx, &request.id)? {
                return Err(decision_id_conflict());
            }
            validate_id_age(&request.id, now)?;
            let observed = missing_work(&tx, request, item, now)?;
            if observed.digest != proof.dirty_digest
                || observed.head != proof.head
                || request.observation_seq != proof.observation_seq
                || request.commit != proof.commit
                || request.retained_ref != proof.retained_ref
            {
                return Err(stale_item());
            }
            let result = serde_json::to_value(proof)
                .map_err(|_| Error::new("internal", "could not encode work commit proof"))?;
            tx.execute(
                "UPDATE workspaces SET outcome = json_set(outcome, '$.work_in_git', json(?3))
                 WHERE agent = ?1 AND target_generation = ?2",
                params![
                    eliot_core::parse_agent_id(&request.agent)?,
                    request.generation,
                    result.to_string()
                ],
            )
            .map_err(sql_error)?;
            insert_write_tx(
                &tx,
                &request.id,
                "owner",
                METHOD,
                &canonical::digest(&json!(request))?,
                &result.to_string(),
                now,
            )
            .map_err(sql_error)?;
            commit_transaction(tx, &self.write_faulted)?;
            Ok(result)
        })();
        self.fault_on_persistence_error(&result);
        result
    }
}

fn work_commit_receipt(db: &Connection, request: &WorkCommitRequest) -> Result<Option<Value>> {
    let Some((principal, method, digest, result)) = write_binding(db, &request.id)? else {
        return Ok(None);
    };
    if principal != "owner" || method != METHOD || digest != canonical::digest(&json!(request))? {
        return Err(decision_id_conflict());
    }
    Ok(Some(decode_json(&result)?))
}

fn missing_work(
    db: &Connection,
    request: &WorkCommitRequest,
    item: &OwnerInboxItemRef,
    now: i64,
) -> Result<MissingWork> {
    if item.kind != OwnerInboxKind::WorkNotInGit
        || item.subject != format!("{}:g{}:worktree", request.agent, request.generation)
    {
        return Err(stale_item());
    }
    let current = current_item(db, item, now)?;
    if !current.choices.iter().any(|action| {
        action.method == METHOD
            && action.args["observation_seq"].as_i64() == Some(request.observation_seq)
    }) {
        return Err(stale_item());
    }
    let agent = eliot_core::parse_agent_id(&request.agent)?;
    let (repo, common_dir, workspace, outcome): (String, String, String, String) = db
        .query_row(
            "SELECT repo_path, repo_common_dir, worktree_path, outcome
             FROM workspaces WHERE agent = ?1 AND target_generation = ?2",
            params![agent, request.generation],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .map_err(sql_error)?;
    let observed = recorded_missing_work(
        db,
        agent,
        request.generation,
        &workspace,
        &decode_json(&outcome)?,
    )?
    .ok_or_else(stale_item)?;
    Ok(MissingWork {
        repo,
        common_dir,
        head: observed.head,
        dirty: observed.dirty,
        digest: observed.digest.ok_or_else(stale_item)?,
    })
}
