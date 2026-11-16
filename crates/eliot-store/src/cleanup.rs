//! Artifact custody and durable, non-replayed cleanup attempts. SQL lives here.
use super::coordination::{Sender, parse_sender, retain_write_tx};
use super::*;
use eliot_core::api::*;

impl Store {
    pub fn known_artifact_path(&self, path: &str) -> Result<Option<ArtifactView>> {
        let body: Option<String> = self
            .connection
            .query_row("SELECT body FROM artifacts WHERE path=?1", [path], |r| {
                r.get(0)
            })
            .optional()
            .map_err(sql_error)?;
        body.map(|s| {
            serde_json::from_str(&s).map_err(|_| Error::new("source_integrity", "invalid artifact"))
        })
        .transpose()
    }

    /// Internal, non-deletable custody row committed before the native Agent
    /// receives its new directory. Legacy paths are never adopted as roots.
    pub fn agent_artifact_root(
        &self,
        agent: &str,
        generation: i64,
    ) -> Result<Option<ArtifactView>> {
        let body: Option<String> = self.connection.query_row(
            "SELECT body FROM artifacts WHERE agent=?1 AND generation=?2 AND json_extract(body,'$.state')='ownership_root'",
            params![parse_agent_id(agent)?, generation], |row| row.get(0),
        ).optional().map_err(sql_error)?;
        body.map(|body| {
            serde_json::from_str(&body)
                .map_err(|_| Error::new("source_integrity", "invalid artifact root"))
        })
        .transpose()
    }
    pub fn cleanup_visible_agent(&self, principal: &str, agent: &Agent) -> Result<bool> {
        match parse_sender(principal, "cleanup")? {
            Sender::Owner => Ok(true),
            Sender::Agent { id, generation, .. } => {
                let manager = self
                    .get_agent(&id)?
                    .ok_or_else(|| Error::new("not_found", "cleanup manager absent"))?;
                if manager.generation != generation {
                    return Err(Error::new(
                        "stale_generation",
                        "cleanup manager generation changed",
                    ));
                }
                let mut current = Some(agent.clone());
                for _ in 0..64 {
                    let Some(row) = current else {
                        return Ok(false);
                    };
                    if row.id == id {
                        return Ok(true);
                    }
                    current = row
                        .parent
                        .as_deref()
                        .map(|p| self.get_agent(p))
                        .transpose()?
                        .flatten();
                }
                Ok(false)
            }
        }
    }
    pub fn artifact_workspace_exclusive(&self, workspace: &str) -> Result<bool> {
        let count: i64 = self
            .connection
            .query_row(
                "SELECT COUNT(*) FROM agents WHERE workspace=?1",
                [workspace],
                |r| r.get(0),
            )
            .map_err(sql_error)?;
        Ok(count == 1)
    }
    /// Derive ownership only from an exact retained generation and its frozen
    /// workspace/profile. Never imports an unregistered filesystem inventory.
    pub fn record_known_artifact(&mut self, mut artifact: ArtifactView) -> Result<()> {
        self.ensure_writer_healthy()?;
        let agent = self
            .get_agent(&artifact.agent)?
            .ok_or_else(|| Error::new("not_found", "artifact Agent absent"))?;
        if agent.generation != artifact.generation {
            return Err(Error::new(
                "stale_generation",
                "automatic artifact generation changed",
            ));
        }
        artifact.tree.clear();
        let mut current = Some(agent);
        for _ in 0..64 {
            let Some(row) = current else {
                break;
            };
            artifact.tree.push(format!("{}:{}", row.id, row.generation));
            current = row
                .parent
                .as_deref()
                .map(|p| self.get_agent(p))
                .transpose()?
                .flatten();
        }
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        let count: i64 = tx
            .query_row("SELECT COUNT(*) FROM artifacts", [], |r| r.get(0))
            .map_err(sql_error)?;
        if count >= 4096 {
            return Err(Error::new("unavailable", "artifact ledger is full"));
        }
        let changed = tx.execute("INSERT OR IGNORE INTO artifacts(id,agent,generation,path,body) VALUES (?1,?2,?3,?4,?5)",params![artifact.id,parse_agent_id(&artifact.agent)?,artifact.generation,artifact.path,encode_json(&artifact)?]).map_err(sql_error)?;
        if changed == 0 && artifact.kind == ArtifactKind::Branch {
            // Branches move as their agent commits: refresh the tracked tip
            // so the plan snapshot anchors exactly what apply re-verifies.
            // Identity, tree and git binding follow the live branch; row id,
            // creation time and tombstone state stay untouched.
            let body: Option<String> = tx
                .query_row(
                    "SELECT body FROM artifacts WHERE path=?1",
                    params![artifact.path],
                    |row| row.get(0),
                )
                .optional()
                .map_err(sql_error)?;
            if let Some(body) = body
                && let Ok(mut stored) = serde_json::from_str::<ArtifactView>(&body)
                && stored.kind == ArtifactKind::Branch
            {
                stored.identity = artifact.identity.clone();
                stored.tree.clone_from(&artifact.tree);
                stored.git.clone_from(&artifact.git);
                tx.execute(
                    "UPDATE artifacts SET body=?1 WHERE path=?2",
                    params![encode_json(&stored)?, artifact.path],
                )
                .map_err(sql_error)?;
            }
        }
        commit_transaction(tx, &self.write_faulted)
    }
    /// Registered branch rows for one agent generation, including the
    /// retained git binding used to re-resolve the live tip after the
    /// worktree is gone.
    pub fn registered_branches(
        &self,
        agent_id: &str,
        generation: i64,
    ) -> Result<Vec<ArtifactView>> {
        let mut statement = self
            .connection
            .prepare(
                "SELECT body FROM artifacts WHERE agent=?1 AND generation=?2 AND json_extract(body,'$.kind')='branch' AND json_extract(body,'$.state')='registered'",
            )
            .map_err(sql_error)?;
        let rows = statement
            .query_map(params![parse_agent_id(agent_id)?, generation], |row| {
                row.get::<_, String>(0)
            })
            .map_err(sql_error)?;
        let mut out = Vec::new();
        for body in rows {
            let artifact: ArtifactView = serde_json::from_str(&body.map_err(sql_error)?)
                .map_err(|_| Error::new("source_integrity", "invalid artifact ledger row"))?;
            out.push(artifact);
        }
        Ok(out)
    }
    pub fn cleanup_authorized(&self, principal: &str, artifact: &ArtifactView) -> Result<bool> {
        match parse_sender(principal, "cleanup")? {
            Sender::Owner => Ok(true),
            Sender::Agent { id, generation, .. } => {
                let manager = self
                    .get_agent(&id)?
                    .ok_or_else(|| Error::new("not_found", "cleanup manager absent"))?;
                if manager.generation != generation || cleanup_terminal(manager.state) {
                    return Err(Error::new(
                        "stale_generation",
                        "cleanup manager is not current",
                    ));
                }
                if !artifact.tree.contains(&format!("{id}:{generation}")) {
                    return Ok(false);
                }
                let mut current = self.get_agent(&artifact.agent)?;
                for _ in 0..64 {
                    let Some(row) = current else {
                        return Ok(false);
                    };
                    if row.id == id {
                        return Ok(true);
                    }
                    current = row
                        .parent
                        .as_deref()
                        .map(|p| self.get_agent(p))
                        .transpose()?
                        .flatten();
                }
                Ok(false)
            }
        }
    }

    pub fn cleanup_inventory(
        &self,
        principal: &str,
        request: &CleanupPlanRequest,
    ) -> Result<(Vec<ArtifactView>, bool)> {
        request.validate()?;
        let caller = parse_sender(principal, "cleanup")?;
        if !matches!(caller, Sender::Owner) && request.scope != CleanupScope::SelfTree {
            return Err(Error::new(
                "not_allowed",
                "Agents may clean only their own subtree",
            ));
        }
        if matches!(caller, Sender::Owner) && request.scope == CleanupScope::SelfTree {
            return Err(Error::new("invalid", "Owner must select project or all"));
        }
        let mut statement = self
            .connection
            .prepare("SELECT body FROM artifacts ORDER BY id LIMIT 4096")
            .map_err(sql_error)?;
        let bodies = statement
            .query_map([], |r| r.get::<_, String>(0))
            .map_err(sql_error)?;
        let mut items = Vec::new();
        let mut truncated = false;
        for body in bodies {
            let artifact: ArtifactView = serde_json::from_str(&body.map_err(sql_error)?)
                .map_err(|_| Error::new("source_integrity", "invalid artifact ledger row"))?;
            if artifact.state == "ownership_root" {
                continue;
            }
            if request
                .project
                .as_ref()
                .is_some_and(|p| p != &artifact.project)
                || request.older_than.is_some_and(|t| artifact.created_at >= t)
                || !self.cleanup_authorized(principal, &artifact)?
            {
                continue;
            }
            if items.len() == MAX_CLEANUP_ITEMS {
                truncated = true;
                break;
            }
            items.push(artifact);
        }
        Ok((items, truncated))
    }

    pub fn cleanup_replay(
        &self,
        principal: &str,
        id: &str,
        method: &str,
        payload: &Value,
    ) -> Result<Option<Value>> {
        let digest = canonical::digest(payload)?;
        if let Some((p, m, d, result)) = write_binding(&self.connection, id)? {
            if p != principal || m != method || d != digest {
                return Err(Error::new(
                    "id_conflict",
                    "cleanup write id has another binding",
                ));
            }
            return Ok(Some(decode_json(&result)?));
        }
        if command_binding(&self.connection, id)?.is_some()
            || digest_id_bound(&self.connection, id)?
        {
            return Err(Error::new("id_conflict", "cleanup id is already in use"));
        }
        Ok(None)
    }

    /// Called after the Router pins and validates the existing local directory.
    pub fn register_artifact(
        &mut self,
        principal: &str,
        request: &ArtifactRegisterRequest,
        path: &str,
        identity: &str,
        now: i64,
    ) -> Result<ArtifactView> {
        self.ensure_writer_healthy()?;
        request.validate()?;
        let payload = serde_json::to_value(request)
            .map_err(|_| Error::new("invalid", "artifact cannot encode"))?;
        if let Some(value) =
            self.cleanup_replay(principal, &request.id, "artifact.register", &payload)?
        {
            return serde_json::from_value(value)
                .map_err(|_| Error::new("source_integrity", "invalid retained artifact"));
        }
        validate_id_age(&request.id, now)?;
        let Sender::Agent {
            num, generation, ..
        } = parse_sender(principal, "artifact")?
        else {
            return Err(Error::new(
                "not_allowed",
                "artifact registration requires an Agent",
            ));
        };
        let agent = self
            .get_agent(&format!("a{num}"))?
            .ok_or_else(|| Error::new("not_found", "artifact Agent is absent"))?;
        if agent.generation != generation || cleanup_terminal(agent.state) {
            return Err(Error::new(
                "stale_generation",
                "artifact needs a current live Agent",
            ));
        }
        let root = self
            .agent_artifact_root(&agent.id, generation)?
            .ok_or_else(|| Error::new("not_allowed", "Agent has no Swarm-created artifact root"))?;
        let normalized = |value: &str| {
            std::path::PathBuf::from(if cfg!(windows) {
                value.to_lowercase()
            } else {
                value.to_owned()
            })
        };
        let proposed = normalized(path);
        let owned = normalized(&root.path);
        if proposed == owned || !proposed.starts_with(&owned) {
            return Err(Error::new(
                "not_allowed",
                "artifact is outside the Agent generation's durable root",
            ));
        }
        let mut tree = Vec::new();
        let mut current = Some(agent.clone());
        while let Some(row) = current {
            if tree.len() >= 64 {
                return Err(Error::new("unavailable", "artifact ancestry exceeds bound"));
            }
            tree.push(format!("{}:{}", row.id, row.generation));
            current = row
                .parent
                .as_deref()
                .map(|id| self.get_agent(id))
                .transpose()?
                .flatten();
        }
        let artifact = ArtifactView {
            id: request.id.clone(),
            agent: agent.id,
            generation,
            project: agent.project,
            tree,
            path: path.to_owned(),
            kind: request.kind,
            purpose: request.purpose.clone(),
            identity: identity.to_owned(),
            created_at: now,
            state: "registered".to_owned(),
            git: None,
        };
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        {
            let mut query = tx
                .prepare("SELECT path FROM artifacts LIMIT 4096")
                .map_err(sql_error)?;
            let paths = query
                .query_map([], |r| r.get::<_, String>(0))
                .map_err(sql_error)?;
            for existing in paths {
                let existing = existing.map_err(sql_error)?;
                if existing == root.path {
                    continue;
                }
                let existing = std::path::PathBuf::from(if cfg!(windows) {
                    existing.to_lowercase()
                } else {
                    existing
                });
                let proposed = std::path::PathBuf::from(if cfg!(windows) {
                    path.to_lowercase()
                } else {
                    path.to_owned()
                });
                if existing.starts_with(&proposed) || proposed.starts_with(&existing) {
                    return Err(Error::new(
                        "conflict",
                        "artifact overlaps an existing ownership record",
                    ));
                }
            }
        }
        let count: i64 = tx
            .query_row("SELECT COUNT(*) FROM artifacts", [], |r| r.get(0))
            .map_err(sql_error)?;
        if count >= 4096 {
            return Err(Error::new(
                "unavailable",
                "artifact ledger is full (4096 entries)",
            ));
        }
        let body = encode_json(&artifact)?;
        tx.execute(
            "INSERT INTO artifacts(id,agent,generation,path,body) VALUES (?1,?2,?3,?4,?5)",
            params![artifact.id, num, generation, artifact.path, body],
        )
        .map_err(sql_error)?;
        retain_write_tx(
            &tx,
            &request.id,
            principal,
            "artifact.register",
            &canonical::digest(&payload)?,
            &serde_json::to_value(&artifact)
                .map_err(|_| Error::new("invalid", "artifact cannot encode"))?,
            now,
        )?;
        commit_transaction(tx, &self.write_faulted)?;
        Ok(artifact)
    }

    pub fn retain_cleanup_plan(&mut self, principal: &str, plan: &CleanupPlan) -> Result<()> {
        self.ensure_writer_healthy()?;
        if plan.items.len() > MAX_CLEANUP_ITEMS {
            return Err(Error::new("invalid", "cleanup plan exceeds bound"));
        }
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        let count: i64 = tx
            .query_row("SELECT COUNT(*) FROM cleanup_plans", [], |r| r.get(0))
            .map_err(sql_error)?;
        if count >= 1024 {
            return Err(Error::new(
                "unavailable",
                "cleanup plan ledger is full (1024 entries)",
            ));
        }
        tx.execute(
            "INSERT INTO cleanup_plans(id,principal,body) VALUES (?1,?2,?3)",
            params![plan.plan_id, principal, encode_json(plan)?],
        )
        .map_err(sql_error)?;
        commit_transaction(tx, &self.write_faulted)
    }

    pub fn cleanup_plan(&self, principal: &str, id: &str) -> Result<CleanupPlan> {
        let row: Option<(String, String)> = self
            .connection
            .query_row(
                "SELECT principal,body FROM cleanup_plans WHERE id=?1",
                [id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()
            .map_err(sql_error)?;
        let (author, body) = row.ok_or_else(|| Error::new("not_found", "cleanup plan absent"))?;
        if author != principal {
            return Err(Error::new(
                "not_allowed",
                "cleanup plan belongs to another principal",
            ));
        }
        serde_json::from_str(&body)
            .map_err(|_| Error::new("source_integrity", "invalid cleanup plan row"))
    }

    /// Persist unknown outcomes before any external effect. A consumed plan cannot
    /// be applied under a different id, even after write-history retention.
    pub fn begin_cleanup_apply(
        &mut self,
        principal: &str,
        request: &CleanupApplyRequest,
        now: i64,
    ) -> Result<(CleanupPlan, CleanupApplyResponse, bool)> {
        self.ensure_writer_healthy()?;
        request.validate()?;
        let payload = serde_json::to_value(request)
            .map_err(|_| Error::new("invalid", "cleanup request cannot encode"))?;
        let plan = self.cleanup_plan(principal, &request.plan_id)?;
        if let Some(value) =
            self.cleanup_replay(principal, &request.id, "cleanup.apply", &payload)?
        {
            return Ok((
                plan,
                serde_json::from_value(value)
                    .map_err(|_| Error::new("source_integrity", "invalid cleanup receipt"))?,
                false,
            ));
        }
        validate_id_age(&request.id, now)?;
        let mut response = CleanupApplyResponse {
            plan_id: plan.plan_id.clone(),
            bytes_reclaimed: 0,
            items: plan
                .items
                .iter()
                .map(|item| CleanupItemOutcome {
                    artifact_id: item.artifact.id.clone(),
                    outcome: if item.verdict == "safe" {
                        "unknown"
                    } else {
                        "skipped"
                    }
                    .to_owned(),
                    reason: Some(
                        if item.verdict == "safe" {
                            "attempt_reserved"
                        } else {
                            &item.reason
                        }
                        .to_owned(),
                    ),
                    bytes_reclaimed: 0,
                })
                .collect(),
        };
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        let changed = tx
            .execute(
                "UPDATE cleanup_plans SET apply_id=?1 WHERE id=?2 AND apply_id IS NULL",
                params![request.id, request.plan_id],
            )
            .map_err(sql_error)?;
        if changed != 1 {
            return Err(Error::new(
                "bad_state",
                "cleanup plan is already consumed; read the original apply id",
            ));
        }
        // Fence every attempted artifact across new plans and daemon restarts.
        for (index, item) in plan.items.iter().enumerate() {
            if item.verdict == "safe" {
                let body: Option<String> = tx
                    .query_row(
                        "SELECT body FROM artifacts WHERE id=?1",
                        [&item.artifact.id],
                        |r| r.get(0),
                    )
                    .optional()
                    .map_err(sql_error)?;
                let current: Option<ArtifactView> = body
                    .map(|body| {
                        serde_json::from_str(&body)
                            .map_err(|_| Error::new("source_integrity", "invalid artifact row"))
                    })
                    .transpose()?;
                if !current.is_some_and(|a| {
                    a.state == "registered"
                        && a.identity == item.artifact.identity
                        && a.generation == item.artifact.generation
                }) {
                    response.items[index].outcome = "skipped".to_owned();
                    response.items[index].reason = Some("prior_cleanup_attempt".to_owned());
                    continue;
                }
                let mut artifact = item.artifact.clone();
                artifact.state = "unknown".to_owned();
                tx.execute(
                    "UPDATE artifacts SET body=?1 WHERE id=?2",
                    params![encode_json(&artifact)?, artifact.id],
                )
                .map_err(sql_error)?;
            }
        }
        retain_write_tx(
            &tx,
            &request.id,
            principal,
            "cleanup.apply",
            &canonical::digest(&payload)?,
            &serde_json::to_value(&response)
                .map_err(|_| Error::new("invalid", "cleanup response cannot encode"))?,
            now,
        )?;
        commit_transaction(tx, &self.write_faulted)?;
        Ok((plan, response, true))
    }

    pub fn cleanup_agent_quiescent(&self, artifact: &ArtifactView) -> Result<bool> {
        let Some(agent) = self.get_agent(&artifact.agent)? else {
            return Ok(false);
        };
        if agent.generation != artifact.generation || !cleanup_terminal(agent.state) {
            return Ok(false);
        }
        if artifact.purpose == "Swarm chat directory" {
            let Some(workspace) = agent.workspace.as_deref() else {
                return Ok(false);
            };
            if !agent.chat || !self.artifact_workspace_exclusive(workspace)? {
                return Ok(false);
            }
        }
        let sources: bool = self.connection.query_row("SELECT EXISTS(SELECT 1 FROM source_state WHERE agent=?1 AND state='open' AND source NOT IN ('daemon','os','git','workspace_process'))",[parse_agent_id(&agent.id)?],|r|r.get(0)).map_err(sql_error)?;
        let pending: bool = self.connection.query_row("SELECT EXISTS(SELECT 1 FROM commands WHERE target=?1 AND state IN ('accepted','queued','dispatched','unknown'))",[parse_agent_id(&agent.id)?],|r|r.get(0)).map_err(sql_error)?;
        let live_process: bool = self.connection.query_row("SELECT EXISTS(SELECT 1 FROM processes WHERE owner IN (?1,?2) AND state != 'exited')",params![format!("agent:{}#{}",agent.id,agent.generation),format!("workspace:{}#{}",agent.id,agent.generation)],|r|r.get(0)).map_err(sql_error)?;
        Ok(!sources && !pending && !live_process)
    }

    pub fn settle_cleanup_item(
        &mut self,
        principal: &str,
        request: &CleanupApplyRequest,
        response: &CleanupApplyResponse,
        artifact: &ArtifactView,
        outcome: &CleanupItemOutcome,
    ) -> Result<()> {
        self.ensure_writer_healthy()?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        let changed = tx.execute("UPDATE writes SET result=?1 WHERE id=?2 AND principal=?3 AND method='cleanup.apply'",params![encode_json(response)?,request.id,principal]).map_err(sql_error)?;
        if changed != 1 {
            return Err(Error::new(
                "source_integrity",
                "cleanup receipt disappeared",
            ));
        }
        let mut updated = artifact.clone();
        updated.state = match outcome.outcome.as_str() {
            "removed" => "removed",
            "skipped" => "registered",
            _ => "unknown",
        }
        .to_owned();
        tx.execute(
            "UPDATE artifacts SET body=?1,path=?3 WHERE id=?2",
            params![
                encode_json(&updated)?,
                artifact.id,
                if outcome.outcome == "removed" {
                    format!("retired:{}", artifact.id)
                } else {
                    artifact.path.clone()
                }
            ],
        )
        .map_err(sql_error)?;
        if artifact.kind == ArtifactKind::Worktree && outcome.outcome == "removed" {
            let git = artifact
                .git
                .as_ref()
                .ok_or_else(|| Error::new("source_integrity", "worktree Git binding absent"))?;
            let result = WorkspaceRemovalOutcome::Removed {
                repo_common_dir: git.common.clone(),
                worktree_path: artifact.path.clone(),
                branch: git.branch.clone(),
                branch_retained: true,
            };
            tx.execute("UPDATE workspaces SET phase='removed',outcome=?1 WHERE agent=?2 AND target_generation=?3 AND worktree_path=?4",params![encode_json(&result)?,parse_agent_id(&artifact.agent)?,artifact.generation,artifact.path]).map_err(sql_error)?;
        }
        commit_transaction(tx, &self.write_faulted)
    }
}

fn cleanup_terminal(state: AgentState) -> bool {
    matches!(state, AgentState::Stopped | AgentState::Failed)
}

/// Fence resume at admission and dispatch while an attempted cleanup is unknown.
pub(super) fn require_no_cleanup_attempt(connection: &Connection, agent: &Agent) -> Result<()> {
    let pending: bool = connection.query_row("SELECT EXISTS(SELECT 1 FROM artifacts WHERE agent=?1 AND generation=?2 AND json_extract(body,'$.state')='unknown')",params![parse_agent_id(&agent.id)?,agent.generation],|r|r.get(0)).map_err(sql_error)?;
    if pending {
        return Err(Error::new(
            "bad_state",
            "Agent has an unresolved cleanup attempt",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registration_requires_durable_generation_root() {
        let mut store = Store::open(Path::new(":memory:")).unwrap();
        let now = i64::try_from(Ulid::new().timestamp_ms()).unwrap();
        store.connection.execute("INSERT INTO agents(project,harness,profile,state,generation,created_at,updated_at) VALUES ('p','codex','p','idle',1,?1,?1)",[now]).unwrap();
        let root = ArtifactView {
            id: Ulid::new().to_string(),
            agent: "a1".to_owned(),
            generation: 1,
            project: "p".to_owned(),
            tree: Vec::new(),
            path: "C:/scratch/a1-1".to_owned(),
            kind: ArtifactKind::Other,
            purpose: "Swarm artifact root".to_owned(),
            identity: "root".to_owned(),
            created_at: now,
            state: "ownership_root".to_owned(),
            git: None,
        };
        store.record_known_artifact(root).unwrap();
        for path in [
            "C:/scratch/a2-1/output",
            "C:/scratch/owner/output",
            "C:/scratch/a1-1",
        ] {
            let request = ArtifactRegisterRequest {
                id: Ulid::new().to_string(),
                path: path.to_owned(),
                kind: ArtifactKind::Temp,
                purpose: "scratch".to_owned(),
            };
            assert!(
                store
                    .register_artifact("agent:a1:1", &request, path, "leaf", now)
                    .is_err(),
                "claimed {path}"
            );
        }
        let path = "C:/scratch/a1-1/output";
        let request = ArtifactRegisterRequest {
            id: Ulid::new().to_string(),
            path: path.to_owned(),
            kind: ArtifactKind::Temp,
            purpose: "scratch".to_owned(),
        };
        store
            .register_artifact("agent:a1:1", &request, path, "leaf", now)
            .unwrap();
    }

    #[test]
    fn consumed_plan_replays_unknown_and_blocks_another_attempt() {
        let mut store = Store::open(Path::new(":memory:")).unwrap();
        let now = i64::try_from(Ulid::new().timestamp_ms()).unwrap();
        store.connection.execute("INSERT INTO agents(project,harness,profile,state,generation,created_at,updated_at) VALUES ('p','codex','p','stopped',1,?1,?1)",[now]).unwrap();
        let artifact = ArtifactView {
            id: Ulid::new().to_string(),
            agent: "a1".to_owned(),
            generation: 1,
            project: "p".to_owned(),
            tree: vec!["a1:1".to_owned()],
            path: "C:/scratch/target".to_owned(),
            kind: ArtifactKind::Build,
            purpose: "build".to_owned(),
            identity: "original".to_owned(),
            created_at: now,
            state: "registered".to_owned(),
            git: None,
        };
        store.record_known_artifact(artifact.clone()).unwrap();
        let plan = CleanupPlan {
            plan_id: Ulid::new().to_string(),
            created_at: now,
            items: vec![CleanupCandidate {
                artifact: artifact.clone(),
                bytes: Some(7),
                verdict: "safe".to_owned(),
                reason: "terminal".to_owned(),
            }],
            truncated: false,
        };
        let mut other_plan = plan.clone();
        other_plan.plan_id = Ulid::new().to_string();
        store.retain_cleanup_plan("owner", &plan).unwrap();
        store.retain_cleanup_plan("owner", &other_plan).unwrap();
        let request = CleanupApplyRequest {
            id: Ulid::new().to_string(),
            plan_id: plan.plan_id.clone(),
            delete_remote: false,
        };
        let (_, mut first, is_new) = store.begin_cleanup_apply("owner", &request, now).unwrap();
        assert!(is_new);
        assert_eq!(first.items[0].outcome, "unknown");
        first.items[0].reason = Some("malformed_archive_acknowledgement".to_owned());
        let outcome = first.items[0].clone();
        store
            .settle_cleanup_item("owner", &request, &first, &artifact, &outcome)
            .unwrap();
        let (_, replay, is_new) = store.begin_cleanup_apply("owner", &request, now).unwrap();
        assert!(!is_new);
        assert_eq!(
            serde_json::to_value(first).unwrap(),
            serde_json::to_value(replay).unwrap()
        );
        let different = CleanupApplyRequest {
            id: Ulid::new().to_string(),
            plan_id: plan.plan_id,
            delete_remote: false,
        };
        assert!(store.begin_cleanup_apply("owner", &different, now).is_err());
        let other = CleanupApplyRequest {
            id: Ulid::new().to_string(),
            plan_id: other_plan.plan_id,
            delete_remote: false,
        };
        let (_, result, _) = store.begin_cleanup_apply("owner", &other, now).unwrap();
        assert_eq!(result.items[0].outcome, "skipped");
        assert_eq!(
            result.items[0].reason.as_deref(),
            Some("prior_cleanup_attempt")
        );
        let agent = store.get_agent("a1").unwrap().unwrap();
        assert!(require_no_cleanup_attempt(&store.connection, &agent).is_err());
    }

    #[test]
    fn branch_backfill_refreshes_tracked_tip_without_changing_row_identity() {
        let mut store = Store::open(Path::new(":memory:")).unwrap();
        let now = i64::try_from(Ulid::new().timestamp_ms()).unwrap();
        store.connection.execute("INSERT INTO agents(project,harness,profile,state,generation,created_at,updated_at) VALUES ('p','codex','p','stopped',1,?1,?1)",[now]).unwrap();
        let branch = |tip: &str| ArtifactView {
            id: Ulid::new().to_string(),
            agent: "a1".to_owned(),
            generation: 1,
            project: "p".to_owned(),
            tree: vec!["a1:1".to_owned()],
            path: "C:/repo/.git::refs/heads/k".to_owned(),
            kind: ArtifactKind::Branch,
            purpose: "Swarm managed branch".to_owned(),
            identity: tip.to_owned(),
            created_at: now,
            state: "registered".to_owned(),
            git: None,
        };
        store.record_known_artifact(branch("base")).unwrap();
        store.record_known_artifact(branch("tip")).unwrap();
        let request = CleanupPlanRequest {
            scope: CleanupScope::All,
            project: None,
            older_than: None,
        };
        let (items, truncated) = store.cleanup_inventory("owner", &request).unwrap();
        assert!(!truncated);
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].identity, "tip");
        assert_eq!(items[0].state, "registered");
    }
}
