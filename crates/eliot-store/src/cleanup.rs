//! Artifact custody and durable, non-replayed cleanup attempts. SQL lives here.
use super::coordination::{Sender, parse_sender, retain_write_tx};
use super::*;
use eliot_core::api::*;

/// Retained cleanup plans. A full ledger self-heals on retain (see
/// `prune_reclaimable_cleanup_plans_tx`) before it refuses admission.
const MAX_CLEANUP_PLANS: i64 = 1_024;
/// Reclaimable plans deleted per self-heal batch: fully settled consumed
/// plans and abandoned never-consumed previews, oldest first.
const CLEANUP_PLAN_RECLAIM_BATCH: i64 = 64;

/// An apply can read back its exact workspace command even after a crash
/// between native removal and cleanup receipt settlement.
pub fn cleanup_worktree_command_id(apply_id: &str, artifact_id: &str) -> Result<String> {
    let apply = validate_ulid(apply_id)?;
    let digest = canonical::digest(&json!(["worktree.remove", apply_id, artifact_id]))?;
    let mut randomness = [0; 16];
    randomness[6..].copy_from_slice(&digest[..10]);
    Ok(Ulid::from_parts(apply.timestamp_ms(), u128::from_be_bytes(randomness)).to_string())
}

impl Store {
    /// A bounded, stable-ID page of registered worktree artifacts whose
    /// retained workspace row is ready and whose owner has ended.
    pub fn auto_cleanup_worktree_inventory(
        &self,
        after: Option<&str>,
    ) -> Result<(Vec<ArtifactView>, bool)> {
        self.ensure_committed_reads()?;
        let mut statement = self
            .connection
            .prepare(
                "SELECT a.body FROM artifacts a
             JOIN workspaces w ON w.agent=a.agent AND w.target_generation=a.generation
               AND w.worktree_path=a.path AND w.phase='ready'
             JOIN agents g ON g.id=a.agent AND g.generation=a.generation
             WHERE a.id > COALESCE(?1, '') AND json_extract(a.body, '$.kind')='worktree'
               AND json_extract(a.body, '$.state')='registered'
               AND g.state IN ('stopped','failed','lost')
             ORDER BY a.id LIMIT 9",
            )
            .map_err(sql_error)?;
        let rows = statement
            .query_map([after], |row| row.get::<_, String>(0))
            .map_err(sql_error)?;
        let mut items = Vec::with_capacity(8);
        let mut more = false;
        for body in rows {
            let body = body.map_err(sql_error)?;
            if items.len() == 8 {
                more = true;
                break;
            }
            items
                .push(serde_json::from_str(&body).map_err(|_| {
                    Error::new("source_integrity", "invalid worktree artifact row")
                })?);
        }
        Ok((items, more))
    }

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
        if matches!(artifact.kind, ArtifactKind::Worktree | ArtifactKind::Branch)
            && let Some(git) = &artifact.git
            && let Some(workspace) = workspace::load_workspace_by_path(&tx, &git.worktree)?
            && workspace.agent != artifact.agent
        {
            return Err(Error::new(
                "stale_generation",
                "worktree artifact custody changed",
            ));
        }
        let count: i64 = tx
            .query_row("SELECT COUNT(*) FROM artifacts", [], |r| r.get(0))
            .map_err(sql_error)?;
        if count >= 4096 {
            return Err(Error::new("unavailable", "artifact ledger is full"));
        }
        let seq: i64 = tx
            .query_row("SELECT COALESCE(MAX(seq), 0) + 1 FROM artifacts", [], |r| {
                r.get(0)
            })
            .map_err(sql_error)?;
        let changed = tx.execute("INSERT OR IGNORE INTO artifacts(id,agent,generation,path,body,seq) VALUES (?1,?2,?3,?4,?5,?6)",params![artifact.id,parse_agent_id(&artifact.agent)?,artifact.generation,artifact.path,encode_json(&artifact)?,seq]).map_err(sql_error)?;
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
    /// Frozen and live subtree membership without consulting the project
    /// assignment gate. Mirrors the membership half of
    /// `cleanup_authorized` so the inventory scan can skip unrelated rows
    /// before their gate is allowed to fail the whole page.
    fn cleanup_subtree_member(&self, caller: &Sender, artifact: &ArtifactView) -> Result<bool> {
        let Sender::Agent { id, generation, .. } = caller else {
            return Ok(true);
        };
        if artifact
            .tree
            .iter()
            .any(|entry| entry == &format!("{id}:{generation}"))
        {
            return Ok(true);
        }
        let mut current = self.get_agent(&artifact.agent)?;
        for _ in 0..64 {
            let Some(row) = current else {
                return Ok(false);
            };
            if row.id == *id {
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
                project_assignment::authorize_coordination(
                    &self.connection,
                    principal,
                    Some(parse_agent_id(&artifact.agent)?),
                )?;
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

    /// Eligible cleanup candidates in final candidate order: ordinary rows
    /// first, then worktrees, then branches and native sessions, each oldest
    /// insertion first. Only `registered` rows paginate: settled rows
    /// (`removed`, `unknown`, `skipped(...)`) are attempt history, not
    /// candidates, and their readback lives in the retained plans. Filtering
    /// them before the page cap keeps a settled prefix from starving later
    /// eligible rows. The page is pinned to a ledger snapshot: rows inserted
    /// after the snapshot land on a fresh listing, never inside a page, and
    /// the cursor resolves against the same snapshot, so a concurrent
    /// registration can neither hide behind nor repeat a candidate. Returns
    /// the page, whether eligible rows remain, and the snapshot to echo.
    pub fn cleanup_inventory(
        &self,
        principal: &str,
        request: &CleanupPlanRequest,
    ) -> Result<(Vec<ArtifactView>, bool, i64)> {
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
        let page = request
            .continuation
            .as_deref()
            .map(PlanContinuation::decode)
            .transpose()?
            .unwrap_or(PlanContinuation {
                artifact_after: None,
                safe_skipped: 0,
                kept_skipped: 0,
                regs_revision: None,
                snapshot_seq: None,
                safe_ids: None,
                kept_ids: None,
                backfill_after: None,
            });
        let snapshot: i64 = match page.snapshot_seq {
            Some(snapshot) => snapshot,
            None => self
                .connection
                .query_row("SELECT COALESCE(MAX(seq), 0) FROM artifacts", [], |r| {
                    r.get(0)
                })
                .map_err(sql_error)?,
        };
        let cursor: Option<(u8, i64)> = match &page.artifact_after {
            None => None,
            Some(id) => {
                let (kind, seq): (String, i64) = self
                    .connection
                    .query_row(
                        "SELECT json_extract(body, '$.kind'), seq FROM artifacts WHERE id = ?1",
                        [id],
                        |r| Ok((r.get(0)?, r.get(1)?)),
                    )
                    .map_err(|error| match error {
                        rusqlite::Error::QueryReturnedNoRows => {
                            Error::new("invalid", "cleanup continuation is stale")
                        }
                        other => sql_error(other),
                    })?;
                let kind: ArtifactKind = serde_json::from_value(Value::String(kind))
                    .map_err(|_| Error::new("source_integrity", "invalid artifact ledger row"))?;
                Some((kind.cleanup_rank(), seq))
            }
        };
        let mut statement = self
            .connection
            .prepare("SELECT body, seq FROM artifacts WHERE seq <= ?1 ORDER BY seq ASC LIMIT 4097")
            .map_err(sql_error)?;
        let bodies = statement
            .query_map([snapshot], |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?))
            })
            .map_err(sql_error)?;
        let mut scanned = 0_usize;
        let mut eligible: Vec<(u8, i64, ArtifactView)> = Vec::new();
        for body in bodies {
            scanned += 1;
            let (body, seq) = body.map_err(sql_error)?;
            let artifact: ArtifactView = serde_json::from_str(&body)
                .map_err(|_| Error::new("source_integrity", "invalid artifact ledger row"))?;
            let rank = artifact.kind.cleanup_rank();
            if artifact.state != "registered"
                || cursor
                    .is_some_and(|(rank_after, seq_after)| (rank, seq) <= (rank_after, seq_after))
            {
                continue;
            }
            if request
                .project
                .as_ref()
                .is_some_and(|p| p != &artifact.project)
                || request.older_than.is_some_and(|t| artifact.created_at >= t)
            {
                continue;
            }
            // Frozen and live subtree membership is checked before the
            // target's assignment gate: a later row from another managed
            // project must skip, never abort the page with `not_allowed`.
            // Only rows in the caller's own subtree reach the gate in
            // `cleanup_authorized`, where a real assignment refusal aborts.
            if !self.cleanup_subtree_member(&caller, &artifact)? {
                continue;
            }
            if !self.cleanup_authorized(principal, &artifact)? {
                continue;
            }
            eligible.push((rank, seq, artifact));
        }
        eligible.sort_by_key(|item| (item.0, item.1));
        let mut truncated = scanned > 4096;
        let items = if eligible.len() > MAX_CLEANUP_ITEMS {
            truncated = true;
            eligible.truncate(MAX_CLEANUP_ITEMS);
            eligible
        } else {
            eligible
        }
        .into_iter()
        .map(|(_, _, artifact)| artifact)
        .collect();
        Ok((items, truncated, snapshot))
    }

    /// Retirement candidates for one exact Agent generation. The Agent,
    /// generation, settled-state and kind filters all apply before the page
    /// cap, so a retired Agent's own rows cannot be starved by the
    /// project-wide prefix the way the shared inventory page could be.
    /// Unknown attempt fences are untouched: this only selects rows.
    pub fn cleanup_retirement_inventory(&self, agent: &Agent) -> Result<(Vec<ArtifactView>, bool)> {
        let mut statement = self
            .connection
            .prepare(
                "SELECT body FROM artifacts WHERE agent = ?1 AND generation = ?2
                 AND json_extract(body, '$.state') = 'registered'
                 AND json_extract(body, '$.kind') IN ('build', 'temp', 'worktree')
                 ORDER BY seq ASC LIMIT 65",
            )
            .map_err(sql_error)?;
        let rows = statement
            .query_map(
                params![parse_agent_id(&agent.id)?, agent.generation],
                |row| row.get::<_, String>(0),
            )
            .map_err(sql_error)?;
        let mut items = Vec::with_capacity(MAX_CLEANUP_ITEMS);
        let mut truncated = false;
        for body in rows {
            let artifact: ArtifactView = serde_json::from_str(&body.map_err(sql_error)?)
                .map_err(|_| Error::new("source_integrity", "invalid artifact ledger row"))?;
            if items.len() == MAX_CLEANUP_ITEMS {
                truncated = true;
                break;
            }
            items.push(artifact);
        }
        Ok((items, truncated))
    }

    /// A missing checkout can still belong to a current workspace, or to a
    /// terminal Agent with unsettled activity. An Agent belongs to the
    /// registration only through its execution project: the registration ID,
    /// or, for a legacy label that names no registration, the registration's
    /// exact checkout under the platform's checkout-key rule. Agents of another
    /// registration never count, even when their workspace lies under this
    /// checkout. The Store holds the complete fleet here, including rows
    /// outside an inventory page.
    pub fn project_in_use(&self, id: &str, path: &str) -> Result<bool> {
        let pending = self
            .connection
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM commands WHERE principal=?1
             AND state IN ('accepted','queued','dispatched','unknown'))",
                [format!("project:{id}")],
                |row| row.get::<_, bool>(0),
            )
            .map_err(sql_error)?;
        if pending {
            return Ok(true);
        }
        let checkout = cleanup_path(path);
        let mut query = self
            .connection
            .prepare(
                "SELECT a.project FROM agents a
              WHERE a.state NOT IN ('stopped','failed')
                 OR EXISTS(SELECT 1 FROM source_state s WHERE s.agent=a.id
                            AND s.state='open' AND s.source NOT IN ('daemon','os','git','workspace_process'))
                 OR EXISTS(SELECT 1 FROM commands c WHERE c.target=a.id
                            AND c.state IN ('accepted','queued','dispatched','unknown'))
                 OR EXISTS(SELECT 1 FROM processes p WHERE p.state!='exited'
                            AND p.owner IN ('agent:a'||a.id||'#'||a.generation,
                                            'workspace:a'||a.id||'#'||a.generation))",
            )
            .map_err(sql_error)?;
        let mut registered = self
            .connection
            .prepare("SELECT EXISTS(SELECT 1 FROM project_registrations WHERE id=?1)")
            .map_err(sql_error)?;
        let mut rows = query.query([]).map_err(sql_error)?;
        while let Some(row) = rows.next().map_err(sql_error)? {
            let project: String = row.get(0).map_err(sql_error)?;
            if project == id {
                return Ok(true);
            }
            if cleanup_path(&project) != checkout {
                continue;
            }
            let names_registration: bool = registered
                .query_row([&project], |row| row.get(0))
                .map_err(sql_error)?;
            if !names_registration {
                return Ok(true);
            }
        }
        Ok(false)
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
        let seq: i64 = tx
            .query_row("SELECT COALESCE(MAX(seq), 0) + 1 FROM artifacts", [], |r| {
                r.get(0)
            })
            .map_err(sql_error)?;
        tx.execute(
            "INSERT INTO artifacts(id,agent,generation,path,body,seq) VALUES (?1,?2,?3,?4,?5,?6)",
            params![artifact.id, num, generation, artifact.path, body, seq],
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

    /// Retain one cleanup plan preview. A full ledger self-heals before it
    /// refuses admission: the retain reclaims fully settled consumed plans,
    /// receipt-less consumed plans with terminal settlement, and abandoned
    /// never-consumed previews, oldest first, so ordinary preview/apply
    /// cycles can never wedge the ledger. Plans with an outstanding `unknown`
    /// attempt, receipt-less consumed plans with an uncertain outcome, and
    /// fresh previews are never reclaimed. Anything still full afterwards
    /// keeps the `unavailable` refusal, naming stranded receipt-less plans as
    /// the blocker when they are the reason: delete or settle plans through
    /// the ordinary apply/settle paths and preview again.
    pub fn retain_cleanup_plan(
        &mut self,
        principal: &str,
        plan: &CleanupPlan,
        now: i64,
    ) -> Result<()> {
        self.ensure_writer_healthy()?;
        if plan.items.len() > MAX_CLEANUP_ITEMS {
            return Err(Error::new("invalid", "cleanup plan exceeds bound"));
        }
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        let mut count: i64 = tx
            .query_row("SELECT COUNT(*) FROM cleanup_plans", [], |r| r.get(0))
            .map_err(sql_error)?;
        if count >= MAX_CLEANUP_PLANS {
            count -= prune_reclaimable_cleanup_plans_tx(&tx, now)?;
            if count >= MAX_CLEANUP_PLANS {
                let stranded = count_stranded_consumed_plans_tx(&tx)?;
                return Err(Error::new(
                    "unavailable",
                    if stranded > 0 {
                        format!(
                            "cleanup plan ledger is full (1024 entries); {stranded} consumed plans stranded by uncertain settlement (receipt pruned)"
                        )
                    } else {
                        "cleanup plan ledger is full (1024 entries)".to_owned()
                    },
                ));
            }
        }
        let assignment_binding = project_assignment::coordination_binding(&tx, principal)?;
        tx.execute(
            "INSERT INTO cleanup_plans(id,principal,body,assignment_binding) VALUES (?1,?2,?3,?4)",
            params![
                plan.plan_id,
                principal,
                encode_json(plan)?,
                assignment_binding
            ],
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
        // Exact replay precedes plan load: a settled plan may already have
        // been reclaimed from the ledger while its receipt is retained, and
        // the retry must still read back the retained response instead of
        // `not_found` for the missing plan row.
        if let Some(value) =
            self.cleanup_replay(principal, &request.id, "cleanup.apply", &payload)?
        {
            let mut response: CleanupApplyResponse = serde_json::from_value(value)
                .map_err(|_| Error::new("source_integrity", "invalid cleanup receipt"))?;
            let plan = match self.cleanup_plan(principal, &request.plan_id) {
                Ok(plan) => plan,
                Err(error) if error.code == "not_found" => CleanupPlan {
                    plan_id: request.plan_id.clone(),
                    created_at: 0,
                    items: Vec::new(),
                    truncated: false,
                    continuation: None,
                },
                Err(error) => return Err(error),
            };
            for (index, candidate) in plan.items.iter().enumerate() {
                if candidate.artifact.kind != ArtifactKind::Worktree
                    || response.items[index].outcome != "unknown"
                {
                    continue;
                }
                let artifact = &candidate.artifact;
                let id = cleanup_worktree_command_id(&request.id, &artifact.id)?;
                let Some(command) = self.get_command(&id)? else {
                    continue;
                };
                if command.verb != "worktree.remove"
                    || command.principal != principal
                    || command.target.as_deref() != Some(artifact.agent.as_str())
                    || command.generation != Some(artifact.generation)
                    || command.state != CommandState::Applied
                {
                    continue;
                }
                let settled = CleanupItemOutcome {
                    artifact_id: artifact.id.clone(),
                    outcome: "removed".to_owned(),
                    reason: None,
                    bytes_reclaimed: 0,
                };
                response.items[index] = settled.clone();
                self.settle_cleanup_item(principal, request, &response, artifact, &settled)?;
            }
            return Ok((plan, response, false));
        }
        let plan = self.cleanup_plan(principal, &request.plan_id)?;
        validate_id_age(&request.id, now)?;
        if let Some(selected) = &request.artifacts
            && selected.iter().any(|id| {
                !plan
                    .items
                    .iter()
                    .any(|item| item.verdict == "safe" && item.artifact.id == *id)
            })
        {
            return Err(Error::new(
                "invalid",
                "cleanup selection names an artifact that is not a safe candidate of the plan",
            ));
        }
        let attempted =
            |item: &CleanupCandidate| item.verdict == "safe" && request.selects(&item.artifact.id);
        let mut response = CleanupApplyResponse {
            plan_id: plan.plan_id.clone(),
            bytes_reclaimed: 0,
            items: plan
                .items
                .iter()
                .map(|item| CleanupItemOutcome {
                    artifact_id: item.artifact.id.clone(),
                    outcome: if attempted(item) {
                        "unknown"
                    } else {
                        "skipped"
                    }
                    .to_owned(),
                    reason: Some(
                        if attempted(item) {
                            "attempt_reserved"
                        } else if item.verdict == "safe" {
                            "not_selected"
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
        let retained_binding: Option<String> = tx
            .query_row(
                "SELECT assignment_binding FROM cleanup_plans WHERE id=?1",
                [&request.plan_id],
                |row| row.get(0),
            )
            .map_err(sql_error)?;
        if retained_binding != project_assignment::coordination_binding(&tx, principal)? {
            return Err(Error::new(
                "stale_assignment",
                "project assignment changed after cleanup planning",
            ));
        }
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
        for (index, item) in plan.items.iter().enumerate() {
            // A project registration belongs to settings, not to an Agent, so
            // Agent deletion never supersedes it.
            if item.artifact.kind == ArtifactKind::ProjectRegistration {
                if attempted(item) {
                    if principal != "owner" {
                        return Err(Error::new("not_allowed", "project cleanup requires Owner"));
                    }
                    if prior_project_cleanup(&tx, &request.id, &item.artifact)? {
                        response.items[index].outcome = "skipped".to_owned();
                        response.items[index].reason = Some("prior_cleanup_attempt".to_owned());
                    }
                }
                continue;
            }
            // Read ownership in the same transaction that reserves attempts. Agent
            // ids are never reused, so deletion supersedes even a snapshot retained
            // after the Agent was removed.
            let agent = parse_agent_id(&item.artifact.agent)?;
            let retained: bool = tx
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM agents WHERE id=?1)",
                    [agent],
                    |r| r.get(0),
                )
                .map_err(sql_error)?;
            if !retained {
                response.items[index].outcome = "skipped".to_owned();
                response.items[index].reason = Some("superseded".to_owned());
                continue;
            }
            // Fence every attempted artifact across new plans and daemon restarts.
            if attempted(item) {
                let body: Option<String> = tx
                    .query_row(
                        "SELECT body FROM artifacts WHERE id=?1 AND agent=?2",
                        params![item.artifact.id, agent],
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
                if item.artifact.kind != ArtifactKind::Worktree {
                    let mut artifact = item.artifact.clone();
                    artifact.state = "unknown".to_owned();
                    tx.execute(
                        "UPDATE artifacts SET body=?1 WHERE id=?2",
                        params![encode_json(&artifact)?, artifact.id],
                    )
                    .map_err(sql_error)?;
                }
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

    /// Retirement planning gate for one artifact: quiescence first, then
    /// the bound-worktree blocker. `None` means planning may proceed to
    /// preflight; `Some(reason)` keeps the row with that reason and
    /// reserves no attempt, so a borrowed worktree is never fenced
    /// `unknown` across a restart. Retirement planning must consult this
    /// before any preflight; the apply-time recheck stays as the second
    /// gate.
    pub fn retirement_plan_gate(&self, artifact: &ArtifactView) -> Result<Option<&'static str>> {
        if !self.cleanup_agent_quiescent(artifact)? {
            return Ok(Some("agent_not_quiescent"));
        }
        self.cleanup_worktree_blocker(artifact)
    }

    pub fn cleanup_worktree_blocker(
        &self,
        artifact: &ArtifactView,
    ) -> Result<Option<&'static str>> {
        self.ensure_committed_reads()?;
        if artifact.kind != ArtifactKind::Worktree {
            return Ok(None);
        }
        if project_assignment::workspace_path_bound(&self.connection, &artifact.path)? {
            return Ok(Some("assignment_bound"));
        }
        if workspace::workspace_has_borrowers(&self.connection, &artifact.agent, &artifact.path)? {
            return Ok(Some("workspace_borrowed"));
        }
        Ok(None)
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

    pub fn settle_project_cleanup(
        &mut self,
        request: &CleanupApplyRequest,
        response: &CleanupApplyResponse,
    ) -> Result<()> {
        self.ensure_writer_healthy()?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        update_cleanup_receipt(&tx, "owner", request, response)?;
        commit_transaction(tx, &self.write_faulted)
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
        update_cleanup_receipt(&tx, principal, request, response)?;
        if artifact.kind != ArtifactKind::Worktree || outcome.outcome == "removed" {
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
        }
        if artifact.kind == ArtifactKind::Worktree && outcome.outcome == "removed" {
            let git = artifact
                .git
                .as_ref()
                .ok_or_else(|| Error::new("source_integrity", "worktree Git binding absent"))?;
            let current: Option<(String, String)> = tx.query_row(
                "SELECT phase, outcome FROM workspaces WHERE agent=?1 AND target_generation=?2 AND worktree_path=?3",
                params![parse_agent_id(&artifact.agent)?, artifact.generation, artifact.path],
                |row| Ok((row.get(0)?, row.get(1)?)),
            ).optional().map_err(sql_error)?;
            let settled = current.is_some_and(|(phase, outcome)| {
                phase == "removed"
                    && serde_json::from_str::<Value>(&outcome)
                        .ok()
                        .is_some_and(|value| {
                            value
                                .get("result")
                                .and_then(|result| result.get("branch"))
                                .and_then(Value::as_str)
                                == Some(git.branch.as_str())
                                && value
                                    .get("result")
                                    .and_then(|result| result.get("branch_retained"))
                                    .and_then(Value::as_bool)
                                    == Some(true)
                        })
            });
            if !settled {
                return Err(Error::new(
                    "bad_state",
                    "worktree removal has not settled in its workspace row",
                ));
            }
        }
        commit_transaction(tx, &self.write_faulted)
    }
}

/// Bounded self-heal for a full cleanup-plan ledger, oldest first. Deletes
/// fully settled consumed plans (their receipt is retained and no item
/// outcome is `unknown`, so the exact-apply replay and the unknown-attempt
/// fences no longer need the row), consumed plans whose receipt is gone but
/// whose own settlement is terminal and known (no item artifact is still
/// `unknown`, so retention-time pruning stranded a row that exact replay can
/// no longer use anyway), and abandoned never-consumed previews (older than
/// the write-id retry window plus clock skew, so no live manager can still
/// be about to apply them). Never touches a plan with an outstanding
/// `unknown` attempt, a receipt-less consumed plan with an uncertain outcome,
/// or a fresh preview. Returns the reclaimed count.
fn prune_reclaimable_cleanup_plans_tx(tx: &Transaction<'_>, now: i64) -> Result<i64> {
    let settled = tx
        .execute(
            "DELETE FROM cleanup_plans WHERE id IN (
                 SELECT p.id FROM cleanup_plans p JOIN writes w ON w.id = p.apply_id
                 WHERE p.apply_id IS NOT NULL
                   AND w.method = 'cleanup.apply'
                   AND NOT EXISTS (
                       SELECT 1 FROM json_each(w.result, '$.items') r
                       WHERE json_extract(r.value, '$.outcome') = 'unknown'
                   )
                 ORDER BY p.id LIMIT ?1
             )",
            [CLEANUP_PLAN_RECLAIM_BATCH],
        )
        .map_err(sql_error)? as i64;
    // A consumed plan whose receipt was pruned before retention pinned
    // consumed receipts can never replay (exact replay needs the receipt) and
    // can never settle further through this row. Reclaim it when its own
    // settlement is terminal and known: none of its item artifacts is still
    // `unknown`. An uncertain outcome keeps the row; the full-ledger refusal
    // names those stranded plans as the blocker. Nothing is replayed and no
    // outcome is invented here, only the unusable plan row is deleted.
    let stranded = tx
        .execute(
            "DELETE FROM cleanup_plans WHERE id IN (
                 SELECT p.id FROM cleanup_plans p
                 WHERE p.apply_id IS NOT NULL
                   AND NOT EXISTS (SELECT 1 FROM writes w WHERE w.id = p.apply_id)
                   AND NOT EXISTS (
                       SELECT 1 FROM json_each(p.body, '$.items') i
                       JOIN artifacts a ON a.id = json_extract(i.value, '$.artifact.id')
                       WHERE json_extract(a.body, '$.state') = 'unknown'
                   )
                 ORDER BY p.id LIMIT ?1
             )",
            [CLEANUP_PLAN_RECLAIM_BATCH],
        )
        .map_err(sql_error)? as i64;
    // A manager that previews applies promptly; a preview nobody consumed
    // within the retry window is abandoned, and its delayed apply reads
    // `not_found` so the manager re-previews through this same path.
    let stale_cutoff = now.saturating_sub(RETRY_WINDOW_MS + RETRY_CLOCK_SKEW_MS);
    let stale = tx
        .execute(
            "DELETE FROM cleanup_plans WHERE id IN (
                 SELECT p.id FROM cleanup_plans p
                 WHERE p.apply_id IS NULL
                   AND json_extract(p.body, '$.created_at') <= ?1
                 ORDER BY p.id LIMIT ?2
             )",
            params![stale_cutoff, CLEANUP_PLAN_RECLAIM_BATCH],
        )
        .map_err(sql_error)? as i64;
    Ok(settled + stranded + stale)
}

/// Consumed plans whose receipt is gone and whose outcome is still uncertain
/// (one of their item artifacts is `unknown`): kept by the reclaim above and
/// reported as the blocker when the ledger stays full. Never replayed, never
/// re-settled, and never given an invented outcome here.
fn count_stranded_consumed_plans_tx(tx: &Transaction<'_>) -> Result<i64> {
    tx.query_row(
        "SELECT COUNT(*) FROM cleanup_plans p
         WHERE p.apply_id IS NOT NULL
           AND NOT EXISTS (SELECT 1 FROM writes w WHERE w.id = p.apply_id)
           AND EXISTS (
               SELECT 1 FROM json_each(p.body, '$.items') i
               JOIN artifacts a ON a.id = json_extract(i.value, '$.artifact.id')
               WHERE json_extract(a.body, '$.state') = 'unknown'
           )",
        [],
        |row| row.get(0),
    )
    .map_err(sql_error)
}

fn update_cleanup_receipt(
    tx: &Transaction<'_>,
    principal: &str,
    request: &CleanupApplyRequest,
    response: &CleanupApplyResponse,
) -> Result<()> {
    let changed = tx
        .execute(
            "UPDATE writes SET result=?1 WHERE id=?2 AND principal=?3 AND method='cleanup.apply'",
            params![encode_json(response)?, request.id, principal],
        )
        .map_err(sql_error)?;
    if changed != 1 {
        return Err(Error::new(
            "source_integrity",
            "cleanup receipt disappeared",
        ));
    }
    Ok(())
}

fn prior_project_cleanup(
    tx: &Transaction<'_>,
    apply_id: &str,
    artifact: &ArtifactView,
) -> Result<bool> {
    // The receipt fences the exact project/settings revision even across
    // plans and restart. Retention pins unresolved project cleanup receipts.
    tx.query_row(
        "SELECT EXISTS(
           SELECT 1 FROM cleanup_plans p
             JOIN json_each(p.body, '$.items') i
             JOIN writes w ON w.id=p.apply_id
            WHERE p.principal='owner' AND p.apply_id <> ?3
              AND json_extract(i.value, '$.verdict')='safe'
              AND json_extract(i.value, '$.artifact.kind')='project_registration'
              AND json_extract(i.value, '$.artifact.project')=?1
              AND json_extract(i.value, '$.artifact.identity')=?2
              AND EXISTS(
                  SELECT 1 FROM json_each(w.result, '$.items') r
                   WHERE json_extract(r.value, '$.artifact_id')=json_extract(i.value, '$.artifact.id')
                     AND json_extract(r.value, '$.outcome')='unknown'
              )
         )",
        params![artifact.project, artifact.identity, apply_id], |row| row.get(0),
    ).map_err(sql_error)
}

fn cleanup_path(path: &str) -> std::path::PathBuf {
    #[cfg(windows)]
    {
        let path = path.replace('\\', "/");
        let path = path.strip_prefix("//?/").unwrap_or(&path);
        std::path::PathBuf::from(path.to_ascii_lowercase())
    }
    #[cfg(not(windows))]
    std::path::PathBuf::from(path)
}

fn cleanup_terminal(state: AgentState) -> bool {
    matches!(state, AgentState::Stopped | AgentState::Failed)
}

/// Fence resume at admission and dispatch while cleanup is unknown, or after
/// the current native session has been removed but before its Agent is deleted.
pub(super) fn require_no_cleanup_attempt(connection: &Connection, agent: &Agent) -> Result<()> {
    let pending: bool = connection.query_row("SELECT EXISTS(SELECT 1 FROM artifacts WHERE agent=?1 AND generation=?2 AND (json_extract(body,'$.state')='unknown' OR (json_extract(body,'$.kind')='native_session' AND json_extract(body,'$.state')='removed')))",params![parse_agent_id(&agent.id)?,agent.generation],|r|r.get(0)).map_err(sql_error)?;
    if pending {
        return Err(Error::new(
            "bad_state",
            "Agent has a cleanup attempt or removed native session",
        ));
    }
    Ok(())
}
