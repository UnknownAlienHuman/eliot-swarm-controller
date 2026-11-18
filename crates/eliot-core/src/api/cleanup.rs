//! Bounded, generation-owned artifact cleanup. Effects and path policy live in the daemon.
use crate::{Error, Result};
use serde::{Deserialize, Serialize};

#[derive(schemars::JsonSchema, Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactKind {
    Build,
    DataDir,
    Temp,
    Worktree,
    Branch,
    NativeSession,
    ProjectRegistration,
    Other,
}
impl ArtifactKind {
    /// Candidate priority band for cleanup display order: ordinary rows
    /// first, then worktrees, then branches and native sessions. The Store
    /// mirrors this order in SQL; keep the two in sync.
    pub fn cleanup_rank(&self) -> u8 {
        match self {
            ArtifactKind::Worktree => 1,
            ArtifactKind::Branch | ArtifactKind::NativeSession => 2,
            _ => 0,
        }
    }
}

pub const MAX_CLEANUP_ITEMS: usize = 64;

#[derive(schemars::JsonSchema, Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactRegisterRequest {
    pub id: String,
    pub path: String,
    pub kind: ArtifactKind,
    pub purpose: String,
}
impl ArtifactRegisterRequest {
    pub fn validate(&self) -> Result<()> {
        cleanup_id(&self.id)?;
        if self.path.is_empty()
            || self.path.len() > 4096
            || self.path.contains('\0')
            || self.purpose.trim().is_empty()
            || self.purpose.len() > 512
            || matches!(
                self.kind,
                ArtifactKind::Branch
                    | ArtifactKind::NativeSession
                    | ArtifactKind::Worktree
                    | ArtifactKind::ProjectRegistration
            )
        {
            return Err(Error::new(
                "invalid",
                "invalid artifact path, kind or purpose",
            ));
        }
        Ok(())
    }
}

#[derive(schemars::JsonSchema, Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactView {
    pub id: String,
    pub agent: String,
    pub generation: i64,
    pub project: String,
    pub tree: Vec<String>,
    pub path: String,
    pub kind: ArtifactKind,
    pub purpose: String,
    pub identity: String,
    pub created_at: i64,
    pub state: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub git: Option<CleanupGitBinding>,
}

#[derive(schemars::JsonSchema, Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CleanupGitBinding {
    pub repo: String,
    pub common: String,
    pub branch: String,
    pub worktree: String,
    pub admin: String,
    pub admin_identity: String,
}

#[derive(schemars::JsonSchema, Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CleanupScope {
    SelfTree,
    Project,
    All,
}

#[derive(schemars::JsonSchema, Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CleanupPlanRequest {
    pub scope: CleanupScope,
    #[serde(default)]
    pub project: Option<String>,
    /// Unix epoch milliseconds, exclusive creation cutoff.
    #[serde(default)]
    pub older_than: Option<i64>,
    /// Opaque continuation token from a previous `cleanup.plan` response.
    /// It encodes the artifact cursor, the consumed registration offsets,
    /// the settings revision those offsets were computed against, and the
    /// ledger snapshot the pages are pinned to. Filters still come from the
    /// request: keep them stable across pages.
    #[serde(default)]
    pub continuation: Option<String>,
}
impl CleanupPlanRequest {
    pub fn validate(&self) -> Result<()> {
        if self.older_than.is_some_and(|v| v < 0)
            || (self.scope == CleanupScope::Project
                && self
                    .project
                    .as_ref()
                    .is_none_or(|p| p.is_empty() || p.len() > 4096))
            || (self.scope != CleanupScope::Project && self.project.is_some())
            || self
                .continuation
                .as_deref()
                .is_some_and(|token| PlanContinuation::decode(token).is_err())
        {
            return Err(Error::new(
                "invalid",
                "cleanup scope, cutoff or continuation is invalid",
            ));
        }
        Ok(())
    }
}

/// Explicit paging state for one `cleanup.plan` listing, serialized as an
/// opaque token in the plan response and echoed back in the next request.
/// The artifact cursor always resolves against the pinned ledger snapshot,
/// so concurrent registrations can neither skip nor repeat candidates, and
/// registration offsets skip already-emitted project rows whose ids are
/// minted fresh on every plan.
#[derive(schemars::JsonSchema, Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlanContinuation {
    /// Artifact id of the last shown candidate, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_after: Option<String>,
    /// Project registrations already emitted, in deterministic project order.
    #[serde(default)]
    pub safe_skipped: usize,
    #[serde(default)]
    pub kept_skipped: usize,
    /// Settings revision the registration offsets were computed against.
    /// A mismatch restarts the whole listing: offsets into a changed
    /// project set are meaningless.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub regs_revision: Option<String>,
    /// Ledger insertion bound the pages are pinned to. New registrations
    /// land past it and surface on a fresh listing, never inside a page.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_seq: Option<i64>,
    /// Ordered project ids behind the registration offsets, split by the
    /// verdict band the router emitted. A page whose traversal reclassifies
    /// any project (a checkout appearing or disappearing moves it between
    /// kept and safe without changing the settings revision) is stale: the
    /// positional offsets would skip or repeat a registration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub safe_ids: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kept_ids: Option<Vec<String>>,
    /// Agent-list position the artifact backfill had reached. Every page
    /// resumes the fleet scan after this id instead of restarting from the
    /// first row, so a persistently capped fleet still terminates instead
    /// of returning the same empty page and token.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backfill_after: Option<String>,
}
impl PlanContinuation {
    pub fn encode(&self) -> Result<String> {
        serde_json::to_string(self)
            .map_err(|_| Error::new("internal", "cleanup continuation could not encode"))
    }
    pub fn decode(text: &str) -> Result<Self> {
        let parsed: PlanContinuation = serde_json::from_str(text)
            .map_err(|_| Error::new("invalid", "cleanup continuation is invalid"))?;
        if parsed
            .artifact_after
            .as_deref()
            .is_some_and(|id| cleanup_id(id).is_err())
        {
            return Err(Error::new("invalid", "cleanup continuation is invalid"));
        }
        Ok(parsed)
    }
}

#[derive(schemars::JsonSchema, Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CleanupCandidate {
    pub artifact: ArtifactView,
    pub bytes: Option<u64>,
    pub verdict: String,
    pub reason: String,
}

#[derive(schemars::JsonSchema, Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CleanupPlan {
    pub plan_id: String,
    pub created_at: i64,
    pub items: Vec<CleanupCandidate>,
    pub truncated: bool,
    /// Opaque token for the next page, absent when the listing is complete.
    /// Echo it as `continuation` to continue in final candidate order.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub continuation: Option<String>,
}

#[derive(schemars::JsonSchema, Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CleanupApplyRequest {
    pub id: String,
    pub plan_id: String,
    /// Delete an already-removed branch from its upstream remote as well.
    /// Owner-only and never automatic: managers and Agents always apply
    /// local-only deletions.
    #[serde(default)]
    pub delete_remote: bool,
    /// Apply only these safe candidates of the plan (artifact ids). The other
    /// safe candidates settle `skipped(not_selected)` with no attempt fence, so
    /// a later plan can offer them again. Omitted applies every safe candidate.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(max = 64))]
    pub artifacts: Option<Vec<String>>,
}
impl CleanupApplyRequest {
    pub fn validate(&self) -> Result<()> {
        cleanup_id(&self.id)?;
        cleanup_id(&self.plan_id)?;
        if let Some(artifacts) = &self.artifacts {
            let mut seen = std::collections::BTreeSet::new();
            if artifacts.len() > MAX_CLEANUP_ITEMS
                || artifacts.iter().any(|id| !seen.insert(id.as_str()))
            {
                return Err(Error::new(
                    "invalid",
                    "cleanup artifact selection must be unique and at most 64 ids",
                ));
            }
            for id in artifacts {
                cleanup_id(id)?;
            }
        }
        Ok(())
    }

    /// Whether this apply attempts `artifact` (a safe candidate's id).
    pub fn selects(&self, artifact: &str) -> bool {
        self.artifacts
            .as_ref()
            .is_none_or(|selected| selected.iter().any(|id| id == artifact))
    }
}

#[derive(schemars::JsonSchema, Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CleanupItemOutcome {
    pub artifact_id: String,
    pub outcome: String,
    pub reason: Option<String>,
    pub bytes_reclaimed: u64,
}

#[derive(schemars::JsonSchema, Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CleanupApplyResponse {
    pub plan_id: String,
    pub items: Vec<CleanupItemOutcome>,
    pub bytes_reclaimed: u64,
}

fn cleanup_id(id: &str) -> Result<()> {
    if id.len() != 26
        || !id
            .bytes()
            .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit())
    {
        return Err(Error::new("invalid", "cleanup id must be a ULID"));
    }
    Ok(())
}
