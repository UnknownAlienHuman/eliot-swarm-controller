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
pub const SWARM_STAND_ARTIFACT_PURPOSE: &str = "Swarm private stand";
pub const MAX_ARTIFACT_TTL_MS: i64 = 30 * 24 * 60 * 60 * 1000;
pub const DEFAULT_STAND_TTL_MS: i64 = 24 * 60 * 60 * 1000;

#[derive(schemars::JsonSchema, Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactRegisterRequest {
    pub id: String,
    pub path: String,
    pub kind: ArtifactKind,
    pub purpose: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<i64>,
}
impl ArtifactRegisterRequest {
    pub fn validate(&self) -> Result<()> {
        cleanup_id(&self.id)?;
        if self.path.is_empty()
            || self.path.len() > 4096
            || self.path.contains('\0')
            || self.purpose.trim().is_empty()
            || self.purpose.len() > 512
            || self.expires_at.is_some_and(|ts| ts <= 0)
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
                "invalid artifact path, kind, purpose or expiry",
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<i64>,
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
    /// Exact branch tip classified for a safe worktree plan item.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub planned_tip: Option<String>,
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
    /// It encodes the displayed-artifact cursor, bounded inventory cursor,
    /// consumed registration offsets and pinned snapshots. New tokens bind
    /// the caller and filters; older JSON-text tokens and continuation
    /// objects remain accepted on input.
    #[serde(default, deserialize_with = "deserialize_cleanup_continuation")]
    #[schemars(
        description = "Opaque cleanup continuation string. Legacy JSON text or a continuation object is accepted on input."
    )]
    #[schemars(with = "Option<CleanupContinuationInputSchema>")]
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
/// The artifact cursor records the last displayed row for legacy callers.
/// New tokens also carry a bounded raw-row rank/sequence cursor, including
/// progress through empty pages. The pinned ledger snapshot excludes later
/// registrations, while project offsets skip rows whose ids are minted fresh
/// on every plan.
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
    /// Rank and ledger sequence reached by the bounded Store inventory scan.
    /// This advances through empty/filtered pages without changing the
    /// displayed-artifact cursor above.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inventory_rank: Option<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inventory_seq_after: Option<i64>,
    /// Binds new continuations to the caller and stable request filters.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_binding: Option<String>,
}

#[derive(schemars::JsonSchema)]
#[schemars(untagged)]
#[doc(hidden)]
pub enum CleanupContinuationInputSchema {
    Token(String),
    State(PlanContinuation),
}

impl PlanContinuation {
    pub fn encode(&self) -> Result<String> {
        self.validate_fields()?;
        let payload = serde_json::to_vec(self)
            .map_err(|_| Error::new("internal", "cleanup continuation could not encode"))?;
        let mut token = String::with_capacity(3 + payload.len() * 2);
        token.push_str("c1_");
        const HEX: &[u8; 16] = b"0123456789abcdef";
        for byte in payload {
            token.push(HEX[(byte >> 4) as usize] as char);
            token.push(HEX[(byte & 0x0f) as usize] as char);
        }
        Ok(token)
    }
    pub fn decode(text: &str) -> Result<Self> {
        let parsed = if let Some(encoded) = text.strip_prefix("c1_") {
            let (pairs, trailing) = encoded.as_bytes().as_chunks::<2>();
            if pairs.is_empty() || !trailing.is_empty() {
                return Err(Error::new("invalid", "cleanup continuation is invalid"));
            }
            let mut payload = Vec::with_capacity(pairs.len());
            for &[high, low] in pairs {
                let high = cleanup_hex_value(high)
                    .ok_or_else(|| Error::new("invalid", "cleanup continuation is invalid"))?;
                let low = cleanup_hex_value(low)
                    .ok_or_else(|| Error::new("invalid", "cleanup continuation is invalid"))?;
                payload.push((high << 4) | low);
            }
            let parsed: PlanContinuation = serde_json::from_slice(&payload)
                .map_err(|_| Error::new("invalid", "cleanup continuation is invalid"))?;
            if parsed.encode()?.as_str() != text {
                return Err(Error::new("invalid", "cleanup continuation is invalid"));
            }
            parsed
        } else {
            // Older releases emitted the continuation state as JSON text.
            // Some shells copied the JSON string quotes from the enclosing
            // response as well, so accept that representation too.
            serde_json::from_str::<PlanContinuation>(text)
                .or_else(|_| {
                    serde_json::from_str::<String>(text)
                        .and_then(|unquoted| serde_json::from_str::<PlanContinuation>(&unquoted))
                })
                .map_err(|_| Error::new("invalid", "cleanup continuation is invalid"))?
        };
        parsed.validate_fields()?;
        Ok(parsed)
    }

    fn validate_fields(&self) -> Result<()> {
        if self
            .artifact_after
            .as_deref()
            .is_some_and(|id| cleanup_id(id).is_err())
            || self
                .backfill_after
                .as_deref()
                .is_some_and(|id| crate::parse_agent_id(id).is_err())
            || self.snapshot_seq.is_some_and(|seq| seq < 0)
            || self.inventory_rank.is_some_and(|rank| rank > 3)
            || (self.inventory_rank.is_none() && self.inventory_seq_after.is_some())
            || self.inventory_seq_after.is_some_and(|seq| seq <= 0)
            || (self.inventory_rank == Some(3) && self.inventory_seq_after.is_some())
            || (self.snapshot_seq.is_none()
                && (self.inventory_rank.is_some() || self.inventory_seq_after.is_some()))
            || self.request_binding.as_deref().is_some_and(|binding| {
                binding.len() != 64
                    || binding
                        .bytes()
                        .any(|byte| cleanup_hex_value(byte).is_none())
            })
        {
            return Err(Error::new("invalid", "cleanup continuation is invalid"));
        }
        Ok(())
    }
}

fn cleanup_hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    }
}

fn deserialize_cleanup_continuation<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = Option::<serde_json::Value>::deserialize(deserializer)?;
    let token = match value {
        None => return Ok(None),
        Some(serde_json::Value::String(text)) => PlanContinuation::decode(&text)
            .and_then(|state| state.encode())
            .map_err(serde::de::Error::custom)?,
        Some(value @ serde_json::Value::Object(_)) => {
            let state: PlanContinuation =
                serde_json::from_value(value).map_err(serde::de::Error::custom)?;
            state.encode().map_err(serde::de::Error::custom)?
        }
        Some(_) => return Err(serde::de::Error::custom("invalid cleanup continuation")),
    };
    Ok(Some(token))
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
pub struct CleanupInventoryIssue {
    pub agent: String,
    pub generation: i64,
    pub path: Option<String>,
    pub reason: String,
    pub detail: String,
}

#[derive(schemars::JsonSchema, Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CleanupPlan {
    pub plan_id: String,
    pub created_at: i64,
    pub items: Vec<CleanupCandidate>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub inventory_issues: Vec<CleanupInventoryIssue>,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
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
