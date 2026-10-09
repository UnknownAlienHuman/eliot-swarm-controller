//! Static MCP catalogue metadata and session-local discovery projections.
//!
//! Profile filters limit this frontend's presentation and dispatch. The
//! authenticated Store remains authoritative for each application method.
//! This module adds presentation only: role cores, bounded
//! `tools/list` pages, and a catalog-only search result that never dispatches
//! an application method.

use super::{profiles, tool_from_spec};
use crate::config::McpToolProfile;
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use rmcp::model::Tool;
use serde::Serialize;
use sha2::{Digest, Sha256};
pub(super) use swarm_contracts::mcp_catalog::{
    AuthorizationBasis, AuthorizationRevision, CatalogError, MAX_SEARCH_RESULTS, Surface,
};
use swarm_contracts::mcp_catalog::{
    LoadTier, MAX_PAGE_ITEMS, MAX_PAGE_JSON_BYTES, SuggestedSurface, TOOL_METADATA, ToolGroup,
    ToolMetadata, find_tool_spec as find_spec, hex_digest, input_schema_bytes,
    mutation_requires_caller_request_id, output_schema_bytes, profile_name, role_core, tool_name,
    update_field, validate_registry_metadata,
};

#[derive(Debug)]
pub struct CatalogPage {
    pub tools: Vec<Tool>,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ActivationDisposition {
    AvailableOnServerSurface,
    ReconnectSurfaceRequired,
}

#[derive(Debug, Clone, Serialize)]
pub struct CatalogMatch {
    pub group: &'static str,
    pub method: &'static str,
    pub audiences: Vec<&'static str>,
    pub load_tier: &'static str,
    pub title: &'static str,
    pub purpose: &'static str,
    pub when_to_use: &'static str,
    pub search_terms: &'static [&'static str],
    pub required_context: &'static [&'static str],
    pub result_policy: &'static str,
    pub activation: ActivationDisposition,
    pub suggested_surface: SuggestedSurface,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SearchCoverage {
    Complete,
    Partial,
}

#[derive(Debug, Clone, Serialize)]
pub struct CatalogSearchResult {
    pub catalog_revision: String,
    pub surface_revision: String,
    pub matches: Vec<CatalogMatch>,
    pub coverage: SearchCoverage,
    pub authorization_revision: String,
    pub authorization_basis: &'static str,
    /// Server-side schema visibility does not establish native client/model loading.
    pub server_surface_visibility: &'static str,
    pub harness_acknowledgement: &'static str,
    /// Only role-specific missing canonical handlers are reported. These are
    /// capability gaps, not synthetic tool definitions.
    pub gaps: Vec<&'static str>,
}

#[derive(Debug, Clone, Copy)]
pub struct SearchRequest<'a> {
    pub query: &'a str,
    pub purpose: Option<&'a str>,
    pub task_id: Option<&'a str>,
    pub exact_method: Option<&'a str>,
    pub loaded_catalog_revision: Option<&'a str>,
    pub max_results: usize,
}

struct AuthorizedView {
    entries: Vec<&'static ToolMetadata>,
    catalog_digest: [u8; 32],
}

struct SurfaceView {
    authorized: AuthorizedView,
    visible: Vec<&'static ToolMetadata>,
    surface_digest: [u8; 32],
}

#[derive(Debug)]
struct CursorClaims {
    catalog_digest: [u8; 32],
    surface_digest: [u8; 32],
    offset: usize,
}

/// Build `tools/list` from a pre-authorized view. The callback is evaluated
/// only after the fixed hard profile and before surface filtering, ordering,
/// revision calculation, cursor validation, or paging.
pub fn list_tools_page<F>(
    profile: McpToolProfile,
    surface: &Surface,
    cursor: Option<&str>,
    authorization: AuthorizationRevision<'_>,
    mut object_authorized: F,
) -> Result<CatalogPage, CatalogError>
where
    F: FnMut(&str, Option<&str>) -> bool,
{
    let view = surface_view(
        profile,
        surface,
        authorization,
        None,
        &mut object_authorized,
    )?;
    let claims = cursor.map(decode_cursor).transpose()?;
    let offset = if let Some(claims) = claims {
        if claims.catalog_digest != view.authorized.catalog_digest
            || claims.surface_digest != view.surface_digest
        {
            return Err(CatalogError::StaleCursor);
        }
        claims.offset
    } else {
        0
    };
    if offset >= view.visible.len() && offset != 0 {
        return Err(CatalogError::CursorOutOfRange);
    }

    let mut tools = Vec::new();
    let mut serialized_bytes = 0usize;
    // Leave room for the MCP result envelope, cursor and JSON punctuation.
    const ENVELOPE_RESERVE_BYTES: usize = 512;
    let mut end = offset;
    while end < view.visible.len() && tools.len() < MAX_PAGE_ITEMS {
        let metadata = view.visible[end];
        let (read_only, spec) =
            find_spec(metadata.method).ok_or(CatalogError::IncompleteRegistry)?;
        let tool = tool_from_spec(
            *read_only,
            spec,
            mutation_requires_caller_request_id(profile, spec.method, *read_only),
        );
        let byte_len = serde_json::to_vec(&tool)
            .map_err(|error| CatalogError::Serialization(error.to_string()))?
            .len();
        if tools.is_empty() && byte_len.saturating_add(ENVELOPE_RESERVE_BYTES) > MAX_PAGE_JSON_BYTES
        {
            return Err(CatalogError::ToolSchemaTooLarge);
        }
        if serialized_bytes
            .saturating_add(byte_len)
            .saturating_add(ENVELOPE_RESERVE_BYTES)
            > MAX_PAGE_JSON_BYTES
        {
            break;
        }
        serialized_bytes += byte_len;
        tools.push(tool);
        end += 1;
    }
    let next_cursor = (end < view.visible.len()).then(|| {
        encode_cursor(&CursorClaims {
            catalog_digest: view.authorized.catalog_digest,
            surface_digest: view.surface_digest,
            offset: end,
        })
    });

    Ok(CatalogPage { tools, next_cursor })
}

/// Search only the authorized registry. The result contains metadata and a
/// truthful activation disposition; it carries no schema, arguments, or
/// execution endpoint. This is catalog lookup, never a generic RPC tool.
pub fn search_catalog<F>(
    profile: McpToolProfile,
    surface: &Surface,
    request: SearchRequest<'_>,
    authorization: AuthorizationRevision<'_>,
    mut object_authorized: F,
) -> Result<CatalogSearchResult, CatalogError>
where
    F: FnMut(&str, Option<&str>) -> bool,
{
    let view = surface_view(
        profile,
        surface,
        authorization,
        request.task_id,
        &mut object_authorized,
    )?;
    let catalog_revision = hex_digest(&view.authorized.catalog_digest);
    if request
        .loaded_catalog_revision
        .is_some_and(|loaded| loaded != catalog_revision)
    {
        return Err(CatalogError::StaleCatalogRevision);
    }

    let query = normalize(request.query);
    let purpose = request.purpose.map(normalize).unwrap_or_default();
    let exact = request.exact_method.map(normalize);
    if query.is_empty() && exact.is_none() {
        return Ok(CatalogSearchResult {
            catalog_revision,
            surface_revision: hex_digest(&view.surface_digest),
            matches: Vec::new(),
            coverage: SearchCoverage::Complete,
            authorization_revision: authorization.value.to_owned(),
            authorization_basis: authorization.basis.as_str(),
            server_surface_visibility: "server_surface_only",
            harness_acknowledgement: "unknown",
            gaps: role_core(surface.core).gaps.to_vec(),
        });
    }

    // Authorization has already been applied while constructing `view`.
    // Search and result limiting never inspect denied metadata.
    let mut ranked = Vec::new();
    for metadata in &view.authorized.entries {
        if metadata.load_tier == LoadTier::ManualOnly && exact.is_none() {
            continue;
        }
        if let Some(exact) = exact.as_deref() {
            let exact_method = normalize(metadata.method);
            let exact_tool = normalize(&tool_name(metadata.method));
            if exact != exact_method && exact != exact_tool {
                continue;
            }
        }
        let score = search_score(metadata, &query, &purpose, exact.is_some());
        if score == 0 {
            continue;
        }
        ranked.push((score, metadata));
    }
    ranked.sort_by(|(left_score, left), (right_score, right)| {
        right_score
            .cmp(left_score)
            .then_with(|| left.group.as_str().cmp(right.group.as_str()))
            .then_with(|| left.load_tier.as_str().cmp(right.load_tier.as_str()))
            .then_with(|| left.method.cmp(right.method))
    });

    let limit = request.max_results.clamp(1, MAX_SEARCH_RESULTS);
    let coverage = if ranked.len() > limit {
        SearchCoverage::Partial
    } else {
        SearchCoverage::Complete
    };
    let matches = ranked
        .into_iter()
        .take(limit)
        .map(|(_, metadata)| {
            let is_loaded = surface.includes(metadata);
            let mut suggested = surface.clone();
            if !is_loaded {
                if metadata.load_tier == LoadTier::ManualOnly {
                    suggested = suggested.with_exact_manual_method(metadata.method);
                } else {
                    suggested = suggested.with_group(metadata.group);
                }
            }
            CatalogMatch {
                group: metadata.group.as_str(),
                method: metadata.method,
                audiences: metadata
                    .audiences
                    .iter()
                    .map(|audience| audience.as_str())
                    .collect(),
                load_tier: metadata.load_tier.as_str(),
                title: metadata.purpose,
                purpose: metadata.purpose,
                when_to_use: metadata.when_to_use,
                search_terms: metadata.search_terms,
                required_context: metadata.required_context,
                result_policy: metadata.result_policy,
                activation: if is_loaded {
                    ActivationDisposition::AvailableOnServerSurface
                } else {
                    // This server does not know whether its harness can safely
                    // refresh the current session. Require an explicit next
                    // surface/reconnect; never claim auto-activation.
                    ActivationDisposition::ReconnectSurfaceRequired
                },
                suggested_surface: suggested.suggested(),
            }
        })
        .collect();

    Ok(CatalogSearchResult {
        catalog_revision,
        surface_revision: hex_digest(&view.surface_digest),
        matches,
        coverage,
        authorization_revision: authorization.value.to_owned(),
        authorization_basis: authorization.basis.as_str(),
        server_surface_visibility: "server_surface_only",
        harness_acknowledgement: "unknown",
        gaps: role_core(surface.core).gaps.to_vec(),
    })
}

fn surface_view<F>(
    profile: McpToolProfile,
    surface: &Surface,
    authorization: AuthorizationRevision<'_>,
    search_context: Option<&str>,
    object_authorized: &mut F,
) -> Result<SurfaceView, CatalogError>
where
    F: FnMut(&str, Option<&str>) -> bool,
{
    validate_registry_metadata()?;
    let mut entries: Vec<_> = TOOL_METADATA
        .iter()
        .filter(|metadata| profiles::exposes_method(profile, metadata.method))
        .filter(|metadata| object_authorized(metadata.method, search_context))
        .collect();
    entries.sort_by(|left, right| metadata_order(left, right));
    let catalog_digest = digest_entries(
        profile,
        authorization.value,
        search_context.unwrap_or_default(),
        &entries,
    )?;
    let visible: Vec<_> = entries
        .iter()
        .copied()
        .filter(|metadata| surface.includes(metadata))
        .collect();
    let surface_digest = digest_surface(surface, catalog_digest, profile, &visible)?;
    Ok(SurfaceView {
        authorized: AuthorizedView {
            entries,
            catalog_digest,
        },
        visible,
        surface_digest,
    })
}

fn metadata_order(left: &ToolMetadata, right: &ToolMetadata) -> std::cmp::Ordering {
    left.group
        .as_str()
        .cmp(right.group.as_str())
        .then_with(|| left.load_tier.cmp(&right.load_tier))
        .then_with(|| left.method.cmp(right.method))
}

fn digest_entries(
    profile: McpToolProfile,
    authorization_revision: &str,
    authorization_context: &str,
    entries: &[&'static ToolMetadata],
) -> Result<[u8; 32], CatalogError> {
    let mut hasher = Sha256::new();
    update_field(&mut hasher, b"eliot-mcp-authorized-catalog-v1");
    update_field(&mut hasher, profile_name(profile).as_bytes());
    update_field(&mut hasher, authorization_revision.as_bytes());
    update_field(&mut hasher, authorization_context.as_bytes());
    for metadata in entries {
        digest_metadata(&mut hasher, metadata);
        digest_tool_schema(&mut hasher, profile, metadata.method)?;
    }
    Ok(hasher.finalize().into())
}

fn digest_surface(
    surface: &Surface,
    catalog_digest: [u8; 32],
    profile: McpToolProfile,
    visible: &[&'static ToolMetadata],
) -> Result<[u8; 32], CatalogError> {
    let mut hasher = Sha256::new();
    update_field(&mut hasher, b"eliot-mcp-surface-view-v1");
    update_field(&mut hasher, &catalog_digest);
    update_field(&mut hasher, surface.id().as_bytes());
    update_field(&mut hasher, profile_name(profile).as_bytes());
    for metadata in visible {
        digest_metadata(&mut hasher, metadata);
        digest_tool_schema(&mut hasher, profile, metadata.method)?;
    }
    Ok(hasher.finalize().into())
}

fn digest_metadata(hasher: &mut Sha256, metadata: &ToolMetadata) {
    update_field(hasher, metadata.method.as_bytes());
    update_field(hasher, metadata.group.as_str().as_bytes());
    update_field(hasher, metadata.load_tier.as_str().as_bytes());
    update_field(hasher, metadata.purpose.as_bytes());
    update_field(hasher, metadata.when_to_use.as_bytes());
    update_field(hasher, metadata.result_policy.as_bytes());
    for audience in metadata.audiences {
        update_field(hasher, audience.as_str().as_bytes());
    }
    for term in metadata.search_terms {
        update_field(hasher, term.as_bytes());
    }
    for context in metadata.required_context {
        update_field(hasher, context.as_bytes());
    }
}

fn digest_tool_schema(
    hasher: &mut Sha256,
    profile: McpToolProfile,
    method: &str,
) -> Result<(), CatalogError> {
    let (read_only, spec) = find_spec(method).ok_or(CatalogError::IncompleteRegistry)?;
    update_field(hasher, tool_name(method).as_bytes());
    update_field(hasher, spec.description.as_bytes());
    let input_bytes = input_schema_bytes(
        spec,
        *read_only,
        mutation_requires_caller_request_id(profile, spec.method, *read_only),
    )
    .map_err(CatalogError::Serialization)?;
    update_field(hasher, input_bytes.as_ref());
    if let Some(output_bytes) = output_schema_bytes(method).map_err(CatalogError::Serialization)? {
        update_field(hasher, output_bytes.as_ref());
    } else {
        update_field(hasher, &[]);
    }
    Ok(())
}

fn search_score(metadata: &ToolMetadata, query: &str, purpose: &str, exact_selected: bool) -> u16 {
    if exact_selected {
        return 10_000;
    }
    let method = normalize(metadata.method);
    let tool = normalize(&tool_name(metadata.method));
    let searchable = normalize(&format!(
        "{} {} {} {} {} {}",
        metadata.group.as_str(),
        metadata.purpose,
        metadata.when_to_use,
        metadata.search_terms.join(" "),
        metadata.required_context.join(" "),
        metadata.result_policy
    ));
    let query_terms = terms(query);
    if query_terms.is_empty() {
        return 0;
    }
    let mut score = 0u16;
    let mut hits = 0usize;
    for term in query_terms {
        if method == term || tool == term {
            score = score.saturating_add(80);
            hits += 1;
        } else if method.split_whitespace().any(|word| word == term)
            || tool.split_whitespace().any(|word| word == term)
        {
            score = score.saturating_add(40);
            hits += 1;
        } else if searchable.split_whitespace().any(|word| word == term) {
            score = score.saturating_add(12);
            hits += 1;
        }
    }
    if hits == 0 || hits.saturating_mul(2) < terms(query).len() {
        return 0;
    }
    if !purpose.is_empty() {
        let purpose_terms = terms(purpose);
        if purpose_terms
            .iter()
            .all(|term| searchable.split_whitespace().any(|word| word == term))
            || purpose_group_matches(metadata.group, &purpose_terms)
        {
            score = score.saturating_add(20);
        } else {
            return 0;
        }
    }
    score
}

fn normalize(value: &str) -> String {
    value
        .chars()
        .flat_map(char::to_lowercase)
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character
            } else {
                ' '
            }
        })
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn terms(value: &str) -> Vec<String> {
    const STOP_WORDS: &[&str] = &[
        "a", "an", "and", "are", "for", "find", "get", "how", "i", "in", "is", "me", "my", "of",
        "please", "search", "show", "the", "to", "use", "when", "with",
    ];
    normalize(value)
        .split_whitespace()
        .filter(|term| !STOP_WORDS.contains(term))
        .map(str::to_owned)
        .collect()
}

fn purpose_group_matches(group: ToolGroup, terms: &[String]) -> bool {
    let purpose = terms.join(" ");
    match purpose.as_str() {
        "implementation" | "build" | "implement" => {
            matches!(group, ToolGroup::TaskManagement | ToolGroup::RuntimeControl)
        }
        "review" | "audit" => matches!(group, ToolGroup::Review | ToolGroup::AssignmentRead),
        "coordination" | "collaboration" => {
            matches!(
                group,
                ToolGroup::ParticipantCoordination | ToolGroup::MailboxRaw
            )
        }
        "monitoring" | "diagnostics" | "debugging" => {
            matches!(group, ToolGroup::Monitoring | ToolGroup::RuntimeRecovery)
        }
        "administration" | "operator" => {
            matches!(
                group,
                ToolGroup::Administration | ToolGroup::AcceptanceEffects
            )
        }
        "acceptance" | "publishing" => group == ToolGroup::AcceptanceEffects,
        "github" | "github intake" | "work pool" => group == ToolGroup::GitHub,
        "goal" | "goals" | "reminder" | "tracking" => group == ToolGroup::Goals,
        "hook" | "hooks" | "git hooks" => group == ToolGroup::Hooks,
        "script" | "scripts" => group == ToolGroup::Scripts,
        _ => false,
    }
}

fn encode_cursor(claims: &CursorClaims) -> String {
    let mut bytes = Vec::with_capacity(73);
    bytes.push(1);
    bytes.extend_from_slice(&claims.catalog_digest);
    bytes.extend_from_slice(&claims.surface_digest);
    bytes.extend_from_slice(&(claims.offset as u64).to_be_bytes());
    URL_SAFE_NO_PAD.encode(bytes)
}

fn decode_cursor(cursor: &str) -> Result<CursorClaims, CatalogError> {
    let bytes = URL_SAFE_NO_PAD
        .decode(cursor)
        .map_err(|_| CatalogError::InvalidCursor)?;
    if bytes.len() != 73 || bytes[0] != 1 {
        return Err(CatalogError::InvalidCursor);
    }
    let catalog_digest = bytes[1..33]
        .try_into()
        .map_err(|_| CatalogError::InvalidCursor)?;
    let surface_digest = bytes[33..65]
        .try_into()
        .map_err(|_| CatalogError::InvalidCursor)?;
    let offset = u64::from_be_bytes(
        bytes[65..73]
            .try_into()
            .map_err(|_| CatalogError::InvalidCursor)?,
    );
    let offset = usize::try_from(offset).map_err(|_| CatalogError::InvalidCursor)?;
    Ok(CursorClaims {
        catalog_digest,
        surface_digest,
        offset,
    })
}

#[cfg(test)]
#[path = "catalog_tests.rs"]
mod tests;
