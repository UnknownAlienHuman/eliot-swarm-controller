use super::*;
use eliot_core::api::{AGENT_LIST_STATES, AgentStateTotal, AgentSummary};

impl Store {
    /// Compact fleet projection for `agent.list`: scope-checked, filtered,
    /// ordered by Agent ID. Every equality filter rides one `agents_list_*`
    /// composite index (forced with `INDEXED BY`, so `EXPLAIN QUERY PLAN`
    /// shows the index); the cursor rides the primary key. The response
    /// carries the matching total ignoring the cursor, honest truncation, and
    /// the continuation cursor at the last returned row.
    pub fn list_agent_summaries(
        &self,
        filter: &AgentListFilter,
        after: Option<&str>,
        limit: usize,
        now_ms: i64,
        growth_thresholds: &core::SessionGrowthThresholds,
    ) -> Result<AgentListPage> {
        self.list_agent_summaries_inner(None, filter, after, limit, now_ms, growth_thresholds)
    }

    /// Scope-checked fleet projection for an Agent caller: itself for `self`
    /// control, its subtree for `tree`, its project for `project`. The caller
    /// row itself is always in scope.
    pub fn list_agent_summaries_scoped(
        &self,
        caller: &Agent,
        filter: &AgentListFilter,
        after: Option<&str>,
        limit: usize,
        now_ms: i64,
        growth_thresholds: &core::SessionGrowthThresholds,
    ) -> Result<AgentListPage> {
        self.list_agent_summaries_inner(
            Some(caller),
            filter,
            after,
            limit,
            now_ms,
            growth_thresholds,
        )
    }

    fn list_agent_summaries_inner(
        &self,
        caller: Option<&Agent>,
        filter: &AgentListFilter,
        after: Option<&str>,
        limit: usize,
        now_ms: i64,
        growth_thresholds: &core::SessionGrowthThresholds,
    ) -> Result<AgentListPage> {
        validate_limit(limit)?;
        filter.validate()?;
        let after_num = after.map(parse_agent_id).transpose()?.unwrap_or(0);
        // Custody-held archived agents for the default exclusion, collected
        // once per page from the live custody tables (bounded by live custody,
        // never by archived history) and reused for rows and totals.
        let custody = if matches!(filter.archived, core::api::ArchivedFilter::Exclude) {
            Some(super::agent_lifecycle::default_list_custody_agents(
                &self.connection,
            )?)
        } else {
            None
        };
        // One extra row probes continuation; the byte bound may trim further.
        let probe = i64::try_from(limit)
            .map_err(|_| Error::new("invalid", "page limit must fit the index bound"))?
            + 1;
        let page = agent_list_select(
            filter,
            caller,
            Some(after_num),
            Some(probe),
            AgentListProjection::Rows,
            custody.as_deref(),
        )?;
        let mut statement = self.connection.prepare(&page.sql).map_err(sql_error)?;
        let rows = statement
            .query_map(
                rusqlite::params_from_iter(page.params.iter()),
                agent_record_from_row,
            )
            .map_err(sql_error)?;
        let mut summaries = Vec::new();
        for row in rows {
            summaries.push(self.summary_from_record(
                row.map_err(sql_error)?,
                now_ms,
                growth_thresholds,
            )?);
        }
        let total_page = agent_list_select(
            filter,
            caller,
            None,
            None,
            AgentListProjection::Count,
            custody.as_deref(),
        )?;
        let total: i64 = self
            .connection
            .query_row(
                &total_page.sql,
                rusqlite::params_from_iter(total_page.params.iter()),
                |row| row.get(0),
            )
            .map_err(sql_error)?;
        let state_totals = if filter.states.is_some() {
            let mut all_states = filter.clone();
            all_states.states = None;
            let select = agent_list_select(
                &all_states,
                caller,
                None,
                None,
                AgentListProjection::StateTotals,
                custody.as_deref(),
            )?;
            let mut statement = self.connection.prepare(&select.sql).map_err(sql_error)?;
            let rows = statement
                .query_map(rusqlite::params_from_iter(select.params.iter()), |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
                })
                .map_err(sql_error)?;
            let mut totals: Vec<_> = AGENT_LIST_STATES
                .into_iter()
                .map(|state| AgentStateTotal { state, total: 0 })
                .collect();
            for row in rows {
                let (state, total) = row.map_err(sql_error)?;
                let state: AgentState = parse_enum(&state, "Agent state")?;
                let entry = totals
                    .iter_mut()
                    .find(|entry| entry.state == state)
                    .ok_or_else(|| {
                        Error::new("internal", "Agent state missing from state totals")
                    })?;
                entry.total = usize::try_from(total)
                    .map_err(|_| Error::new("internal", "Agent state count exceeds usize"))?;
            }
            Some(totals)
        } else {
            None
        };
        let mut truncated = summaries.len() > limit;
        summaries.truncate(limit);
        let kept = summaries.len();
        enforce_page_bytes(&mut summaries)?;
        truncated = truncated || summaries.len() < kept;
        let next = summaries.last().map(|summary| summary.id.clone());
        Ok(AgentListPage {
            summaries,
            state_totals,
            total: usize::try_from(total.max(0)).unwrap_or(0),
            truncated,
            next,
        })
    }

    fn summary_from_record(
        &self,
        record: AgentRecord,
        now_ms: i64,
        growth_thresholds: &core::SessionGrowthThresholds,
    ) -> Result<AgentSummary> {
        let state: AgentState = parse_enum(&record.state, "Agent state")?;
        let id = format_agent_id(record.id);
        let terminal = self.last_terminal_turn(record.id, record.generation, &record.harness)?;
        let last_progress_at =
            native_children::native_progress(&self.connection, record.id, record.generation)?;
        // Fleet usage prefers the newest terminal turn's native turn usage.
        // When the last turn carries none (Muse never sends turn usage), fall
        // back to the generation's latest retained session snapshot so a
        // fleet scan never reads heavy session spend as no usage. The
        // fallback keeps only cumulative input/output/cache counters with the
        // session scope marker; it never fabricates a turn total.
        let (last_turn_id, last_outcome, last_resolution_source, turn_usage) = match terminal {
            Some((turn_id, outcome, source, usage)) => {
                (Some(turn_id), Some(outcome), source, usage)
            }
            None => (None, None, None, None),
        };
        let (usage, usage_scope) = match turn_usage {
            Some(usage) => (Some(usage), Some("turn".to_owned())),
            None => {
                let fallback = self
                    .get_session_usage(&id, record.generation)?
                    .and_then(|snapshot| session_fallback_usage(&snapshot));
                let scope = fallback.as_ref().map(|_| "session".to_owned());
                (fallback, scope)
            }
        };
        let heavy = self.heavy_rollup(&id, record.generation)?;
        let session_growth =
            self.session_growth_view(&record.harness, &id, record.generation, growth_thresholds)?;
        let preview = self.first_input_preview(record.id)?;
        let repo_common_dir = self
            .connection
            .query_row(
                "SELECT repo_common_dir FROM workspaces WHERE agent = ?1",
                [record.id],
                |row| row.get(0),
            )
            .optional()
            .map_err(sql_error)?;
        Ok(AgentSummary {
            archived: record.archived,
            chat: record.chat,
            id,
            generation: record.generation,
            project: record.project,
            repo_common_dir,
            workspace: record.workspace,
            harness: record.harness,
            name: record.name.filter(|name| !name.trim().is_empty()),
            profile: Some(record.profile).filter(|profile| !profile.trim().is_empty()),
            preview,
            state,
            parent: record.parent.map(format_agent_id),
            // A NULL root only arises from raw test rows without hierarchy
            // metadata and reads self-rooted, matching `agent_from_record`.
            root: format_agent_id(record.root.unwrap_or(record.id)),
            active_turn: record.active_turn,
            last_progress_at,
            progress_age_ms: last_progress_at
                .and_then(|at| now_ms.checked_sub(at))
                .filter(|age| *age >= 0),
            last_turn_id,
            last_outcome,
            last_resolution_source,
            activity: if record.origin == "native" && record.observe_only {
                Some(native_children::activity(
                    &self.connection,
                    record.id,
                    record.generation,
                )?)
            } else {
                None
            },
            usage,
            usage_scope,
            updated_at: record.updated_at,
            // The authorization scope the server compares for moves and
            // handoffs; the UI falls back to `project` when absent.
            project_scope: record.project_scope,
            heavy,
            session_growth,
        })
    }

    /// The opening characters of the Agent's oldest retained `send` text (a
    /// launch's linked first input included): one seek on
    /// `commands_target`. `None` when no send with text is retained.
    fn first_input_preview(&self, agent: i64) -> Result<Option<String>> {
        let preview: Option<Option<String>> = self
            .connection
            .query_row(
                "SELECT substr(json_extract(args, '$.text'), 1, ?2)
                 FROM commands INDEXED BY commands_target
                 WHERE target = ?1 AND verb = 'send'
                   AND json_type(args, '$.text') = 'text'
                 ORDER BY created_at, rowid LIMIT 1",
                params![agent, core::api::AGENT_PREVIEW_CHARS as i64],
                |row| row.get(0),
            )
            .optional()
            .map_err(sql_error)?;
        Ok(preview
            .flatten()
            .filter(|preview| !preview.trim().is_empty()))
    }

    /// Newest terminal turn for one Agent generation: its canonical outcome
    /// (legacy rows classify explicitly, never defaulting to success) with
    /// the retained usage where the Store kept one. No terminal turn reads
    /// back as no outcome, never a fabricated one.
    fn last_terminal_turn(
        &self,
        agent: i64,
        generation: i64,
        harness: &str,
    ) -> Result<Option<LastTerminalTurn>> {
        let row: Option<StoredTerminalTurn> = self
            .connection
            .query_row(
                "SELECT turn_id, outcome, status, usage, resolution_source FROM turns
                 WHERE agent = ?1 AND generation = ?2 AND state = 'completed'
                 ORDER BY COALESCE(completed_at, started_at, 0) DESC, rowid DESC LIMIT 1",
                params![agent, generation],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                },
            )
            .optional()
            .map_err(sql_error)?;
        row.map(|(turn_id, outcome, status, usage, source)| {
            let source: Option<TurnResolutionSource> = source
                .as_deref()
                .map(|value| parse_enum(value, "turn resolution source"))
                .transpose()?;
            let canonical = match outcome {
                Some(value) => TurnOutcome::parse(&value).ok_or_else(|| {
                    Error::new("internal", format!("invalid stored turn outcome: {value}"))
                })?,
                None => TurnOutcome::classify_legacy_status_for_harness(harness, status.as_deref()),
            };
            let outcome = source.map_or(canonical, |source| source.reported_outcome(canonical));
            let usage = usage.map(|value| decode_turn_usage(&value)).transpose()?;
            Ok((turn_id, outcome, source, usage))
        })
        .transpose()
    }
}

/// Indexed equality filters for the compact `agent.list` fleet projection.
/// Unknown shapes reject at the Core DTO boundary, never as empty matches.
#[derive(Clone, Debug, Default)]
pub struct AgentListFilter {
    pub archived: core::api::ArchivedFilter,
    pub state: Option<AgentState>,
    pub states: Option<Vec<AgentState>>,
    pub project: Option<String>,
    /// Exact authorization-scope match (`COALESCE(project_scope, project)`),
    /// the namespace reparent and handoff compare. Combined with `project`
    /// by conjunction when both are present.
    pub project_scope: Option<String>,
    pub harness: Option<String>,
    pub parent: Option<String>,
    pub root: Option<String>,
}

impl AgentListFilter {
    fn validate(&self) -> Result<()> {
        if let Some(states) = &self.states
            && (self.state.is_some()
                || states.is_empty()
                || states.len() > 9
                || states
                    .iter()
                    .enumerate()
                    .any(|(i, state)| states[..i].contains(state)))
        {
            return Err(Error::new("invalid", "invalid Agent state set"));
        }
        if let Some(project) = &self.project {
            require_filter_text(project)?;
        }
        if let Some(scope) = &self.project_scope {
            require_filter_text(scope)?;
        }
        if let Some(harness) = &self.harness {
            require_filter_text(harness)?;
        }
        if let Some(parent) = &self.parent {
            require_filter_text(parent)?;
            parse_agent_id(parent)?;
        }
        if let Some(root) = &self.root {
            require_filter_text(root)?;
            parse_agent_id(root)?;
        }
        Ok(())
    }
}

/// One filtered, scope-checked fleet page: ordered summaries, the matching
/// total ignoring the cursor, honest truncation, and the continuation cursor.
pub struct AgentListPage {
    pub summaries: Vec<AgentSummary>,
    pub state_totals: Option<Vec<AgentStateTotal>>,
    pub total: usize,
    pub truncated: bool,
    pub next: Option<String>,
}

/// Newest terminal turn behind one summary row: turn id, canonical outcome,
/// and retained usage where the Store kept one.
type LastTerminalTurn = (
    String,
    TurnOutcome,
    Option<TurnResolutionSource>,
    Option<TurnUsage>,
);

/// Stored terminal-turn columns before canonical classification.
type StoredTerminalTurn = (
    String,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
);

#[derive(Clone, Copy)]
enum AgentListProjection {
    Rows,
    Count,
    StateTotals,
}

struct AgentListSelect {
    sql: String,
    params: Vec<rusqlite::types::Value>,
}

fn require_filter_text(value: &str) -> Result<()> {
    if value.trim().is_empty() {
        return Err(Error::new("invalid", "Agent list filter must be nonblank"));
    }
    Ok(())
}

/// Assemble the fleet query for one scope, filter set, and window. Equality
/// filters ride their `agents_list_*` composite (forced with `INDEXED BY`,
/// so `EXPLAIN QUERY PLAN` reports the index, preferring the selective
/// structural/scope index when several filters are present); the cursor rides
/// the primary key; tree scope walks descendants through the bounded subtree
/// CTE shared with direct reads. The count variant drops the cursor and limit
/// but keeps scope and filters. The default archived exclusion splits into
/// unarchived and custody-visible archived branches merged in id order, so
/// each branch seeks its own archived partition and the merge stays bounded
/// by the page instead of scanning the fleet for a temporary sort.
fn agent_list_select(
    filter: &AgentListFilter,
    caller: Option<&Agent>,
    after_num: Option<i64>,
    limit: Option<i64>,
    projection: AgentListProjection,
    custody: Option<&[i64]>,
) -> Result<AgentListSelect> {
    let count = !matches!(projection, AgentListProjection::Rows);
    let columns = if matches!(projection, AgentListProjection::StateTotals) {
        "agents.state, COUNT(*)".to_owned()
    } else if count {
        "COUNT(*)".to_owned()
    } else {
        "agents.id, agents.generation, agents.name, agents.project, agents.harness, agents.profile,
                agents.profile_snapshot, agents.native_scope, agents.native_ref, agents.workspace, agents.effective,
                agents.pending, agents.state, agents.active_turn, agents.created_at, agents.updated_at, agents.origin, agents.observe_only, (agents.token IS NOT NULL AND agents.observe_only = 0),
                agents.parent, agents.root, agents.depth, agents.control, agents.profile_inherited, agents.profile_choice, agents.archived, agents.chat, agents.project_scope"
            .to_owned()
    };
    let mut predicates = Vec::new();
    let mut params: Vec<rusqlite::types::Value> = Vec::new();
    // Prefer the selective structural/scope index (parent/root/project/scope)
    // when present; state/harness composites only lead when no structural
    // filter does.
    fn push_equality(
        predicates: &mut Vec<String>,
        params: &mut Vec<rusqlite::types::Value>,
        column: &str,
        value: rusqlite::types::Value,
    ) {
        predicates.push(format!("agents.{column} = ?"));
        params.push(value);
    }
    let mut structural: Option<&str> = None;
    let mut fallback: Option<&str> = None;
    let mut note_index = |index: &'static str, structural_filter: bool| {
        if structural_filter {
            if structural.is_none() {
                structural = Some(index);
            }
        } else if fallback.is_none() {
            fallback = Some(index);
        }
    };
    if let Some(parent) = &filter.parent {
        push_equality(
            &mut predicates,
            &mut params,
            "parent",
            rusqlite::types::Value::from(parse_agent_id(parent)?),
        );
        note_index("agents_list_parent", true);
    }
    if let Some(root) = &filter.root {
        push_equality(
            &mut predicates,
            &mut params,
            "root",
            rusqlite::types::Value::from(parse_agent_id(root)?),
        );
        note_index("agents_list_root", true);
    }
    if let Some(project) = &filter.project {
        push_equality(
            &mut predicates,
            &mut params,
            "project",
            rusqlite::types::Value::from(project.clone()),
        );
        note_index("agents_list_project", true);
    }
    if let Some(scope) = &filter.project_scope {
        predicates.push("COALESCE(agents.project_scope, agents.project) = ?".to_owned());
        params.push(rusqlite::types::Value::from(scope.clone()));
        note_index("agents_list_project_scope", true);
    }
    if let Some(state) = filter.state {
        push_equality(
            &mut predicates,
            &mut params,
            "state",
            rusqlite::types::Value::from(agent_state_name(state).to_owned()),
        );
        note_index("agents_list_state", false);
    }
    if let Some(harness) = &filter.harness {
        push_equality(
            &mut predicates,
            &mut params,
            "harness",
            rusqlite::types::Value::from(harness.clone()),
        );
        note_index("agents_list_harness", false);
    }
    // Archive never hides custody the owner must still resolve: an ended
    // Agent that lost control after it was archived stays listed while its
    // process, artifact or native source is unresolved. The default exclusion
    // splits into two id-ordered branches below (live rows, then
    // custody-visible archived rows) instead of one OR across both archived
    // partitions, so each branch seeks its own partition and the merge stays
    // bounded by the page.
    let mut exclude_split = false;
    match filter.archived {
        core::api::ArchivedFilter::Exclude => exclude_split = true,
        core::api::ArchivedFilter::Only => {
            push_equality(&mut predicates, &mut params, "archived", 1.into())
        }
        core::api::ArchivedFilter::Include => {}
    }
    if let Some(states) = &filter.states {
        let placeholders = vec!["?"; states.len()].join(",");
        predicates.push(format!("agents.state IN ({placeholders})"));
        params.extend(
            states
                .iter()
                .map(|state| rusqlite::types::Value::from(agent_state_name(*state).to_owned())),
        );
        if fallback.is_none() {
            fallback = Some("agents_list_state");
        }
    }
    let mut forced_index = structural.or(fallback).or(match filter.archived {
        core::api::ArchivedFilter::Include => None,
        _ => Some("agents_list_archived"),
    });
    let mut prefix = String::new();
    if let Some(caller) = caller {
        let caller_num = parse_agent_id(&caller.id)?;
        match caller.control {
            AgentControl::Slf => {
                predicates.push("agents.id = ?".to_owned());
                params.push(rusqlite::types::Value::from(caller_num));
            }
            AgentControl::Project => {
                predicates.push("COALESCE(agents.project_scope, agents.project) = ?".to_owned());
                params.push(rusqlite::types::Value::from(
                    caller.project_scope().to_owned(),
                ));
                if structural.is_none() {
                    forced_index = Some("agents_list_project_scope");
                }
            }
            AgentControl::Tree => {
                // Same depth bound as direct reads (`hierarchy::is_in_subtree_of`):
                // descendants past the walk cap stay outside tree scope.
                prefix = format!(
                    "WITH RECURSIVE subtree(id, depth) AS \
                     (SELECT {caller_num}, 0 UNION ALL \
                     SELECT a.id, s.depth + 1 FROM agents a JOIN subtree s ON a.parent = s.id \
                     WHERE s.depth + 1 < {} ) ",
                    hierarchy::ANCESTOR_WALK_CAP
                );
                predicates.push("agents.id IN (SELECT id FROM subtree)".to_owned());
            }
        }
    }
    if let Some(after) = after_num {
        predicates.push("agents.id > ?".to_owned());
        params.push(rusqlite::types::Value::from(after));
    }
    let table = match forced_index {
        Some(index) => format!("agents INDEXED BY {index}"),
        None => "agents".to_owned(),
    };
    if !exclude_split {
        let mut sql = format!("{prefix}SELECT {columns} FROM {table}");
        if !predicates.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&predicates.join(" AND "));
        }
        if matches!(projection, AgentListProjection::StateTotals) {
            sql.push_str(" GROUP BY agents.state");
        }
        if !count {
            sql.push_str(" ORDER BY agents.id");
        }
        if let Some(limit) = limit {
            sql.push_str(" LIMIT ?");
            params.push(rusqlite::types::Value::from(limit));
        }
        return Ok(AgentListSelect { sql, params });
    }
    // Default exclusion as two bounded id-ordered branches: unarchived rows
    // seek the archived=0 partition, custody-visible archived rows seek by
    // primary key over the live custody set collected once per page, and the
    // outer merge stays bounded by the page. Each branch keeps its own
    // `ORDER BY agents.id LIMIT` so the global top page is exactly the merge
    // of the branch tops. Counts use the same custody set, so no page runs an
    // unbounded count across archived history with per-row custody probes.
    // Aggregate branches name their partial count so the outer merge sums
    // disjoint partitions instead of returning one row per branch.
    let branch_columns = match projection {
        AgentListProjection::Rows => columns.clone(),
        AgentListProjection::Count => "COUNT(*) AS branch_count".to_owned(),
        AgentListProjection::StateTotals => "agents.state, COUNT(*) AS branch_count".to_owned(),
    };
    let branch = |archived: i64, extra: Option<(String, Vec<rusqlite::types::Value>)>| {
        let mut branch_predicates = predicates.clone();
        branch_predicates.push(format!("agents.archived = {archived}"));
        let mut branch_params = params.clone();
        if let Some((extra_sql, extra_params)) = extra {
            branch_predicates.push(extra_sql);
            branch_params.extend(extra_params);
        }
        let mut select = format!(
            "SELECT {branch_columns} FROM {table} WHERE {}",
            branch_predicates.join(" AND ")
        );
        if matches!(projection, AgentListProjection::StateTotals) {
            select.push_str(" GROUP BY agents.state");
        }
        if !count {
            select.push_str(" ORDER BY agents.id");
        }
        if let Some(limit) = limit {
            select.push_str(" LIMIT ?");
            branch_params.push(rusqlite::types::Value::from(limit));
        }
        // Each leg reads its own subquery so its `ORDER BY ... LIMIT`
        // binds to the leg: SQLite rejects `ORDER BY` inside a bare
        // parenthesized compound leg.
        (format!("SELECT * FROM ({select})"), branch_params)
    };
    let (archived_live, live_params) = branch(0, None);
    let custody_ids: &[i64] = custody.unwrap_or(&[]);
    // Custody-held archived rows: terminal states holding live custody. The
    // id list is bounded by live custody (typically a handful), so this leg
    // seeks primary keys instead of walking the archived partition to fill
    // its page, and counts never probe custody per archived row.
    let held_extra: Option<(String, Vec<rusqlite::types::Value>)> = if custody_ids.is_empty() {
        None
    } else {
        let placeholders = custody_ids
            .iter()
            .map(|_| "?")
            .collect::<Vec<_>>()
            .join(",");
        let extra_sql = format!(
            "(agents.state IN ('stopped','failed','lost') AND agents.id IN ({placeholders}))"
        );
        let extra_params = custody_ids
            .iter()
            .map(|id| rusqlite::types::Value::from(*id))
            .collect();
        Some((extra_sql, extra_params))
    };
    let (archived_held, held_params) = branch(1, held_extra);
    // An empty custody set leaves only the live branch: no UNION arm can
    // contribute rows, counts, or state buckets.
    let use_held = !custody_ids.is_empty();
    let mut sql = if use_held {
        format!("{prefix}SELECT * FROM ({archived_live} UNION ALL {archived_held}) AS merged")
    } else {
        format!("{prefix}SELECT * FROM ({archived_live}) AS merged")
    };
    let mut merged_params = live_params;
    if use_held {
        merged_params.extend(held_params);
    }
    match projection {
        AgentListProjection::Rows => {
            sql.push_str(" ORDER BY merged.id LIMIT ?");
            merged_params.push(
                limit
                    .map(rusqlite::types::Value::from)
                    .expect("row pages always carry a limit"),
            );
        }
        AgentListProjection::Count => {
            if use_held {
                sql = format!(
                    "{prefix}SELECT SUM(branch_count) FROM ({archived_live} UNION ALL {archived_held}) AS merged"
                );
            } else {
                sql = format!("{prefix}SELECT SUM(branch_count) FROM ({archived_live}) AS merged");
            }
        }
        AgentListProjection::StateTotals => {
            if use_held {
                sql = format!(
                    "{prefix}SELECT state, SUM(branch_count) FROM ({archived_live} UNION ALL {archived_held}) AS merged GROUP BY state"
                );
            } else {
                sql = format!(
                    "{prefix}SELECT state, SUM(branch_count) FROM ({archived_live}) AS merged GROUP BY state"
                );
            }
        }
    }
    Ok(AgentListSelect {
        sql,
        params: merged_params,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn agent_summaries_carry_name_profile_and_first_send_preview() {
        let mut store = Store::open(Path::new(":memory:")).expect("open Store");
        let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
        let brief = format!(
            "# k75: sidebar titles

    {}",
            "word ".repeat(100)
        );
        let mut launch = SubmitCommand::new(Ulid::new().to_string(), "launch");
        launch.expected_generation = Some(0);
        launch.args = json!({"harness": "codex", "profile": "codex-lite",
            "profile_snapshot": {}, "project": "p", "workspace": "w", "text": brief});
        let titled = store
            .submit("owner", launch, now)
            .expect("admit launch with text")
            .command
            .target
            .expect("launch target");
        let mut bare = SubmitCommand::new(Ulid::new().to_string(), "launch");
        bare.expected_generation = Some(0);
        bare.args = json!({"harness": "muse", "profile": "muse",
            "profile_snapshot": {}, "project": "p", "workspace": "w"});
        let untitled = store
            .submit("owner", bare, now + 1)
            .expect("admit launch without text")
            .command
            .target
            .expect("launch target");
        store
            .connection
            .execute(
                "UPDATE agents SET name = 'planner' WHERE id = ?1",
                [eliot_core::parse_agent_id(&untitled).expect("parse id")],
            )
            .expect("name the Agent");

        let page = store
            .list_agent_summaries(
                &super::AgentListFilter::default(),
                None,
                10,
                now + 2,
                &core::SessionGrowthThresholds::default(),
            )
            .expect("list summaries");
        let row = |id: &str| {
            page.summaries
                .iter()
                .find(|summary| summary.id == id)
                .expect("listed")
        };
        let first = row(&titled);
        let expected: String = brief
            .chars()
            .take(eliot_core::api::AGENT_PREVIEW_CHARS)
            .collect();
        assert_eq!(first.preview.as_deref(), Some(expected.as_str()));
        assert_eq!(first.profile.as_deref(), Some("codex-lite"));
        assert_eq!(first.name, None);
        first.validate().expect("bounded preview validates");
        let second = row(&untitled);
        assert_eq!(second.preview, None);
        assert_eq!(second.name.as_deref(), Some("planner"));
        assert_eq!(second.profile.as_deref(), Some("muse"));
    }

    #[test]
    fn agent_list_filters_use_indexes_and_pages_stay_ordered() {
        use super::{AgentListFilter, agent_list_select};

        fn fleet_agent(store: &mut Store, project: &str, now: i64) -> String {
            let mut launch = SubmitCommand::new(Ulid::new().to_string(), "launch");
            launch.expected_generation = Some(0);
            launch.args = json!({"harness": "codex", "profile": "codex-test",
                "profile_snapshot": {}, "project": project, "workspace": "synthetic-workspace"});
            let command = launch.id.clone();
            let id = store
                .submit("owner", launch, now)
                .expect("admit fleet launch")
                .command
                .target
                .expect("launch target");
            store
                .dispatch(&command, now)
                .expect("dispatch fleet launch");
            id
        }

        let mut store = Store::open(Path::new(":memory:")).expect("open Store");
        let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
        let first = fleet_agent(&mut store, "fleet-a", now);
        let second = fleet_agent(&mut store, "fleet-a", now + 1);
        let third = fleet_agent(&mut store, "fleet-b", now + 2);
        // Shape one child link so subtree filters have a descendant to find.
        let first_num = eliot_core::parse_agent_id(&first).expect("parse fleet root");
        let second_num = eliot_core::parse_agent_id(&second).expect("parse fleet child");
        store
            .connection
            .execute(
                "UPDATE agents SET parent = ?1, root = ?1, depth = 1 WHERE id = ?2",
                [first_num, second_num],
            )
            .expect("link fleet child");

        // Every new filter rides its composite index.
        for (filter, index) in [
            (
                AgentListFilter {
                    state: Some(AgentState::Idle),
                    ..Default::default()
                },
                "agents_list_state",
            ),
            (
                AgentListFilter {
                    project: Some("fleet-a".to_owned()),
                    ..Default::default()
                },
                "agents_list_project",
            ),
            (
                AgentListFilter {
                    project_scope: Some("fleet-a".to_owned()),
                    ..Default::default()
                },
                "agents_list_project_scope",
            ),
            (
                AgentListFilter {
                    harness: Some("codex".to_owned()),
                    ..Default::default()
                },
                "agents_list_harness",
            ),
            (
                AgentListFilter {
                    parent: Some(first.clone()),
                    ..Default::default()
                },
                "agents_list_parent",
            ),
            (
                AgentListFilter {
                    root: Some(first.clone()),
                    ..Default::default()
                },
                "agents_list_root",
            ),
        ] {
            let select = agent_list_select(
                &filter,
                None,
                Some(0),
                Some(2),
                AgentListProjection::Rows,
                None,
            )
            .expect("build fleet query");
            let mut statement = store
                .connection
                .prepare(&format!("EXPLAIN QUERY PLAN {}", select.sql))
                .expect("explain fleet query");
            let plan = statement
                .query_map(rusqlite::params_from_iter(select.params.iter()), |row| {
                    row.get::<_, String>(3)
                })
                .expect("read fleet plan")
                .collect::<rusqlite::Result<Vec<_>>>()
                .expect("collect fleet plan")
                .join(" | ");
            assert!(
                plan.contains(&format!("USING INDEX {index}")),
                "filter must use {index}: {plan}"
            );
        }

        // Ordered continuation across pages with honest counts.
        let filter = AgentListFilter::default();
        let mut after = None;
        let mut seen = Vec::new();
        for last in [false, false, true] {
            let page = store
                .list_agent_summaries(
                    &filter,
                    after.as_deref(),
                    1,
                    now + 9,
                    &core::SessionGrowthThresholds::default(),
                )
                .expect("page the fleet");
            assert_eq!(page.total, 3);
            assert_eq!(page.truncated, !last);
            assert_eq!(page.summaries.len(), 1);
            assert_eq!(page.next.as_deref(), Some(page.summaries[0].id.as_str()));
            assert!(seen.iter().all(|id| id != &page.summaries[0].id));
            if last {
                assert!(page.summaries[0].id == third);
            }
            after = page.next;
            seen.push(page.summaries.into_iter().next().unwrap().id);
        }
        assert!(seen.windows(2).all(|pair| pair[0] < pair[1]));
        let drained = store
            .list_agent_summaries(
                &filter,
                after.as_deref(),
                1,
                now + 9,
                &core::SessionGrowthThresholds::default(),
            )
            .expect("drain the fleet");
        assert!(drained.summaries.is_empty());
        assert_eq!(drained.total, 3);
        assert!(!drained.truncated);

        // Filtered totals ignore the cursor.
        let project = store
            .list_agent_summaries(
                &AgentListFilter {
                    project: Some("fleet-a".to_owned()),
                    ..Default::default()
                },
                Some(seen[1].as_str()),
                10,
                now + 9,
                &core::SessionGrowthThresholds::default(),
            )
            .expect("filter the fleet by project");
        assert_eq!(project.total, 2);
        assert!(!project.truncated);
        let root = store
            .list_agent_summaries(
                &AgentListFilter {
                    root: Some(first.clone()),
                    ..Default::default()
                },
                None,
                10,
                now + 9,
                &core::SessionGrowthThresholds::default(),
            )
            .expect("filter the fleet by root");
        assert_eq!(root.total, 2);
        let parent = store
            .list_agent_summaries(
                &AgentListFilter {
                    parent: Some(first.clone()),
                    ..Default::default()
                },
                None,
                10,
                now + 9,
                &core::SessionGrowthThresholds::default(),
            )
            .expect("filter the fleet by parent");
        assert_eq!(parent.total, 1);
        assert_eq!(parent.summaries[0].id, second);
        assert_eq!(parent.summaries[0].root, first);

        // Compact rows carry freshness without fabricating outcomes.
        let probe = &root.summaries[0];
        assert_eq!(probe.last_outcome, None);
        assert_eq!(probe.last_turn_id, None);
        assert_eq!(probe.last_progress_at, None);
        assert_eq!(probe.progress_age_ms, None);
        assert!(probe.updated_at >= now);

        // Scopes: self sees one row, tree sees the subtree, project the label.
        let agent = store
            .get_agent(&first)
            .expect("read fleet root")
            .expect("fleet root");
        let mut tree = agent.clone();
        tree.control = AgentControl::Tree;
        let scoped = store
            .list_agent_summaries_scoped(
                &tree,
                &AgentListFilter::default(),
                None,
                10,
                now + 9,
                &core::SessionGrowthThresholds::default(),
            )
            .expect("list the fleet subtree");
        assert_eq!(scoped.total, 2);
        let mut project_caller = agent.clone();
        project_caller.control = AgentControl::Project;
        let scoped = store
            .list_agent_summaries_scoped(
                &project_caller,
                &AgentListFilter::default(),
                None,
                10,
                now + 9,
                &core::SessionGrowthThresholds::default(),
            )
            .expect("list the fleet project");
        assert_eq!(scoped.total, 2);
        let scoped = store
            .list_agent_summaries_scoped(
                &agent,
                &AgentListFilter::default(),
                None,
                10,
                now + 9,
                &core::SessionGrowthThresholds::default(),
            )
            .expect("list the fleet self row");
        assert_eq!(scoped.total, 1);
        assert_eq!(scoped.summaries[0].id, first);

        // One state-set page is bounded globally; all-state totals share the
        // row scope and filters, but ignore the state set and cursor.
        store
            .connection
            .execute(
                "UPDATE agents SET state='stopped' WHERE id=?1",
                [second_num],
            )
            .unwrap();
        store
            .connection
            .execute(
                "UPDATE agents SET state='busy' WHERE id=?1",
                [parse_agent_id(&third).unwrap()],
            )
            .unwrap();
        let states = AgentListFilter {
            states: Some(vec![AgentState::Starting, AgentState::Busy]),
            ..Default::default()
        };
        for (caller, expected, stopped, busy) in [
            (None, 2, 1, 1),
            (Some(&agent), 1, 0, 0),
            (Some(&tree), 1, 1, 0),
            (Some(&project_caller), 1, 1, 0),
        ] {
            for after in [None, Some(first.as_str())] {
                let page = store
                    .list_agent_summaries_inner(
                        caller,
                        &states,
                        after,
                        1,
                        now + 10,
                        &core::SessionGrowthThresholds::default(),
                    )
                    .unwrap();
                assert_eq!(page.total, expected);
                assert!(page.summaries.len() <= 1);
                assert_eq!(page.truncated, after.is_none() && expected > 1);
                let totals = page.state_totals.unwrap();
                assert_eq!(totals.len(), 9);
                let count = |state| {
                    totals
                        .iter()
                        .find(|entry| entry.state == state)
                        .unwrap()
                        .total
                };
                assert_eq!(count(AgentState::Stopped), stopped);
                assert_eq!(count(AgentState::Busy), busy);
                assert_eq!(count(AgentState::Starting), 1);
                assert_eq!(count(AgentState::Idle), 0);
            }
        }
        let select = agent_list_select(
            &states,
            None,
            Some(0),
            Some(1),
            AgentListProjection::Rows,
            None,
        )
        .unwrap();
        let mut statement = store
            .connection
            .prepare(&format!("EXPLAIN QUERY PLAN {}", select.sql))
            .unwrap();
        let plan = statement
            .query_map(rusqlite::params_from_iter(select.params.iter()), |row| {
                row.get::<_, String>(3)
            })
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap()
            .join(" | ");
        assert!(plan.contains("USING INDEX agents_list_state"), "{plan}");
    }

    #[test]
    fn project_scope_filter_reaches_legacy_and_registered_rows_together() {
        fn scope_agent(store: &mut Store, project: &str, now: i64) -> String {
            let mut launch = SubmitCommand::new(Ulid::new().to_string(), "launch");
            launch.expected_generation = Some(0);
            launch.args = json!({"harness": "codex", "profile": "codex-test",
                "profile_snapshot": {}, "project": project, "workspace": "synthetic-workspace"});
            let command = launch.id.clone();
            let id = store
                .submit("owner", launch, now)
                .expect("admit scope launch")
                .command
                .target
                .expect("launch target");
            store
                .dispatch(&command, now)
                .expect("dispatch scope launch");
            id
        }

        let mut store = Store::open(Path::new(":memory:")).expect("open Store");
        let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
        let legacy = scope_agent(&mut store, "scope-s", now);
        let registered = scope_agent(&mut store, "reg-id", now + 1);
        // A registered row keeps its registration label and retains the scope.
        store
            .connection
            .execute(
                "UPDATE agents SET project_scope = 'scope-s' WHERE id = ?1",
                [eliot_core::parse_agent_id(&registered).expect("parse registered")],
            )
            .expect("retain the registered scope");

        // One scope read reaches both rows; the label filter keeps only the
        // legacy row with that label.
        let scoped = store
            .list_agent_summaries(
                &AgentListFilter {
                    project_scope: Some("scope-s".to_owned()),
                    ..Default::default()
                },
                None,
                10,
                now + 2,
                &core::SessionGrowthThresholds::default(),
            )
            .expect("filter the fleet by scope");
        assert_eq!(scoped.total, 2);
        let mut ids: Vec<_> = scoped.summaries.iter().map(|row| row.id.clone()).collect();
        ids.sort();
        let mut expected = vec![legacy.clone(), registered.clone()];
        expected.sort();
        assert_eq!(ids, expected);
        let labeled = store
            .list_agent_summaries(
                &AgentListFilter {
                    project: Some("scope-s".to_owned()),
                    ..Default::default()
                },
                None,
                10,
                now + 2,
                &core::SessionGrowthThresholds::default(),
            )
            .expect("filter the fleet by label");
        assert_eq!(labeled.total, 1);
        assert_eq!(labeled.summaries[0].id, legacy);
    }

    #[test]
    fn default_exclusion_seeks_partitions_and_keeps_custody_visible() {
        use super::{AgentListFilter, agent_list_select};

        fn fleet_agent(store: &mut Store, project: &str, now: i64) -> String {
            let mut launch = SubmitCommand::new(Ulid::new().to_string(), "launch");
            launch.expected_generation = Some(0);
            launch.args = json!({"harness": "codex", "profile": "codex-test",
                "profile_snapshot": {}, "project": project, "workspace": "synthetic-workspace"});
            let command = launch.id.clone();
            let id = store
                .submit("owner", launch, now)
                .expect("admit fleet launch")
                .command
                .target
                .expect("launch target");
            store
                .dispatch(&command, now)
                .expect("dispatch fleet launch");
            id
        }

        fn explain(store: &Store, select: &super::AgentListSelect) -> String {
            let mut statement = store
                .connection
                .prepare(&format!("EXPLAIN QUERY PLAN {}", select.sql))
                .expect("explain default fleet query");
            statement
                .query_map(rusqlite::params_from_iter(select.params.iter()), |row| {
                    row.get::<_, String>(3)
                })
                .expect("read default fleet plan")
                .collect::<rusqlite::Result<Vec<_>>>()
                .expect("collect default fleet plan")
                .join(" | ")
        }

        let mut store = Store::open(Path::new(":memory:")).expect("open Store");
        let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
        let live = fleet_agent(&mut store, "fleet-a", now);
        let quiet = fleet_agent(&mut store, "fleet-a", now + 1);
        let held = fleet_agent(&mut store, "fleet-a", now + 2);
        // A stopped archived Agent with no unresolved custody stays hidden;
        // one with an unknown artifact stays listed while its custody is
        // unresolved.
        for (agent, archived) in [(&quiet, 1), (&held, 1)] {
            let num = eliot_core::parse_agent_id(agent).expect("parse fleet id");
            store
                .connection
                .execute(
                    "UPDATE agents SET state = 'stopped', archived = ?1 WHERE id = ?2",
                    rusqlite::params![archived, num],
                )
                .expect("archive fleet agent");
        }
        let held_num = eliot_core::parse_agent_id(&held).expect("parse held id");
        // Launch seeds an open stream source per Agent; close them all so the
        // only remaining custody is the unknown artifact below.
        store
            .connection
            .execute("UPDATE source_state SET state = 'closed'", [])
            .expect("close fixture sources");
        store
            .connection
            .execute(
                "INSERT INTO artifacts(id, agent, generation, path, body) VALUES ('custody-1', ?1, 1, 'C:/custody', '{\"state\":\"unknown\"}')",
                [held_num],
            )
            .expect("retain unresolved custody");
        let filter = AgentListFilter::default();
        // The default page and its deep continuation must seek, never scan
        // the fleet: the live branch seeks its partition and the
        // custody-held archived leg seeks primary keys over the live custody
        // set, so LIMIT bounds the response only when the access path is
        // already bounded. Counts use the same custody set and never probe
        // custody per archived row.
        let custody = super::agent_lifecycle::default_list_custody_agents(&store.connection)
            .expect("collect custody");
        for after in [0, 10_000] {
            let select = agent_list_select(
                &filter,
                None,
                Some(after),
                Some(2),
                AgentListProjection::Rows,
                Some(&custody),
            )
            .expect("build default fleet query");
            let plan = explain(&store, &select);
            assert!(
                !plan.contains("SCAN agents"),
                "default page must not scan the fleet (after={after}): {plan}"
            );
            assert!(
                !plan.contains("EXISTS"),
                "default page must not probe custody per archived row (after={after}): {plan}"
            );
        }
        // Totals use the same custody set: no unbounded count across archived
        // history with per-row custody probes.
        for projection in [AgentListProjection::Count, AgentListProjection::StateTotals] {
            let select = agent_list_select(&filter, None, None, None, projection, Some(&custody))
                .expect("build default totals query");
            let plan = explain(&store, &select);
            assert!(
                !plan.contains("SCAN agents"),
                "default totals must not scan the fleet: {plan}"
            );
            assert!(
                !plan.contains("EXISTS"),
                "default totals must not probe custody per archived row: {plan}"
            );
        }
        // Both branches still read back: the quiet archived Agent is hidden
        // while the custody-held one stays listed, in id order.
        let page = store
            .list_agent_summaries(
                &filter,
                None,
                10,
                now + 9,
                &core::SessionGrowthThresholds::default(),
            )
            .expect("list the fleet");
        assert_eq!(page.total, 2);
        let ids: Vec<_> = page
            .summaries
            .iter()
            .map(|summary| summary.id.clone())
            .collect();
        assert_eq!(ids, vec![live.clone(), held.clone()]);
        // The continuation cursor crosses the branch boundary in order.
        let first = store
            .list_agent_summaries(
                &filter,
                None,
                1,
                now + 9,
                &core::SessionGrowthThresholds::default(),
            )
            .expect("first fleet page");
        assert_eq!(first.summaries.len(), 1);
        assert_eq!(first.summaries[0].id, live);
        assert!(first.truncated);
        let second = store
            .list_agent_summaries(
                &filter,
                first.next.as_deref(),
                1,
                now + 9,
                &core::SessionGrowthThresholds::default(),
            )
            .expect("second fleet page");
        assert_eq!(second.summaries.len(), 1);
        assert_eq!(second.summaries[0].id, held);
        assert!(!second.truncated);
    }

    #[test]
    fn tree_scope_listing_shares_bounded_depth_with_direct_reads() {
        use super::super::hierarchy::ANCESTOR_WALK_CAP;
        use super::{AgentListFilter, agent_list_select};

        // Listing, totals and direct reads share one bounded scope rule: the
        // tree CTE carries the same walk cap that gates parent-chain direct
        // reads, so a tree caller cannot list or count an Agent its `agent.get`
        // would deny.
        let mut caller = eliot_core::Agent::new("a1", "p", "codex", "fixture", 7);
        caller.control = eliot_core::AgentControl::Tree;
        let filter = AgentListFilter::default();
        for count in [false, true] {
            let select = agent_list_select(
                &filter,
                Some(&caller),
                None,
                Some(2),
                if count {
                    AgentListProjection::Count
                } else {
                    AgentListProjection::Rows
                },
                None,
            )
            .expect("build tree scope query");
            assert!(
                select.sql.contains(&ANCESTOR_WALK_CAP.to_string()),
                "tree scope must bound depth with the shared cap: {}",
                select.sql
            );
            assert!(
                select.sql.contains("subtree(id, depth)"),
                "tree scope must track CTE depth: {}",
                select.sql
            );
        }
        // The selective structural index wins over state when both are present,
        // so `state=idle,parent=a42` totals do not scan all idle Agents.
        let combined = AgentListFilter {
            state: Some(eliot_core::AgentState::Idle),
            parent: Some("a42".to_owned()),
            ..Default::default()
        };
        let select = agent_list_select(
            &combined,
            None,
            Some(0),
            Some(2),
            AgentListProjection::Rows,
            None,
        )
        .expect("build combined filter query");
        assert!(
            select.sql.contains("INDEXED BY agents_list_parent"),
            "combined filters must prefer the parent index: {}",
            select.sql
        );
        assert!(
            !select.sql.contains("INDEXED BY agents_list_state"),
            "combined filters must not force the state index: {}",
            select.sql
        );
    }
}
