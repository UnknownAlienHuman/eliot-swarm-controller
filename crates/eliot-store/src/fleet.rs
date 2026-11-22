use super::status_views::{AgentStatusLookup, AgentStatusProjection};
use super::*;
use eliot_core::api::{
    AGENT_LIST_STATES, AgentListOrder, AgentStateTotal, AgentSummary, decode_recent_cursor,
    encode_recent_cursor,
};

impl Store {
    /// Compact fleet projection for `agent.list`: scope-checked, filtered,
    /// ordered by Agent ID (`order: id`) or newest activity first
    /// (`order: recent`: `updated_at` descending, then ID descending, with a
    /// stable `(updated_at, id)` cursor on the `agents_recent` index).
    /// Every equality filter rides one `agents_list_*` composite index
    /// (forced with `INDEXED BY`, so `EXPLAIN QUERY PLAN` shows the index);
    /// the id cursor rides the primary key. The response carries the
    /// matching total ignoring the cursor, honest truncation, and the
    /// continuation cursor at the last returned row.
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
        if filter.order == AgentListOrder::Recent {
            return self.list_agent_summaries_recent(
                caller,
                filter,
                after,
                limit,
                now_ms,
                growth_thresholds,
            );
        }
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
        self.read_agent_list_page(
            filter,
            page,
            limit,
            |records| self.summaries_from_records(records, now_ms, growth_thresholds),
            |scoped, projection| {
                agent_list_select(scoped, caller, None, None, projection, custody.as_deref())
            },
            |summary| summary.id.clone(),
        )
    }

    /// Shared fleet page tail for both orders: the row query, the total and
    /// optional per-state totals (both ignore the cursor and come from
    /// `select_page`), then the byte bound. Each caller builds its own row
    /// select for its cursor and encodes its own `next` cursor.
    fn read_agent_list_page(
        &self,
        filter: &AgentListFilter,
        page: AgentListSelect,
        limit: usize,
        summarize: impl Fn(Vec<AgentRecord>) -> Result<Vec<AgentSummary>>,
        select_page: impl Fn(&AgentListFilter, AgentListProjection) -> Result<AgentListSelect>,
        next_cursor: impl Fn(&AgentSummary) -> String,
    ) -> Result<AgentListPage> {
        let records = {
            let mut statement = self.connection.prepare(&page.sql).map_err(sql_error)?;
            let rows = statement
                .query_map(
                    rusqlite::params_from_iter(page.params.iter()),
                    agent_record_from_row,
                )
                .map_err(sql_error)?;
            let mut records = Vec::new();
            for row in rows {
                records.push(row.map_err(sql_error)?);
            }
            records
        };
        let mut summaries = summarize(records)?;
        let total_page = select_page(filter, AgentListProjection::Count)?;
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
            let select = select_page(&all_states, AgentListProjection::StateTotals)?;
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
        let next = summaries.last().map(next_cursor);
        Ok(AgentListPage {
            summaries,
            state_totals,
            total: usize::try_from(total.max(0)).unwrap_or(0),
            truncated,
            next,
        })
    }

    /// Recent-order fleet page: newest activity first (`updated_at`
    /// descending, then numeric ID descending) with a stable
    /// `(updated_at, id)` cursor seeking the `agents_recent` index.
    /// `updated_at` is bumped by the core reducer on every native event,
    /// so a busy Agent with fresh progress outranks an idle one touched
    /// long ago, and the key never regresses when old events are pruned.
    /// Totals ignore the cursor; `next` encodes the last row as
    /// `id@updated_at`.
    fn list_agent_summaries_recent(
        &self,
        caller: Option<&Agent>,
        filter: &AgentListFilter,
        after: Option<&str>,
        limit: usize,
        now_ms: i64,
        growth_thresholds: &core::SessionGrowthThresholds,
    ) -> Result<AgentListPage> {
        let after_cursor: Option<(i64, i64)> = after
            .map(decode_recent_cursor)
            .transpose()?
            .map(|(id, activity)| Ok::<_, Error>((activity, parse_agent_id(&id)?)))
            .transpose()?;
        let (after_activity, after_id) = after_cursor.unwrap_or((i64::MAX, i64::MAX));
        let custody = if matches!(filter.archived, core::api::ArchivedFilter::Exclude) {
            Some(super::agent_lifecycle::default_list_custody_agents(
                &self.connection,
            )?)
        } else {
            None
        };
        let probe = i64::try_from(limit)
            .map_err(|_| Error::new("invalid", "page limit must fit the index bound"))?
            + 1;
        let page = agent_list_select_recent(
            filter,
            caller,
            Some((after_activity, after_id)),
            Some(probe),
            AgentListProjection::Rows,
            custody.as_deref(),
        )?;
        self.read_agent_list_page(
            filter,
            page,
            limit,
            |records| self.summaries_from_records(records, now_ms, growth_thresholds),
            |scoped, projection| {
                agent_list_select_recent(scoped, caller, None, None, projection, custody.as_deref())
            },
            |summary| encode_recent_cursor(&summary.id, summary.updated_at),
        )
    }

    fn summaries_from_records(
        &self,
        records: Vec<AgentRecord>,
        now_ms: i64,
        growth_thresholds: &core::SessionGrowthThresholds,
    ) -> Result<Vec<AgentSummary>> {
        let agents: Vec<_> = records
            .iter()
            .map(|record| (record.id, record.generation))
            .collect();
        let progress = native_children::native_progress_for_page(&self.connection, &agents)?;
        if progress.len() != records.len() {
            return Err(Error::new("internal", "Agent progress page is incomplete"));
        }
        let mut lookups = Vec::with_capacity(records.len());
        for (record, &last_progress_at) in records.iter().zip(&progress) {
            let profile_snapshot: Value = decode_json(&record.profile_snapshot)?;
            lookups.push(AgentStatusLookup {
                agent_num: record.id,
                generation: record.generation,
                active_turn: record.active_turn.clone(),
                last_progress_at,
                provider_scope: core::frozen_provider_scope_for(
                    &record.profile,
                    &profile_snapshot,
                )?,
            });
        }
        let statuses = self.project_agent_statuses(&lookups)?;
        if statuses.len() != records.len() {
            return Err(Error::new("internal", "Agent status page is incomplete"));
        }
        records
            .into_iter()
            .zip(progress)
            .zip(statuses)
            .map(|((record, last_progress_at), status)| {
                self.summary_from_record(
                    record,
                    now_ms,
                    growth_thresholds,
                    last_progress_at,
                    status,
                )
            })
            .collect()
    }

    fn summary_from_record(
        &self,
        record: AgentRecord,
        now_ms: i64,
        growth_thresholds: &core::SessionGrowthThresholds,
        last_progress_at: Option<i64>,
        status: AgentStatusProjection,
    ) -> Result<AgentSummary> {
        let state: AgentState = parse_enum(&record.state, "Agent state")?;
        let id = format_agent_id(record.id);
        let terminal = self.last_terminal_turn(record.id, record.generation, &record.harness)?;
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
        let (usage, usage_scope) = match turn_usage.filter(|usage| usage.reported_total().is_some())
        {
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
        let work = Some(self.work_summary_by_num(
            record.id,
            record.generation,
            record.workspace.as_deref(),
            record.active_turn.as_deref(),
        )?);
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
        let (organization, tags, organization_revision) =
            organization::agent_organization(&self.connection, record.id)?;
        // The retained `effective.model` of the row already read (the value
        // `agent.get`/`agent.result` return), so the list needs no extra read.
        let model = record
            .effective
            .as_deref()
            .map(decode_json)
            .transpose()?
            .and_then(|effective| {
                effective
                    .get("model")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
            })
            .filter(|model| !model.trim().is_empty());
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
            organization,
            tags,
            organization_revision,
            model,
            work,
            provider_hold: status.provider_hold,
            native_notice: status.native_notice,
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
            let usage = usage
                .map(|value| decode_turn_usage(&value, harness))
                .transpose()?;
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
    pub order: AgentListOrder,
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
    /// Word search ([`agent_search_predicate`]) over the id, retained
    /// name, first-input preview, project label, workspace and harness.
    /// Rows, counts and state totals of both orders apply it alike; it
    /// never rides an index.
    pub query: Option<String>,
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
        if let Some(query) = &self.query {
            require_filter_text(query)?;
        }
        Ok(())
    }
}

/// The `query` predicate over one Agent row (`alias` names the `agents`
/// table), shared by `agent.list` and the organization projection so both
/// surfaces match the same fields: the `aN` id, the retained name, the
/// first-input preview (the oldest retained `send` text, capped at
/// `core::api::AGENT_PREVIEW_CHARS` characters, a launch's linked first
/// input included), the project label, the execution directory and the
/// harness. Every whitespace-separated word must be contained, ASCII
/// case-insensitively (`instr`, so `%` and `_` stay literal). `params`
/// yields one placeholder per word (`?` or a numbered `?N`); the words are
/// bound, never interpolated.
pub(super) fn agent_search_predicate(
    alias: &str,
    params: impl IntoIterator<Item = String>,
) -> String {
    let haystack = format!(
        "lower('a' || {alias}.id || ' ' || COALESCE({alias}.name, '') || ' ' || COALESCE(( \
         SELECT substr(json_extract(c.args, '$.text'), 1, {preview}) FROM commands c \
         WHERE c.target = {alias}.id AND c.verb = 'send' \
         AND json_type(c.args, '$.text') = 'text' \
         ORDER BY c.created_at, c.rowid LIMIT 1), '') || ' ' || {alias}.project || ' ' \
         || COALESCE({alias}.workspace, '') || ' ' || {alias}.harness)",
        preview = core::api::AGENT_PREVIEW_CHARS,
    );
    params
        .into_iter()
        .map(|param| format!("instr({haystack}, lower({param})) > 0"))
        .collect::<Vec<_>>()
        .join(" AND ")
}

/// The words of one search `query`, each bound to one placeholder of
/// [`agent_search_predicate`].
pub(super) fn search_words(query: &str) -> Vec<rusqlite::types::Value> {
    query
        .split_whitespace()
        .map(|word| rusqlite::types::Value::from(word.to_owned()))
        .collect()
}

/// Push the `query` predicate with bare placeholders and its bound words.
fn push_search(
    predicates: &mut Vec<String>,
    params: &mut Vec<rusqlite::types::Value>,
    query: &str,
) {
    let words = search_words(query);
    predicates.push(agent_search_predicate(
        "agents",
        words.iter().map(|_| "?".to_owned()),
    ));
    params.extend(words);
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

/// Assemble the `recent` fleet query: newest activity first
/// (`updated_at` descending, then numeric ID descending) with a stable
/// `(updated_at, id)` cursor. The ordering and the continuation predicate
/// both seek the `agents_recent` index, so the first and continuation pages
/// stay bounded on real volume. Unlike the ID-order path (which splits the
/// archived exclusion into two indexed branches), the recent path keeps one
/// predicate with the same custody set, so the first screen always carries
/// the newest Agents.
#[allow(clippy::too_many_arguments)]
fn agent_list_select_recent(
    filter: &AgentListFilter,
    caller: Option<&Agent>,
    after: Option<(i64, i64)>,
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
        AGENT_RECORD_COLUMNS.to_owned()
    };
    let mut predicates = Vec::new();
    let mut params: Vec<rusqlite::types::Value> = Vec::new();
    if let Some(parent) = &filter.parent {
        predicates.push("agents.parent = ?".to_owned());
        params.push(rusqlite::types::Value::from(parse_agent_id(parent)?));
    }
    if let Some(root) = &filter.root {
        predicates.push("agents.root = ?".to_owned());
        params.push(rusqlite::types::Value::from(parse_agent_id(root)?));
    }
    if let Some(project) = &filter.project {
        predicates.push("agents.project = ?".to_owned());
        params.push(rusqlite::types::Value::from(project.clone()));
    }
    if let Some(scope) = &filter.project_scope {
        predicates.push("COALESCE(agents.project_scope, agents.project) = ?".to_owned());
        params.push(rusqlite::types::Value::from(scope.clone()));
    }
    if let Some(state) = filter.state {
        predicates.push("agents.state = ?".to_owned());
        params.push(rusqlite::types::Value::from(
            agent_state_name(state).to_owned(),
        ));
    }
    if let Some(harness) = &filter.harness {
        predicates.push("agents.harness = ?".to_owned());
        params.push(rusqlite::types::Value::from(harness.clone()));
    }
    if let Some(query) = &filter.query {
        push_search(&mut predicates, &mut params, query);
    }
    match filter.archived {
        core::api::ArchivedFilter::Exclude => {
            let held: &[i64] = custody.unwrap_or(&[]);
            if held.is_empty() {
                predicates.push("agents.archived = 0".to_owned());
            } else {
                let placeholders = held.iter().map(|_| "?").collect::<Vec<_>>().join(",");
                predicates.push(format!(
                    "(agents.archived = 0 OR (agents.archived = 1 AND agents.state IN ({}) AND agents.id IN ({placeholders})))",
                    super::agent_lifecycle::CUSTODY_VISIBLE_STATES,
                ));
                params.extend(held.iter().map(|id| rusqlite::types::Value::from(*id)));
            }
        }
        core::api::ArchivedFilter::Only => {
            predicates.push("agents.archived = 1".to_owned());
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
    }
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
            }
            AgentControl::Tree => {
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
    if let Some((after_activity, after_id)) = after {
        predicates.push(
            "(agents.updated_at < ? OR (agents.updated_at = ? AND agents.id < ?))".to_owned(),
        );
        params.push(rusqlite::types::Value::from(after_activity));
        params.push(rusqlite::types::Value::from(after_activity));
        params.push(rusqlite::types::Value::from(after_id));
    }
    // The unfiltered pagination path (the sidebar's head and history pages)
    // rides `agents_recent` outright, so the ordering never builds a
    // temporary B-tree over the fleet. Filtered rows (attention states,
    // project, branch children, scoped callers) let the planner pick the
    // selective index and sort the bounded match instead of scanning the
    // whole fleet in recency order.
    let selective = filter.parent.is_some()
        || filter.root.is_some()
        || filter.project.is_some()
        || filter.project_scope.is_some()
        || filter.state.is_some()
        || filter.harness.is_some()
        || filter.states.is_some()
        || filter.query.is_some()
        || caller.is_some();
    let table = if !count && !selective {
        "agents INDEXED BY agents_recent"
    } else {
        "agents"
    };
    let mut sql = format!("{prefix}SELECT {columns} FROM {table}");
    if !predicates.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&predicates.join(" AND "));
    }
    if matches!(projection, AgentListProjection::StateTotals) {
        sql.push_str(" GROUP BY agents.state");
    }
    if !count {
        sql.push_str(" ORDER BY agents.updated_at DESC, agents.id DESC");
    }
    if let Some(limit) = limit {
        sql.push_str(" LIMIT ?");
        params.push(rusqlite::types::Value::from(limit));
    }
    Ok(AgentListSelect { sql, params })
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
        AGENT_RECORD_COLUMNS.to_owned()
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
    if let Some(query) = &filter.query {
        push_search(&mut predicates, &mut params, query);
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
            "(agents.state IN ({}) AND agents.id IN ({placeholders}))",
            super::agent_lifecycle::CUSTODY_VISIBLE_STATES,
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
