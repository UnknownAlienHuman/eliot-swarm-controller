use super::*;

/// Before scoped coverage, a retained creation proves that prefix pruning
/// never reached this Agent. Otherwise resume before its first surviving row.
pub(super) const LEGACY_AGENT_EVENT_FLOORS: &str = "
    SELECT a.id, MIN(c.pruned_through, COALESCE(
        (SELECT MIN(seq) - 1 FROM events WHERE agent = a.id), c.pruned_through
    )) AS floor
    FROM rule_history_coverage c CROSS JOIN agents a
    WHERE c.singleton = 1 AND c.pruned_through > 0 AND NOT EXISTS (
        SELECT 1 FROM events WHERE agent = a.id AND kind = 'agent.created'
    )";

impl Store {
    /// Maximum removed sequence visible to the same filter used for replay.
    /// `None` selects the whole journal; an empty named set matches no Agent.
    pub fn event_replay_floor(
        &self,
        agents: Option<&[i64]>,
        kinds: &[&str],
        sources: &[&str],
        var_scopes: &[&str],
    ) -> Result<i64> {
        let journal_floor = retention::coverage_pruned_through(&self.connection)?;
        if journal_floor == 0 || (agents.is_none() && kinds.is_empty() && sources.is_empty()) {
            return Ok(journal_floor);
        }
        let legacy_uncertain = if let Some(agents) = agents {
            let includes_variables =
                !var_scopes.is_empty() && (kinds.is_empty() || kinds.contains(&"var.changed"));
            self.legacy_uncertain_event_floor(agents, includes_variables)?
        } else {
            0
        };
        let placeholders = |count: usize| vec!["?"; count].join(",");
        if !self.has_event_prune_coverage {
            let Some(agents) = agents.filter(|agents| !agents.is_empty()) else {
                return Ok(if agents.is_none() {
                    journal_floor
                } else {
                    legacy_uncertain
                });
            };
            return self
                .connection
                .query_row(
                    &format!(
                        "SELECT COALESCE(MAX(floor), 0) FROM ({LEGACY_AGENT_EVENT_FLOORS}
                         AND a.id IN ({}))",
                        placeholders(agents.len())
                    ),
                    rusqlite::params_from_iter(agents.iter()),
                    |row| row.get(0),
                )
                .map(|floor: i64| floor.max(legacy_uncertain))
                .map_err(sql_error);
        }

        let mut sql = String::from("SELECT COALESCE(MAX(seq), 0) FROM event_prune_coverage WHERE ");
        let mut values = Vec::<rusqlite::types::Value>::new();
        match agents {
            None => sql.push_str("agent = 0"),
            Some(agents) => {
                sql.push_str(&format!("(agent IN ({})", placeholders(agents.len())));
                values.extend(agents.iter().copied().map(Into::into));
                if !var_scopes.is_empty() {
                    sql.push_str(&format!(
                        " OR (agent = -1 AND var_scope IN ({}))",
                        placeholders(var_scopes.len())
                    ));
                    values.extend(var_scopes.iter().map(|scope| (*scope).to_owned().into()));
                }
                sql.push(')');
            }
        }
        for (column, names) in [("kind", kinds), ("source", sources)] {
            if !names.is_empty() {
                sql.push_str(&format!(
                    " AND ({column} = '' OR {column} IN ({}))",
                    placeholders(names.len())
                ));
                values.extend(names.iter().map(|name| (*name).to_owned().into()));
            }
        }
        self.connection
            .query_row(&sql, rusqlite::params_from_iter(values), |row| row.get(0))
            .map(|floor: i64| floor.max(legacy_uncertain))
            .map_err(sql_error)
    }

    // For pre-upgrade deletes we only have the journal floor. Surviving
    // Agents and Agents with a recorded delete use their own coverage.
    fn legacy_uncertain_event_floor(&self, agents: &[i64], variables: bool) -> Result<i64> {
        let (snapshot, has_scoped_coverage) = if self.has_event_prune_coverage {
            (
                "SELECT seq AS floor, legacy_agent_hi AS agent_hi FROM event_prune_coverage
                 WHERE agent = 0 AND kind = '' AND source = '' AND var_scope = ''",
                "AND NOT EXISTS (SELECT 1 FROM event_prune_coverage WHERE agent = candidate.value)",
            )
        } else {
            (
                "SELECT pruned_through AS floor,
                   (SELECT seq FROM sqlite_sequence WHERE name = 'agents') AS agent_hi
                 FROM rule_history_coverage WHERE singleton = 1",
                "",
            )
        };
        let agents = serde_json::to_string(agents)
            .map_err(|_| Error::new("internal", "event filter could not be encoded"))?;
        self.connection
            .prepare_cached(&format!(
                "SELECT COALESCE((SELECT floor FROM ({snapshot})
                 WHERE ?1 OR EXISTS (
                     SELECT 1 FROM json_each(?2) AS candidate
                     WHERE candidate.value <= agent_hi AND NOT EXISTS (
                         SELECT 1 FROM agents WHERE id = candidate.value
                     ) {has_scoped_coverage}
                 )), 0)"
            ))
            .and_then(|mut stmt| stmt.query_row(params![variables, agents], |row| row.get(0)))
            .map_err(sql_error)
    }
}

/// Retain only the filter keys and highest removed sequence per key. The
/// indexed Agent key 0 summarizes all rows for kind/source-only filters;
/// -1 holds Agent-less variable facts for scoped Agent replay.
pub(super) fn record_event_pruning_tx(
    tx: &Transaction<'_>,
    predicate: &str,
    params: impl rusqlite::Params,
) -> rusqlite::Result<usize> {
    tx.execute(
        &format!(
            "WITH removed AS MATERIALIZED (
                SELECT agent, kind, source, seq, CASE
                    WHEN agent IS NULL AND kind = 'var.changed'
                    THEN json_extract(data, '$.scope')
                END AS var_scope
                FROM events WHERE {predicate}
             )
             INSERT INTO event_prune_coverage (agent, kind, source, var_scope, seq)
             SELECT 0, kind, source, '', MAX(seq)
             FROM removed GROUP BY kind, source
             UNION ALL
             SELECT COALESCE(agent, -1), kind, source, COALESCE(var_scope, ''), MAX(seq)
             FROM removed WHERE agent IS NOT NULL OR var_scope IS NOT NULL
             GROUP BY agent, kind, source, var_scope
             ON CONFLICT (agent, kind, source, var_scope)
             DO UPDATE SET seq = MAX(event_prune_coverage.seq, excluded.seq)"
        ),
        params,
    )
}
