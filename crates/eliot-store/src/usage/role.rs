use eliot_core::{
    Error, Result, TurnUsage,
    api::{UsageRoleSummary, UsageRoleTotals},
};

/// Both callers supply static SQL columns. Keep autonomy and scoped usage on
/// the same retained-child and creation-time predicate.
pub(crate) fn manager_at_sql(agent: &str, observed_at: &str) -> String {
    format!(
        "EXISTS (SELECT 1 FROM agents AS child INDEXED BY autonomy_child_creation
                 WHERE child.parent = {agent} AND child.created_at <= {observed_at})"
    )
}

pub(crate) struct RoleInputSummary {
    pub(crate) managers: u64,
    pub(crate) executors: u64,
    pub(crate) manager_share: Option<f64>,
    pub(crate) observed: bool,
    pub(crate) incomplete: bool,
}

#[derive(Default)]
pub(crate) struct RoleUsage {
    managers: Totals,
    executors: Totals,
}

#[derive(Default)]
struct Totals {
    samples: UsageRoleTotals,
    input: u64,
    input_incomplete: bool,
    output: i128,
    context: i128,
}

impl RoleUsage {
    fn totals(&mut self, manager: bool) -> &mut Totals {
        if manager {
            &mut self.managers
        } else {
            &mut self.executors
        }
    }

    pub(crate) fn observe_turn(&mut self, manager: bool, usage: &TurnUsage) -> Result<()> {
        let totals = self.totals(manager);
        let input = [usage.input_tokens, usage.cache_read_tokens];
        totals.input_incomplete |= input.iter().any(Option::is_none);
        if input.iter().any(Option::is_some) {
            totals.samples.input_samples += 1;
        }
        // Decoding folds cache writes into uncached input when the vendor
        // reports them separately. Add cache reads once to that normalized input.
        for value in input.into_iter().flatten() {
            let value = u64::try_from(value)
                .map_err(|_| Error::new("source_integrity", "negative input token count"))?;
            totals.input = totals
                .input
                .checked_add(value)
                .ok_or_else(|| Error::new("source_integrity", "role input token count overflow"))?;
        }
        if let Some(output) = usage.output_tokens.filter(|tokens| *tokens >= 0) {
            totals.output += i128::from(output);
            totals.samples.output_samples += 1;
        }
        Ok(())
    }

    pub(super) fn observe_context(&mut self, manager: bool, tokens: Option<i64>) {
        let totals = self.totals(manager);
        totals.samples.context_observations += 1;
        if let Some(tokens) = tokens {
            totals.context += i128::from(tokens);
            totals.samples.context_samples += 1;
        } else {
            totals.samples.context_missing += 1;
        }
    }

    pub(crate) fn input_summary(&self) -> RoleInputSummary {
        let managers = self.managers.input;
        let executors = self.executors.input;
        let total = u128::from(managers) + u128::from(executors);
        RoleInputSummary {
            managers,
            executors,
            manager_share: (total > 0).then(|| managers as f64 / total as f64),
            observed: self.managers.samples.input_samples > 0
                || self.executors.samples.input_samples > 0,
            incomplete: self.managers.input_incomplete || self.executors.input_incomplete,
        }
    }

    pub(super) fn finish(self) -> UsageRoleSummary {
        UsageRoleSummary {
            managers: self.managers.finish(),
            executors: self.executors.finish(),
        }
    }
}

impl Totals {
    fn finish(mut self) -> UsageRoleTotals {
        self.samples.input_tokens =
            (self.samples.input_samples > 0).then_some(self.input.min(i64::MAX as u64) as i64);
        self.samples.output_tokens = (self.samples.output_samples > 0)
            .then_some(self.output.min(i128::from(i64::MAX)) as i64);
        self.samples.average_context_tokens = (self.samples.context_samples > 0)
            .then(|| (self.context / self.samples.context_samples as i128) as i64);
        self.samples
    }
}
