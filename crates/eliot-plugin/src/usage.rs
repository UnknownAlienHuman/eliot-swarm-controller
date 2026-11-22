//! First-call accounting for native turn projections.

use eliot_core::TurnUsage;

/// The first eligible call may itself lack usable counters. Later calls
/// must not fill that gap.
#[derive(Default)]
pub struct FirstCallUsage {
    input_tokens: Option<Option<i64>>,
}

impl FirstCallUsage {
    pub fn observe(&mut self, response: bool, input_tokens: Option<i64>) {
        if response {
            self.input_tokens.get_or_insert(input_tokens);
        }
    }

    pub fn finish(&mut self, scope: &str, usage: Option<TurnUsage>) -> Option<TurnUsage> {
        with_first_call(scope, usage, self.input_tokens.take().flatten())
    }
}

/// Count fresh and cached input on the common accounting basis. Some
/// terminals carry aggregate counters only; those callers do not use this.
pub fn call_input_tokens(
    harness: &str,
    input: Option<i64>,
    cache_read: Option<i64>,
    cache_write: Option<i64>,
) -> Option<i64> {
    let input = input.filter(|tokens| *tokens >= 0)?;
    if harness == "codex" {
        return Some(input);
    }
    let cached = cache_read.filter(|tokens| *tokens >= 0)?;
    let counters = TurnUsage {
        input_tokens: Some(input),
        cache_read_tokens: Some(cached),
        cache_write_tokens: cache_write.filter(|tokens| *tokens >= 0),
        ..TurnUsage::default()
    };
    counters
        .normalized(harness)
        .input_tokens?
        .checked_add(cached)
}

/// The native message list is newest-first. The oldest assistant response
/// within the exact input-to-terminal interval has the largest position.
pub fn summarize_newest_first_usage(
    harness: &str,
    scope: &str,
    newest_terminal: usize,
    input_position: usize,
    messages: impl IntoIterator<Item = (usize, TurnUsage)>,
) -> Option<TurnUsage> {
    let mut first = None;
    let bounded = messages
        .into_iter()
        .filter(|(position, _)| *position > newest_terminal && *position < input_position)
        .map(|(position, usage)| {
            if first.is_none_or(|(previous, _)| position > previous) {
                let input = call_input_tokens(
                    harness,
                    usage.input_tokens,
                    usage.cache_read_tokens,
                    usage.cache_write_tokens,
                );
                first = Some((position, input));
            }
            usage
        });
    let usage = TurnUsage::sum_native(scope, bounded);
    with_first_call(scope, usage, first.and_then(|(_, input)| input))
}

fn with_first_call(
    scope: &str,
    mut usage: Option<TurnUsage>,
    input_tokens: Option<i64>,
) -> Option<TurnUsage> {
    if let Some(input) = input_tokens {
        usage
            .get_or_insert_with(|| TurnUsage {
                scope: Some(scope.to_owned()),
                ..TurnUsage::default()
            })
            .first_call_input_tokens = Some(input);
    }
    usage
}
