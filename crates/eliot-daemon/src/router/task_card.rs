use eliot_core::{Result, SecretPolicy, api::GoalDefinition};
use eliot_store::Store;

const TASK_CARD_MAX_BYTES: usize = 600;
const TASK_BYTES: usize = 160;
const GOAL_BYTES: usize = 125;
const SCOPE_BYTES: usize = 100;
const LIMITS_BYTES: usize = 55;
const CHECK_BYTES: usize = 116;
const SOURCE_BLOCK_MARKERS: [&str; 2] = [
    "Required sources (delivered by Swarm with path, file identity and completeness; read them here instead of reopening the files):",
    "Required sources (delivered by Swarm with path, content digest and completeness; read them here instead of reopening the files):",
];

pub(super) fn read_task_card(
    store: &Store,
    agent: &str,
    generation: i64,
    handoff: Option<&str>,
    policy: &SecretPolicy,
) -> Result<Option<String>> {
    let Some(current) = store.get_agent(agent)? else {
        return Ok(None);
    };
    if current.generation != generation || current.is_terminal() {
        return Ok(None);
    }
    let brief = match handoff {
        Some(id) => store.get_command(id)?,
        None => store.task_card_handoff_brief(agent, generation)?,
    }
    .filter(|brief| brief.target.as_deref() == Some(agent) && brief.verb == "send");
    let text = if let Some(brief) = brief {
        brief.args["text"]
            .as_str()
            .map(original_task)
            .map(str::to_owned)
    } else if let Some(launch) = store.generation_start_command(agent, 1)? {
        let linked = store.get_linked_send(&launch.id)?;
        launch.args["text"]
            .as_str()
            .or_else(|| linked.as_ref().and_then(|send| send.args["text"].as_str()))
            .map(str::to_owned)
    } else {
        None
    };
    let goal = store
        .goal_get(agent, "eliot")?
        .filter(|goal| goal.generation == generation)
        .map(|goal| goal.definition);
    Ok(render_task_card(text.as_deref(), goal.as_ref(), policy))
}

fn original_task(brief: &str) -> &str {
    if !brief.starts_with("Handoff brief:") {
        return brief;
    }
    brief
        .split_once("\n\nOriginal task:\n")
        .and_then(|(_, contents)| contents.split_once("\n\nLater input to the source"))
        .map_or(brief, |(task, _)| task)
}

fn brief_field<'a>(brief: Option<&'a str>, labels: &[&str]) -> Option<&'a str> {
    brief?.lines().find_map(|line| {
        let (label, value) = line.trim_start_matches(['-', ' ']).split_once(':')?;
        labels
            .iter()
            .any(|expected| label.eq_ignore_ascii_case(expected))
            .then(|| value.trim())
            .filter(|value| !value.is_empty())
    })
}

fn render_task_card(
    launch_brief: Option<&str>,
    goal: Option<&GoalDefinition>,
    policy: &SecretPolicy,
) -> Option<String> {
    let launch_brief = launch_brief.map(without_source_block);
    let launch_brief = launch_brief.filter(|text| !text.trim().is_empty());
    if launch_brief.is_none() && goal.is_none() {
        return None;
    }

    let task = launch_brief
        .map(|text| bounded_field(text, TASK_BYTES, policy))
        .unwrap_or_else(|| "not retained".to_owned());
    let goal_text = goal
        .map(|goal| bounded_field(&goal.text, GOAL_BYTES, policy))
        .or_else(|| {
            brief_field(launch_brief, &["goal", "result"])
                .map(|text| bounded_field(text, GOAL_BYTES, policy))
        })
        .unwrap_or_else(|| "not set".to_owned());
    let scope = goal
        .filter(|goal| !goal.scope.is_empty() || !goal.policy_scope.is_empty())
        .map(scope_text)
        .or_else(|| {
            brief_field(launch_brief, &["write scope", "scope", "change"]).map(str::to_owned)
        })
        .map(|scope| bounded_field(&scope, SCOPE_BYTES, policy))
        .unwrap_or_else(|| "not set".to_owned());
    let limits = brief_field(launch_brief, &["limits", "constraints"])
        .map(str::to_owned)
        .or_else(|| goal.map(limits_text))
        .map(|limits| bounded_field(&limits, LIMITS_BYTES, policy))
        .unwrap_or_else(|| "not set".to_owned());
    let check = goal
        .filter(|goal| !goal.check.is_empty())
        .map(check_text)
        .or_else(|| {
            brief_field(launch_brief, &["check", "checks", "verification"]).map(str::to_owned)
        })
        .map(|check| bounded_field(&check, CHECK_BYTES, policy))
        .unwrap_or_else(|| "not set".to_owned());

    let mut card = format!(
        "Task: {task}\nGoal: {goal_text}\nWrite scope: {scope}\nLimits: {limits}\nCheck: {check}"
    );
    if card.len() > TASK_CARD_MAX_BYTES {
        card.truncate(card.floor_char_boundary(TASK_CARD_MAX_BYTES));
    }
    Some(card)
}

fn without_source_block(text: &str) -> &str {
    SOURCE_BLOCK_MARKERS
        .iter()
        .filter_map(|marker| text.find(marker))
        .min()
        .map_or(text, |index| &text[..index])
}

fn bounded_field(text: &str, max_bytes: usize, policy: &SecretPolicy) -> String {
    let redacted = policy.redact_text(text);
    let single_line = redacted.split_whitespace().collect::<Vec<_>>().join(" ");
    if single_line.len() <= max_bytes {
        return single_line;
    }
    let ellipsis = "…";
    let end = single_line.floor_char_boundary(max_bytes.saturating_sub(ellipsis.len()));
    format!("{}{}", &single_line[..end], ellipsis)
}

fn scope_text(goal: &GoalDefinition) -> String {
    let mut scopes = Vec::new();
    if !goal.scope.is_empty() {
        scopes.push(format!("Goal: {}", goal.scope.join(", ")));
    }
    if !goal.policy_scope.is_empty() {
        scopes.push(format!("Policy: {}", goal.policy_scope.join(", ")));
    }
    if scopes.is_empty() {
        "not specified in Goal".to_owned()
    } else {
        scopes.join("; ")
    }
}

fn limits_text(goal: &GoalDefinition) -> String {
    let mut limits = Vec::new();
    if let Some(max_turns) = goal.max_turns {
        limits.push(format!("turns ≤{max_turns}"));
    }
    if let Some(max_rounds) = goal.max_rounds {
        limits.push(format!("rounds ≤{max_rounds}"));
    }
    if let Some(max_without_progress) = goal.max_without_progress {
        limits.push(format!("no-progress ≤{max_without_progress}"));
    }
    if let Some(max_cost) = &goal.max_cost {
        limits.push(format!(
            "cost ≤{} micro-{}",
            max_cost.amount_micros, max_cost.unit
        ));
    }
    if limits.is_empty() {
        "none configured".to_owned()
    } else {
        limits.join("; ")
    }
}

fn check_text(goal: &GoalDefinition) -> String {
    let mut check = serde_json::to_string(&goal.check).unwrap_or_else(|_| "unavailable".to_owned());
    if let Some(cwd) = &goal.check_cwd {
        check.push_str("; cwd=");
        check.push_str(cwd);
    }
    check
}
