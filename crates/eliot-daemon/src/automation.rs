mod definition;

use crate::router::Router;
use eliot_core::{Error, Result, SubmitCommand, api::RuleStateView};
use eliot_store::{RuleCursorOutcome, RuleDefinitionInput};
use serde_json::json;
use std::{
    collections::HashSet,
    fs::{self, File},
    io::ErrorKind,
    path::{Path, PathBuf},
};
use tokio::sync::{broadcast, watch};

const RULE_EVENT_PAGE_SIZE: usize = 128;

pub(crate) struct ActiveRule {
    definition: definition::RuleDefinition,
    state: RuleStateView,
}

pub(crate) async fn initialize_global_rules(
    router: &Router,
    data_root: &Path,
) -> Result<Vec<ActiveRule>> {
    let (definitions, inputs) = load_global_rules(data_root)?;
    let states = router.initialize_global_rules(&inputs).await?;
    if definitions.len() != states.len() {
        return Err(Error::new(
            "internal",
            "global rule initialization returned an incomplete state set",
        ));
    }
    Ok(definitions
        .into_iter()
        .zip(states)
        .map(|(definition, state)| ActiveRule { definition, state })
        .collect())
}

pub(crate) async fn run_global_rules(
    router: Router,
    mut rules: Vec<ActiveRule>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    if rules.is_empty() {
        return Ok(());
    }

    // Bus messages are wake hints; durable Store pages remain authoritative.
    // Register before the first high-water read so no event can fall between them.
    let mut events = router.subscribe_rule_events();
    let mut progress = router.progress_receiver();
    loop {
        if *shutdown.borrow() || !router.rule_firings_open() {
            return Ok(());
        }
        let high_water = router.rule_high_water().await?;

        for rule in &mut rules {
            if rule.state.current_cursor > high_water {
                return Err(Error::new(
                    "source_integrity",
                    "rule cursor is above the event high-water mark",
                ));
            }
            if rule.state.current_cursor == high_water {
                continue;
            }
            if rule.state.paused_reason.is_some() && rule.state.enabled {
                continue;
            }
            if !rule.state.enabled || !rule.definition.enabled {
                // One silent skipped range; cursor-only updates never feed the Bus.
                rule.state = router
                    .advance_rule_cursor(&rule.state, high_water, RuleCursorOutcome::Disabled)
                    .await?;
                continue;
            }

            while rule.state.current_cursor < high_water {
                if *shutdown.borrow() || !router.rule_firings_open() {
                    return Ok(());
                }
                let page = router
                    .read_rule_events(rule.state.current_cursor, high_water, RULE_EVENT_PAGE_SIZE)
                    .await?;
                if page.is_empty() {
                    return Err(Error::new(
                        "history_gap",
                        "rule cursor is behind unavailable event history",
                    ));
                }

                let mut no_match_through = None;
                for event in page {
                    if event.seq <= rule.state.current_cursor {
                        continue;
                    }
                    if event.kind != "turn.completed" {
                        no_match_through = Some(event.seq);
                        continue;
                    }

                    let stop = match definition::evaluate(&rule.definition, &event) {
                        Ok(Some(stop)) => stop,
                        Ok(None) => {
                            no_match_through = Some(event.seq);
                            continue;
                        }
                        Err(error) => {
                            flush_no_match(&router, rule, &mut no_match_through).await?;
                            rule.state = router
                                .advance_rule_cursor(
                                    &rule.state,
                                    event.seq,
                                    RuleCursorOutcome::Rejected { code: error.code },
                                )
                                .await?;
                            continue;
                        }
                    };

                    flush_no_match(&router, rule, &mut no_match_through).await?;
                    if !router.rule_firings_open() {
                        return Ok(());
                    }
                    let request = stop_command(stop);
                    let prepared = match router.prepare_owner_submission(request).await {
                        Ok(prepared) => prepared,
                        Err(_) if !router.rule_firings_open() => return Ok(()),
                        Err(error) if deterministic_rejection(&error.code) => {
                            rule.state = router
                                .advance_rule_cursor(
                                    &rule.state,
                                    event.seq,
                                    RuleCursorOutcome::Rejected { code: error.code },
                                )
                                .await?;
                            continue;
                        }
                        Err(error) => return Err(error),
                    };

                    let Some(admission) = router
                        .admit_rule_stop(&rule.state, event.seq, prepared)
                        .await?
                    else {
                        // Shutdown won the shared Store-lock ordering. Leave the
                        // cursor at this event; a later open daemon can process it.
                        return Ok(());
                    };
                    rule.state = admission.state;
                    // Router schedules only a new child after the commit lock drops.
                }
                flush_no_match(&router, rule, &mut no_match_through).await?;
            }
        }

        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return Ok(());
                }
            }
            changed = progress.changed() => {
                if changed.is_err() || !router.rule_firings_open() {
                    return Ok(());
                }
            }
            received = events.recv() => match received {
                Ok(_) | Err(broadcast::error::RecvError::Lagged(_)) => {}
                Err(broadcast::error::RecvError::Closed) => {
                    return Err(Error::new("unavailable", "event bus is closed"));
                }
            }
        }
    }
}

async fn flush_no_match(
    router: &Router,
    rule: &mut ActiveRule,
    through: &mut Option<i64>,
) -> Result<()> {
    if let Some(through) = through.take()
        && through > rule.state.current_cursor
    {
        rule.state = router
            .advance_rule_cursor(&rule.state, through, RuleCursorOutcome::NoMatch)
            .await?;
    }
    Ok(())
}

fn load_global_rules(
    data_root: &Path,
) -> Result<(Vec<definition::RuleDefinition>, Vec<RuleDefinitionInput>)> {
    let directory = data_root.join("rules");
    let entries = match fs::read_dir(&directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok((Vec::new(), Vec::new())),
        Err(_) => return Err(Error::new("unavailable", "cannot enumerate global rules")),
    };

    let mut paths = Vec::<PathBuf>::new();
    for entry in entries {
        let entry =
            entry.map_err(|_| Error::new("unavailable", "cannot read global rule entry"))?;
        let file_type = entry
            .file_type()
            .map_err(|_| Error::new("unavailable", "cannot inspect global rule entry"))?;
        let path = entry.path();
        if file_type.is_file()
            && path
                .extension()
                .is_some_and(|extension| extension == "toml")
        {
            paths.push(path);
        }
    }
    paths.sort();

    let mut ids = HashSet::new();
    let mut definitions = Vec::new();
    let mut inputs = Vec::new();
    for path in paths {
        let file = File::open(&path)
            .map_err(|_| Error::new("unavailable", "cannot open global rule file"))?;
        for definition in definition::load(file)? {
            if !ids.insert(definition.id.clone()) {
                return Err(Error::new(
                    "invalid",
                    "global rule files contain duplicate IDs",
                ));
            }
            let canonical_value = serde_json::to_value(&definition)
                .map_err(|_| Error::new("invalid", "rule definition cannot be encoded"))?;
            let canonical_definition = serde_json::to_string(&canonical_value)
                .map_err(|_| Error::new("invalid", "rule definition cannot be canonicalized"))?;
            let definition_digest = definition.semantic_digest()?;
            inputs.push(RuleDefinitionInput {
                id: definition.id.clone(),
                canonical_definition,
                definition_digest,
                enabled: definition.enabled,
            });
            definitions.push(definition);
        }
    }
    Ok((definitions, inputs))
}

fn stop_command(stop: definition::StopAction) -> SubmitCommand {
    // This unbound request ID is used only by Router preparation. The atomic
    // Store rule admission assigns the deterministic rule/event child ID.
    SubmitCommand {
        id: String::new(),
        target: Some(stop.target),
        verb: "stop".to_owned(),
        args: json!({}),
        expected_generation: Some(stop.expected_generation),
        expected_turn_id: None,
        request_id: None,
        probe: false,
    }
}

fn deterministic_rejection(code: &str) -> bool {
    matches!(
        code,
        "bad_state"
            | "frame_too_large"
            | "invalid"
            | "not_allowed"
            | "not_found"
            | "secret_in_payload"
            | "stale_generation"
            | "unsupported"
    )
}
