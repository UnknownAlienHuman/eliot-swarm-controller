//! Bounded global-rule snapshots and coalesced filesystem wake hints.

use super::definition::{self, RuleDefinition};
use eliot_core::{Error, Result};
use eliot_store::{RuleDefinitionInput, RuleProcessingState};
use notify::{Config, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use std::{
    collections::{HashMap, HashSet},
    fs::{self, File},
    io::ErrorKind,
    path::{Path, PathBuf},
};
use tokio::sync::watch;

pub(crate) struct RuleSnapshot {
    pub(crate) definitions: Vec<RuleDefinition>,
    pub(crate) inputs: Vec<RuleDefinitionInput>,
}

impl RuleSnapshot {
    pub(crate) fn matches(&self, current: &[RuleDefinition]) -> Result<bool> {
        if self.definitions.len() != current.len() {
            return Ok(false);
        }
        let current_by_id: HashMap<_, _> = current
            .iter()
            .map(|definition| (definition.id.as_str(), definition))
            .collect();
        for loaded in &self.definitions {
            let Some(active) = current_by_id.get(loaded.id.as_str()) else {
                return Ok(false);
            };
            if loaded.enabled != active.enabled
                || loaded.semantic_digest()? != active.semantic_digest()?
            {
                return Ok(false);
            }
        }
        Ok(true)
    }
}

pub(crate) fn load(data_root: &Path) -> Result<RuleSnapshot> {
    load_rule_directory(&data_root.join("rules"))
}

/// Load the shared rule-file grammar from the registered checkout's rules path.
/// Project authorization, link rejection, and overlay rejection stay in
/// `automation::projects` before this parser is called.
pub(crate) fn load_project(checkout: &Path) -> Result<RuleSnapshot> {
    let checkout = fs::canonicalize(checkout)
        .map_err(|_| Error::new("unavailable", "registered project checkout is unavailable"))?;
    let requested = checkout.join(".eliot").join("rules");
    let rules = match fs::canonicalize(&requested) {
        Ok(rules) if rules.starts_with(&checkout) => rules,
        Ok(_) => {
            return Err(Error::new(
                "not_allowed",
                "registered project rule directory escapes its checkout",
            ));
        }
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(empty_snapshot()),
        Err(_) => {
            return Err(Error::new(
                "unavailable",
                "registered project rule directory is unavailable",
            ));
        }
    };
    load_rule_directory(&rules)
}

fn empty_snapshot() -> RuleSnapshot {
    RuleSnapshot {
        definitions: Vec::new(),
        inputs: Vec::new(),
    }
}

fn load_rule_directory(directory: &Path) -> Result<RuleSnapshot> {
    let entries = match fs::read_dir(directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == ErrorKind::NotFound => {
            return Ok(empty_snapshot());
        }
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

    let mut definitions = Vec::new();
    for path in paths {
        let file = File::open(&path)
            .map_err(|_| Error::new("unavailable", "cannot open global rule file"))?;
        definitions.extend(definition::load(file)?);
    }
    let mut ids = HashSet::new();
    if definitions
        .iter()
        .any(|definition| !ids.insert(definition.id.clone()))
    {
        return Err(Error::new(
            "invalid",
            "global rule files contain duplicate IDs",
        ));
    }
    from_definitions(definitions)
}

pub(crate) fn from_definitions(definitions: Vec<RuleDefinition>) -> Result<RuleSnapshot> {
    let mut inputs = Vec::with_capacity(definitions.len());
    for definition in &definitions {
        let canonical_value = serde_json::to_value(definition)
            .map_err(|_| Error::new("invalid", "rule definition cannot be encoded"))?;
        let canonical_definition = serde_json::to_string(&canonical_value)
            .map_err(|_| Error::new("invalid", "rule definition cannot be canonicalized"))?;
        inputs.push(RuleDefinitionInput {
            id: definition.id.clone(),
            canonical_definition,
            definition_digest: definition.semantic_digest()?,
            enabled: definition.enabled,
        });
    }
    Ok(RuleSnapshot {
        definitions,
        inputs,
    })
}

pub(crate) fn from_processing(states: &[RuleProcessingState]) -> Result<RuleSnapshot> {
    let mut definitions = Vec::with_capacity(states.len());
    let mut inputs = Vec::with_capacity(states.len());
    for state in states {
        let definition = from_canonical(&state.current_definition)?;
        if definition.id != state.view.id {
            return Err(Error::new(
                "source_integrity",
                "stored rule definition does not match its state ID",
            ));
        }
        let definition_digest = definition.semantic_digest()?;
        inputs.push(RuleDefinitionInput {
            id: definition.id.clone(),
            canonical_definition: state.current_definition.clone(),
            definition_digest,
            enabled: definition.enabled,
        });
        definitions.push(definition);
    }
    Ok(RuleSnapshot {
        definitions,
        inputs,
    })
}

/// Reparse a Store-retained canonical definition through the same bounded rule
/// parser used for files, including schedule reconstruction.
pub(crate) fn from_canonical(canonical: &str) -> Result<RuleDefinition> {
    definition::RuleDefinition::deserialize_saved(canonical)
        .map_err(|_| Error::new("source_integrity", "stored rule definition is invalid"))
}

pub(crate) struct RuleWatcher {
    changed: watch::Receiver<u64>,
    _watcher: RecommendedWatcher,
}

impl RuleWatcher {
    pub(crate) fn watch(data_root: &Path) -> Result<Self> {
        Self::watch_paths(data_root, &data_root.join("rules"))
    }

    pub(crate) fn watch_project(checkout: &Path) -> Result<Self> {
        let checkout = fs::canonicalize(checkout)
            .map_err(|_| Error::new("unavailable", "registered project checkout is unavailable"))?;
        let rules_root = fs::canonicalize(checkout.join(".eliot").join("rules")).map_err(|_| {
            Error::new(
                "unavailable",
                "registered project rule directory is unavailable",
            )
        })?;
        if !rules_root.starts_with(&checkout) {
            return Err(Error::new(
                "not_allowed",
                "registered project rule directory escapes its checkout",
            ));
        }
        Self::watch_paths(&rules_root, &rules_root)
    }

    fn watch_paths(watched_root: &Path, rules_root: &Path) -> Result<Self> {
        let watched_root = watched_root.to_path_buf();
        let rules_root = rules_root.to_path_buf();
        let (sender, changed) = watch::channel(0u64);
        let callback_rules = rules_root.clone();
        let mut watcher = RecommendedWatcher::new(
            move |result: notify::Result<notify::Event>| match result {
                Ok(event)
                    if !matches!(event.kind, EventKind::Access(_))
                        && event.paths.iter().any(|path| {
                            path == &callback_rules || path.starts_with(&callback_rules)
                        }) =>
                {
                    sender.send_modify(|revision| *revision = revision.wrapping_add(1));
                }
                Err(_) => {
                    sender.send_modify(|revision| *revision = revision.wrapping_add(1));
                }
                _ => {}
            },
            Config::default().with_follow_symlinks(false),
        )
        .map_err(|_| {
            Error::new(
                "unavailable",
                "global-rule filesystem watcher could not start",
            )
        })?;
        watcher
            .watch(&watched_root, RecursiveMode::Recursive)
            .map_err(|_| Error::new("unavailable", "global-rule directory could not be watched"))?;
        Ok(Self {
            changed,
            _watcher: watcher,
        })
    }

    pub(crate) async fn changed(&mut self) -> std::result::Result<(), watch::error::RecvError> {
        self.changed.changed().await
    }
}
