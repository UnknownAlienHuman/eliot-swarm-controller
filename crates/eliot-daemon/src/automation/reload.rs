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
    let mut snapshot = load_rule_directory(&data_root.join("rules"))?;
    let config = crate::config::load_automation_config(data_root)?;
    if !config.assignments.is_empty() {
        let mut definitions = snapshot.definitions;
        for (key, assignment) in config.assignments {
            let id = format!("assignment-{key}");
            let toml = format!(
                "[[rule]]\nid = {}\nenabled = {}\non = {{ every = {} }}\ndo = [{{ action = 'launch', workspace = {}, role = {}, assignment = {{ key = {}, folder = {}, max_busy = {} }} }}]\n",
                quote_toml(&id),
                assignment.enabled,
                quote_toml(&assignment.interval),
                quote_toml(&assignment.workspace),
                quote_toml(&assignment.role),
                quote_toml(&key),
                quote_toml(&assignment.folder),
                assignment.max_busy,
            );
            definitions.extend(definition::load(toml.as_bytes())?);
            // Assigned Agents own one brief. Use the existing event-rule stop
            // action to retire the exact generation after its terminal turn,
            // freeing configured live capacity for the next timer launch.
            // The rule processor additionally verifies Store launch ancestry;
            // this cause label alone is not enough to authorize cleanup.
            let completion = format!(
                "[[rule]]\nid = {}\non = {{ event = 'turn.completed', match = {{ 'event.cause.rule' = {} }} }}\ndo = [{{ action = 'stop', target = '${{event.agent}}', expected_generation = '${{event.generation}}' }}]\n",
                quote_toml(&format!("{id}.complete")),
                quote_toml(&id),
            );
            definitions.extend(definition::load(completion.as_bytes())?);
        }
        snapshot = from_definitions(definitions)?;
    }
    Ok(snapshot)
}

fn quote_toml(value: &str) -> String {
    toml::Value::String(value.to_owned()).to_string()
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
    let mut ids = HashSet::new();
    if definitions
        .iter()
        .any(|definition| !ids.insert(&definition.id))
    {
        return Err(Error::new(
            "invalid",
            "file rules and settings assignments contain duplicate IDs",
        ));
    }
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
    for state in states.iter().filter(|state| state.source_present) {
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
    watcher: RecommendedWatcher,
    /// A rules directory distinct from the watched root (the global
    /// `data_root/rules`), re-armed after creation or replacement. `None`
    /// when the root itself is the rules directory (project lanes), which
    /// the worker rebuilds from the checkout on every reload instead.
    rules_dir: Option<PathBuf>,
    rules_active: bool,
    /// Identity of the bound rules directory the last time the watch was
    /// (re-)armed. A rename-away plus fresh-directory replacement keeps the
    /// path present, so presence alone cannot tell the watcher is still
    /// bound to the old inode.
    rules_identity: Option<(u64, u64)>,
}

/// Stable identity for a watched directory: the OS handle the active watch
/// is bound to, so a replacement directory rebinds even though the path
/// never goes absent.
fn dir_identity(path: &Path) -> Option<(u64, u64)> {
    let metadata = std::fs::metadata(path).ok()?;
    if !metadata.is_dir() {
        return None;
    }
    Some(platform_dir_identity(path, &metadata))
}

#[cfg(unix)]
fn platform_dir_identity(_path: &Path, metadata: &std::fs::Metadata) -> (u64, u64) {
    use std::os::unix::fs::MetadataExt;
    (metadata.dev(), metadata.ino())
}

#[cfg(windows)]
fn platform_dir_identity(path: &Path, _metadata: &std::fs::Metadata) -> (u64, u64) {
    // The open can race a removal the metadata check just passed: fall back
    // to a sentinel no real directory has so the caller re-arms instead of
    // trusting the stale watch.
    crate::platform::windows::directory_identity(path).unwrap_or((0, 0))
}

#[cfg(not(any(unix, windows)))]
fn platform_dir_identity(_path: &Path, _metadata: &std::fs::Metadata) -> (u64, u64) {
    // No stable directory identity here: every present directory compares
    // equal, so replacement falls back to presence re-arming.
    (0, 0)
}

/// Whether a filesystem event can change the loaded rule snapshot: an edit
/// under the rules directory itself, or (for the global lane) the config
/// source that also feeds the snapshot. Managed worktrees, plugin data and
/// every other subtree are never relevant, which is why the watcher stays
/// non-recursive.
fn rule_watch_event_is_relevant(
    event: &notify::Event,
    rules_root: &Path,
    config: Option<&Path>,
) -> bool {
    if matches!(event.kind, EventKind::Access(_)) {
        return false;
    }
    event.paths.iter().any(|path| {
        config.is_some_and(|config| path.as_path() == config)
            || path == rules_root
            || path.starts_with(rules_root)
    })
}

impl RuleWatcher {
    pub(crate) fn watch(data_root: &Path) -> Result<Self> {
        Self::watch_paths(
            data_root,
            &data_root.join("rules"),
            Some(data_root.join("config.toml")),
        )
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
        Self::watch_paths(&rules_root, &rules_root, None)
    }

    fn watch_paths(
        watched_root: &Path,
        rules_root: &Path,
        config: Option<PathBuf>,
    ) -> Result<Self> {
        let (sender, changed) = watch::channel(0u64);
        let callback_rules = rules_root.to_path_buf();
        let callback_config = config.clone();
        let mut watcher = RecommendedWatcher::new(
            move |result: notify::Result<notify::Event>| match result {
                Ok(event)
                    if rule_watch_event_is_relevant(
                        &event,
                        &callback_rules,
                        callback_config.as_deref(),
                    ) =>
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
        // The loader reads only immediate `*.toml` files (plus the config
        // source), so both watches stay non-recursive: managed worktrees,
        // plugin data and build trees under the root cost no watch resources.
        watcher
            .watch(watched_root, RecursiveMode::NonRecursive)
            .map_err(|_| Error::new("unavailable", "global-rule directory could not be watched"))?;
        // A missing rules directory is normal (it is created later); the
        // worker re-arms it through `ensure_watches` instead of failing here.
        let distinct_rules = (rules_root != watched_root).then(|| rules_root.to_path_buf());
        let mut rules_active = false;
        let mut rules_identity = None;
        if let Some(rules) = &distinct_rules {
            // A replacement between the pre-watch identity read and
            // registration would record the new identity against the old
            // watch. Verify the identity across registration and retry once
            // on a mismatch so the watch binds the current directory.
            for _ in 0..2 {
                let Some(before) = dir_identity(rules) else {
                    break;
                };
                if watcher.watch(rules, RecursiveMode::NonRecursive).is_err() {
                    break;
                }
                let after = dir_identity(rules);
                if after == Some(before) {
                    rules_active = true;
                    rules_identity = after;
                    break;
                }
                let _ = watcher.unwatch(rules);
            }
        }
        Ok(Self {
            changed,
            watcher,
            rules_dir: distinct_rules,
            rules_active,
            rules_identity,
        })
    }

    /// Re-establish the rules-directory watch after creation or replacement.
    /// The worker loop calls this after wakeups; the periodic reconciliation
    /// reloads from disk regardless, so a missed watch event only delays a
    /// reload, never loses one. The watch rebinds whenever the directory
    /// identity changes: a rename-away plus fresh-directory replacement
    /// keeps the path present, so comparing presence alone would leave the
    /// watcher bound to the old inode.
    pub(crate) fn ensure_watches(&mut self) {
        let Some(rules) = self.rules_dir.as_ref() else {
            return;
        };
        let identity = dir_identity(rules);
        if identity == self.rules_identity {
            return;
        }
        if identity.is_some() {
            // Unwatch first so the re-arm binds the replacement directory
            // instead of keeping the stale handle.
            let _ = self.watcher.unwatch(rules);
            if self
                .watcher
                .watch(rules, RecursiveMode::NonRecursive)
                .is_ok()
            {
                self.rules_identity = identity;
                self.rules_active = true;
                return;
            }
        } else {
            let _ = self.watcher.unwatch(rules);
        }
        // Absent, or the re-arm failed and must be retried: stay inactive
        // without recording an identity.
        self.rules_identity = None;
        self.rules_active = false;
    }

    pub(crate) async fn changed(&mut self) -> std::result::Result<(), watch::error::RecvError> {
        self.changed.changed().await
    }
}
