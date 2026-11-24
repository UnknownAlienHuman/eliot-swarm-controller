//! Change scope of a Goal. A Goal with `scope` or `check_inputs` gets a
//! starting point at `set`, or during launch preflight after workspace setup:
//! HEAD and the content of every path that already differed from it. Each
//! round compares the workspace with that starting point, so pre-existing
//! work is never attributed to the Goal. A change
//! outside `scope` blocks the Goal before its check. A passing check after a
//! check input changed, or with the change set unverified, blocks it instead
//! of completing it. Swarm never reverts or deletes anything here.

use super::{Router, goals::program};
use crate::git_monitor::{GitProbe, scope_digests};
use eliot_core::{
    Error, Result,
    api::{
        GoalDefinition, GoalScopeBase, GoalScopeCheckFile, GoalScopeCheckIdentity,
        GoalScopeCheckResolution, MAX_GOAL_SCOPE_BASE_PATHS, claim_glob_matches_normalized,
        normalize_claim_glob, truncate_bytes,
    },
};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Component, Path, PathBuf},
};

/// Most paths one scope finding names; the rest are counted.
const LISTED_PATHS: usize = 20;
/// Room a scope finding keeps in the 4 KiB Goal observation.
const FINDING_BYTES: usize = 1536;
const PREVIOUS_CHECK_IDENTITY_VERSION: u8 = 3;
const CHECK_IDENTITY_VERSION: u8 = 4;

/// The Goal's work since its starting point, or why it is not known.
pub(super) enum ScopeFinding {
    Verified(BTreeSet<String>),
    Unverified(String),
}

impl Router {
    /// Record the starting point of a Goal with `scope` or `check_inputs`.
    pub(super) async fn goal_scope_base(
        &self,
        agent: &str,
        definition: &GoalDefinition,
    ) -> Result<Option<GoalScopeBase>> {
        if !definition.tracks_changes() {
            return Ok(None);
        }
        let workspace = self
            .inner
            .store
            .lock()
            .await
            .get_agent(agent)?
            .and_then(|agent| agent.workspace)
            .ok_or_else(|| Error::new("invalid", "a scoped Goal needs an Agent workspace"))?;
        self.goal_scope_base_for_workspace(&workspace, definition)
            .await
            .map(Some)
    }

    /// Capture the baseline for the exact workspace path read with the
    /// launch Goal's generation and revision.
    pub(super) async fn goal_scope_base_for_workspace(
        &self,
        workspace: &str,
        definition: &GoalDefinition,
    ) -> Result<GoalScopeBase> {
        if !definition.tracks_changes() {
            return Err(Error::new(
                "internal",
                "an unscoped Goal cannot capture a Git starting point",
            ));
        }
        let unavailable = |error: Error| {
            Error::new(
                "unavailable",
                format!(
                    "the Goal's starting point cannot be recorded: {}",
                    error.message
                ),
            )
        };
        let state = GitProbe::new()
            .map_err(unavailable)?
            .scope_state(workspace)
            .await
            .map_err(unavailable)?;
        if state.changed.len() > MAX_GOAL_SCOPE_BASE_PATHS {
            return Err(Error::new(
                "invalid",
                format!(
                    "{} paths already differ from HEAD; a scoped Goal records at most {MAX_GOAL_SCOPE_BASE_PATHS}",
                    state.changed.len()
                ),
            ));
        }
        let root = state.root.clone();
        let definition = definition.clone();
        let workspace = workspace.to_owned();
        let (check_files, check_identity, changed) = tokio::task::spawn_blocking(move || {
            let (check_files, identity) =
                check_files(&root, &definition, &workspace, CHECK_IDENTITY_VERSION)?;
            let changed = scope_digests(&root, state.changed.iter().map(String::as_str))
                .map_err(unavailable)?;
            Ok::<_, Error>((check_files, identity, changed))
        })
        .await
        .map_err(|_| Error::new("internal", "scope read task failed"))??;
        Ok(GoalScopeBase {
            root: state
                .root
                .to_str()
                .ok_or_else(|| Error::new("unavailable", "workspace is not UTF-8"))?
                .to_owned(),
            head: state.head,
            changed,
            check_files,
            check_identity: Some(check_identity),
        })
    }

    /// Paths the Goal's work changed since `base`: committed since its HEAD,
    /// changed now, or a pre-existing change whose content moved.
    pub(super) async fn goal_scope_changes(
        &self,
        workspace: Option<&str>,
        base: &GoalScopeBase,
        definition: &GoalDefinition,
    ) -> ScopeFinding {
        match scope_changes(workspace, base, definition).await {
            Ok(changed) => ScopeFinding::Verified(changed),
            Err(error) => ScopeFinding::Unverified(*error.message),
        }
    }
}

async fn scope_changes(
    workspace: Option<&str>,
    base: &GoalScopeBase,
    definition: &GoalDefinition,
) -> Result<BTreeSet<String>> {
    let workspace =
        workspace.ok_or_else(|| Error::new("unavailable", "the Agent has no workspace"))?;
    let git = GitProbe::new()?;
    let state = git.scope_state(workspace).await?;
    let state_root = state
        .root
        .to_str()
        .ok_or_else(|| Error::new("unavailable", "workspace root is not UTF-8"))?;
    if !same_path_text(state_root, &base.root) {
        return Err(Error::new(
            "unavailable",
            "the workspace is not the Git root recorded at goal set",
        ));
    }
    let check_identity = match base.check_identity.as_ref() {
        Some(identity)
            if matches!(
                identity.version,
                PREVIOUS_CHECK_IDENTITY_VERSION | CHECK_IDENTITY_VERSION
            ) =>
        {
            Some(identity)
        }
        Some(_) => {
            return Err(Error::new(
                "scope_unverified",
                "the Goal's checker identity version is not supported",
            ));
        }
        None => None,
    };
    if check_identity.is_some_and(|identity| identity.unverified_entry_point) {
        return Err(checker_entry_unverified());
    }
    let committed = match (&base.head, &state.head) {
        (Some(start), Some(head)) if start != head => {
            git.scope_committed(&state.root, start, head).await?
        }
        (Some(_), Some(_)) | (None, None) => Vec::new(),
        _ => {
            return Err(Error::new(
                "unavailable",
                "HEAD was unborn at goal set or is unborn now",
            ));
        }
    };
    let root = state.root.clone();
    let paths: Vec<String> = base.changed.keys().cloned().collect();
    let baseline = check_identity.cloned();
    let definition = definition.clone();
    let workspace = workspace.to_owned();
    let (now, changed_check_files) = tokio::task::spawn_blocking(move || {
        let now = scope_digests(&root, paths.iter().map(String::as_str))?;
        let changed_check_files = if let Some(baseline) = baseline {
            let (_, current) = check_files(&root, &definition, &workspace, baseline.version)?;
            if current.unverified_entry_point {
                return Err(checker_entry_unverified());
            }
            changed_check_identity_paths(&baseline.files, &current.files)
        } else {
            BTreeSet::new()
        };
        Ok::<_, Error>((now, changed_check_files))
    })
    .await
    .map_err(|_| Error::new("internal", "scope read task failed"))??;
    let committed: BTreeSet<String> = committed.into_iter().collect();
    let mut changed: BTreeSet<String> = committed.iter().cloned().chain(state.changed).collect();
    // A pre-existing change is the Goal's work once its content moved or
    // the Goal committed it; unchanged and uncommitted, it is not.
    for (path, start) in &base.changed {
        if now.get(path) == Some(start) && !committed.contains(path) {
            changed.remove(path);
        } else {
            changed.insert(path.clone());
        }
    }
    // Keep explicit checker identity changes after pruning unchanged
    // pre-existing workspace changes.
    changed.extend(changed_check_files);
    Ok(changed)
}

/// Changed paths outside the Goal's `scope`; none without one.
pub(super) fn outside_scope<'a>(
    definition: &GoalDefinition,
    changed: &'a BTreeSet<String>,
) -> Vec<&'a str> {
    if definition.scope.is_empty() && definition.policy_scope.is_empty() {
        return Vec::new();
    }
    let scopes = [&definition.scope, &definition.policy_scope]
        .into_iter()
        .filter(|scope| !scope.is_empty())
        .map(|scope| normalized(scope))
        .collect::<Vec<_>>();
    changed
        .iter()
        .map(String::as_str)
        .filter(|path| {
            scopes.iter().any(|scope| {
                !scope
                    .iter()
                    .any(|glob| claim_glob_matches_normalized(glob, path))
            })
        })
        .collect()
}

/// Changed paths that are the check's own inputs.
pub(super) fn changed_check_inputs<'a>(
    definition: &GoalDefinition,
    base: &GoalScopeBase,
    changed: &'a BTreeSet<String>,
) -> Vec<&'a str> {
    let globs = normalized(&definition.check_inputs);
    let identity = base.check_identity.as_ref();
    changed
        .iter()
        .map(String::as_str)
        .filter(|path| {
            let path = *path;
            identity.is_some_and(|identity| {
                identity
                    .files
                    .iter()
                    .any(|file| same_workspace_relative_path(&file.path, path))
            }) || base
                .check_files
                .iter()
                .any(|target| same_workspace_relative_path(target, path))
                || globs
                    .iter()
                    .any(|glob| claim_glob_matches_normalized(glob, path))
        })
        .collect()
}

/// One bounded finding: a title, the path count and the first paths.
pub(super) fn finding(title: &str, paths: &[&str]) -> String {
    let mut text = format!("{title} ({} paths):", paths.len());
    for path in paths.iter().take(LISTED_PATHS) {
        text.push_str("\n- ");
        text.push_str(path);
    }
    if paths.len() > LISTED_PATHS {
        text.push_str(&format!("\n- … and {} more", paths.len() - LISTED_PATHS));
    }
    truncate_bytes(&text, FINDING_BYTES).0.to_owned()
}

/// The finding first, then as much of the check observation as fits.
pub(super) fn with_finding(finding: &str, observation: Option<&str>, bound: usize) -> String {
    match observation {
        Some(observation) => {
            let room = bound.saturating_sub(finding.len() + 2);
            format!("{finding}\n\n{}", truncate_bytes(observation, room).0)
        }
        None => finding.to_owned(),
    }
}

/// The scope line a continuation repeats so the limit survives every round.
pub(super) fn scope_reminder(definition: &GoalDefinition) -> Option<String> {
    if !definition.policy_scope.is_empty() {
        let policy = definition.policy_scope.join(", ");
        if !definition.scope.is_empty() {
            return Some(format!(
                "Change only paths matching both the project scope ({policy}) and the launch scope ({}).",
                definition.scope.join(", ")
            ));
        }
        return Some(format!(
            "Change only paths within the project scope ({policy})."
        ));
    }
    (!definition.scope.is_empty()).then(|| {
        format!(
            "Change only these paths; Swarm blocks the Goal on any other change: {}",
            definition.scope.join(", ")
        )
    })
}

fn normalized(globs: &[String]) -> Vec<String> {
    globs
        .iter()
        .filter_map(|glob| normalize_claim_glob(glob).ok())
        .collect()
}

/// Capture check argv identities while preserving the legacy display list of
/// existing resolved file targets.
fn check_files(
    root: &Path,
    definition: &GoalDefinition,
    workspace: &str,
    version: u8,
) -> Result<(Vec<String>, GoalScopeCheckIdentity)> {
    let cwd = PathBuf::from(definition.check_cwd.as_deref().unwrap_or(workspace));
    let mut files = BTreeSet::new();
    let mut identities = BTreeMap::new();
    let mut operands = Vec::new();
    let mut unverified_entry_point = false;
    let legacy_argv = version == PREVIOUS_CHECK_IDENTITY_VERSION;
    if let Some((program, args)) = definition.check.split_first() {
        if legacy_argv || !program::is_bare_program(program) {
            operands.push(PathBuf::from(program));
        }
        if !legacy_argv && let Ok(executable) = program::resolve_program(program, Some(&cwd)) {
            operands.push(executable);
        }
        match interpreter_entry(program, args, legacy_argv) {
            CheckerEntry::Executable => {}
            CheckerEntry::Path(path) => {
                if path.is_empty() {
                    unverified_entry_point = true;
                } else {
                    operands.push(PathBuf::from(path));
                }
            }
            CheckerEntry::PythonModule(module) => {
                if let Some(paths) = python_module_paths(&cwd, module) {
                    operands.extend(paths);
                } else {
                    unverified_entry_point = true;
                }
            }
            CheckerEntry::Unverified => unverified_entry_point = true,
        }
    }
    for arg in &operands {
        let candidate = Path::new(arg);
        let joined = if candidate.is_absolute() {
            candidate.to_path_buf()
        } else {
            cwd.join(candidate)
        };
        let joined = absolute_check_path(&joined)?;
        let Some(path) = lexical_workspace_relative(root, &joined)? else {
            // Preserve protection for a previously recognized in-workspace
            // file reached through an absolute alias outside the workspace.
            if let Ok(target) = fs::canonicalize(&joined)
                && target.is_file()
                && let Some(relative) = lexical_workspace_relative(root, &target)?
            {
                files.insert(relative);
            }
            continue;
        };
        if !is_normalized_workspace_path(&path) {
            continue;
        }
        // Use the original lookup here. Normalizing an argument containing
        // "link/../file" before reading it can change what the check opens.
        let resolution = check_file_resolution(&joined)?;
        if let GoalScopeCheckResolution::Resolved { target, .. } = &resolution
            && Path::new(target).is_file()
            && let Some(relative_target) = lexical_workspace_relative(root, Path::new(target))?
        {
            files.insert(relative_target);
        }
        if let Some(previous) = identities.insert(path, resolution.clone())
            && previous != resolution
        {
            return Err(Error::new(
                "unavailable",
                "a check argv path changed while its starting point was recorded",
            ));
        }
    }
    let identity = GoalScopeCheckIdentity {
        version,
        unverified_entry_point,
        files: identities
            .into_iter()
            .map(|(path, resolution)| GoalScopeCheckFile { path, resolution })
            .collect(),
    };
    Ok((files.into_iter().collect(), identity))
}

fn checker_entry_unverified() -> Error {
    Error::new(
        "scope_unverified",
        "the checker interpreter entry point could not be verified",
    )
}

enum CheckerEntry<'a> {
    Executable,
    Path(&'a str),
    PythonModule(&'a str),
    Unverified,
}

fn interpreter_entry<'a>(program: &str, args: &'a [String], legacy_argv: bool) -> CheckerEntry<'a> {
    use CheckerEntry::{Executable, Path as Script, PythonModule, Unverified};
    let Some(name) = Path::new(program)
        .file_name()
        .and_then(|name| name.to_str())
    else {
        return Unverified;
    };
    let name = name.to_ascii_lowercase();
    let name = name.strip_suffix(".exe").unwrap_or(&name);
    let python = matches!(name, "python" | "py" | "pypy" | "pypy3")
        || name.strip_prefix("python").is_some_and(|version| {
            !version.is_empty()
                && version
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || byte == b'.')
        });
    let powershell = matches!(name, "powershell" | "pwsh");
    let cmd = name == "cmd";
    let shell = matches!(name, "sh" | "bash" | "dash" | "zsh" | "ksh");
    let node = matches!(name, "node" | "nodejs");
    if !(python || powershell || cmd || shell || node || matches!(name, "ruby" | "perl" | "php")) {
        return Executable;
    }
    let mut args = args.iter();
    let mut local_modules = true;
    while let Some(arg) = args.next() {
        let folded = arg.to_ascii_lowercase();
        let option = if powershell || cmd {
            folded.as_str()
        } else {
            arg.as_str()
        };
        let option_name = option.split_once('=').map_or(option, |(name, _)| name);
        match option {
            "--" if !cmd => return args.next().map_or(Unverified, |arg| Script(arg)),
            "-file" | "-f" if powershell || name == "php" => {
                return args.next().map_or(Unverified, |arg| Script(arg));
            }
            "/c" | "/k" if cmd => {
                return args
                    .next()
                    .filter(|arg| simple_cmd_script(arg))
                    .map_or(Unverified, |arg| Script(arg));
            }
            "-" | "-h" | "--help" | "--version" => return Unverified,
            "-0" | "-0p" | "--list" | "--list-paths" if name == "py" => return Unverified,
            option if python && option.starts_with('-') && !option.starts_with("--") => {
                if name == "py" && option.starts_with("-V:") {
                    continue;
                }
                // CPython permits clustered flags and attached option values.
                for (offset, flag) in option[1..].char_indices() {
                    match flag {
                        'I' | 'P' => local_modules = false,
                        'c' | 'h' | 'V' => return Unverified,
                        'm' | 'W' | 'X' => {
                            let attached = &arg[offset + 2..];
                            let value = if attached.is_empty() {
                                args.next().map(String::as_str)
                            } else {
                                Some(attached)
                            };
                            let Some(value) = value else {
                                return Unverified;
                            };
                            if flag == 'm' {
                                return if local_modules {
                                    PythonModule(value)
                                } else {
                                    Unverified
                                };
                            }
                            break;
                        }
                        _ => {}
                    }
                }
            }
            "-command" | "-c" | "-commandwithargs" | "-cwa" | "-encodedcommand" | "-enc" | "-e"
            | "-ec" | "-workingdirectory" | "-configurationname" | "-settingsfile"
                if powershell =>
            {
                return Unverified;
            }
            "-executionpolicy" | "-inputformat" | "-outputformat" | "-windowstyle" | "-version"
            | "-configurationfile" | "-custompipename"
                if powershell =>
            {
                if args.next().is_none() {
                    return Unverified;
                }
            }
            _ if node
                && matches!(
                    option_name,
                    "-e" | "--eval"
                        | "-p"
                        | "-pe"
                        | "--print"
                        | "-i"
                        | "--interactive"
                        | "--run"
                        | "-v"
                ) =>
            {
                return Unverified;
            }
            _ if node
                && matches!(
                    if legacy_argv { option } else { option_name },
                    "-r" | "--require"
                        | "--import"
                        | "--loader"
                        | "--experimental-loader"
                        | "-C"
                        | "--conditions"
                        | "--env-file"
                        | "--env-file-if-exists"
                        | "--experimental-config-file"
                        | "--redirect-warnings"
                        | "--input-type"
                ) =>
            {
                if option_name == option && args.next().is_none() {
                    return Unverified;
                }
            }
            "--rcfile" | "--init-file" if shell => {
                if args.next().is_none() {
                    return Unverified;
                }
            }
            "-nop" | "-noprofile" | "-nologo" | "-noninteractive" | "-sta" | "-mta"
                if powershell => {}
            "/d" | "/s" | "/q" | "/a" | "/u" | "/e:on" | "/e:off" | "/v:on" | "/v:off" if cmd => {}
            "--noprofile" | "--norc" | "--posix" | "--login" if shell => {}
            "--enable-source-maps" | "--no-warnings" | "--trace-warnings" | "--no-deprecation"
                if node => {}
            option if shell && option.starts_with(['-', '+']) && !option.starts_with("--") => {
                let mut flags = option[1..].chars();
                if let Some(mode) = flags.find(|flag| matches!(flag, 'c' | 's' | 'o' | 'O'))
                    && (matches!(mode, 'c' | 's')
                        || (flags.as_str().is_empty() && args.next().is_none()))
                {
                    return Unverified;
                }
            }
            option
                if matches!(name, "ruby" | "perl")
                    && ["-e", "-E", "-S"]
                        .iter()
                        .any(|prefix| option.starts_with(prefix)) =>
            {
                return Unverified;
            }
            option
                if matches!(name, "ruby" | "perl")
                    && ["-I", "-r", "-M", "-m"].into_iter().any(|flag| {
                        option
                            .strip_prefix(flag)
                            .is_some_and(|value| !value.is_empty())
                    }) => {}
            "-I" | "-r" | "-M" | "-m" if matches!(name, "ruby" | "perl") => {
                if args.next().is_none() {
                    return Unverified;
                }
            }
            option
                if name == "perl"
                    && option.starts_with('-')
                    && option[1..].chars().all(|flag| {
                        matches!(
                            flag,
                            'w' | 'W' | 'T' | 't' | 'U' | 'n' | 'p' | 'a' | 'l' | '0'..='9'
                        )
                    }) => {}
            "-r" | "-B" | "-R" | "-F" | "-E" | "-a" if name == "php" => return Unverified,
            "-n" if name == "php" => {}
            "-w" | "-W0" | "-W1" | "-W2" if name == "ruby" => {}
            "-d" | "-c" | "-z" if name == "php" => {
                if args.next().is_none() {
                    return Unverified;
                }
            }
            option if option.starts_with('-') || (cmd && option.starts_with('/')) => {
                // Preserve version 3 argv selection. The current parser can
                // skip an inline long option without guessing about the next token.
                if !(legacy_argv || (option_name.starts_with("--") && option_name != option)) {
                    return Unverified;
                }
            }
            _ if cmd => return Unverified,
            _ if powershell && !arg.to_ascii_lowercase().ends_with(".ps1") => return Unverified,
            _ => return Script(arg),
        }
    }
    Unverified
}

fn simple_cmd_script(arg: &str) -> bool {
    !arg.chars()
        .any(|character| character.is_whitespace() || r#"&|<>%^!()"'"#.contains(character))
        && Path::new(arg)
            .extension()
            .and_then(|ext| ext.to_str())
            .is_some_and(|ext| ext.eq_ignore_ascii_case("bat") || ext.eq_ignore_ascii_case("cmd"))
}

/// Resolve ordinary source modules in the effective check directory without
/// executing import hooks or allowing the checker to supply a resolution.
fn python_module_paths(cwd: &Path, module: &str) -> Option<Vec<PathBuf>> {
    let cwd = absolute_check_path(cwd).ok()?;
    let mut path = cwd;
    let mut files = Vec::new();
    let mut components = module.split('.').peekable();
    while let Some(part) = components.next() {
        let mut name = part.bytes();
        if !name
            .next()
            .is_some_and(|byte| byte == b'_' || byte.is_ascii_alphabetic())
            || !name.all(|byte| byte == b'_' || byte.is_ascii_alphanumeric())
        {
            return None;
        }
        path.push(part);
        let init = path.join("__init__.py");
        if components.peek().is_some() {
            if !init.is_file() {
                return None;
            }
            files.push(init);
        } else {
            let source = if init.is_file() {
                files.push(init);
                path.join("__main__.py")
            } else if path.with_extension("py").is_file() {
                path.with_extension("py")
            } else {
                path.join("__main__.py")
            };
            if !source.is_file() {
                return None;
            }
            files.push(source);
        }
    }
    Some(files)
}

fn absolute_check_path(path: &Path) -> Result<PathBuf> {
    if path.is_absolute() {
        return Ok(path.to_path_buf());
    }
    std::env::current_dir()
        .map(|current| current.join(path))
        .map_err(|_| Error::new("unavailable", "the check argv path cannot be resolved"))
}

/// Convert an absolute lexical path to a normalized workspace-relative path
/// without following symlinks.
fn lexical_workspace_relative(root: &Path, path: &Path) -> Result<Option<String>> {
    let root = root
        .to_str()
        .ok_or_else(|| Error::new("unavailable", "workspace root is not UTF-8"))?;
    let path = path
        .to_str()
        .ok_or_else(|| Error::new("unavailable", "a check argv path is not UTF-8"))?;
    let root = display_path_key(root);
    let path = display_path_key(path);
    let Some(relative) = strip_workspace_prefix(&root, &path) else {
        return Ok(None);
    };
    let mut components = Vec::new();
    for component in relative.split('/') {
        match component {
            "" | "." => {}
            ".." => {
                if components.pop().is_none() {
                    return Ok(None);
                }
            }
            component => components.push(component.to_owned()),
        }
    }
    if components.is_empty() {
        Ok(None)
    } else {
        Ok(Some(components.join("/")))
    }
}

fn check_file_resolution(lexical: &Path) -> Result<GoalScopeCheckResolution> {
    let metadata = match fs::symlink_metadata(lexical) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(GoalScopeCheckResolution::Missing);
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotADirectory => {
            return Ok(GoalScopeCheckResolution::Unresolved { link_target: None });
        }
        Err(_) => {
            return Err(Error::new(
                "unavailable",
                "a check argv path cannot be inspected",
            ));
        }
        Ok(metadata) => metadata,
    };
    let link_target = if metadata.file_type().is_symlink() {
        Some(
            fs::read_link(lexical)
                .map_err(|_| Error::new("unavailable", "a check argv link cannot be read"))?
                .to_str()
                .ok_or_else(|| Error::new("unavailable", "a check argv link is not UTF-8"))?
                .to_owned(),
        )
    } else {
        None
    };
    match fs::canonicalize(lexical) {
        Ok(target) => Ok(GoalScopeCheckResolution::Resolved {
            target: target
                .to_str()
                .ok_or_else(|| Error::new("unavailable", "a check target is not UTF-8"))?
                .to_owned(),
            link_target,
        }),
        Err(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::NotFound | std::io::ErrorKind::NotADirectory
            ) =>
        {
            Ok(GoalScopeCheckResolution::Unresolved { link_target })
        }
        Err(_) => Err(Error::new(
            "unavailable",
            "a check argv target cannot be resolved",
        )),
    }
}

fn is_normalized_workspace_path(path: &str) -> bool {
    !path.is_empty()
        && !path.starts_with('/')
        && !path.contains('\0')
        && !(cfg!(windows)
            && path
                .chars()
                .any(|character| character.is_control() || r#"\<>:"|?*"#.contains(character)))
        && path
            .split('/')
            .all(|component| !component.is_empty() && !matches!(component, "." | ".."))
        && Path::new(path)
            .components()
            .all(|component| matches!(component, Component::Normal(_)))
}

fn changed_check_identity_paths(
    baseline: &[GoalScopeCheckFile],
    current: &[GoalScopeCheckFile],
) -> BTreeSet<String> {
    baseline
        .iter()
        .filter(|file| {
            current
                .binary_search_by(|candidate| candidate.path.cmp(&file.path))
                .map_or(true, |index| {
                    !same_check_resolution(&current[index].resolution, &file.resolution)
                })
        })
        .map(|file| file.path.clone())
        .collect()
}

fn display_path_key(path: &str) -> String {
    let path = eliot_core::display_path(path);
    #[cfg(windows)]
    {
        path.replace('\\', "/")
    }
    #[cfg(not(windows))]
    {
        path
    }
}

fn same_path_text(left: &str, right: &str) -> bool {
    let left = display_path_key(left);
    let right = display_path_key(right);
    #[cfg(windows)]
    {
        left.eq_ignore_ascii_case(&right)
    }
    #[cfg(not(windows))]
    {
        left == right
    }
}

fn strip_workspace_prefix<'a>(root: &str, path: &'a str) -> Option<&'a str> {
    let root = root.trim_end_matches('/');
    #[cfg(windows)]
    let matches_root = path
        .get(..root.len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case(root));
    #[cfg(not(windows))]
    let matches_root = path.starts_with(root);
    if !matches_root {
        return None;
    }
    let suffix = path.get(root.len()..)?;
    if suffix.is_empty() {
        return Some("");
    }
    suffix.strip_prefix('/')
}

fn same_check_resolution(
    left: &GoalScopeCheckResolution,
    right: &GoalScopeCheckResolution,
) -> bool {
    match (left, right) {
        (GoalScopeCheckResolution::Missing, GoalScopeCheckResolution::Missing) => true,
        (
            GoalScopeCheckResolution::Unresolved {
                link_target: left_target,
            },
            GoalScopeCheckResolution::Unresolved {
                link_target: right_target,
            },
        ) => same_optional_path_text(left_target.as_deref(), right_target.as_deref()),
        (
            GoalScopeCheckResolution::Resolved {
                target: left_target,
                link_target: left_link,
            },
            GoalScopeCheckResolution::Resolved {
                target: right_target,
                link_target: right_link,
            },
        ) => {
            same_path_text(left_target, right_target)
                && same_optional_path_text(left_link.as_deref(), right_link.as_deref())
        }
        _ => false,
    }
}

fn same_optional_path_text(left: Option<&str>, right: Option<&str>) -> bool {
    match (left, right) {
        (Some(left), Some(right)) => same_path_text(left, right),
        (None, None) => true,
        _ => false,
    }
}

fn same_workspace_relative_path(left: &str, right: &str) -> bool {
    #[cfg(windows)]
    {
        left.eq_ignore_ascii_case(right)
    }
    #[cfg(not(windows))]
    {
        left == right
    }
}
