//! Local, bounded Git readback. Wire formats follow the official Git 2.55
//! git-status (porcelain v2/-z), git-ls-files, git-log and git-check-ignore docs.
//! No donor application code, shell, index/ref/object writes, or network reads.
//! Reads require leaf custody. Workspace writes are only prepared here; their
//! manager owns ordinary scoped execution and cancellation serialization.

use crate::platform::OwnedChild;
use eliot_core::{Error, GitBaseline, GitCommit, GitPathStatus, GitSnapshot, Result};
use std::{
    collections::{BTreeMap, BTreeSet},
    env,
    ffi::OsString,
    fs,
    path::{Component, Path, PathBuf},
    process::Stdio,
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    process::Command,
    time::{Instant, timeout_at},
};

const OUTPUT_BYTES: usize = 1024 * 1024;
const STDERR_BYTES: usize = 32 * 1024;
const INPUT_BYTES: usize = 32 * 1024;
const CONFIG_PAIRS: usize = 256;
const CONFIG_BYTES: usize = 16 * 1024;
const HISTORY_COMMITS: usize = 64;
const PROBE_TIME: Duration = Duration::from_secs(15);
const CLEANUP_TIME: Duration = Duration::from_secs(2);
const STATUS_ARGS: &[&str] = &[
    "status",
    "--porcelain=v2",
    "--branch",
    "-z",
    "--untracked-files=all",
    "--ignore-submodules=all",
    "--no-ahead-behind",
    "--renames",
];
const INDEX_ARGS: &[&str] = &["ls-files", "--stage", "-v", "-z"];

#[derive(Clone)]
pub(crate) struct GitProbe {
    program: PathBuf,
}

pub(crate) struct Captured {
    pub code: i32,
    pub stdout: Vec<u8>,
}

#[derive(Clone, Copy)]
pub(crate) struct GitBinding<'a> {
    pub git_dir: &'a Path,
    pub common_dir: &'a Path,
}

struct Status {
    head: Option<String>,
    branch: Option<String>,
    dirty: Vec<GitPathStatus>,
    renamed_from: BTreeSet<String>,
}

impl GitProbe {
    /// Resolve only absolute PATH entries, never the implicit Windows/CWD search.
    pub(crate) fn new() -> Result<Self> {
        let search = env::var_os("PATH").ok_or_else(|| unavailable("Git PATH is unavailable"))?;
        for directory in env::split_paths(&search).filter(|path| path.is_absolute()) {
            let candidate = directory.join(if cfg!(windows) { "git.exe" } else { "git" });
            let Ok(metadata) = fs::metadata(&candidate) else {
                continue;
            };
            if !metadata.is_file() {
                continue;
            }
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                if metadata.permissions().mode() & 0o111 == 0 {
                    continue;
                }
            }
            let program = fs::canonicalize(candidate)
                .map_err(|_| unavailable("Git executable cannot be resolved"))?;
            #[cfg(windows)]
            let program = resolve_windows_git(program)?;
            return Ok(Self { program });
        }
        Err(unavailable(
            "Git executable was not found on absolute PATH entries",
        ))
    }

    pub(crate) async fn probe(
        &self,
        workspace: &str,
        baseline: Option<&GitBaseline>,
        base_oid: Option<&str>,
        edited_paths: &[String],
        now: i64,
    ) -> Result<GitSnapshot> {
        let root = canonical_root(workspace)?;
        let root_text = root
            .to_str()
            .ok_or_else(|| unavailable("workspace is not UTF-8"))?;
        let deadline = Instant::now() + PROBE_TIME;
        let overrides = self.safe_overrides(&root, deadline).await?;
        let top = self
            .checked(
                &root,
                &["rev-parse", "--path-format=absolute", "--show-toplevel"],
                &overrides,
                &[],
                deadline,
            )
            .await?;
        let top = std::str::from_utf8(&top).map_err(|_| unavailable("Git root is not UTF-8"))?;
        // rev-parse adds exactly one record terminator; do not trim valid spaces.
        let top = top
            .strip_suffix('\n')
            .ok_or_else(|| unavailable("Git root omitted its terminator"))?;
        if canonical_root(top)? != root {
            return Err(unavailable("workspace must be the exact Git worktree root"));
        }

        let index_bytes = self
            .checked(&root, INDEX_ARGS, &overrides, &[], deadline)
            .await?;
        let index = parse_index(&index_bytes)?;
        let status_bytes = self
            .checked(&root, STATUS_ARGS, &overrides, &[], deadline)
            .await?;
        let status = parse_status(&status_bytes)?;

        let mut edited = BTreeSet::new();
        let mut input = Vec::new();
        for path in edited_paths {
            if Instant::now() >= deadline {
                return Err(unavailable("Git observation deadline elapsed"));
            }
            let path = normalize_hint(root_text, path)?;
            if edited.insert(path.clone()) {
                input.extend_from_slice(path.as_bytes());
                input.push(0);
                if input.len() > INPUT_BYTES {
                    return Err(unavailable(
                        "edited-path input exceeds the observation bound",
                    ));
                }
            }
        }
        let ignored = if input.is_empty() {
            BTreeSet::new()
        } else {
            let output = self
                .run(
                    &root,
                    &["check-ignore", "--stdin", "-z"],
                    &overrides,
                    &input,
                    deadline,
                )
                .await?;
            if !matches!(output.code, 0 | 1) {
                return Err(unavailable("Git ignore readback failed"));
            }
            parse_paths(&output.stdout)?.into_iter().collect()
        };
        let mut attribute_paths: BTreeSet<String> = index.keys().cloned().collect();
        attribute_paths.extend(edited.iter().cloned());
        for dirty in &status.dirty {
            attribute_paths.insert(dirty.path.clone());
            attribute_paths.extend(dirty.original_path.iter().cloned());
        }
        let mut attribute_input = Vec::new();
        for path in &attribute_paths {
            attribute_input.extend_from_slice(path.as_bytes());
            attribute_input.push(0);
            if attribute_input.len() > OUTPUT_BYTES {
                return Err(unavailable(
                    "Git attribute input exceeds the observation bound",
                ));
            }
        }
        let filter_affected = if attribute_input.is_empty() {
            BTreeSet::new()
        } else {
            let attributes = self
                .checked(
                    &root,
                    &["check-attr", "--stdin", "-z", "filter"],
                    &overrides,
                    &attribute_input,
                    deadline,
                )
                .await?;
            parse_filter_attributes(&attributes, &attribute_paths)?
        };
        // Disabling executable conversions can differ from the owner's normal
        // comparison. Publish bounded raw status as partial, never normal-Git
        // clean coverage, when an applicable filter attribute is present.
        let complete = filter_affected.is_empty() && index.values().all(|covered| *covered);
        let mut clean_paths = Vec::new();
        for path in edited {
            if ignored.contains(&path)
                || status
                    .dirty
                    .iter()
                    .any(|dirty| related_path(&dirty.path, &path))
                || status
                    .renamed_from
                    .iter()
                    .any(|original| related_path(original, &path))
            {
                continue;
            }
            match index.get(&path) {
                Some(true) if !filter_affected.contains(&path) => clean_paths.push(path),
                Some(true) => {}
                Some(false) => {} // assume-unchanged/skip-worktree/unmerged is not proof.
                None => match fs::symlink_metadata(root.join(&path)) {
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                        clean_paths.push(path)
                    }
                    Ok(_) => {}
                    Err(_) => return Err(unavailable("edited-path existence is unavailable")),
                },
            }
        }

        let (commits, history_complete, history_gap) = self
            .history(
                &root,
                baseline,
                status.head.as_deref(),
                &overrides,
                deadline,
            )
            .await?;
        let (committed_paths, base_complete) = self
            .committed_paths(
                &root,
                base_oid,
                status.head.as_deref(),
                &overrides,
                deadline,
            )
            .await?;
        // Do not clear attribution from a poll that crossed a checkout/index/status
        // change. The history commands themselves use immutable object IDs.
        let final_index = self
            .checked(&root, INDEX_ARGS, &overrides, &[], deadline)
            .await?;
        let final_status = self
            .checked(&root, STATUS_ARGS, &overrides, &[], deadline)
            .await?;
        if index_bytes != final_index || status_bytes != final_status {
            return Err(unavailable("Git worktree changed during observation"));
        }
        if !complete || !base_complete {
            clean_paths.clear();
        } else {
            clean_paths.retain(|path| {
                !committed_paths
                    .iter()
                    .any(|changed| related_path(changed, path))
            });
        }
        Ok(GitSnapshot {
            workspace: root_text.to_owned(),
            head: status.head,
            branch: status.branch,
            dirty: status.dirty,
            commits,
            clean_paths,
            observed_at: now,
            complete,
            history_complete,
            history_gap,
            base_oid: base_oid.map(str::to_owned),
            committed_paths,
            base_complete,
        })
    }

    async fn safe_overrides(
        &self,
        root: &Path,
        deadline: Instant,
    ) -> Result<Vec<(String, String)>> {
        self.safe_overrides_for(root, deadline, None).await
    }

    async fn safe_overrides_for(
        &self,
        root: &Path,
        deadline: Instant,
        binding: Option<GitBinding<'_>>,
    ) -> Result<Vec<(String, String)>> {
        // Read keys only: filter/alias command values may contain credentials.
        let output = self
            .run_scoped(
                root,
                &[
                    "config",
                    "--null",
                    "--name-only",
                    "--get-regexp",
                    "^(filter\\..*|alias\\..*)$",
                ],
                &[],
                &[],
                deadline,
                binding,
            )
            .await?;
        if !matches!(output.code, 0 | 1) {
            return Err(unavailable("Git configuration is unavailable"));
        }
        let mut filters = BTreeSet::new();
        let mut overrides = BTreeMap::new();
        for key in nul_records(&output.stdout)? {
            if key.starts_with("alias.") {
                overrides.insert(key.to_owned(), String::new());
            } else if let Some((prefix, _)) = key.rsplit_once('.')
                && prefix.starts_with("filter.")
            {
                filters.insert(prefix.to_owned());
            }
        }
        // Git 2.55 convert.c apply_filter explicitly requires a nonempty command
        // before starting either a single-file or long-running filter. All three
        // command fields must be blank; process takes precedence over clean/smudge.
        for filter in filters {
            // check-attr uses these strings for absent/disabled attributes too.
            // Do not mistake a literal driver with the same name for no filter.
            if matches!(filter.as_str(), "filter.unset" | "filter.unspecified") {
                return Err(unavailable(
                    "Git filter name is ambiguous in attribute readback",
                ));
            }
            for command in ["clean", "smudge", "process"] {
                overrides.insert(format!("{filter}.{command}"), String::new());
            }
            overrides.insert(format!("{filter}.required"), "false".to_owned());
        }
        if overrides.len() > CONFIG_PAIRS
            || overrides
                .iter()
                .map(|(key, value)| key.len() + value.len())
                .sum::<usize>()
                > CONFIG_BYTES
        {
            return Err(unavailable(
                "Git executable configuration exceeds the observation bound",
            ));
        }
        Ok(overrides.into_iter().collect())
    }

    async fn history(
        &self,
        root: &Path,
        baseline: Option<&GitBaseline>,
        head: Option<&str>,
        overrides: &[(String, String)],
        deadline: Instant,
    ) -> Result<(Vec<GitCommit>, bool, bool)> {
        let Some(baseline) = baseline else {
            return Ok((Vec::new(), false, true));
        };
        if baseline.head.as_deref() == head {
            return Ok((Vec::new(), true, false));
        }
        let Some(head) = head else {
            return Ok((Vec::new(), false, baseline.head.is_some()));
        };
        if !is_oid(head) {
            return Err(unavailable("Git HEAD is malformed"));
        }
        let mut gap = false;
        let range = if let Some(previous) = baseline.head.as_deref() {
            if !is_oid(previous) {
                return Err(unavailable("stored Git baseline is malformed"));
            }
            let ancestor = self
                .run(
                    root,
                    &["merge-base", "--is-ancestor", previous, head],
                    overrides,
                    &[],
                    deadline,
                )
                .await?;
            match ancestor.code {
                0 => format!("{previous}..{head}"),
                1 => {
                    gap = true;
                    format!("{previous}..{head}")
                }
                _ => {
                    gap = true;
                    head.to_owned()
                }
            }
        } else {
            head.to_owned()
        };
        let limit = format!("--max-count={}", HISTORY_COMMITS + 1);
        let output = self
            .checked(
                root,
                &[
                    "log",
                    "--no-ext-diff",
                    "--no-textconv",
                    "--no-decorate",
                    "--no-notes",
                    "--no-show-signature",
                    "--format=%H",
                    "-z",
                    &limit,
                    &range,
                    "--",
                ],
                overrides,
                &[],
                deadline,
            )
            .await?;
        let ids = nul_records(&output)?;
        if ids.iter().any(|oid| !is_oid(oid)) {
            return Err(unavailable("Git history contains a malformed object id"));
        }
        let mut complete = ids.len() <= HISTORY_COMMITS;
        let mut commits = Vec::new();
        let history_deadline = deadline
            .checked_sub(Duration::from_secs(2))
            .unwrap_or(deadline);
        for oid in ids.into_iter().take(HISTORY_COMMITS) {
            if Instant::now() >= history_deadline {
                complete = false;
                break;
            }
            let paths = match self
                .checked(
                    root,
                    &[
                        "diff-tree",
                        "--no-ext-diff",
                        "--no-textconv",
                        "--no-commit-id",
                        "--name-only",
                        "-z",
                        "-r",
                        "-m",
                        "--root",
                        "--no-renames",
                        oid,
                        "--",
                    ],
                    overrides,
                    &[],
                    history_deadline,
                )
                .await
            {
                Ok(paths) => parse_paths(&paths)?,
                Err(error) if error.code == "git_leaf_fence_unavailable" => return Err(error),
                Err(_) => {
                    complete = false;
                    break;
                }
            };
            commits.push(GitCommit {
                oid: oid.to_owned(),
                paths: paths
                    .into_iter()
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect(),
            });
        }
        Ok((commits, complete && !gap, gap))
    }

    async fn committed_paths(
        &self,
        root: &Path,
        base: Option<&str>,
        head: Option<&str>,
        overrides: &[(String, String)],
        deadline: Instant,
    ) -> Result<(Vec<String>, bool)> {
        let (Some(base), Some(head)) = (base, head) else {
            return Ok((Vec::new(), false));
        };
        if !is_oid(base) || !is_oid(head) {
            return Err(unavailable(
                "committed-path comparison requires exact object ids",
            ));
        }
        let read_deadline = deadline
            .checked_sub(Duration::from_secs(2))
            .unwrap_or(deadline);
        let output = match self
            .run(
                root,
                &[
                    "diff",
                    "--no-ext-diff",
                    "--no-textconv",
                    "--name-only",
                    "--no-renames",
                    "-z",
                    base,
                    head,
                    "--",
                ],
                overrides,
                &[],
                read_deadline,
            )
            .await
        {
            Ok(output) if output.code == 0 => output,
            Err(error) if error.code == "git_leaf_fence_unavailable" => return Err(error),
            _ => return Ok((Vec::new(), false)),
        };
        let paths = parse_paths(&output.stdout)?;
        Ok((
            paths
                .into_iter()
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect(),
            true,
        ))
    }

    /// Read actual workspace identity without masking it with frozen bindings.
    pub(crate) async fn workspace_command(
        &self,
        root: &Path,
        args: &[&str],
        deadline: Instant,
    ) -> Result<Captured> {
        let overrides = self.safe_overrides(root, deadline).await?;
        self.run(root, args, &overrides, &[], deadline).await
    }

    /// Build, but never execute, an explicitly authorized workspace mutation.
    pub(crate) async fn prepare_workspace_command(
        &self,
        root: &Path,
        args: &[&str],
        binding: GitBinding<'_>,
        deadline: Instant,
    ) -> Result<Command> {
        let overrides = self
            .safe_overrides_for(root, deadline, Some(binding))
            .await?;
        if Instant::now() >= deadline {
            return Err(unavailable(
                "workspace command preparation deadline elapsed",
            ));
        }
        self.build_command(root, args, &overrides, Some(binding))
    }

    async fn checked(
        &self,
        root: &Path,
        args: &[&str],
        overrides: &[(String, String)],
        input: &[u8],
        deadline: Instant,
    ) -> Result<Vec<u8>> {
        let output = self.run(root, args, overrides, input, deadline).await?;
        if output.code != 0 {
            return Err(unavailable("Git local readback failed"));
        }
        Ok(output.stdout)
    }

    async fn run(
        &self,
        root: &Path,
        args: &[&str],
        overrides: &[(String, String)],
        input: &[u8],
        deadline: Instant,
    ) -> Result<Captured> {
        self.run_scoped(root, args, overrides, input, deadline, None)
            .await
    }

    fn build_command(
        &self,
        root: &Path,
        args: &[&str],
        overrides: &[(String, String)],
        binding: Option<GitBinding<'_>>,
    ) -> Result<Command> {
        let mut command = Command::new(&self.program);
        command.current_dir(root).env_clear();
        if let Some(binding) = binding {
            command
                .env("GIT_DIR", git_path_argument(binding.git_dir)?)
                .env("GIT_COMMON_DIR", git_path_argument(binding.common_dir)?)
                .env("GIT_WORK_TREE", git_path_argument(root)?);
        }
        // Only the executable's trusted directory is searched by Git's own helpers.
        let executable_dir = self
            .program
            .parent()
            .ok_or_else(|| unavailable("Git executable has no absolute parent"))?;
        command.env("PATH", git_path_argument(executable_dir)?);
        // Preserve normal ignore/normalization configuration, but never inherit
        // GIT_* routing, execution, trace, index, or configuration overrides.
        for name in [
            "SystemRoot",
            "WINDIR",
            "TEMP",
            "TMP",
            "HOME",
            "USERPROFILE",
            "HOMEDRIVE",
            "HOMEPATH",
            "XDG_CONFIG_HOME",
        ] {
            if let Some(value) = env::var_os(name) {
                command.env(name, value);
            }
        }
        for (name, value) in [
            ("GIT_OPTIONAL_LOCKS", "0"),
            ("GIT_NO_LAZY_FETCH", "1"),
            ("GIT_NO_REPLACE_OBJECTS", "1"),
            ("GIT_TERMINAL_PROMPT", "0"),
            ("GIT_ASKPASS", ""),
            ("SSH_ASKPASS", ""),
            ("GIT_EDITOR", ""),
            ("GIT_SEQUENCE_EDITOR", ""),
            ("GIT_PAGER", ""),
            ("PAGER", ""),
            ("GIT_EXTERNAL_DIFF", ""),
            ("GIT_ALLOW_PROTOCOL", ""),
            ("GIT_PROTOCOL_FROM_USER", "0"),
            ("LC_ALL", "C"),
        ] {
            command.env(name, value);
        }
        // Environment key/value pairs avoid -c's ambiguous '=' split in driver names.
        command.env("GIT_CONFIG_COUNT", overrides.len().to_string());
        for (index, (key, value)) in overrides.iter().enumerate() {
            command.env(format!("GIT_CONFIG_KEY_{index}"), key);
            command.env(format!("GIT_CONFIG_VALUE_{index}"), value);
        }
        command.args(["--no-pager", "--no-optional-locks"]);
        // check-ignore --stdin -z consumes pathnames and rejects literal
        // pathspec magic. env_clear above also removes inherited pathspec flags.
        if args.first() != Some(&"check-ignore") {
            command
                .arg("--literal-pathspecs")
                .env("GIT_LITERAL_PATHSPECS", "1");
        }
        for (key, value) in [
            ("core.fsmonitor", "false"),
            ("core.preloadIndex", "false"),
            ("index.threads", "1"),
            ("checkout.workers", "1"),
            ("core.hooksPath", null_device()),
            ("core.untrackedCache", "false"),
            ("submodule.recurse", "false"),
            ("protocol.allow", "never"),
            ("log.showSignature", "false"),
            ("core.alternateRefsCommand", ""),
            ("gc.auto", "0"),
            ("maintenance.auto", "false"),
            ("color.ui", "false"),
        ] {
            command.arg("-c").arg(format!("{key}={value}"));
        }
        command
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        Ok(command)
    }

    async fn run_scoped(
        &self,
        root: &Path,
        args: &[&str],
        overrides: &[(String, String)],
        input: &[u8],
        deadline: Instant,
        binding: Option<GitBinding<'_>>,
    ) -> Result<Captured> {
        if Instant::now() >= deadline {
            return Err(unavailable("Git observation deadline elapsed"));
        }
        let command = self.build_command(root, args, overrides, binding)?;
        let mut child = OwnedChild::spawn_leaf(command).map_err(|error| {
            if error.code == "unavailable" {
                Error::new("git_leaf_fence_unavailable", error.message)
            } else {
                error
            }
        })?;
        let stdout = child
            .take_stdout()
            .ok_or_else(|| unavailable("Git stdout is unavailable"))?;
        let stderr = child
            .take_stderr()
            .ok_or_else(|| unavailable("Git stderr is unavailable"))?;
        let mut stdin = child
            .take_stdin()
            .ok_or_else(|| unavailable("Git stdin is unavailable"))?;
        let result = {
            let capture = async {
                let write = async move {
                    if !input.is_empty() {
                        stdin
                            .write_all(input)
                            .await
                            .map_err(|_| unavailable("Git stdin failed"))?;
                    }
                    stdin
                        .shutdown()
                        .await
                        .map_err(|_| unavailable("Git stdin close failed"))?;
                    drop(stdin);
                    Ok::<_, Error>(())
                };
                let (stdout, stderr, (), status) = tokio::try_join!(
                    bounded_read(stdout, OUTPUT_BYTES),
                    bounded_read(stderr, STDERR_BYTES),
                    write,
                    child.wait(),
                )?;
                let code = status
                    .code()
                    .ok_or_else(|| unavailable("Git exit code is unknown"))?;
                // Optional filters may swallow a denied child spawn and return
                // success. Never turn their diagnostics into a successful poll.
                // Retain neither the diagnostic text nor possible command secrets.
                if matches!(code, 0 | 1) && !stderr.is_empty() {
                    return Err(unavailable(
                        "Git emitted diagnostics under the leaf process fence",
                    ));
                }
                Ok::<_, Error>(Captured { code, stdout })
            };
            timeout_at(deadline, capture).await
        };
        // On timeout/cap/error, stop only this owned family and bound cleanup.
        // wait() remains leader-only; Windows separately confirms its Job empty.
        let _ = child.terminate();
        let cleanup = Instant::now() + CLEANUP_TIME;
        if !matches!(timeout_at(cleanup, child.wait()).await, Ok(Ok(_))) {
            return Err(unavailable("Git process cleanup is unconfirmed"));
        }
        #[cfg(windows)]
        if !matches!(
            child
                .wait_family_empty(cleanup.saturating_duration_since(Instant::now()))
                .await,
            Ok(true)
        ) {
            return Err(unavailable("Git family cleanup is unconfirmed"));
        }
        result.map_err(|_| unavailable("Git observation timed out"))?
    }
}

#[cfg(windows)]
fn resolve_windows_git(program: PathBuf) -> Result<PathBuf> {
    let parent = program
        .parent()
        .ok_or_else(|| unavailable("Git executable has no parent"))?;
    let directory = parent
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("");
    let launcher = directory.eq_ignore_ascii_case("cmd");
    let mut selected = program.clone();
    if (launcher || directory.eq_ignore_ascii_case("bin"))
        && let Some(root) = parent.parent()
    {
        // Git for Windows cmd/bin entry points may be launchers. Resolve a real
        // installed executable once, before any fenced read; never retry a failed
        // command with a launcher, different executable, or weaker custody.
        for architecture in ["ucrt64", "mingw64", "mingw32", "clangarm64"] {
            let native = root.join(architecture).join("bin").join("git.exe");
            if fs::metadata(&native).is_ok_and(|metadata| metadata.is_file()) {
                selected = fs::canonicalize(native)
                    .map_err(|_| unavailable("installed native Git cannot be resolved"))?;
                break;
            }
        }
    }
    if launcher && selected == program {
        return Err(unavailable(
            "Git launcher has no installed native sibling for leaf execution",
        ));
    }
    if !fs::metadata(&selected).is_ok_and(|metadata| metadata.is_file()) {
        return Err(unavailable("resolved Git executable is not a regular file"));
    }
    Ok(selected)
}

async fn bounded_read<R: AsyncRead + Unpin>(mut reader: R, cap: usize) -> Result<Vec<u8>> {
    let mut output = Vec::new();
    let mut buffer = [0u8; 8192];
    loop {
        let read = reader
            .read(&mut buffer)
            .await
            .map_err(|_| unavailable("Git capture failed"))?;
        if read == 0 {
            return Ok(output);
        }
        if output.len().saturating_add(read) > cap {
            return Err(unavailable("Git capture exceeds the observation bound"));
        }
        output.extend_from_slice(&buffer[..read]);
    }
}

fn parse_status(bytes: &[u8]) -> Result<Status> {
    let records = nul_records(bytes)?;
    let mut records = records.into_iter();
    let mut head = None;
    let mut branch = None;
    let mut seen_head = false;
    let mut seen_branch = false;
    let mut dirty = Vec::new();
    let mut renamed_from = BTreeSet::new();
    while let Some(record) = records.next() {
        if let Some(oid) = record.strip_prefix("# branch.oid ") {
            if seen_head || (oid != "(initial)" && !is_oid(oid)) {
                return Err(unavailable("Git branch header is malformed"));
            }
            seen_head = true;
            head = (oid != "(initial)").then(|| oid.to_owned());
        } else if let Some(name) = record.strip_prefix("# branch.head ") {
            if seen_branch || name.is_empty() {
                return Err(unavailable("Git branch name is malformed"));
            }
            seen_branch = true;
            branch = (name != "(detached)").then(|| name.to_owned());
        } else if record.starts_with("# ") {
            continue;
        } else if let Some(path) = record.strip_prefix("? ") {
            dirty.push(GitPathStatus {
                path: git_path(path)?,
                status: "??".to_owned(),
                original_path: None,
            });
        } else if let Some(path) = record.strip_prefix("! ") {
            git_path(path)?;
        } else {
            let fields = match record.as_bytes().first() {
                Some(b'1') => 9,
                Some(b'2') => 10,
                Some(b'u') => 11,
                _ => return Err(unavailable("Git status contains an unsupported record")),
            };
            let parts: Vec<_> = record.splitn(fields, ' ').collect();
            if parts.len() != fields
                || !matches!(parts[0], "1" | "2" | "u")
                || parts[1].len() != 2
                || !parts[1].is_ascii()
            {
                return Err(unavailable("Git tracked status is malformed"));
            }
            if parts[2].starts_with('S') || parts[3..fields - 1].contains(&"160000") {
                return Err(unavailable(
                    "Git submodule status is not covered by this local probe",
                ));
            }
            let original_path = if parts[0] == "2" {
                parts[8]
                    .strip_prefix('R')
                    .or_else(|| parts[8].strip_prefix('C'))
                    .and_then(|score| score.parse::<u8>().ok())
                    .filter(|score| *score <= 100)
                    .ok_or_else(|| unavailable("Git rename/copy score is malformed"))?;
                let original = git_path(
                    records
                        .next()
                        .ok_or_else(|| unavailable("Git rename omitted its original path"))?,
                )?;
                if parts[8].starts_with('R') {
                    renamed_from.insert(original.clone());
                }
                Some(original)
            } else {
                None
            };
            dirty.push(GitPathStatus {
                path: git_path(parts[fields - 1])?,
                status: parts[1].to_owned(),
                original_path,
            });
        }
    }
    if !seen_head || !seen_branch {
        return Err(unavailable("Git status omitted its exact HEAD/branch"));
    }
    dirty.sort_by(|left, right| left.path.cmp(&right.path));
    Ok(Status {
        head,
        branch,
        dirty,
        renamed_from,
    })
}

fn parse_index(bytes: &[u8]) -> Result<BTreeMap<String, bool>> {
    let mut index = BTreeMap::<String, bool>::new();
    for record in nul_records(bytes)? {
        let (metadata, path) = record
            .split_once('\t')
            .ok_or_else(|| unavailable("Git index record is malformed"))?;
        let fields: Vec<_> = metadata.split(' ').collect();
        if fields.len() != 4 || fields[0].len() != 1 || !is_oid(fields[2]) {
            return Err(unavailable("Git index metadata is malformed"));
        }
        if fields[1] == "160000" {
            return Err(unavailable(
                "Git submodule worktrees are not covered by this local probe",
            ));
        }
        let path = git_path(path)?;
        let clean_eligible = fields[0] == "H" && fields[3] == "0";
        index
            .entry(path)
            .and_modify(|eligible| *eligible &= clean_eligible)
            .or_insert(clean_eligible);
    }
    Ok(index)
}

fn nul_records(bytes: &[u8]) -> Result<Vec<&str>> {
    if bytes.is_empty() {
        return Ok(Vec::new());
    }
    let text = std::str::from_utf8(bytes)
        .map_err(|_| unavailable("Git output contains non-UTF-8 data"))?;
    let text = text
        .strip_suffix('\0')
        .ok_or_else(|| unavailable("Git NUL record is truncated"))?;
    let records: Vec<_> = text.split('\0').collect();
    if records.iter().any(|record| record.is_empty()) {
        return Err(unavailable("Git output contains an empty record"));
    }
    Ok(records)
}

fn parse_paths(bytes: &[u8]) -> Result<Vec<String>> {
    nul_records(bytes)?.into_iter().map(git_path).collect()
}

fn parse_filter_attributes(bytes: &[u8], expected: &BTreeSet<String>) -> Result<BTreeSet<String>> {
    let records = nul_records(bytes)?;
    if records.len() != expected.len() * 3 {
        return Err(unavailable("Git filter attribute coverage is incomplete"));
    }
    let mut seen = BTreeSet::new();
    let mut affected = BTreeSet::new();
    for entry in records.as_chunks::<3>().0 {
        let path = git_path(entry[0])?;
        if entry[1] != "filter" || !expected.contains(&path) || !seen.insert(path.clone()) {
            return Err(unavailable("Git filter attribute identity is malformed"));
        }
        if !matches!(entry[2], "unspecified" | "unset") {
            affected.insert(path);
        }
    }
    Ok(affected)
}

pub(crate) fn is_oid(oid: &str) -> bool {
    matches!(oid.len(), 40 | 64) && oid.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn related_path(left: &str, right: &str) -> bool {
    left == right
        || left
            .strip_prefix(right)
            .is_some_and(|tail| tail.starts_with('/'))
        || right
            .strip_prefix(left)
            .is_some_and(|tail| tail.starts_with('/'))
}

fn git_path(path: &str) -> Result<String> {
    if path.is_empty()
        || path.starts_with('/')
        || path.contains('\0')
        || path
            .split('/')
            .any(|part| part.is_empty() || matches!(part, "." | ".."))
    {
        return Err(unavailable("Git path is not a canonical relative path"));
    }
    Ok(path.to_owned())
}

/// Normalize verified native file-change hints, including deleted files, without
/// following a symlink/reparse point outside (or within) the selected worktree.
pub(crate) fn normalize_hint(workspace: &str, path: &str) -> Result<String> {
    let root = canonical_root(workspace)?;
    if path.is_empty() || path.contains('\0') {
        return Err(unavailable("file-change hint is empty or malformed"));
    }
    let input = Path::new(path);
    if input
        .components()
        .any(|part| matches!(part, Component::ParentDir))
    {
        return Err(unavailable("file-change hint contains parent traversal"));
    }
    if !input.is_absolute()
        && input
            .components()
            .any(|part| matches!(part, Component::RootDir | Component::Prefix(_)))
    {
        return Err(unavailable("file-change hint has an ambiguous root"));
    }
    let target = if input.is_absolute() {
        input.to_path_buf()
    } else {
        root.join(input)
    };
    reject_links(&target)?;
    let mut existing = target.clone();
    let mut missing: Vec<OsString> = Vec::new();
    loop {
        match fs::symlink_metadata(&existing) {
            Ok(metadata) => {
                if !missing.is_empty() && !metadata.is_dir() {
                    return Err(unavailable("hint parent is not a directory"));
                }
                break;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                missing.push(
                    existing
                        .file_name()
                        .ok_or_else(|| unavailable("hint has no existing root"))?
                        .to_owned(),
                );
                if !existing.pop() {
                    return Err(unavailable("hint has no existing parent"));
                }
            }
            Err(_) => return Err(unavailable("hint path cannot be inspected")),
        }
    }
    let mut canonical =
        fs::canonicalize(existing).map_err(|_| unavailable("hint parent cannot be resolved"))?;
    for component in missing.into_iter().rev() {
        canonical.push(component);
    }
    let relative = canonical
        .strip_prefix(&root)
        .map_err(|_| unavailable("file-change hint escapes its worktree"))?;
    let mut parts = Vec::new();
    for component in relative.components() {
        match component {
            Component::Normal(name) => {
                let name = name
                    .to_str()
                    .ok_or_else(|| unavailable("hint path is not UTF-8"))?;
                #[cfg(windows)]
                if name.contains(':') {
                    return Err(unavailable("hint path contains a Windows alternate stream"));
                }
                parts.push(name);
            }
            Component::CurDir => {}
            _ => return Err(unavailable("hint path has an invalid component")),
        }
    }
    git_path(&parts.join("/"))
}

/// Git's path parser does not accept Windows verbatim prefixes in its own
/// routing values. Keep OS paths opaque and convert only at the Git boundary.
pub(crate) fn git_path_argument(path: &Path) -> Result<String> {
    let text = path
        .to_str()
        .ok_or_else(|| unavailable("Git-facing path is not UTF-8"))?;
    #[cfg(windows)]
    {
        let text = text.replace('\\', "/");
        if let Some(share) = text.strip_prefix("//?/UNC/") {
            return Ok(format!("//{share}"));
        }
        Ok(text.strip_prefix("//?/").unwrap_or(&text).to_owned())
    }
    #[cfg(not(windows))]
    {
        Ok(text.to_owned())
    }
}

pub(crate) fn canonical_root(workspace: &str) -> Result<PathBuf> {
    let path = Path::new(workspace);
    if !path.is_absolute() {
        return Err(unavailable("workspace must be absolute"));
    }
    reject_links(path)?;
    let root = fs::canonicalize(path).map_err(|_| unavailable("workspace cannot be resolved"))?;
    if !root.is_dir() {
        return Err(unavailable("workspace is not a directory"));
    }
    Ok(root)
}

pub(crate) fn reject_links(path: &Path) -> Result<()> {
    for prefix in path.ancestors() {
        match fs::symlink_metadata(prefix) {
            Ok(metadata) => {
                #[cfg(windows)]
                let reparse = {
                    use std::os::windows::fs::MetadataExt;
                    metadata.file_attributes() & 0x400 != 0
                };
                #[cfg(not(windows))]
                let reparse = false;
                if metadata.file_type().is_symlink() || reparse {
                    return Err(unavailable(
                        "workspace path contains a symlink or reparse point",
                    ));
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => return Err(unavailable("workspace path cannot be inspected")),
        }
    }
    Ok(())
}

fn null_device() -> &'static str {
    if cfg!(windows) {
        "//./NUL"
    } else {
        "/dev/null"
    }
}

fn unavailable(message: &str) -> Error {
    Error::new("unavailable", message)
}

#[cfg(all(test, windows))]
mod windows_selector_tests {
    use super::*;

    struct SelectorFiles(PathBuf);

    impl Drop for SelectorFiles {
        fn drop(&mut self) {
            // Only files and empty directories created by this selector test.
            for file in ["bin/git.exe", "cmd/git.exe", "ucrt64/bin/git.exe"] {
                let _ = fs::remove_file(self.0.join(file));
            }
            for directory in ["ucrt64/bin", "ucrt64", "cmd", "bin"] {
                let _ = fs::remove_dir(self.0.join(directory));
            }
            let _ = fs::remove_dir(&self.0);
        }
    }

    #[test]
    fn ucrt64_git_selector_handles_launchers_and_direct_native_paths() {
        let root = env::temp_dir().join(format!("eliot git selector {}", ulid::Ulid::new()));
        fs::create_dir(&root).expect("create unique selector fixture");
        let fixture = SelectorFiles(root);
        for directory in ["bin", "cmd", "ucrt64", "ucrt64/bin"] {
            fs::create_dir(fixture.0.join(directory)).expect("create installed layout directory");
        }
        for file in ["bin/git.exe", "cmd/git.exe", "ucrt64/bin/git.exe"] {
            // Resolution uses metadata only; no Git/native process is executed.
            fs::write(fixture.0.join(file), b"selector fixture")
                .expect("create executable placeholder");
        }
        let native = fs::canonicalize(fixture.0.join("ucrt64/bin/git.exe"))
            .expect("canonical native executable");
        for launcher in ["bin/git.exe", "cmd/git.exe"] {
            let launcher =
                fs::canonicalize(fixture.0.join(launcher)).expect("canonical launcher executable");
            assert_eq!(
                resolve_windows_git(launcher).expect("resolve UCRT layout"),
                native
            );
        }
        assert_eq!(
            resolve_windows_git(native.clone()).expect("accept direct native path"),
            native
        );
    }
}
