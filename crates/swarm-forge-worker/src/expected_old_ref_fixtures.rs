use super::{
    Capture, ancestry_status, exact_push_rejection, predecessor_ancestry_args,
    publication_push_args,
};
use std::{
    env,
    ffi::OsString,
    fs,
    path::{Path, PathBuf},
    process::{Command, Output, Stdio},
    sync::atomic::{AtomicU64, Ordering},
};

static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[test]
fn lease_rejection_requires_exact_complete_target_porcelain() {
    let rejected = b"To remote\n!\tdeadbeef:refs/heads/main\t[rejected] (stale info)\nDone\n";
    let mut capture = Capture {
        prefix: rejected.to_vec(),
        byte_count: rejected.len() as u64,
        truncated: false,
        sha256: super::digest(rejected),
    };
    assert!(exact_push_rejection(Some(&capture), "refs/heads/main"));
    assert!(!exact_push_rejection(Some(&capture), "refs/heads/other"));
    capture.truncated = true;
    assert!(!exact_push_rejection(Some(&capture), "refs/heads/main"));
    capture.truncated = false;
    capture.prefix = b"fatal: connection closed after remote update\n".to_vec();
    assert!(!exact_push_rejection(Some(&capture), "refs/heads/main"));
    capture.prefix = b" \tdeadbeef:refs/heads/main\tdeadbeef..feedface\n".to_vec();
    assert!(!exact_push_rejection(Some(&capture), "refs/heads/main"));
    assert!(!exact_push_rejection(None, "refs/heads/main"));
}

struct TempDir(PathBuf);

impl TempDir {
    fn new() -> Self {
        for _ in 0..128 {
            let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
            let path = env::temp_dir().join(format!(
                "eliot-forge-expected-ref-{}-{sequence}",
                std::process::id()
            ));
            match fs::create_dir(&path) {
                Ok(()) => return Self(path),
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => panic!("cannot create disposable Git fixture: {error}"),
            }
        }
        panic!("could not allocate a unique disposable Git fixture directory");
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn git_command() -> Command {
    let mut command = Command::new("git");
    for key in [
        "GIT_DIR",
        "GIT_WORK_TREE",
        "GIT_INDEX_FILE",
        "GIT_OBJECT_DIRECTORY",
        "GIT_ALTERNATE_OBJECT_DIRECTORIES",
        "GIT_COMMON_DIR",
        "GIT_NAMESPACE",
        "GIT_SSH",
        "GIT_SSH_COMMAND",
        "GIT_ASKPASS",
        "SSH_ASKPASS",
        "GIT_CONFIG",
        "GIT_CONFIG_PARAMETERS",
        "GIT_CONFIG_COUNT",
    ] {
        command.env_remove(key);
    }
    for (key, _) in env::vars_os() {
        let folded = key.to_string_lossy().to_ascii_uppercase();
        if folded.starts_with("GIT_CONFIG_KEY_")
            || folded.starts_with("GIT_CONFIG_VALUE_")
            || folded.starts_with("GIT_TRACE")
            || folded == "GIT_CURL_VERBOSE"
        {
            command.env_remove(key);
        }
    }
    command
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("GIT_OPTIONAL_LOCKS", "0")
        .stdin(Stdio::null());
    command
}

fn git_root(args: &[OsString]) -> Output {
    git_command()
        .args(["--no-optional-locks", "--no-replace-objects"])
        .args(args)
        .output()
        .expect("Git must be available for disposable remote fixtures")
}

fn git_in(repository: &Path, args: &[OsString]) -> Output {
    git_command()
        .args([
            "--no-optional-locks",
            "--no-replace-objects",
            "-c",
            "core.fsmonitor=false",
            "-C",
        ])
        .arg(repository)
        .args(args)
        .output()
        .expect("Git must be available for disposable remote fixtures")
}

fn os_args(values: &[&str]) -> Vec<OsString> {
    values.iter().map(OsString::from).collect()
}

fn assert_success(output: Output, action: &str) -> Output {
    assert!(
        output.status.success(),
        "{action} failed\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    output
}

fn git_root_ok(args: &[OsString], action: &str) -> Output {
    assert_success(git_root(args), action)
}

fn git_in_ok(repository: &Path, args: &[&str], action: &str) -> Output {
    assert_success(git_in(repository, &os_args(args)), action)
}

fn git_in_os_ok(repository: &Path, args: &[OsString], action: &str) -> Output {
    assert_success(git_in(repository, args), action)
}

fn init_bare(path: &Path) {
    let args = [
        OsString::from("init"),
        OsString::from("--bare"),
        OsString::from("--initial-branch=main"),
        path.as_os_str().to_os_string(),
    ];
    git_root_ok(&args, "initialize bare remote");
}

fn init_repository(path: &Path) {
    fs::create_dir_all(path).expect("create fixture repository directory");
    git_in_ok(
        path,
        &["init", "--initial-branch=main"],
        "initialize repository",
    );
    git_in_ok(
        path,
        &["config", "user.name", "Forge fixture"],
        "set fixture author",
    );
    git_in_ok(
        path,
        &["config", "user.email", "forge-fixture@example.invalid"],
        "set fixture email",
    );
}

fn commit_file(repository: &Path, name: &str, contents: &str, message: &str) -> String {
    fs::write(repository.join(name), contents).expect("write fixture commit file");
    git_in_ok(repository, &["add", "--all"], "stage fixture commit");
    git_in_ok(
        repository,
        &["commit", "--quiet", "-m", message],
        "create fixture commit",
    );
    String::from_utf8_lossy(
        &git_in_ok(
            repository,
            &["rev-parse", "--verify", "HEAD^{commit}"],
            "read fixture commit",
        )
        .stdout,
    )
    .trim()
    .to_owned()
}

fn seed_remote(temp: &TempDir) -> (PathBuf, PathBuf, String) {
    let remote = temp.path().join("remote.git");
    init_bare(&remote);
    let writer = temp.path().join("writer");
    init_repository(&writer);
    let base = commit_file(&writer, "history.txt", "A\n", "base A");
    let remote_add = [
        OsString::from("remote"),
        OsString::from("add"),
        OsString::from("origin"),
        remote.as_os_str().to_os_string(),
    ];
    git_in_os_ok(&writer, &remote_add, "configure fixture writer remote");
    git_in_ok(
        &writer,
        &["push", "origin", "refs/heads/main:refs/heads/main"],
        "seed fixture remote",
    );
    (remote, writer, base)
}

fn clone_candidate(temp: &TempDir, remote: &Path, name: &str) -> PathBuf {
    let candidate = temp.path().join(name);
    let args = [
        OsString::from("clone"),
        OsString::from("--quiet"),
        OsString::from("--branch"),
        OsString::from("main"),
        remote.as_os_str().to_os_string(),
        candidate.as_os_str().to_os_string(),
    ];
    git_root_ok(&args, "clone fixture candidate");
    git_in_ok(
        &candidate,
        &["config", "user.name", "Forge candidate"],
        "set candidate author",
    );
    git_in_ok(
        &candidate,
        &["config", "user.email", "forge-candidate@example.invalid"],
        "set candidate email",
    );
    candidate
}

fn bare_ref(remote: &Path) -> String {
    let args = [
        OsString::from("--git-dir"),
        remote.as_os_str().to_os_string(),
        OsString::from("rev-parse"),
        OsString::from("--verify"),
        OsString::from("refs/heads/main"),
    ];
    String::from_utf8_lossy(&git_root_ok(&args, "read fixture remote ref").stdout)
        .trim()
        .to_owned()
}

fn push_with_lease(
    repository: &Path,
    remote: &Path,
    expected_old_ref: Option<&str>,
    candidate_commit: &str,
) -> Output {
    let endpoint = remote.to_string_lossy();
    let args = publication_push_args(
        "refs/heads/main",
        expected_old_ref,
        candidate_commit,
        &endpoint,
    );
    let args = args.into_iter().map(OsString::from).collect::<Vec<_>>();
    git_in(repository, &args)
}

fn ancestry_output(repository: &Path, expected: &str, candidate: &str) -> Output {
    let args = predecessor_ancestry_args(expected, candidate)
        .into_iter()
        .map(OsString::from)
        .collect::<Vec<_>>();
    git_in(repository, &args)
}

#[test]
fn intermediate_fast_forward_race_is_rejected_by_the_admitted_lease() {
    let temp = TempDir::new();
    let (remote, writer, expected) = seed_remote(&temp);
    let candidate = clone_candidate(&temp, &remote, "candidate");

    let observed_before = bare_ref(&remote);
    assert_eq!(observed_before, expected, "fixture pre-read must observe A");

    let winner = commit_file(&writer, "history.txt", "A\nB\n", "winner B");
    git_in_ok(
        &writer,
        &["push", "origin", "refs/heads/main:refs/heads/main"],
        "advance fixture remote to B",
    );
    git_in_ok(&candidate, &["fetch", "origin"], "fetch competing B");
    git_in_ok(
        &candidate,
        &["merge", "--ff-only", "origin/main"],
        "advance candidate through B",
    );
    let candidate_commit = commit_file(&candidate, "history.txt", "A\nB\nC\n", "candidate C");

    let ancestry = ancestry_output(&candidate, &observed_before, &candidate_commit);
    assert_eq!(
        ancestry_status(ancestry.status.success(), ancestry.status.code()),
        Some(true),
        "candidate C must descend from the admitted A"
    );
    assert!(
        ancestry_output(&candidate, &winner, &candidate_commit)
            .status
            .success(),
        "candidate C must contain the competing fast-forward B"
    );

    let push = push_with_lease(
        &candidate,
        &remote,
        Some(&observed_before),
        &candidate_commit,
    );
    assert!(
        !push.status.success(),
        "stale A lease must reject candidate C"
    );
    assert_eq!(
        bare_ref(&remote),
        winner,
        "the competing B must remain published"
    );
}

#[test]
fn exact_fast_forward_from_expected_ref_is_accepted() {
    let temp = TempDir::new();
    let (remote, _writer, expected) = seed_remote(&temp);
    let candidate = clone_candidate(&temp, &remote, "candidate");
    let candidate_commit = commit_file(&candidate, "history.txt", "A\nC\n", "candidate C");

    let ancestry = ancestry_output(&candidate, &expected, &candidate_commit);
    assert_eq!(
        ancestry_status(ancestry.status.success(), ancestry.status.code()),
        Some(true),
        "A must be an ancestor of C"
    );
    assert!(
        push_with_lease(&candidate, &remote, Some(&expected), &candidate_commit)
            .status
            .success(),
        "the exact A-to-C lease update must succeed"
    );
    assert_eq!(bare_ref(&remote), candidate_commit);
}

#[test]
fn create_lease_succeeds_only_for_an_absent_ref() {
    let temp = TempDir::new();
    let remote = temp.path().join("remote.git");
    init_bare(&remote);
    let candidate = temp.path().join("candidate");
    init_repository(&candidate);
    let candidate_commit = commit_file(&candidate, "history.txt", "C\n", "candidate C");

    assert!(
        push_with_lease(&candidate, &remote, None, &candidate_commit)
            .status
            .success(),
        "an explicit empty expectation must create an absent ref"
    );
    assert_eq!(bare_ref(&remote), candidate_commit);

    let raced_remote = temp.path().join("raced.git");
    init_bare(&raced_remote);
    let winner = temp.path().join("winner");
    init_repository(&winner);
    let winner_commit = commit_file(&winner, "history.txt", "B\n", "winner B");
    let origin_add = [
        OsString::from("remote"),
        OsString::from("add"),
        OsString::from("origin"),
        raced_remote.as_os_str().to_os_string(),
    ];
    git_in_os_ok(&winner, &origin_add, "configure create-race winner remote");
    git_in_ok(
        &winner,
        &["push", "origin", "refs/heads/main:refs/heads/main"],
        "win create race",
    );

    let raced_candidate = temp.path().join("raced-candidate");
    init_repository(&raced_candidate);
    let raced_commit = commit_file(&raced_candidate, "history.txt", "C\n", "candidate C");
    let push = push_with_lease(&raced_candidate, &raced_remote, None, &raced_commit);
    assert!(
        !push.status.success(),
        "empty expectation must reject a raced create"
    );
    assert_eq!(bare_ref(&raced_remote), winner_commit);
}

#[test]
fn unchanged_expected_ref_blocks_a_non_ancestor_before_lease_push() {
    let temp = TempDir::new();
    let (remote, _writer, expected) = seed_remote(&temp);
    let unrelated = temp.path().join("unrelated");
    init_repository(&unrelated);
    let origin_add = [
        OsString::from("remote"),
        OsString::from("add"),
        OsString::from("origin"),
        remote.as_os_str().to_os_string(),
    ];
    git_in_os_ok(&unrelated, &origin_add, "configure ancestry fixture remote");
    git_in_ok(
        &unrelated,
        &["fetch", "--quiet", "origin", "refs/heads/main"],
        "fetch exact expected commit object",
    );
    let unrelated_commit = commit_file(&unrelated, "other.txt", "D\n", "unrelated D");

    let ancestry = ancestry_output(&unrelated, &expected, &unrelated_commit);
    let ancestry_decision = ancestry_status(ancestry.status.success(), ancestry.status.code());
    assert_eq!(
        ancestry_decision,
        Some(false),
        "an unrelated commit must fail the exact ancestry check"
    );
    if ancestry_decision == Some(true) {
        let push = push_with_lease(&unrelated, &remote, Some(&expected), &unrelated_commit);
        assert!(
            push.status.success(),
            "the admitted ancestry path should push"
        );
    }
    assert_eq!(
        bare_ref(&remote),
        expected,
        "the worker must not issue a lease push after failed ancestry"
    );
}
