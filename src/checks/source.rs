//! Read exact Git objects, not a moving checkout or an attribute-filtered archive.
use super::model::CaptureRequest;
use crate::{
    artifacts::{ArtifactFiles, ArtifactRecord},
    error::{Error, Result},
    model,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeSet,
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, Read, Write},
    path::{Component, Path, PathBuf},
    process::{Command, Stdio},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceFile {
    pub path: String,
    pub mode: String,
    pub object_id: String,
    pub byte_length: u64,
    pub sha256: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceManifest {
    pub version: u32,
    pub commit: String,
    pub tree: String,
    pub files: Vec<SourceFile>,
}
pub fn path(root: &Path, id: &str) -> Result<PathBuf> {
    let suffix = id.strip_prefix("source-").unwrap_or("");
    if suffix.len() != 64 || !suffix.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(Error::invalid("invalid source snapshot ID"));
    }
    Ok(root.join("sources").join(id))
}
fn safe_path(s: &str) -> Result<PathBuf> {
    let p = PathBuf::from(s);
    if s.is_empty()
        || s.contains(['\\', ':', '\0'])
        || p.components().any(|c| !matches!(c, Component::Normal(_)))
        || s.split('/')
            .any(|s| s.eq_ignore_ascii_case(".git") || s.ends_with(['.', ' ']))
    {
        return Err(Error::new(
            "SOURCE_PATH",
            "source contains an unportable or unsafe path",
        ));
    }
    #[cfg(windows)]
    for component in s.split('/') {
        let base = component
            .split('.')
            .next()
            .unwrap_or("")
            .to_ascii_uppercase();
        if matches!(base.as_str(), "CON" | "PRN" | "AUX" | "NUL")
            || ((base.starts_with("COM") || base.starts_with("LPT"))
                && base.len() == 4
                && matches!(base.as_bytes()[3], b'1'..=b'9'))
        {
            return Err(Error::new(
                "SOURCE_PATH",
                "reserved Windows filename in source",
            ));
        }
    }
    Ok(p)
}
fn git(executable: &Path, repository: &Path) -> Command {
    let mut c = Command::new(executable);
    c.args([
        "--no-optional-locks",
        "--no-replace-objects",
        "-c",
        "core.fsmonitor=false",
        "-C",
    ])
    .arg(repository)
    .stdin(Stdio::null());
    // Repository discovery must not be redirected by a surrounding shell session.
    for k in [
        "GIT_DIR",
        "GIT_WORK_TREE",
        "GIT_INDEX_FILE",
        "GIT_OBJECT_DIRECTORY",
        "GIT_ALTERNATE_OBJECT_DIRECTORIES",
    ] {
        c.env_remove(k);
    }
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        c.creation_flags(0x08000000);
    }
    c
}
fn output(mut c: Command) -> Result<Vec<u8>> {
    let r = c.output()?;
    if !r.status.success() {
        return Err(Error::new(
            "GIT_SOURCE_ERROR",
            String::from_utf8_lossy(&r.stderr)
                .chars()
                .take(2000)
                .collect::<String>(),
        ));
    }
    Ok(r.stdout)
}
fn object(executable: &Path, repository: &Path, expr: &str) -> Result<String> {
    let mut c = git(executable, repository);
    c.args(["rev-parse", "--verify", expr]);
    let s =
        String::from_utf8(output(c)?).map_err(|_| Error::invalid("Git object ID was not UTF-8"))?;
    Ok(s.trim().to_string())
}
pub fn capture(
    root: &Path,
    files: &ArtifactFiles,
    input: &CaptureRequest,
    operation: &str,
    git_executable: &Path,
    identity: Value,
) -> Result<ArtifactRecord> {
    let repository = fs::canonicalize(&input.repository)?;
    let commit = object(
        git_executable,
        &repository,
        &format!("{}^{{commit}}", input.commit),
    )?;
    if !commit.eq_ignore_ascii_case(&input.commit) {
        return Err(Error::invalid("capture did not resolve the exact commit"));
    }
    let tree = object(git_executable, &repository, &format!("{commit}^{{tree}}"))?;
    let mut list = git(git_executable, &repository);
    list.args(["ls-tree", "-r", "-z", "--full-tree", &tree]);
    let entries = output(list)?;
    let mut specifications = Vec::new();
    let mut seen = BTreeSet::new();
    for entry in entries.split(|b| *b == 0).filter(|s| !s.is_empty()) {
        let tab = entry
            .iter()
            .position(|b| *b == b'\t')
            .ok_or_else(|| Error::new("GIT_SOURCE_ERROR", "malformed ls-tree record"))?;
        let (head, name) = (&entry[..tab], &entry[tab + 1..]);
        let head = std::str::from_utf8(head).map_err(|_| Error::invalid("invalid Git metadata"))?;
        let parts: Vec<_> = head.split_whitespace().collect();
        if parts.len() != 3 || parts[1] != "blob" || !matches!(parts[0], "100644" | "100755") {
            return Err(Error::new(
                "SOURCE_ENTRY_UNSUPPORTED",
                "source contains symlinks/gitlinks; provide a self-contained normal-file snapshot",
            ));
        }
        let name = std::str::from_utf8(name)
            .map_err(|_| Error::new("SOURCE_PATH", "non-UTF8 source path"))?;
        safe_path(name)?;
        if !seen.insert(name.to_lowercase()) {
            return Err(Error::new("SOURCE_PATH", "case-colliding source paths"));
        }
        specifications.push((parts[0].to_string(), parts[2].to_string(), name.to_string()));
    }
    let id = format!("source-{}", model::digest(operation.as_bytes()));
    let destination = path(root, &id)?;
    fs::create_dir_all(root.join("sources"))?;
    let temp = root
        .join("sources")
        .join(format!(".{}.tmp", model::new_id()));
    fs::create_dir(&temp)?;
    let result = (|| -> Result<ArtifactRecord> {
        let mut c = git(git_executable, &repository);
        c.args(["cat-file", "--batch"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        let mut child = c.spawn()?;
        let copied = (|| -> Result<Vec<SourceFile>> {
            let mut stdin = child
                .stdin
                .take()
                .ok_or_else(|| Error::new("GIT_SOURCE_ERROR", "missing cat-file input"))?;
            let mut stdout = BufReader::new(
                child
                    .stdout
                    .take()
                    .ok_or_else(|| Error::new("GIT_SOURCE_ERROR", "missing cat-file output"))?,
            );
            let mut manifest = Vec::new();
            for (mode, oid, name) in specifications {
                writeln!(stdin, "{oid}")?;
                stdin.flush()?;
                let mut header = String::new();
                stdout.read_line(&mut header)?;
                let fields: Vec<_> = header.split_whitespace().collect();
                if fields.len() != 3 || fields[0] != oid || fields[1] != "blob" {
                    return Err(Error::new(
                        "GIT_SOURCE_ERROR",
                        "unexpected cat-file response",
                    ));
                }
                let length: u64 = fields[2]
                    .parse()
                    .map_err(|_| Error::invalid("invalid Git blob length"))?;
                let target = temp.join(safe_path(&name)?);
                fs::create_dir_all(
                    target
                        .parent()
                        .ok_or_else(|| Error::invalid("missing parent"))?,
                )?;
                let mut out = OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&target)?;
                let mut hash = Sha256::new();
                let mut left = length;
                let mut buf = [0u8; 65536];
                let mut first = true;
                while left > 0 {
                    let n = left.min(buf.len() as u64) as usize;
                    stdout.read_exact(&mut buf[..n])?;
                    if first && buf[..n].starts_with(b"version https://git-lfs.github.com/spec/v1")
                    {
                        return Err(Error::new(
                            "SOURCE_LFS_UNSUPPORTED",
                            "Git LFS pointer requires explicit LFS materialization",
                        ));
                    }
                    first = false;
                    out.write_all(&buf[..n])?;
                    hash.update(&buf[..n]);
                    left -= n as u64;
                }
                let mut newline = [0];
                stdout.read_exact(&mut newline)?;
                if newline != *b"\n" {
                    return Err(Error::new("GIT_SOURCE_ERROR", "invalid blob separator"));
                }
                out.sync_all()?;
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    fs::set_permissions(
                        &target,
                        fs::Permissions::from_mode(if mode == "100755" { 0o700 } else { 0o600 }),
                    )?;
                }
                manifest.push(SourceFile {
                    path: name,
                    mode,
                    object_id: oid,
                    byte_length: length,
                    sha256: format!("{:x}", hash.finalize()),
                });
            }
            drop(stdin);
            Ok(manifest)
        })();
        if copied.is_err() {
            let _ = child.kill();
        }
        let status = child.wait()?;
        let manifest = copied?;
        if !status.success() {
            return Err(Error::new(
                "GIT_SOURCE_ERROR",
                "cat-file did not finish successfully",
            ));
        }
        let manifest = SourceManifest {
            version: 1,
            commit: commit.clone(),
            tree: tree.clone(),
            files: manifest,
        };
        let mut metadata = identity;
        metadata["commit"] = json!(commit);
        metadata["tree"] = json!(tree);
        metadata["file_count"] = json!(manifest.files.len());
        metadata["coverage"] = json!("complete");
        let (record, bytes) =
            ArtifactFiles::document("source_snapshot", &id, &json!(manifest), metadata)?;
        if destination.exists() {
            verify_directory(&destination, &manifest)?;
        } else {
            fs::rename(&temp, &destination)?;
        }
        files.publish(&record, &bytes)?;
        Ok(record)
    })();
    let _ = fs::remove_dir_all(&temp);
    result
}
pub fn manifest(files: &ArtifactFiles, record: &ArtifactRecord) -> Result<SourceManifest> {
    if record.kind != "source_snapshot" {
        return Err(Error::invalid(
            "check candidate must be an exact source snapshot",
        ));
    }
    let bytes = files.document_bytes(record)?;
    let manifest: SourceManifest = serde_json::from_slice(&bytes)?;
    if manifest.version != 1
        || manifest.commit != record.metadata["commit"]
        || manifest.tree != record.metadata["tree"]
    {
        return Err(Error::new(
            "SOURCE_MANIFEST",
            "source version or identity differs",
        ));
    }
    Ok(manifest)
}
pub fn verify_directory(directory: &Path, manifest: &SourceManifest) -> Result<()> {
    let mut expected = BTreeSet::new();
    for f in &manifest.files {
        let p = directory.join(safe_path(&f.path)?);
        expected.insert(f.path.clone());
        let m = fs::symlink_metadata(&p)?;
        if !m.is_file() || m.file_type().is_symlink() || m.len() != f.byte_length {
            return Err(Error::new("SOURCE_CHANGED", &f.path));
        }
        let mut file = File::open(p)?;
        let mut hash = Sha256::new();
        let mut buf = [0; 65536];
        let mut nbytes = 0u64;
        loop {
            let n = file.read(&mut buf)?;
            if n == 0 {
                break;
            }
            hash.update(&buf[..n]);
            nbytes += n as u64;
            if nbytes > f.byte_length {
                break;
            }
        }
        if nbytes != f.byte_length || format!("{:x}", hash.finalize()) != f.sha256 {
            return Err(Error::new("SOURCE_CHANGED", &f.path));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if (m.permissions().mode() & 0o111 != 0) != (f.mode == "100755") {
                return Err(Error::new("SOURCE_CHANGED", &f.path));
            }
        }
    }
    let mut pending = vec![directory.to_path_buf()];
    let mut actual = BTreeSet::new();
    while let Some(d) = pending.pop() {
        for e in fs::read_dir(&d)? {
            let e = e?;
            let ty = e.file_type()?;
            if ty.is_symlink() {
                return Err(Error::new("SOURCE_CHANGED", "unexpected symlink"));
            }
            if ty.is_dir() {
                pending.push(e.path());
            } else if ty.is_file() {
                actual.insert(
                    e.path()
                        .strip_prefix(directory)
                        .map_err(|_| Error::invalid("source path escaped"))?
                        .to_string_lossy()
                        .replace('\\', "/"),
                );
            } else {
                return Err(Error::new("SOURCE_CHANGED", "non-regular source entry"));
            }
        }
    }
    if actual != expected {
        return Err(Error::new(
            "SOURCE_CHANGED",
            "source file inventory changed",
        ));
    }
    Ok(())
}
pub fn materialize(
    root: &Path,
    files: &ArtifactFiles,
    record: &ArtifactRecord,
    destination: &Path,
) -> Result<SourceManifest> {
    let manifest = manifest(files, record)?;
    let source = path(root, &record.artifact_id)?;
    verify_directory(&source, &manifest)?;
    fs::create_dir(destination)?;
    for f in &manifest.files {
        let rel = safe_path(&f.path)?;
        let to = destination.join(&rel);
        fs::create_dir_all(
            to.parent()
                .ok_or_else(|| Error::invalid("source parent missing"))?,
        )?;
        fs::copy(source.join(rel), to)?;
    }
    verify_directory(destination, &manifest)?;
    Ok(manifest)
}
