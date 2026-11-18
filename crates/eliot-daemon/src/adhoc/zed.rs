//! Discovery for the standalone Zed evaluation runner.

use std::{path::Path, time::Instant};

use eliot_core::{Error, Result};

use super::{
    VERSION_PROBE_TIMEOUT, executable_contains, native_capture, native_exe_name,
    resolve_native_executable,
};

const DESCRIPTION: &str = "Run Zed's agent headlessly in evaluation/benchmark environments";

pub(super) fn missing() -> Error {
    Error::new(
        "unsupported",
        "Zed headless eval-cli was not found on PATH; see docs/harnesses/zed.md for the separate Zed source build",
    )
}

pub(crate) fn eval_cli_name(cmd: &str) -> bool {
    cmd.rsplit(['/', '\\'])
        .next()
        .is_some_and(|file| file == native_exe_name("eval-cli"))
}

pub(super) fn discover() -> Result<String> {
    super::find_on_path(&native_exe_name("eval-cli"))
        .filter(|path| path.is_absolute())
        .and_then(|path| path.into_os_string().into_string().ok())
        .ok_or_else(missing)
}

pub(super) fn probe(cmd: &str, deadline: Instant) -> Result<String> {
    let program = resolve(cmd)?;
    if !cfg!(target_os = "linux") {
        return Err(Error::new(
            "unsupported",
            "Zed eval-cli requires a Linux runtime with private XDG data directories",
        ));
    }
    let deadline = deadline.min(Instant::now() + VERSION_PROBE_TIMEOUT);
    // The help description from the pinned runner is embedded in the binary.
    // Check it before executing even a renamed native payload.
    let proven = executable_contains(&program, DESCRIPTION.as_bytes(), deadline)
        && native_capture(&program, &["--help"], deadline).is_some_and(|capture| {
            capture.status.success() && capture.stdout_complete && eval_help(&capture.stdout)
        });
    if !proven {
        return Err(Error::new(
            "unsupported",
            "resolved executable did not establish the Zed eval-cli batch capability",
        ));
    }
    Ok(program)
}

fn resolve(cmd: &str) -> Result<String> {
    if !eval_cli_name(cmd) {
        return Err(missing());
    }
    let program = resolve_native_executable(cmd, false)?;
    if !eval_cli_name(&program) || !Path::new(&program).is_absolute() {
        return Err(missing());
    }
    Ok(program)
}

fn eval_help(bytes: &[u8]) -> bool {
    std::str::from_utf8(bytes).is_ok_and(|help| {
        help.lines().any(|line| line.trim() == DESCRIPTION)
            && help
                .lines()
                .any(|line| line.starts_with("Usage: eval-cli "))
            && [
                "--workdir",
                "--model",
                "--instruction",
                "--output-dir",
                "--timeout",
                "--no-staff",
            ]
            .iter()
            .all(|flag| {
                help.lines()
                    .any(|line| line.split_whitespace().next() == Some(flag))
            })
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zed_help_requires_the_native_batch_contract() {
        let help = format!(
            "{DESCRIPTION}\nUsage: eval-cli [OPTIONS]\n  --workdir <DIR>\n  --model <MODEL>\n  --instruction <INSTRUCTION>\n  --output-dir <DIR>\n  --timeout <SECONDS>\n  --no-staff"
        );
        assert!(eval_help(help.as_bytes()));
        assert!(!eval_help(
            help.replace("--output-dir", "--output").as_bytes()
        ));
        assert!(!eval_help(b"Usage: zed [OPTIONS] [PATHS]..."));
    }

    #[test]
    fn zed_discovery_rejects_the_editor_and_nonexecutables_before_any_probe() {
        assert!(eval_cli_name(&native_exe_name("eval-cli")));
        for path in [
            "Zed.exe",
            "zed",
            "eval-cli.cmd",
            "zed-remote-server",
            "missing/eval-cli",
        ] {
            assert_eq!(resolve(path).unwrap_err().code, "unsupported");
        }
    }

    #[cfg(unix)]
    #[test]
    fn zed_resolves_the_actual_target_and_rejects_an_editor_alias() {
        let root = std::env::temp_dir().join(format!("eliot-zed-discovery-{}", ulid::Ulid::new()));
        let _temp = crate::test_support::TempDir::own(root.clone());
        std::fs::create_dir(&root).unwrap();
        let alias = root.join("eval-cli");
        std::os::unix::fs::symlink(std::env::current_exe().unwrap(), &alias).unwrap();
        assert_eq!(
            resolve(alias.to_str().unwrap()).unwrap_err().code,
            "unsupported"
        );
    }
}
