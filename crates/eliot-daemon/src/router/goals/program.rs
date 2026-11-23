//! Resolve a Goal check without executing it. Rust's process launcher still
//! owns the final check for races, sharing violations and platform custody.

use eliot_core::{Error, Result};
use std::{
    env,
    path::{Component, Path, PathBuf},
};

pub(super) async fn resolve(program: String, cwd: Option<String>) -> Result<PathBuf> {
    tokio::task::spawn_blocking(move || resolve_program(&program, cwd.as_deref().map(Path::new)))
        .await
        .map_err(|_| Error::new("unavailable", "Goal executable lookup was interrupted"))?
}

fn resolve_program(program: &str, cwd: Option<&Path>) -> Result<PathBuf> {
    let path = Path::new(program);
    let bare = matches!(path.components().next(), Some(Component::Normal(_)))
        && path.components().count() == 1;
    let candidates = if bare {
        path_candidates(path)
    } else {
        vec![path.to_owned()]
    };

    let mut refusal = None;
    let suffixes: &[Option<&str>] = if cfg!(windows) && path.extension().is_none() {
        &[Some("exe"), Some("com"), Some("cmd"), Some("bat"), None]
    } else {
        &[None]
    };
    for suffix in suffixes {
        for candidate in &candidates {
            let candidate = match suffix {
                Some(extension) => candidate.with_extension(extension),
                None => candidate.to_owned(),
            };
            let candidate = if let Some(cwd) = cwd {
                cwd.join(candidate)
            } else {
                candidate
            };
            let candidate = std::path::absolute(&candidate).map_err(|error| {
                unrunnable(format!("Cannot resolve Goal check {program:?}: {error}"))
            })?;
            match std::fs::metadata(&candidate) {
                Ok(metadata) => match check_executable(&candidate, &metadata) {
                    Ok(()) => return Ok(candidate),
                    Err(error) => {
                        refusal.get_or_insert(error);
                    }
                },
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    refusal.get_or_insert_with(|| {
                        unrunnable(format!(
                            "Cannot inspect Goal check executable {}: {error}",
                            candidate.display()
                        ))
                    });
                }
            }
        }
    }
    Err(refusal.unwrap_or_else(|| {
        unrunnable(format!(
            "Goal check executable {program:?} was not found. Use an executable on PATH or an explicit path."
        ))
    }))
}

fn path_candidates(program: &Path) -> Vec<PathBuf> {
    let search_path = env::var_os("PATH");
    #[cfg(unix)]
    let search_path = search_path.or_else(|| Some("/usr/bin:/bin".into()));
    let dirs = search_path.iter().flat_map(env::split_paths);
    #[cfg(windows)]
    let dirs = env::var_os("SystemRoot")
        .map(|root| PathBuf::from(root).join("System32"))
        .into_iter()
        .chain(dirs);
    dirs.map(|dir| dir.join(program)).collect()
}

fn unrunnable(message: String) -> Error {
    Error::new("check_unrunnable", message)
}

fn check_executable(path: &Path, metadata: &std::fs::Metadata) -> Result<()> {
    if !metadata.is_file() {
        return Err(unrunnable(format!(
            "Goal check {} is not a regular executable file",
            path.display()
        )));
    }
    #[cfg(windows)]
    {
        if eliot_plugin::install::is_script_wrapper(&path.to_string_lossy()) {
            return Err(unrunnable(format!(
                "Goal check {} is a script wrapper. Name its interpreter explicitly, for example cmd.exe /d /c npm test for a .cmd or .bat wrapper.",
                path.display()
            )));
        }
        if let Err(error) = crate::platform::windows::check_executable_image(path)
            && error.raw_os_error()
                != Some(windows_sys::Win32::Foundation::ERROR_SHARING_VIOLATION as i32)
        {
            return Err(unrunnable(format!(
                "Goal check {} is not executable: {error}",
                path.display()
            )));
        }
    }
    #[cfg(unix)]
    if let Err(error) = crate::platform::unix::check_execute_access(path) {
        return Err(unrunnable(format!(
            "Goal check {} cannot be executed: {error}",
            path.display()
        )));
    }
    Ok(())
}
