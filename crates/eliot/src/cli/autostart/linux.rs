use super::super::support::{Failure, print_value};
use eliot_daemon::DataDir;
use serde_json::{Value, json};
use std::{
    env, fs,
    fs::OpenOptions,
    io::Write,
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::{Component, Path, PathBuf},
    time::Duration,
};
#[expect(
    clippy::disallowed_types,
    reason = "Linux CLI autostart invokes the user-owned systemd manager."
)]
use tokio::process::Command;
use tokio::time::timeout;

const SYSTEMCTL_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_OUTPUT_BYTES: usize = 16 * 1024;
const RESTART_DELAY_SECONDS: u64 = 60;
const START_LIMIT_INTERVAL_SECONDS: u64 = 4 * 60;
const START_LIMIT_BURST: u32 = 4;

struct UnitPlan {
    data_dir: PathBuf,
    executable: PathBuf,
    unit: String,
    unit_file: PathBuf,
    marker: String,
    contents: String,
}

pub(super) async fn install(
    data_dir: &Path,
    json_output: bool,
    dry_run: bool,
) -> Result<u8, Failure> {
    let plan = build_plan(data_dir, !dry_run, dry_run)?;
    let existing = read_owned_unit(&plan.unit_file, &plan.marker)?;
    if dry_run {
        let would = match existing.as_deref() {
            Some(contents) if contents == plan.contents => "enable_unchanged_unit",
            Some(_) => "update",
            None => "install",
        };
        report_plan(&plan, "install", would, json_output);
        return Ok(0);
    }

    let state = match existing.as_deref() {
        Some(contents) if contents == plan.contents => "already_installed",
        Some(_) => "updated",
        None => "installed",
    };
    if existing.as_deref() != Some(plan.contents.as_str()) {
        let parent = plan
            .unit_file
            .parent()
            .ok_or_else(|| Failure::new(1, "systemd user unit path has no parent directory"))?;
        fs::create_dir_all(parent)
            .map_err(|error| io_failure("create the systemd user unit directory", error))?;
        write_unit_atomically(&plan.unit_file, &plan.contents)?;
    }
    run_systemctl(&["--user", "daemon-reload"]).await?;
    run_systemctl(&["--user", "enable", plan.unit.as_str()]).await?;
    report_result(
        &json!({
            "state": state,
            "platform": "systemd_user",
            "unit": plan.unit,
            "unit_file": plan.unit_file,
            "trigger": "user_default_target",
            "restart_on_failure": restart_policy(),
        }),
        json_output,
    );
    Ok(0)
}

pub(super) async fn uninstall(
    data_dir: &Path,
    json_output: bool,
    dry_run: bool,
) -> Result<u8, Failure> {
    let plan = build_plan(data_dir, false, true)?;
    let existing = read_owned_unit(&plan.unit_file, &plan.marker)?;
    if dry_run {
        let would = if existing.is_some() {
            "remove"
        } else {
            "no_op_not_installed"
        };
        report_plan(&plan, "uninstall", would, json_output);
        return Ok(0);
    }
    if existing.is_none() {
        report_result(
            &json!({
                "state": "not_installed",
                "platform": "systemd_user",
                "unit": plan.unit,
                "unit_file": plan.unit_file,
            }),
            json_output,
        );
        return Ok(0);
    }

    run_systemctl(&["--user", "disable", plan.unit.as_str()]).await?;
    fs::remove_file(&plan.unit_file)
        .map_err(|error| io_failure("remove the verified systemd user unit", error))?;
    run_systemctl(&["--user", "daemon-reload"]).await?;
    report_result(
        &json!({
            "state": "removed",
            "platform": "systemd_user",
            "unit": plan.unit,
            "unit_file": plan.unit_file,
        }),
        json_output,
    );
    Ok(0)
}

fn build_plan(
    data_dir: &Path,
    create_data_dir: bool,
    allow_missing_data_dir: bool,
) -> Result<UnitPlan, Failure> {
    let data_root = canonical_data_dir(data_dir, create_data_dir, allow_missing_data_dir)?;
    let executable =
        env::current_exe().map_err(|error| io_failure("locate Swarm executable", error))?;
    let executable = canonical_path(&executable, "Swarm executable")?;
    if !executable.is_file() {
        return Err(Failure::new(
            1,
            "the current Swarm executable is unavailable",
        ));
    }
    let root_text = valid_systemd_path(&data_root, "data directory")?;
    let digest = eliot_core::api::inference::sha256_hex(root_text.as_bytes());
    let unit = format!("eliot-daemon-{digest}.service");
    let config_dir = systemd_user_config_dir()?;
    let unit_file = config_dir.join(&unit);
    let marker = format!("# eliot-autostart-v1:{digest}");
    let contents = unit_contents(&marker, &executable, &data_root)?;
    Ok(UnitPlan {
        data_dir: data_root,
        executable,
        unit,
        unit_file,
        marker,
        contents,
    })
}

fn canonical_data_dir(
    data_dir: &Path,
    create_data_dir: bool,
    allow_missing_data_dir: bool,
) -> Result<PathBuf, Failure> {
    if create_data_dir {
        let private_dir = DataDir::open(data_dir).map_err(data_dir_failure)?;
        return canonical_path(private_dir.path(), "data directory");
    }
    match fs::symlink_metadata(data_dir) {
        Ok(_) => {
            let private_dir = DataDir::open_existing(data_dir).map_err(data_dir_failure)?;
            canonical_path(private_dir.path(), "data directory")
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound && allow_missing_data_dir => {
            canonical_missing_path(data_dir, "data directory")
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Err(Failure::new(
            1,
            "cannot plan daemon autostart because the data directory does not exist",
        )),
        Err(error) => Err(io_failure("inspect the private data directory", error)),
    }
}

fn systemd_user_config_dir() -> Result<PathBuf, Failure> {
    let config_home = match env::var_os("XDG_CONFIG_HOME").filter(|value| !value.is_empty()) {
        Some(value) => PathBuf::from(value),
        None => {
            let home = env::var_os("HOME")
                .filter(|value| !value.is_empty())
                .ok_or_else(|| Failure::new(1, "HOME is unavailable for systemd user autostart"))?;
            PathBuf::from(home).join(".config")
        }
    };
    if !config_home.is_absolute() {
        return Err(Failure::new(
            2,
            "XDG_CONFIG_HOME or HOME must be an absolute path for systemd user autostart",
        ));
    }
    canonical_missing_path(
        &config_home.join("systemd").join("user"),
        "systemd user unit directory",
    )
}

fn canonical_path(path: &Path, label: &str) -> Result<PathBuf, Failure> {
    let canonical = fs::canonicalize(path)
        .map_err(|error| io_failure(&format!("canonicalize {label}"), error))?;
    validate_absolute_path(&canonical, label)
}

fn canonical_missing_path(path: &Path, label: &str) -> Result<PathBuf, Failure> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        env::current_dir()
            .map_err(|error| io_failure("locate current directory", error))?
            .join(path)
    };
    if absolute
        .components()
        .any(|component| matches!(component, Component::ParentDir))
    {
        return Err(Failure::new(
            2,
            format!("{label} paths may not contain parent traversal"),
        ));
    }
    let mut ancestor = absolute.clone();
    let mut missing = Vec::new();
    loop {
        match fs::metadata(&ancestor) {
            Ok(metadata) => {
                if !metadata.is_dir() {
                    return Err(Failure::new(
                        2,
                        format!("the existing {label} ancestor is not a directory"),
                    ));
                }
                let mut resolved = canonical_path(&ancestor, label)?;
                for component in missing.iter().rev() {
                    resolved.push(component);
                }
                return validate_absolute_path(&resolved, label);
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let name = ancestor.file_name().ok_or_else(|| {
                    Failure::new(
                        2,
                        format!("cannot resolve {label} without an existing ancestor"),
                    )
                })?;
                missing.push(name.to_os_string());
                ancestor = ancestor
                    .parent()
                    .ok_or_else(|| {
                        Failure::new(
                            2,
                            format!("cannot resolve {label} without an existing ancestor"),
                        )
                    })?
                    .to_path_buf();
            }
            Err(error) => return Err(io_failure(&format!("inspect a {label} ancestor"), error)),
        }
    }
}

fn validate_absolute_path(path: &Path, label: &str) -> Result<PathBuf, Failure> {
    if !path.is_absolute() {
        return Err(Failure::new(2, format!("{label} must be an absolute path")));
    }
    valid_systemd_path(path, label)?;
    Ok(path.to_path_buf())
}

fn valid_systemd_path<'a>(path: &'a Path, label: &str) -> Result<&'a str, Failure> {
    let text = path
        .to_str()
        .ok_or_else(|| Failure::new(2, format!("{label} must be valid UTF-8 for systemd")))?;
    if text.chars().any(char::is_control) {
        return Err(Failure::new(
            2,
            format!("{label} contains unsupported control characters"),
        ));
    }
    Ok(text)
}

fn unit_contents(marker: &str, executable: &Path, data_dir: &Path) -> Result<String, Failure> {
    let executable = valid_systemd_path(executable, "Swarm executable")?;
    let data_dir = valid_systemd_path(data_dir, "data directory")?;
    let command = [executable, "--data-dir", data_dir, "daemon", "--foreground"]
        .into_iter()
        .map(escape_systemd_argument)
        .collect::<Result<Vec<_>, _>>()?
        .join(" ");
    Ok(format!(
        "{marker}\n\
[Unit]\n\
Description=Eliot per-user daemon\n\
StartLimitIntervalSec={START_LIMIT_INTERVAL_SECONDS}s\n\
StartLimitBurst={START_LIMIT_BURST}\n\
\n\
[Service]\n\
Type=simple\n\
ExecStart=:{command}\n\
Restart=on-failure\n\
RestartSec={RESTART_DELAY_SECONDS}s\n\
\n\
[Install]\n\
WantedBy=default.target\n"
    ))
}

fn escape_systemd_argument(argument: &str) -> Result<String, Failure> {
    if argument.chars().any(char::is_control) {
        return Err(Failure::new(
            2,
            "systemd command arguments may not contain control characters",
        ));
    }
    let mut escaped = String::from("\"");
    for character in argument.chars() {
        match character {
            '\\' => escaped.push_str("\\\\"),
            '"' => escaped.push_str("\\\""),
            '%' => escaped.push_str("%%"),
            other => escaped.push(other),
        }
    }
    escaped.push('"');
    Ok(escaped)
}

fn read_owned_unit(path: &Path, marker: &str) -> Result<Option<String>, Failure> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(io_failure("inspect the systemd user unit", error)),
    };
    if !metadata.file_type().is_file() {
        return Err(Failure::new(
            1,
            "the deterministic systemd unit path is not a regular file; it was not changed",
        ));
    }
    let contents = fs::read_to_string(path)
        .map_err(|error| io_failure("read the systemd user unit", error))?;
    if contents.lines().next() != Some(marker) {
        return Err(Failure::new(
            1,
            "the deterministic systemd unit name is occupied by a different unit; it was not changed",
        ));
    }
    Ok(Some(contents))
}

fn write_unit_atomically(path: &Path, contents: &str) -> Result<(), Failure> {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| Failure::new(1, "systemd unit filename is invalid"))?;
    let temporary = path.with_file_name(format!(".{file_name}.tmp-{}", ulid::Ulid::new()));
    let mut options = OpenOptions::new();
    options.write(true).create_new(true).mode(0o600);
    let mut file = options
        .open(&temporary)
        .map_err(|error| io_failure("create a temporary systemd user unit", error))?;
    if let Err(error) = file
        .write_all(contents.as_bytes())
        .and_then(|()| file.sync_all())
    {
        let _ = fs::remove_file(&temporary);
        return Err(io_failure("write the systemd user unit", error));
    }
    if let Err(error) = fs::set_permissions(&temporary, fs::Permissions::from_mode(0o600)) {
        let _ = fs::remove_file(&temporary);
        return Err(io_failure("secure the systemd user unit", error));
    }
    if let Err(error) = fs::rename(&temporary, path) {
        let _ = fs::remove_file(&temporary);
        return Err(io_failure("install the systemd user unit", error));
    }
    Ok(())
}

async fn run_systemctl(arguments: &[&str]) -> Result<(), Failure> {
    #[expect(
        clippy::disallowed_methods,
        reason = "This statement constructs a process owned by the Linux autostart module."
    )]
    #[expect(
        clippy::disallowed_types,
        reason = "This invokes the current user's systemd manager for autostart."
    )]
    let mut command = Command::new("systemctl");
    command
        .args(arguments)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true);
    let output = timeout(SYSTEMCTL_TIMEOUT, command.output())
        .await
        .map_err(|_| {
            Failure::new(
                1,
                "systemd user manager did not finish within 30 seconds; unit state may be unknown",
            )
        })?
        .map_err(|error| io_failure("start systemctl for the current user", error))?;
    if output.stdout.len() > MAX_OUTPUT_BYTES || output.stderr.len() > MAX_OUTPUT_BYTES {
        return Err(Failure::new(
            1,
            "systemd user manager returned oversized output; unit state may be unknown",
        ));
    }
    if !output.status.success() {
        let detail = safe_output(&output.stderr);
        return Err(Failure::new(
            1,
            if detail.is_empty() {
                "systemd user manager rejected the requested operation".to_owned()
            } else {
                format!("systemd user manager rejected the requested operation: {detail}")
            },
        ));
    }
    Ok(())
}

fn report_plan(plan: &UnitPlan, operation: &str, would: &str, json_output: bool) {
    let commands = match operation {
        "install" => vec![
            "systemctl --user daemon-reload".to_owned(),
            format!("systemctl --user enable {}", plan.unit),
        ],
        _ if would == "remove" => vec![
            format!("systemctl --user disable {}", plan.unit),
            format!("remove {}", plan.unit_file.display()),
            "systemctl --user daemon-reload".to_owned(),
        ],
        _ => Vec::new(),
    };
    let mut response = json!({
        "state": "dry_run",
        "operation": operation,
        "would": would,
        "platform": "systemd_user",
        "unit": plan.unit,
        "unit_file": plan.unit_file,
        "trigger": "user_default_target",
        "restart_on_failure": restart_policy(),
        "ownership_marker": plan.marker,
        "commands": commands,
        "os_mutations": false,
    });
    if operation == "install" {
        response["unit_contents"] = Value::String(plan.contents.clone());
        response["data_dir"] = json!(plan.data_dir);
        response["executable"] = json!(plan.executable);
    }
    report_result(&response, json_output);
}

fn report_result(result: &Value, json_output: bool) {
    if json_output {
        print_value(result, true);
        return;
    }
    let state = result
        .get("state")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let unit = result
        .get("unit")
        .and_then(Value::as_str)
        .unwrap_or("<unknown>");
    let unit_file = result
        .get("unit_file")
        .and_then(Value::as_str)
        .unwrap_or("<unknown>");
    match state {
        "dry_run" => {
            let operation = result
                .get("operation")
                .and_then(Value::as_str)
                .unwrap_or("autostart");
            let would = result
                .get("would")
                .and_then(Value::as_str)
                .unwrap_or("review");
            println!(
                "dry run: would {operation} systemd user unit {unit} ({would}); no OS mutations performed"
            );
            println!("Unit file: {unit_file}");
            if let Some(contents) = result.get("unit_contents").and_then(Value::as_str) {
                println!("Unit contents:\n{contents}");
            }
            if let Some(commands) = result.get("commands").and_then(Value::as_array) {
                for command in commands.iter().filter_map(Value::as_str) {
                    println!("Would run: {command}");
                }
            }
        }
        "installed" => println!("installed and enabled systemd user unit {unit} ({unit_file})"),
        "updated" => {
            println!("updated and enabled verified systemd user unit {unit} ({unit_file})")
        }
        "already_installed" => println!("systemd user unit already installed and enabled: {unit}"),
        "removed" => println!("removed verified systemd user unit {unit}"),
        "not_installed" => println!("no systemd user unit is installed for this data directory"),
        _ => println!("systemd user autostart returned state {state} for {unit}"),
    }
}

fn restart_policy() -> Value {
    json!({
        "delay_seconds": RESTART_DELAY_SECONDS,
        "start_limit_interval_seconds": START_LIMIT_INTERVAL_SECONDS,
        "start_limit_burst": START_LIMIT_BURST,
        "mode": "on-failure",
    })
}

fn data_dir_failure(error: eliot_core::Error) -> Failure {
    Failure::new(
        1,
        format!(
            "cannot validate the private data directory for autostart: {} ({})",
            error.message, error.code
        ),
    )
}

fn safe_output(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes)
        .chars()
        .map(|character| {
            if character.is_control() {
                ' '
            } else {
                character
            }
        })
        .take(400)
        .collect::<String>()
        .trim()
        .to_owned()
}

fn io_failure(action: &str, error: std::io::Error) -> Failure {
    Failure::new(1, format!("{action}: {error}"))
}
