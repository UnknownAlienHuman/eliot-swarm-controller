use super::super::support::{Failure, print_value};
use eliot_daemon::DataDir;
use serde_json::Value;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::process::Command;
use tokio::time::timeout;

const POWERSHELL_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_OUTPUT_BYTES: usize = 16 * 1024;
const RESTART_COUNT: u32 = 3;

// Fixed code only. Canonical paths and the already-quoted action arguments enter
// through the child environment, never through script interpolation.
const TASK_SCHEDULER_SCRIPT: &str = r##"
$ErrorActionPreference = 'Stop'

function Test-TaskUser($UserId, $Sid) {
    if ([string]::Equals([string]$UserId, $Sid, [StringComparison]::OrdinalIgnoreCase)) { return $true }
    if ([string]::IsNullOrWhiteSpace($UserId)) { return $false }
    try {
        $account = [Security.Principal.NTAccount]::new([string]$UserId)
        return $account.Translate([Security.Principal.SecurityIdentifier]).Value -eq $Sid
    } catch {
        return $false
    }
}

function Test-ExpectedTask($Task, $TaskName, $Description, $Sid, $Executable, $Arguments) {
    if ($null -eq $Task -or -not $Task.Enabled) { return $false }
    $definition = $Task.Definition
    if (-not [string]::Equals([string]$Task.Name, $TaskName, [StringComparison]::Ordinal)) { return $false }
    if (-not [string]::Equals([string]$definition.RegistrationInfo.Description, $Description, [StringComparison]::Ordinal)) { return $false }
    if ($definition.Actions.Count -ne 1 -or $definition.Triggers.Count -ne 1) { return $false }

    $action = $definition.Actions.Item(1)
    if ([int]$action.Type -ne 0) { return $false }
    if (-not [string]::Equals([string]$action.Path, $Executable, [StringComparison]::OrdinalIgnoreCase)) { return $false }
    if (-not [string]::Equals([string]$action.Arguments, $Arguments, [StringComparison]::Ordinal)) { return $false }

    $trigger = $definition.Triggers.Item(1)
    if ([int]$trigger.Type -ne 9 -or -not $trigger.Enabled) { return $false }
    if (-not (Test-TaskUser $trigger.UserId $Sid)) { return $false }

    $principal = $definition.Principal
    if (-not (Test-TaskUser $principal.UserId $Sid)) { return $false }
    if ([int]$principal.LogonType -ne 3 -or [int]$principal.RunLevel -ne 0) { return $false }

    $settings = $definition.Settings
    if (-not $settings.Enabled -or -not $settings.StartWhenAvailable) { return $false }
    if ($settings.DisallowStartIfOnBatteries -or $settings.StopIfGoingOnBatteries) { return $false }
    if ([int]$settings.MultipleInstances -ne 2) { return $false }
    if ([int]$settings.RestartCount -ne 3 -or [string]$settings.RestartInterval -ne 'PT1M') { return $false }
    if ([string]$settings.ExecutionTimeLimit -ne 'PT0S') { return $false }
    return $true
}

function Write-Result($State, $Name) {
    [Console]::Out.WriteLine((ConvertTo-Json -Compress -InputObject @{
        state = $State
        task = $Name
        scope = 'current_user'
        trigger = 'logon'
        restart_on_failure = @{ interval_seconds = 60; max_restarts = 3 }
    }))
}

try {
    $root = $env:SWARM_AUTOSTART_DATA_DIR
    $executable = $env:SWARM_AUTOSTART_EXECUTABLE
    $arguments = $env:SWARM_AUTOSTART_ARGUMENTS
    if ([string]::IsNullOrWhiteSpace($root) -or -not [IO.Path]::IsPathRooted($root)) { throw 'canonical data directory is missing' }
    if ([string]::IsNullOrWhiteSpace($executable) -or -not [IO.Path]::IsPathRooted($executable)) { throw 'canonical executable is missing' }
    if ($null -eq $arguments) { throw 'daemon arguments are missing' }

    $sid = [Security.Principal.WindowsIdentity]::GetCurrent().User.Value
    $identityPath = $root.TrimEnd([char[]]@('\', '/')).ToUpperInvariant()
    $identity = $sid.ToUpperInvariant() + "`n" + $identityPath
    $hasher = [Security.Cryptography.SHA256]::Create()
    try {
        $digest = $hasher.ComputeHash([Text.Encoding]::UTF8.GetBytes($identity))
    } finally {
        $hasher.Dispose()
    }
    $hash = [BitConverter]::ToString($digest).Replace('-', '')
    $taskName = 'Eliot Swarm Daemon ' + $hash
    $description = 'Eliot per-user daemon autostart v1:' + $hash

    $service = New-Object -ComObject 'Schedule.Service'
    $null = $service.Connect()
    $folder = $service.GetFolder('\')
    $task = $null
    try {
        $task = $folder.GetTask($taskName)
    } catch {
        $missing = $_.Exception.HResult -eq -2147024894 -or $_.Exception.HResult -eq -2147216625
        if (-not $missing) { throw }
    }

    if ($env:SWARM_AUTOSTART_OPERATION -eq 'install') {
        if ($null -ne $task) {
            if (-not (Test-ExpectedTask $task $taskName $description $sid $executable $arguments)) {
                throw 'the deterministic task name is occupied by a different task; it was not changed'
            }
            Write-Result 'already_installed' $taskName
            exit 0
        }

        $definition = $service.NewTask(0)
        $definition.RegistrationInfo.Author = 'Eliot'
        $definition.RegistrationInfo.Description = $description
        $definition.Principal.UserId = $sid
        $definition.Principal.LogonType = 3
        $definition.Principal.RunLevel = 0

        $trigger = $definition.Triggers.Create(9)
        $trigger.Enabled = $true
        $trigger.UserId = $sid

        $settings = $definition.Settings
        $settings.Enabled = $true
        $settings.StartWhenAvailable = $true
        $settings.DisallowStartIfOnBatteries = $false
        $settings.StopIfGoingOnBatteries = $false
        $settings.MultipleInstances = 2
        $settings.RestartCount = 3
        $settings.RestartInterval = 'PT1M'
        $settings.ExecutionTimeLimit = 'PT0S'

        $action = $definition.Actions.Create(0)
        $action.Path = $executable
        $action.Arguments = $arguments

        # TASK_CREATE prevents replacing a task that appeared after the readback.
        $null = $folder.RegisterTaskDefinition($taskName, $definition, 2, $sid, $null, 3, $null)
        $task = $folder.GetTask($taskName)
        if (-not (Test-ExpectedTask $task $taskName $description $sid $executable $arguments)) {
            throw 'Task Scheduler did not retain the exact requested task definition'
        }
        Write-Result 'installed' $taskName
        exit 0
    }

    if ($env:SWARM_AUTOSTART_OPERATION -eq 'uninstall') {
        if ($null -eq $task) {
            Write-Result 'not_installed' $taskName
            exit 0
        }
        if (-not (Test-ExpectedTask $task $taskName $description $sid $executable $arguments)) {
            throw 'the registered task has changed; refusing to delete it'
        }
        $null = $folder.DeleteTask($taskName, 0)
        Write-Result 'removed' $taskName
        exit 0
    }

    throw 'unknown autostart operation'
} catch {
    [Console]::Error.WriteLine($_.Exception.Message)
    exit 1
}
"##;

pub(super) async fn install(data_dir: &Path, json_output: bool) -> Result<u8, Failure> {
    let (root, executable, arguments) = canonical_action(data_dir)?;
    let result = run_task_scheduler("install", &root, &executable, &arguments).await?;
    report_result(&result, json_output);
    Ok(0)
}

pub(super) async fn uninstall(data_dir: &Path, json_output: bool) -> Result<u8, Failure> {
    match fs::symlink_metadata(data_dir) {
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            report_not_installed(json_output);
            return Ok(0);
        }
        Err(error) => return Err(io_failure("inspect the private data directory", error)),
    }
    let private_dir = DataDir::open_existing(data_dir).map_err(|error| {
        Failure::new(
            1,
            format!(
                "cannot validate the private data directory for autostart: {} ({})",
                error.message, error.code
            ),
        )
    })?;
    let root = canonical_path(private_dir.path(), "data directory")?;
    let executable = canonical_path(
        &std::env::current_exe().map_err(|error| io_failure("locate Swarm executable", error))?,
        "Swarm executable",
    )?;
    let arguments = daemon_arguments(&root)?;
    let result = run_task_scheduler("uninstall", &root, &executable, &arguments).await?;
    report_result(&result, json_output);
    Ok(0)
}

fn canonical_action(data_dir: &Path) -> Result<(PathBuf, PathBuf, String), Failure> {
    let private_dir = DataDir::open(data_dir).map_err(|error| {
        Failure::new(
            1,
            format!(
                "cannot open the private data directory for autostart: {} ({})",
                error.message, error.code
            ),
        )
    })?;
    let root = canonical_path(private_dir.path(), "data directory")?;
    let current_executable =
        std::env::current_exe().map_err(|error| io_failure("locate Swarm executable", error))?;
    let executable = canonical_path(&current_executable, "Swarm executable")?;
    if !executable.is_file() {
        return Err(Failure::new(
            1,
            "the current Swarm executable is unavailable",
        ));
    }
    let arguments = daemon_arguments(&root)?;
    Ok((root, executable, arguments))
}

fn canonical_path(path: &Path, label: &str) -> Result<PathBuf, Failure> {
    let canonical = fs::canonicalize(path)
        .map_err(|error| io_failure(&format!("canonicalize {label}"), error))?;
    if !canonical.is_absolute() {
        return Err(Failure::new(2, format!("{label} must be an absolute path")));
    }
    let text = canonical.to_str().ok_or_else(|| {
        Failure::new(
            2,
            format!("{label} cannot be represented losslessly for Task Scheduler"),
        )
    })?;
    if text.chars().any(char::is_control) {
        return Err(Failure::new(
            2,
            format!("{label} contains unsupported control characters"),
        ));
    }
    if text.contains('%') {
        return Err(Failure::new(
            2,
            format!("{label} contains a percent sign that Task Scheduler could expand"),
        ));
    }
    Ok(canonical)
}

fn daemon_arguments(root: &Path) -> Result<String, Failure> {
    let root = root.to_str().ok_or_else(|| {
        Failure::new(
            2,
            "data directory cannot be represented losslessly for Task Scheduler",
        )
    })?;
    let arguments = ["--data-dir", root, "daemon", "--foreground"]
        .iter()
        .map(|argument| quote_windows_argument(argument))
        .collect::<Vec<_>>()
        .join(" ");
    if arguments.encode_utf16().count() >= 32_767 {
        return Err(Failure::new(
            2,
            "daemon arguments exceed the Windows process limit",
        ));
    }
    Ok(arguments)
}

fn quote_windows_argument(argument: &str) -> String {
    let mut output = String::from("\"");
    let mut backslashes = 0usize;
    for character in argument.chars() {
        if character == '\\' {
            backslashes += 1;
            continue;
        }
        if character == '"' {
            output.extend(std::iter::repeat_n('\\', backslashes * 2 + 1));
            output.push('"');
        } else {
            output.extend(std::iter::repeat_n('\\', backslashes));
            output.push(character);
        }
        backslashes = 0;
    }
    output.extend(std::iter::repeat_n('\\', backslashes * 2));
    output.push('"');
    output
}

async fn run_task_scheduler(
    operation: &str,
    root: &Path,
    executable: &Path,
    arguments: &str,
) -> Result<Value, Failure> {
    let powershell = powershell_executable()?;
    let mut command = Command::new(powershell);
    command
        .args([
            "-NoLogo",
            "-NoProfile",
            "-NonInteractive",
            "-Command",
            TASK_SCHEDULER_SCRIPT,
        ])
        .env("SWARM_AUTOSTART_OPERATION", operation)
        .env("SWARM_AUTOSTART_DATA_DIR", root)
        .env("SWARM_AUTOSTART_EXECUTABLE", executable)
        .env("SWARM_AUTOSTART_ARGUMENTS", arguments)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true);
    let output = timeout(POWERSHELL_TIMEOUT, command.output())
        .await
        .map_err(|_| {
            Failure::new(
                1,
                "Windows Task Scheduler did not finish within 30 seconds; task state may be unknown",
            )
        })?
        .map_err(|error| io_failure("start the Windows Task Scheduler API", error))?;
    if output.stdout.len() > MAX_OUTPUT_BYTES || output.stderr.len() > MAX_OUTPUT_BYTES {
        return Err(Failure::new(
            1,
            "Windows Task Scheduler returned oversized output; task state may be unknown",
        ));
    }
    if !output.status.success() {
        let message = safe_output(&output.stderr);
        return Err(Failure::new(
            1,
            if message.is_empty() {
                "Windows Task Scheduler rejected the requested operation".to_owned()
            } else {
                format!("Windows Task Scheduler rejected the requested operation: {message}")
            },
        ));
    }
    serde_json::from_slice(&output.stdout)
        .map_err(|_| Failure::new(1, "Windows Task Scheduler returned an invalid result"))
}

fn powershell_executable() -> Result<PathBuf, Failure> {
    let system_root = std::env::var_os("SystemRoot")
        .or_else(|| std::env::var_os("WINDIR"))
        .ok_or_else(|| Failure::new(1, "Windows system directory is unavailable"))?;
    let executable = PathBuf::from(system_root)
        .join("System32")
        .join("WindowsPowerShell")
        .join("v1.0")
        .join("powershell.exe");
    if !executable.is_absolute() || !executable.is_file() {
        return Err(Failure::new(
            1,
            "Windows PowerShell is unavailable in the system directory",
        ));
    }
    Ok(executable)
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

fn report_result(result: &Value, json_output: bool) {
    if json_output {
        print_value(result, true);
        return;
    }
    let state = result
        .get("state")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let task = result
        .get("task")
        .and_then(Value::as_str)
        .unwrap_or("<unknown>");
    match state {
        "installed" => println!(
            "installed current-user logon task {task}; retries up to {RESTART_COUNT} failures at one-minute intervals"
        ),
        "already_installed" => {
            println!("daemon autostart already installed for the current user: {task}")
        }
        "removed" => println!("removed verified daemon autostart task {task}"),
        "not_installed" => {
            println!("no daemon autostart task is installed for this data directory")
        }
        _ => println!("Windows Task Scheduler returned state {state} for {task}"),
    }
}

fn report_not_installed(json_output: bool) {
    if json_output {
        print_value(&serde_json::json!({ "state": "not_installed" }), true);
    } else {
        println!("no daemon autostart task is installed for this data directory");
    }
}

fn io_failure(action: &str, error: std::io::Error) -> Failure {
    Failure::new(1, format!("{action}: {error}"))
}
