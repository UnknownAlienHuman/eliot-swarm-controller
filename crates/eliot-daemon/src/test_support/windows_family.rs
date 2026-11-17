//! Real OS descendants for Windows custody regressions, not a vendor harness.
use crate::platform::windows::recorded_process_alive;
use std::{path::PathBuf, time::Duration};
use tokio::{
    process::Command,
    time::{Instant, timeout},
};

pub(crate) struct Family {
    pub root: PathBuf,
    _temp: super::TempDir,
}

pub(crate) struct Owner(pub std::process::Child);

impl Drop for Owner {
    fn drop(&mut self) {
        if !matches!(self.0.try_wait(), Ok(Some(_))) {
            let _ = self.0.kill();
        }
        let _ = self.0.wait();
    }
}

impl Family {
    pub fn new() -> Self {
        let root = std::env::current_dir()
            .unwrap()
            .join("target")
            .join(format!("family-{}", ulid::Ulid::new()));
        std::fs::create_dir_all(&root).unwrap();
        let family = Self {
            _temp: super::TempDir::own(root.clone()),
            root,
        };
        std::fs::write(family.root.join("family.ps1"), SCRIPT).unwrap();
        family
    }

    pub fn at(root: PathBuf) -> Self {
        Self {
            _temp: super::TempDir::own(root.clone()),
            root,
        }
    }

    pub fn command(&self, mode: &str) -> Command {
        let mut command = Command::new(
            std::path::Path::new(&std::env::var_os("SystemRoot").unwrap())
                .join("System32/WindowsPowerShell/v1.0/powershell.exe"),
        );
        command
            .args([
                "-NoProfile",
                "-NonInteractive",
                "-ExecutionPolicy",
                "Bypass",
                "-File",
            ])
            .arg(self.root.join("family.ps1"))
            .arg(&self.root)
            .arg("root")
            .arg(mode);
        command
    }

    pub async fn ready(&self) -> Vec<(u32, u64)> {
        timeout(Duration::from_secs(25), async {
            loop {
                let identities: Option<Vec<_>> = ["root", "child", "grandchild"]
                    .iter()
                    .map(|role| {
                        let text =
                            std::fs::read_to_string(self.root.join(format!("{role}.pid"))).ok()?;
                        let (pid, birth) = text.split_once(':')?;
                        Some((pid.parse().ok()?, birth.parse().ok()?))
                    })
                    .collect();
                if let Some(identities) = identities {
                    return identities;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("three-generation family must start")
    }

    pub fn release(&self) {
        std::fs::write(self.root.join("release"), []).unwrap();
    }

    pub async fn assert_closed(&self, identities: &[(u32, u64)]) {
        timeout(Duration::from_secs(5), async {
            loop {
                if identities
                    .iter()
                    .all(|(pid, birth)| !recorded_process_alive(*pid, *birth).unwrap())
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("all owned family identities must be gone");
        // Immediate removal proves no surviving process has the private cwd open.
        std::fs::remove_dir_all(&self.root).unwrap();
    }

    pub async fn root_exited(&self, root: (u32, u64)) {
        let until = Instant::now() + Duration::from_secs(5);
        while recorded_process_alive(root.0, root.1).unwrap() {
            assert!(Instant::now() < until, "root must exit before grandchild");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

// Every process uses CreateProcess through .NET, without shell/service or
// breakaway. Descendants close inherited stdio; only the private cwd stays held.
const SCRIPT: &str = r#"
param($Root, $Role, $Mode)
$ErrorActionPreference = 'Stop'
Set-Location -LiteralPath $Root
$birth = ([Diagnostics.Process]::GetCurrentProcess()).StartTime.ToUniversalTime().ToFileTimeUtc()
[IO.File]::WriteAllText((Join-Path $Root ($Role + '.pid')), ([string]$PID + ':' + [string]$birth))
if ($Role -eq 'grandchild') {
    $until = [DateTime]::UtcNow.AddSeconds(60)
    while (!(Test-Path -LiteralPath (Join-Path $Root 'release')) -and [DateTime]::UtcNow -lt $until) { Start-Sleep -Milliseconds 10 }
    exit 0
}
$next = if ($Role -eq 'root') { 'child' } else { 'grandchild' }
$info = New-Object Diagnostics.ProcessStartInfo
$info.FileName = Join-Path $PSHOME 'powershell.exe'
$info.Arguments = '-NoProfile -NonInteractive -ExecutionPolicy Bypass -File "' + $PSCommandPath + '" "' + $Root + '" ' + $next + ' ' + $Mode
$info.UseShellExecute = $false
$info.CreateNoWindow = $true
$info.RedirectStandardInput = $true
$info.RedirectStandardOutput = $true
$info.RedirectStandardError = $true
$descendant = [Diagnostics.Process]::Start($info)
if ($Role -eq 'child') { exit 0 }
$until = [DateTime]::UtcNow.AddSeconds(25)
while (!(Test-Path -LiteralPath (Join-Path $Root 'grandchild.pid'))) {
    if ([DateTime]::UtcNow -ge $until) { exit 2 }
    Start-Sleep -Milliseconds 10
}
if ($Mode -eq 'read') { [Console]::Out.WriteLine('catalogue'); exit 0 }
if ($Mode -eq 'probe') {
    $null = [Console]::ReadLine()
    [Console]::Out.WriteLine('{"jsonrpc":"2.0","id":"muse-bootstrap-1","result":{}}')
    $null = [Console]::ReadLine()
    $null = [Console]::ReadLine()
    [Console]::Out.WriteLine('{"jsonrpc":"2.0","id":"muse-bootstrap-2","result":{"models":[{"id":"metadata"}]}}')
    exit 0
}
if ($Mode -eq 'reader') {
    $request = [Console]::ReadLine() | ConvertFrom-Json
    [Console]::Out.WriteLine(('{"jsonrpc":"2.0","id":' + ($request.id | ConvertTo-Json -Compress) + ',"result":{}}'))
    exit 0
}
Start-Sleep -Seconds 60
"#;
