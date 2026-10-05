use serde_json::json;
#[cfg(target_os = "linux")]
use std::time::Instant;
use std::{
    fs,
    path::{Path, PathBuf},
    process::{Command, Stdio},
};

const MAGIC: &[u8; 8] = b"SWPRB01\0";

struct Response {
    success: bool,
    timed_out: bool,
    output_limited: bool,
    group_empty: bool,
    exit_code: Option<i32>,
    message: String,
    stdout: Vec<u8>,
}

fn decode(bytes: &[u8]) -> Response {
    assert!(bytes.len() >= 23);
    assert_eq!(&bytes[..8], MAGIC);
    let flags = bytes[8];
    let exit_code = i32::from_le_bytes(bytes[9..13].try_into().unwrap());
    let stdout_len = u32::from_le_bytes(bytes[13..17].try_into().unwrap()) as usize;
    let stderr_len = u32::from_le_bytes(bytes[17..21].try_into().unwrap()) as usize;
    let message_len = u16::from_le_bytes(bytes[21..23].try_into().unwrap()) as usize;
    let stdout_start = 23;
    let stderr_start = stdout_start + stdout_len;
    let message_start = stderr_start + stderr_len;
    assert_eq!(message_start + message_len, bytes.len());
    Response {
        success: flags & 1 != 0,
        timed_out: flags & 2 != 0,
        output_limited: flags & 4 != 0,
        group_empty: flags & 8 != 0,
        exit_code: (exit_code >= 0).then_some(exit_code),
        message: String::from_utf8_lossy(&bytes[message_start..]).into_owned(),
        stdout: bytes[stdout_start..stderr_start].to_vec(),
    }
}

fn assert_probe_success(response: &Response, expected_stdout_prefix: &[u8], stdout_limit: usize) {
    assert!(
        response.success,
        "probe response: success={} timed_out={} output_limited={} group_empty={} exit_code={:?} message={:?} stdout_len={}",
        response.success,
        response.timed_out,
        response.output_limited,
        response.group_empty,
        response.exit_code,
        response.message,
        response.stdout.len(),
    );
    assert!(
        response.stdout.len() <= stdout_limit,
        "probe stdout exceeded its declared cap: stdout_len={} cap={stdout_limit}",
        response.stdout.len()
    );
    assert!(
        response.stdout.starts_with(expected_stdout_prefix),
        "probe stdout omitted expected content: stdout_len={} expected_prefix_len={}",
        response.stdout.len(),
        expected_stdout_prefix.len()
    );
}

fn response_summary(response: &Response) -> String {
    format!(
        "success={} timed_out={} output_limited={} group_empty={} exit_code={:?} message={:?} stdout_len={}",
        response.success,
        response.timed_out,
        response.output_limited,
        response.group_empty,
        response.exit_code,
        response.message,
        response.stdout.len(),
    )
}

fn run_probe(program: &Path, args: &[&str], timeout_ms: u64, stdout_limit: usize) -> Response {
    let directory =
        std::env::temp_dir().join(format!("swarm-check-probe-test-{}", uuid::Uuid::new_v4()));
    fs::create_dir(&directory).unwrap();
    let request_path = directory.join("request.json");
    let request = json!({
        "check_probe": {
            "probe_version":1,
            "program":program,
            "args":args,
            "cwd":std::env::current_dir().unwrap(),
            "timeout_ms":timeout_ms,
            "stdout_limit":stdout_limit,
            "stderr_limit":64*1024,
        }
    });
    fs::write(&request_path, serde_json::to_vec(&request).unwrap()).unwrap();
    let mut helper = Command::new(env!("CARGO_BIN_EXE_swarm"));
    helper
        .arg("check-worker")
        .arg("--file")
        .arg(&request_path)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        // Match the production input resolver's windowless helper. A console
        // allocated for the fixture is not part of the command being probed.
        helper.creation_flags(0x08000000);
    }
    let output = helper.spawn().unwrap().wait_with_output().unwrap();
    let _ = fs::remove_file(&request_path);
    let _ = fs::remove_dir(&directory);
    assert!(
        output.status.success(),
        "probe helper failed: status={:?} stdout_len={} stderr_len={}",
        output.status.code(),
        output.stdout.len(),
        output.stderr.len(),
    );
    let payload_kind = if output.stdout.is_empty() {
        "empty"
    } else if output.stdout.starts_with(b"SWPRB01\0") {
        "probe_frame"
    } else {
        "unknown"
    };
    assert!(
        output.stdout.len() >= 23,
        "probe helper returned no complete frame: status={:?} success={} stdout_len={} stderr_len={} payload_kind={}",
        output.status.code(),
        output.status.success(),
        output.stdout.len(),
        output.stderr.len(),
        payload_kind,
    );
    decode(&output.stdout)
}

#[cfg(target_os = "linux")]
fn shell() -> PathBuf {
    PathBuf::from("/bin/sh")
}

#[test]
fn owned_probe_bounds_output_deadline_and_descendant_lifetime() {
    #[cfg(target_os = "linux")]
    {
        let program = shell();
        let success = run_probe(&program, &["-c", "printf probe-ok"], 5_000, 1024);
        assert_probe_success(&success, b"probe-ok", 1024);
        assert!(
            success.group_empty && success.stdout == b"probe-ok",
            "success response: {}",
            response_summary(&success)
        );

        let overflow = run_probe(&program, &["-c", "yes x"], 5_000, 1024);
        assert!(
            !overflow.success && overflow.output_limited && overflow.group_empty,
            "overflow response: {}",
            response_summary(&overflow)
        );

        let timeout = run_probe(&program, &["-c", "sleep 30 & wait"], 100, 1024);
        assert!(
            !timeout.success && timeout.timed_out && timeout.group_empty,
            "timeout response: {}",
            response_summary(&timeout)
        );

        let started = Instant::now();
        let orphan = run_probe(&program, &["-c", "sleep 30 & exit 0"], 5_000, 1024);
        assert!(
            !orphan.success && orphan.group_empty && orphan.message.contains("descendants"),
            "orphan response: {}",
            response_summary(&orphan)
        );
        assert!(
            started.elapsed().as_secs() < 10,
            "orphan cleanup waited for pipe EOF; {}",
            response_summary(&orphan)
        );
    }

    #[cfg(windows)]
    {
        // Use the already-built controller CLI as a deterministic leaf. A
        // system shell can have environment-specific Job descendants even
        // when its command body is only an internal echo.
        let leaf = PathBuf::from(env!("CARGO_BIN_EXE_swarm"));
        let success = run_probe(&leaf, &["--version"], 5_000, 1024);
        assert_probe_success(&success, b"swarm ", 1024);
        assert!(
            success.group_empty,
            "success response: {}",
            response_summary(&success)
        );

        // The already-built CLI emits over 1 KiB for --help. This keeps the
        // overflow probe inside a deterministic native leaf and avoids a
        // cold PowerShell startup consuming the five-second probe deadline.
        let overflow = run_probe(&leaf, &["--help"], 5_000, 1024);
        assert!(
            !overflow.success && overflow.output_limited && overflow.group_empty,
            "overflow response: {}",
            response_summary(&overflow)
        );

        let powershell = PathBuf::from(std::env::var_os("SystemRoot").unwrap())
            .join("System32\\WindowsPowerShell\\v1.0\\powershell.exe");
        if powershell.is_file() {
            let timeout = run_probe(
                &powershell,
                &["-NoProfile", "-Command", "Start-Sleep -Seconds 30"],
                100,
                1024,
            );
            assert!(
                !timeout.success && timeout.timed_out && timeout.group_empty,
                "timeout response: {}",
                response_summary(&timeout)
            );

            let lingering_child = run_probe(
                &powershell,
                &[
                    "-NoProfile",
                    "-Command",
                    "$null = Start-Process -FilePath $env:ComSpec -ArgumentList '/c','ping -n 30 127.0.0.1' -NoNewWindow -PassThru; exit 0",
                ],
                5_000,
                64 * 1024,
            );
            assert!(
                !lingering_child.success
                    && lingering_child.group_empty
                    && !lingering_child.output_limited
                    && lingering_child.message.contains("descendants"),
                "orphan response: {}",
                response_summary(&lingering_child)
            );
        }
    }
}
