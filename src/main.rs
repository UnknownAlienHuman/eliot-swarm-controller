use std::process::{Command, ExitCode};

fn main() -> ExitCode {
    let mut executable = match std::env::current_exe() {
        Ok(path) => path,
        Err(_) => {
            eprintln!(
                "{{\"error\":{{\"code\":\"KERNEL_HOST_PATH_FAILED\",\"message\":\"could not locate the public host executable\"}}}}"
            );
            return ExitCode::FAILURE;
        }
    };
    executable.set_file_name(if cfg!(windows) {
        "swarm-kernel-host.exe"
    } else {
        "swarm-kernel-host"
    });
    let sibling_is_file = executable.is_file();
    match Command::new(&executable)
        .args(std::env::args_os().skip(1))
        .status()
    {
        Ok(status) if status.success() => ExitCode::SUCCESS,
        Ok(status) => {
            let exit_code = status
                .code()
                .map_or_else(|| "null".to_owned(), |code| code.to_string());
            eprintln!(
                "{{\"error\":{{\"code\":\"KERNEL_HOST_COMMAND_FAILED\",\"message\":\"the relocated kernel host exited unsuccessfully\",\"exit_code\":{exit_code}}}}}"
            );
            ExitCode::from(
                status
                    .code()
                    .and_then(|code| u8::try_from(code).ok())
                    .filter(|code| *code != 0)
                    .unwrap_or(1),
            )
        }
        Err(_) => {
            let code = if sibling_is_file {
                "KERNEL_HOST_START_FAILED"
            } else {
                "KERNEL_HOST_BINARY_MISSING"
            };
            eprintln!(
                "{{\"error\":{{\"code\":\"{code}\",\"message\":\"install the swarm-kernel-host sibling beside swarm-host and retry\"}}}}}"
            );
            ExitCode::FAILURE
        }
    }
}
