use swarm_contracts::error::Error;

#[tokio::main]
async fn main() {
    if let Err(error) = swarm_adapter_command::run().await {
        // Keep credentials, prompt text, filesystem paths, and native stderr
        // out of the process-level diagnostic channel.
        eprintln!("component=swarm-adapter-command code={}", safe_code(&error));
        std::process::exit(1);
    }
}

fn safe_code(error: &Error) -> &str {
    if error.code.is_empty()
        || !error
            .code
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
    {
        "ADAPTER_ERROR"
    } else {
        &error.code
    }
}
