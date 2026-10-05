use std::path::PathBuf;

fn main() {
    let mut args = std::env::args_os().skip(1);
    let file = match (args.next(), args.next(), args.next()) {
        (Some(flag), Some(path), None) if flag == "--file" => PathBuf::from(path),
        _ => {
            eprintln!("script worker requires exactly --file <receipt>");
            std::process::exit(2);
        }
    };
    if let Err(error) = swarm_script_worker::run_worker(&file) {
        eprintln!("{}", serde_json::json!({"error":error}));
        std::process::exit(1);
    }
}
