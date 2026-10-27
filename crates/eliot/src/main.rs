use std::process::ExitCode;

fn main() -> ExitCode {
    let mut args = std::env::args_os().skip(1);
    let argument = args.next();
    if args.next().is_some() {
        eprintln!("error: expected at most one option; use --help");
        return ExitCode::from(2);
    }

    match argument.as_deref() {
        None => print_help(),
        Some(arg) if arg == "--help" || arg == "-h" => print_help(),
        Some(arg) if arg == "--version" || arg == "-V" => {
            println!("eliot {}", env!("CARGO_PKG_VERSION"));
        }
        Some(arg) => {
            eprintln!("error: unsupported argument {arg:?}; use --help");
            return ExitCode::from(2);
        }
    }
    ExitCode::SUCCESS
}

fn print_help() {
    println!(
        "Eliot v2 development skeleton\n\n\
         Usage: eliot [--version | --help]\n\n\
         Agent control is not implemented yet. Next: docs/roadmap.md, L1.1."
    );
}
