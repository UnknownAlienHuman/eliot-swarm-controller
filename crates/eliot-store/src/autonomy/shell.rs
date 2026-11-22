const CARGO_CHECKS: &[&str] = &["fmt", "build", "check", "clippy", "test"];
const MAX_WRAPPER_DEPTH: usize = 8;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Shell {
    Posix,
    PowerShell,
}

pub(super) fn matches(command: &str, project_check: Option<&[String]>, tool: &str) -> (u64, bool) {
    let mut checks = 0;
    let mut wait = false;
    visit(
        command,
        project_check,
        shell_style(command, tool),
        0,
        &mut checks,
        &mut wait,
    );
    (checks, wait)
}

fn visit(
    command: &str,
    project_check: Option<&[String]>,
    shell: Shell,
    depth: usize,
    checks: &mut u64,
    wait: &mut bool,
) {
    if depth > MAX_WRAPPER_DEPTH {
        return;
    }

    for words in command_segments(command, shell) {
        let words = normalize(&words);
        let Some((program, args)) = words.split_first() else {
            continue;
        };
        if let Some((inner, inner_shell)) = unwrap_shell(program, args) {
            visit(&inner, project_check, inner_shell, depth + 1, checks, wait);
            continue;
        }

        if is_check(program, args, project_check) {
            *checks = checks.saturating_add(1);
        }
        let program = executable_name(program);
        *wait |= program.eq_ignore_ascii_case("sleep")
            || program.eq_ignore_ascii_case("start-sleep")
            || (program.eq_ignore_ascii_case("timeout")
                && args
                    .first()
                    .is_some_and(|arg| arg.eq_ignore_ascii_case("/t")));
    }
}

fn shell_style(command: &str, tool: &str) -> Shell {
    if tool.eq_ignore_ascii_case("bash") {
        return Shell::Posix;
    }
    let lower = command.to_ascii_lowercase();
    if tool.eq_ignore_ascii_case("powershell")
        || command.contains('`')
        || [
            "powershell",
            "pwsh",
            "$env:",
            "$true",
            "$false",
            "$null",
            "start-sleep",
            "write-output",
            "write-host",
            "get-childitem",
            "set-location",
            "get-location",
            "test-path",
            "remove-item",
        ]
        .iter()
        .any(|marker| lower.contains(marker))
    {
        Shell::PowerShell
    } else {
        Shell::Posix
    }
}

fn command_segments(command: &str, shell: Shell) -> Vec<Vec<String>> {
    let mut segments = Vec::new();
    let mut words = Vec::new();
    let mut word = String::new();
    let mut started = false;
    let mut quote = None;
    let mut chars = command.chars().peekable();

    while let Some(ch) = chars.next() {
        if let Some(active_quote) = quote {
            if ch == active_quote {
                if active_quote == '\'' && shell == Shell::PowerShell && chars.peek() == Some(&'\'')
                {
                    chars.next();
                    word.push('\'');
                } else {
                    quote = None;
                }
            } else if ch == '`' && shell == Shell::PowerShell && active_quote == '"' {
                if let Some(escaped) = chars.next() {
                    word.push(escaped);
                } else {
                    word.push(ch);
                }
            } else if ch == '\\' && shell == Shell::Posix && active_quote == '"' {
                if chars
                    .peek()
                    .is_some_and(|next| matches!(next, '"' | '\\' | '$' | '`' | '\n'))
                {
                    let escaped = chars.next().unwrap();
                    if escaped != '\n' {
                        word.push(escaped);
                    }
                } else {
                    word.push(ch);
                }
            } else {
                word.push(ch);
            }
            continue;
        }

        match ch {
            // In a mixed or partial capture an unmatched intra-word
            // apostrophe is literal. Balanced quotes can join bare text.
            '\'' if !started || word.ends_with('=') || chars.clone().any(|next| next == '\'') => {
                quote = Some('\'');
                started = true;
            }
            '"' => {
                quote = Some('"');
                started = true;
            }
            '`' if shell == Shell::PowerShell => {
                if let Some(escaped) = chars.next() {
                    word.push(escaped);
                    started = true;
                } else {
                    word.push(ch);
                    started = true;
                }
            }
            '\\' if shell == Shell::Posix => {
                if chars.peek().is_some_and(|next| posix_escaped(*next)) {
                    let escaped = chars.next().unwrap();
                    if escaped != '\n' {
                        word.push(escaped);
                        started = true;
                    }
                } else {
                    word.push(ch);
                    started = true;
                }
            }
            ';' | '&' | '|' | '\n' | '(' | ')' | '{' | '}' => {
                push_word(&mut words, &mut word, &mut started);
                push_segment(&mut segments, &mut words);
            }
            ch if ch.is_ascii_whitespace() => {
                push_word(&mut words, &mut word, &mut started);
            }
            _ => {
                word.push(ch);
                started = true;
            }
        }
    }

    push_word(&mut words, &mut word, &mut started);
    push_segment(&mut segments, &mut words);
    segments
}

fn posix_escaped(ch: char) -> bool {
    ch.is_ascii_whitespace()
        || matches!(
            ch,
            ';' | '&' | '|' | '(' | ')' | '{' | '}' | '\'' | '"' | '\\' | '$' | '`'
        )
}

fn push_word(words: &mut Vec<String>, word: &mut String, started: &mut bool) {
    if *started {
        words.push(std::mem::take(word));
        *started = false;
    }
}

fn push_segment(segments: &mut Vec<Vec<String>>, words: &mut Vec<String>) {
    if !words.is_empty() {
        segments.push(std::mem::take(words));
    }
}

fn normalize(words: &[String]) -> &[String] {
    let first = words
        .iter()
        .position(|word| {
            !(word.eq_ignore_ascii_case("do")
                || word.eq_ignore_ascii_case("then")
                || is_assignment(word))
        })
        .unwrap_or(words.len());
    &words[first..]
}

fn is_assignment(word: &str) -> bool {
    let Some((name, _)) = word.split_once('=') else {
        return false;
    };
    let mut chars = name.chars();
    matches!(chars.next(), Some(ch) if ch == '_' || ch.is_ascii_alphabetic())
        && chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
}

fn unwrap_shell(program: &str, args: &[String]) -> Option<(String, Shell)> {
    let program = executable_name(program);
    if program.eq_ignore_ascii_case("bash") || program.eq_ignore_ascii_case("sh") {
        for (index, arg) in args.iter().enumerate() {
            if !arg.starts_with('-') || arg == "--" {
                break;
            }
            let is_command_flag = arg == "-c"
                || (arg.starts_with('-') && !arg.starts_with("--") && arg[1..].contains('c'));
            if is_command_flag {
                return args
                    .get(index + 1)
                    .cloned()
                    .map(|text| (text, Shell::Posix));
            }
        }
    } else if (program.eq_ignore_ascii_case("powershell") || program.eq_ignore_ascii_case("pwsh"))
        && let Some(index) = args
            .iter()
            .position(|arg| arg.eq_ignore_ascii_case("-command") || arg.eq_ignore_ascii_case("-c"))
    {
        let text = args[index + 1..].join(" ");
        return Some((text, Shell::PowerShell));
    }
    None
}

fn is_check(program: &str, args: &[String], project_check: Option<&[String]>) -> bool {
    let program = executable_name(program);
    let args = cargo_args(program, args);
    let cargo_check = program.eq_ignore_ascii_case("cargo")
        && args.first().is_some_and(|arg| {
            CARGO_CHECKS
                .iter()
                .any(|check| arg.eq_ignore_ascii_case(check))
        });
    let configured_check = project_check
        .and_then(|argv| argv.split_first())
        .is_some_and(|(configured, expected)| {
            let configured_program = executable_name(configured);
            let expected = cargo_args(configured_program, expected);
            configured_program.eq_ignore_ascii_case(program)
                && args.len() >= expected.len()
                && args
                    .iter()
                    .zip(expected)
                    .all(|(actual, expected)| actual == expected)
        });
    cargo_check || configured_check
}

fn cargo_args<'a>(program: &str, args: &'a [String]) -> &'a [String] {
    if !program.eq_ignore_ascii_case("cargo") {
        return args;
    }

    let mut index = 0;
    while let Some(arg) = args.get(index) {
        if matches!(
            arg.as_str(),
            "--color"
                | "--config"
                | "-Z"
                | "-C"
                | "--directory"
                | "--explain"
                | "--manifest-path"
                | "--target-dir"
                | "--target"
        ) {
            index = (index + 2).min(args.len());
        } else if arg.starts_with('-') || (arg.starts_with('+') && arg.len() > 1) {
            index += 1;
        } else {
            break;
        }
    }
    &args[index..]
}

fn executable_name(path: &str) -> &str {
    let name = path.rsplit(['/', '\\']).next().unwrap_or(path);
    if name.len() >= 4 && name.as_bytes()[name.len() - 4..].eq_ignore_ascii_case(b".exe") {
        &name[..name.len() - 4]
    } else {
        name
    }
}
