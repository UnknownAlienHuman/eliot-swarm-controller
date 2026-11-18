use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

pub fn embed(binary: &str, description: &str) {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=../../build/windows_resources.rs");

    if env::var("CARGO_CFG_TARGET_OS").as_deref() != Ok("windows")
        || env::var("CARGO_CFG_TARGET_ENV").as_deref() != Ok("msvc")
    {
        return;
    }

    println!("cargo:rerun-if-changed=../../assets/eliot-swarm.ico");
    for key in ["PATH", "WindowsSdkDir", "ProgramFiles(x86)"] {
        println!("cargo:rerun-if-env-changed={key}");
    }

    let rc = resource_compiler().expect(
        "Windows MSVC builds require Windows SDK rc.exe. Install the Windows SDK or put its rc.exe on PATH.",
    );
    let version = env::var("CARGO_PKG_VERSION").expect("Cargo package version");
    let escaped_version = rc_string(&version);
    let numeric_version = version.split(['-', '+']).next().expect("version core");
    let parts: Vec<u16> = numeric_version
        .split('.')
        .map(|part| {
            part.parse()
                .expect("Windows version components must fit in u16")
        })
        .collect();
    let [major, minor, patch] = parts.as_slice() else {
        panic!("Expected a Cargo major.minor.patch version: {version}");
    };
    let out_dir = PathBuf::from(env::var_os("OUT_DIR").expect("Cargo output directory"));
    // Use a local filename so RC does not need to decode a checkout path.
    fs::copy(
        "../../assets/eliot-swarm.ico",
        out_dir.join("eliot-swarm.ico"),
    )
    .expect("Copy the app icon into OUT_DIR");
    let script = format!(
        r#"101 ICON "eliot-swarm.ico"

1 VERSIONINFO
 FILEVERSION {major},{minor},{patch},0
 PRODUCTVERSION {major},{minor},{patch},0
 FILEOS 0x00040004
 FILETYPE 0x00000001
BEGIN
    BLOCK "StringFileInfo"
    BEGIN
        BLOCK "040904B0"
        BEGIN
            VALUE "FileDescription", "{description}\0"
            VALUE "ProductName", "Eliot Swarm Controller\0"
            VALUE "FileVersion", "{version}\0"
            VALUE "ProductVersion", "{version}\0"
            VALUE "OriginalFilename", "{binary}.exe\0"
        END
    END
    BLOCK "VarFileInfo"
    BEGIN
        VALUE "Translation", 0x0409, 1200
    END
END
"#,
        description = rc_string(description),
        version = escaped_version,
        binary = rc_string(binary),
    );
    fs::write(out_dir.join("eliot-swarm.rc"), script).expect("Write the Windows resource script");
    let status = Command::new(&rc)
        .current_dir(&out_dir)
        .args(["/nologo", "/fo", "eliot-swarm.res", "eliot-swarm.rc"])
        .status()
        .unwrap_or_else(|error| panic!("Failed to run {}: {error}", rc.display()));
    assert!(status.success(), "{} failed with {status}", rc.display());
    println!(
        "cargo:rustc-link-arg-bin={binary}={}",
        out_dir.join("eliot-swarm.res").display()
    );
}

fn rc_string(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '"' => escaped.push_str("\"\""),
            '\\' => escaped.push_str("\\\\"),
            ch if ch.is_control() || matches!(ch, '\u{2028}' | '\u{2029}') => {
                panic!(
                    "Windows RC strings cannot contain control or newline characters (U+{:04X})",
                    ch as u32
                );
            }
            ch => escaped.push(ch),
        }
    }
    escaped
}

fn resource_compiler() -> Option<PathBuf> {
    let sdk_arch = match env::var("HOST").expect("Cargo host").split('-').next() {
        Some("aarch64") => "arm64",
        Some("i686") => "x86",
        _ => "x64",
    };
    let kits = env::var_os("ProgramFiles(x86)")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(r"C:\Program Files (x86)"))
        .join("Windows Kits")
        .join("10");
    let preferred = kits
        .join("bin")
        .join("10.0.26100.0")
        .join(sdk_arch)
        .join("rc.exe");
    if preferred.is_file() {
        return Some(preferred);
    }
    if let Some(rc) = env::var_os("PATH").and_then(|path| {
        env::split_paths(&path)
            .map(|dir| dir.join("rc.exe"))
            .find(|rc| rc.is_absolute() && rc.is_file())
    }) {
        return Some(rc);
    }
    env::var_os("WindowsSdkDir")
        .and_then(|dir| sdk_compiler(&PathBuf::from(dir), sdk_arch))
        .or_else(|| sdk_compiler(&kits, sdk_arch))
}

fn sdk_compiler(root: &Path, arch: &str) -> Option<PathBuf> {
    let bin = root.join("bin");
    fs::read_dir(&bin)
        .ok()?
        .filter_map(Result::ok)
        .filter_map(|entry| {
            let version: Vec<u32> = entry
                .file_name()
                .to_str()?
                .split('.')
                .map(str::parse)
                .collect::<Result<_, _>>()
                .ok()?;
            let rc = entry.path().join(arch).join("rc.exe");
            rc.is_file().then_some((version, rc))
        })
        .max_by(|(left, _), (right, _)| left.cmp(right))
        .map(|(_, rc)| rc)
}

#[cfg(test)]
mod tests {
    use super::rc_string;

    #[test]
    fn escapes_rc_quotes_and_backslashes() {
        assert_eq!(rc_string(r#"a "quote"\new\"#), r#"a ""quote""\\new\\"#);
    }

    #[test]
    fn rejects_control_and_newline_characters() {
        for ch in ['\0', '\t', '\r', '\n', '\u{0085}', '\u{2028}', '\u{2029}'] {
            let input = format!("before{ch}after");
            let error = std::panic::catch_unwind(|| rc_string(&input)).unwrap_err();
            let message = error.downcast_ref::<String>().unwrap();
            assert!(message.contains(&format!("U+{:04X}", ch as u32)));
        }
    }
}
