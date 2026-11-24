//! The brand book's fonts, shipped inside the executable: Roboto (text),
//! Roboto Mono (code) and Eliot Symbols (Material Symbols Rounded, cut to
//! the desktop's icons by `design/fonts/make_fonts.py`; outline glyphs at
//! their own code points, filled ones at `0xF0000 + code - 0xE000`).
//! Licences: SIL OFL 1.1 for Roboto and Roboto Mono, Apache 2.0 for
//! Material Symbols; their texts are written beside the fonts.
//!
//! XAML loads a font only from a file, and an unpackaged WinUI app finds
//! it only by a path relative to the executable (observed live: absolute
//! paths, `file:` and `ms-appx:` URIs fall back to Segoe UI). So on first
//! use the files are written into `fonts` beside the executable, named
//! after their content hash, and referenced as `fonts/<file>#<face>`.
//! Every icon is a Material Symbols code point, which no Windows font
//! draws, so a folder that cannot be written ends the start with that
//! reason instead of showing wrong icons.

use std::path::Path;
use std::sync::OnceLock;

use sha2::{Digest, Sha256};

const ROBOTO: &[u8] = include_bytes!("../design/fonts/Roboto.ttf");
const ROBOTO_MONO: &[u8] = include_bytes!("../design/fonts/RobotoMono.ttf");
const SYMBOLS: &[u8] = include_bytes!("../design/fonts/EliotSymbols.ttf");
const SYMBOLS_DARK: &[u8] = include_bytes!("../design/fonts/EliotSymbolsDark.ttf");

/// The licence texts that travel with the fonts.
const LICENCES: [(&str, &[u8]); 3] = [
    (
        "OFL-Roboto.txt",
        include_bytes!("../design/fonts/OFL-Roboto.txt"),
    ),
    (
        "OFL-RobotoMono.txt",
        include_bytes!("../design/fonts/OFL-RobotoMono.txt"),
    ),
    (
        "LICENSE-MaterialSymbols.txt",
        include_bytes!("../design/fonts/LICENSE-MaterialSymbols.txt"),
    ),
];

/// The Windows fonts behind the shipped faces: text, code, and the glyphs
/// WinUI's own templates draw (a text box's clear button, a submenu
/// chevron), which Eliot Symbols does not carry.
const TEXT_FALLBACK: &str = "Segoe UI Variable Text, Segoe UI";
const MONO_FALLBACK: &str = "Cascadia Mono, Consolas";
const SYMBOLS_FALLBACK: &str = "Segoe Fluent Icons";

/// XAML `FontFamily` values, already escaped for an attribute.
pub struct Families {
    /// Roboto, then Segoe UI.
    pub text: String,
    /// Roboto Mono, then Cascadia Mono. Reactor 0.100 sets no per-element
    /// family; code blocks take it through their control type's style.
    #[allow(
        dead_code,
        reason = "code blocks move to Roboto Mono with the styles increment"
    )]
    pub mono: String,
    /// Eliot Symbols at grade 0, for light themes.
    pub symbols: String,
    /// Eliot Symbols at grade -25, for dark themes.
    pub symbols_dark: String,
}

/// The families, worked out once per process.
pub fn families() -> &'static Families {
    static FAMILIES: OnceLock<Families> = OnceLock::new();
    FAMILIES.get_or_init(|| match write_fonts() {
        Ok(names) => Families {
            text: family(&names[0], "Roboto", TEXT_FALLBACK),
            mono: family(&names[1], "Roboto Mono", MONO_FALLBACK),
            symbols: family(&names[2], "Eliot Symbols", SYMBOLS_FALLBACK),
            symbols_dark: family(&names[3], "Eliot Symbols Dark", SYMBOLS_FALLBACK),
        },
        Err(error) => crate::startup::fatal(&format!(
            "The desktop's fonts could not be written into the \"{FOLDER}\" folder beside the executable: {error}"
        )),
    })
}

/// `fonts/<file>#<face>`, then the fallback families, escaped for a XAML
/// attribute.
fn family(file: &str, face: &str, fallback: &str) -> String {
    format!("{FOLDER}/{file}#{face}, {fallback}")
        .replace('&', "&amp;")
        .replace('"', "&quot;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

/// The folder beside the executable that holds the fonts.
const FOLDER: &str = "fonts";

/// The shipped files: stem and bytes.
const FILES: [(&str, &[u8]); 4] = [
    ("Roboto", ROBOTO),
    ("RobotoMono", ROBOTO_MONO),
    ("EliotSymbols", SYMBOLS),
    ("EliotSymbolsDark", SYMBOLS_DARK),
];

/// Write each font as `fonts/<stem>-<hash>.ttf` beside the executable
/// (once per content) with the licence texts, remove older copies, and
/// return the file names in [`FILES`] order.
fn write_fonts() -> std::io::Result<[String; 4]> {
    let exe = std::env::current_exe()?;
    let dir = exe
        .parent()
        .ok_or_else(|| std::io::Error::other("the executable has no folder"))?
        .join(FOLDER);
    std::fs::create_dir_all(&dir)?;
    let mut names: [String; 4] = Default::default();
    for (index, (stem, bytes)) in FILES.into_iter().enumerate() {
        let digest = Sha256::digest(bytes);
        let tag: String = digest[..6].iter().map(|b| format!("{b:02x}")).collect();
        let name = format!("{stem}-{tag}.ttf");
        write_once(&dir, &name, bytes)?;
        remove_older(&dir, stem, &name);
        names[index] = name;
    }
    for (name, bytes) in LICENCES {
        write_once(&dir, name, bytes)?;
    }
    Ok(names)
}

/// Write `bytes` as `dir/name` unless that file already holds exactly
/// these bytes (a stale or damaged copy of the same name is replaced):
/// through a `.part` file and a rename, so a reader never sees half a file.
fn write_once(dir: &Path, name: &str, bytes: &[u8]) -> std::io::Result<()> {
    let path = dir.join(name);
    let current = |path: &Path| std::fs::read(path).is_ok_and(|held| held == bytes);
    if current(&path) {
        return Ok(());
    }
    let partial = dir.join(format!("{name}.{}.part", std::process::id()));
    std::fs::write(&partial, bytes)?;
    if let Err(error) = std::fs::rename(&partial, &path) {
        let _ = std::fs::remove_file(&partial);
        // Another desktop may have written the same file first.
        if !current(&path) {
            return Err(error);
        }
    }
    Ok(())
}

/// Remove an older build's `<stem>-<12 hex>.ttf` files other than `keep`,
/// and `.part` files a killed desktop left. A file another running desktop
/// still has open stays until a later start; one it has not loaded yet is
/// written again when it starts next.
fn remove_older(dir: &Path, stem: &str, keep: &str) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    let ours = format!(".{}.part", std::process::id());
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        let Some(rest) = name
            .strip_prefix(stem)
            .and_then(|rest| rest.strip_prefix('-'))
        else {
            continue;
        };
        let (tag, stale) = match rest.split_once(".ttf") {
            Some((tag, "")) => (tag, name != keep),
            Some((tag, part)) => (tag, part.ends_with(".part") && !part.ends_with(&ours)),
            None => continue,
        };
        if stale && tag.len() == 12 && tag.bytes().all(|b| b.is_ascii_hexdigit()) {
            let _ = std::fs::remove_file(entry.path());
        }
    }
}
