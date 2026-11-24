//! The brand book's fonts, shipped inside the executable: Roboto (text),
//! Roboto Mono (code) and Eliot Symbols (Material Symbols Rounded, cut to
//! the desktop's icons by `design/fonts/make_fonts.py`; outline glyphs at
//! their own code points, filled ones at `0xF0000 + code - 0xE000`).
//! Licences: SIL OFL 1.1 for Roboto and Roboto Mono, Apache 2.0 for
//! Material Symbols (texts beside the font files in `design/fonts`).
//!
//! XAML loads a font only from a file, and an unpackaged WinUI app finds
//! it only by a path relative to the executable (observed live: absolute
//! paths, `file:` and `ms-appx:` URIs fall back to Segoe UI). So on first
//! use the files are written into `fonts` beside the executable, named
//! after their content hash, and referenced as `fonts/<file>#<face>`. If
//! that folder cannot be written the text falls back to Segoe UI and the
//! failure is noted in `desktop.log`.

use std::path::Path;
use std::sync::OnceLock;

use sha2::{Digest, Sha256};

const ROBOTO: &[u8] = include_bytes!("../design/fonts/Roboto.ttf");
const ROBOTO_MONO: &[u8] = include_bytes!("../design/fonts/RobotoMono.ttf");
const SYMBOLS: &[u8] = include_bytes!("../design/fonts/EliotSymbols.ttf");
const SYMBOLS_DARK: &[u8] = include_bytes!("../design/fonts/EliotSymbolsDark.ttf");

/// The Windows fonts behind the shipped text faces.
const TEXT_FALLBACK: &str = "Segoe UI Variable Text, Segoe UI";
const MONO_FALLBACK: &str = "Cascadia Mono, Consolas";

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
            text: family(&names[0], "Roboto", Some(TEXT_FALLBACK)),
            mono: family(&names[1], "Roboto Mono", Some(MONO_FALLBACK)),
            symbols: family(&names[2], "Eliot Symbols", None),
            symbols_dark: family(&names[3], "Eliot Symbols Dark", None),
        },
        Err(error) => {
            crate::startup::note(&format!("fonts: {error}; using the Windows fonts"));
            Families {
                text: TEXT_FALLBACK.to_owned(),
                mono: MONO_FALLBACK.to_owned(),
                symbols: "Segoe Fluent Icons".to_owned(),
                symbols_dark: "Segoe Fluent Icons".to_owned(),
            }
        }
    })
}

/// `fonts/<file>#<face>`, then the fallback families, escaped for a XAML
/// attribute.
fn family(file: &str, face: &str, fallback: Option<&str>) -> String {
    let mut value = format!("{FOLDER}/{file}#{face}");
    if let Some(fallback) = fallback {
        value.push_str(", ");
        value.push_str(fallback);
    }
    value
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
/// (once per content), remove this desktop's older copies, and return the
/// file names in [`FILES`] order.
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
        let path = dir.join(&name);
        if std::fs::metadata(&path).map(|m| m.len()).ok() != Some(bytes.len() as u64) {
            let partial = dir.join(format!("{name}.{}.part", std::process::id()));
            std::fs::write(&partial, bytes)?;
            if std::fs::rename(&partial, &path).is_err() {
                // Another desktop wrote it first; the content is the same.
                let _ = std::fs::remove_file(&partial);
            }
        }
        remove_older(&dir, stem, &name);
        names[index] = name;
    }
    Ok(names)
}

/// Remove `<stem>-<12 hex>.ttf` files other than `keep` (an older build's
/// fonts); a file still in use stays until a later start.
fn remove_older(dir: &Path, stem: &str, keep: &str) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        let Some(tag) = name
            .strip_prefix(stem)
            .and_then(|rest| rest.strip_prefix('-'))
            .and_then(|rest| rest.strip_suffix(".ttf"))
        else {
            continue;
        };
        if name != keep && tag.len() == 12 && tag.bytes().all(|b| b.is_ascii_hexdigit()) {
            let _ = std::fs::remove_file(entry.path());
        }
    }
}
