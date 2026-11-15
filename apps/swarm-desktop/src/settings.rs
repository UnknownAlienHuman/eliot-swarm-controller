//! Per-user desktop preferences, persisted as small JSON (`swarm-desktop.json`).
//!
//! The theme, what opens at start, the profile Auto launches until daemon
//! routing lands, and the last conversation. The unified `settings.*` API
//! (k55) moves these into the daemon's settings store; the settings UI
//! reaches them only through [`crate::config_client`]. Loading never fails: a missing,
//! unreadable or invalid file yields the defaults. Unknown fields are ignored
//! and missing fields take their defaults, so older and newer files both load.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

const FILE_NAME: &str = "swarm-desktop.json";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ThemeChoice {
    #[default]
    System,
    Light,
    Dark,
}

impl ThemeChoice {
    pub fn label(self) -> &'static str {
        match self {
            Self::System => "System",
            Self::Light => "Light",
            Self::Dark => "Dark",
        }
    }
}

/// What the window opens at start.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Startup {
    /// The empty new-task view.
    #[default]
    NewTask,
    /// The conversation open when the window last closed.
    LastConversation,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct Settings {
    pub theme: ThemeChoice,
    pub startup: Startup,
    /// The profile "Auto" launches until daemon routing lands; unset means
    /// the first launchable profile the project allows.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_profile: Option<String>,
    /// The Agent of the last opened conversation.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_agent: Option<String>,
}

/// Settings file location: `%APPDATA%`-equivalent roaming folder from the Known
/// Folder API on Windows, `$HOME/.config/eliot` elsewhere.
#[cfg(windows)]
#[allow(unsafe_code)]
pub fn path() -> Option<PathBuf> {
    use std::ffi::c_void;
    use std::os::windows::ffi::OsStringExt;

    use windows_sys::Win32::System::Com::CoTaskMemFree;
    use windows_sys::Win32::UI::Shell::{
        FOLDERID_RoamingAppData, KF_FLAG_DEFAULT, SHGetKnownFolderPath,
    };

    let mut raw: *mut u16 = std::ptr::null_mut();
    // SAFETY: the GUID and out-pointer are valid for the call; on success the
    // shell allocates `raw` with CoTaskMemAlloc, which is freed below.
    let hresult = unsafe {
        SHGetKnownFolderPath(
            &FOLDERID_RoamingAppData,
            KF_FLAG_DEFAULT as u32,
            std::ptr::null_mut(),
            &mut raw,
        )
    };
    let root = if hresult >= 0 && !raw.is_null() {
        // SAFETY: on success `raw` points to a NUL-terminated UTF-16 string.
        let len = unsafe { (0..).take_while(|&i| *raw.add(i) != 0).count() };
        // SAFETY: `len` code units starting at `raw` were just measured.
        let wide = unsafe { std::slice::from_raw_parts(raw, len) };
        Some(PathBuf::from(std::ffi::OsString::from_wide(wide)))
    } else {
        None
    };
    if !raw.is_null() {
        // SAFETY: `raw` was allocated by SHGetKnownFolderPath with CoTaskMemAlloc.
        unsafe { CoTaskMemFree(raw as *const c_void) };
    }
    root.map(|root| root.join("Eliot").join(FILE_NAME))
}

#[cfg(not(windows))]
pub fn path() -> Option<PathBuf> {
    std::env::var_os("HOME")
        .filter(|home| !home.is_empty())
        .map(|home| {
            PathBuf::from(home)
                .join(".config")
                .join("eliot")
                .join(FILE_NAME)
        })
}

/// Reads settings from `path`. Any failure (missing, unreadable, invalid JSON)
/// returns the defaults.
pub fn load_from(path: &Path) -> Settings {
    fs::read(path)
        .ok()
        .and_then(|bytes| serde_json::from_slice(&bytes).ok())
        .unwrap_or_default()
}

/// Reads the per-user settings file, or the defaults when there is no path or
/// no usable file.
pub fn load() -> Settings {
    path().map(|path| load_from(&path)).unwrap_or_default()
}

/// Writes settings to `path` atomically: pretty JSON goes to a sibling
/// `<name>.tmp` file that is then renamed over the target.
pub fn save_to(path: &Path, settings: &Settings) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let json = serde_json::to_vec_pretty(settings).map_err(io::Error::other)?;
    let mut tmp = path.as_os_str().to_owned();
    tmp.push(".tmp");
    let tmp = PathBuf::from(tmp);
    fs::write(&tmp, json)?;
    if let Err(error) = fs::rename(&tmp, path) {
        let _ = fs::remove_file(&tmp);
        return Err(error);
    }
    Ok(())
}

/// Writes settings to the per-user settings file.
pub fn save(settings: &Settings) -> Result<(), String> {
    let path = path().ok_or_else(|| "no per-user settings folder is available".to_owned())?;
    save_to(&path, settings).map_err(|error| format!("{}: {error}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Unique temp directory, removed when dropped (including on panic).
    struct ScratchDir(PathBuf);

    impl ScratchDir {
        fn new(name: &str) -> Self {
            let dir = std::env::temp_dir().join(format!(
                "swarm-desktop-settings-{}-{name}",
                std::process::id()
            ));
            let _ = fs::remove_dir_all(&dir);
            fs::create_dir_all(&dir).expect("create scratch dir");
            Self(dir)
        }

        fn file(&self) -> PathBuf {
            self.0.join("nested").join(FILE_NAME)
        }
    }

    impl Drop for ScratchDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn settings_round_trip_each_choice() {
        let scratch = ScratchDir::new("round-trip");
        for theme in [ThemeChoice::System, ThemeChoice::Light, ThemeChoice::Dark] {
            let settings = Settings {
                theme,
                startup: Startup::LastConversation,
                default_profile: Some("codex-lite".to_owned()),
                last_agent: Some("a1".to_owned()),
            };
            save_to(&scratch.file(), &settings).expect("save");
            assert_eq!(load_from(&scratch.file()), settings);
        }
    }

    #[test]
    fn settings_missing_file_loads_default() {
        let scratch = ScratchDir::new("missing");
        assert_eq!(load_from(&scratch.file()), Settings::default());
        assert_eq!(Settings::default().theme, ThemeChoice::System);
    }

    #[test]
    fn settings_invalid_json_loads_default() {
        let scratch = ScratchDir::new("invalid");
        fs::create_dir_all(scratch.file().parent().unwrap()).unwrap();
        fs::write(scratch.file(), b"{ \"theme\": ").unwrap();
        assert_eq!(load_from(&scratch.file()), Settings::default());
    }

    #[test]
    fn settings_unknown_field_still_loads_theme() {
        let scratch = ScratchDir::new("unknown");
        fs::create_dir_all(scratch.file().parent().unwrap()).unwrap();
        fs::write(scratch.file(), br#"{"theme":"dark","future":{"x":1}}"#).unwrap();
        assert_eq!(load_from(&scratch.file()).theme, ThemeChoice::Dark);
    }

    #[test]
    fn a_theme_only_file_from_before_startup_still_loads() {
        let scratch = ScratchDir::new("theme-only");
        fs::create_dir_all(scratch.file().parent().unwrap()).unwrap();
        fs::write(scratch.file(), br#"{"theme":"light"}"#).unwrap();
        let loaded = load_from(&scratch.file());
        assert_eq!(loaded.theme, ThemeChoice::Light);
        assert_eq!(loaded.startup, Startup::NewTask);
        assert_eq!(loaded.default_profile, None);
    }

    #[test]
    fn settings_serialized_form_is_lowercase_theme_object() {
        let settings = Settings {
            theme: ThemeChoice::Dark,
            ..Settings::default()
        };
        assert_eq!(
            serde_json::to_value(&settings).unwrap(),
            serde_json::json!({"theme": "dark", "startup": "new_task"})
        );
    }
}
