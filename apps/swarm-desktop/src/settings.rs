//! Desktop preferences share the daemon settings source and revision guard.
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};

use eliot_core::api::{
    SettingsClientSource, SettingsReadRequest, SettingsReadResponse, SettingsSaveRequest,
    SettingsSaveResponse, SettingsSection,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const MAX_CONFIG_BYTES: usize = 128 * 1024;
const MAX_LEGACY_BYTES: usize = 8 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ThemeChoice {
    #[default]
    System,
    Light,
    Dark,
}
impl ThemeChoice {
    pub fn next(self) -> Self {
        match self {
            Self::System => Self::Light,
            Self::Light => Self::Dark,
            Self::Dark => Self::System,
        }
    }
    pub fn label(self) -> &'static str {
        match self {
            Self::System => "System",
            Self::Light => "Light",
            Self::Dark => "Dark",
        }
    }
}

pub async fn read(data_dir: &Path) -> eliot_core::Result<SettingsReadResponse> {
    let request = SettingsReadRequest::default();
    request.validate()?;
    let mut consistent = None;
    for attempt in 0..2 {
        let reply = eliot_daemon::client_call_typed(data_dir, "settings.read", &request).await?;
        request.validate_response(&reply)?;
        let source = read_config_source(data_dir)?;
        if source.as_deref().map(source_revision) == reply.revision {
            consistent = Some((reply, source));
            break;
        }
        if attempt == 1 {
            return Err(eliot_core::Error::new(
                "conflict",
                "settings changed while desktop preferences were read",
            ));
        }
    }
    let (reply, source) = consistent.expect("second read either matches or returns conflict");
    if !needs_legacy_import(source.as_deref(), reply.revision.as_deref())? {
        return Ok(reply);
    }

    let Some(legacy_path) = legacy_path() else {
        return Ok(reply);
    };
    let Ok(bytes) = read_bounded(&legacy_path, MAX_LEGACY_BYTES) else {
        return Ok(reply);
    };
    let Ok(legacy) = serde_json::from_slice::<LegacySettings>(&bytes) else {
        return Ok(reply);
    };
    let migration = SettingsSaveRequest {
        expected_revision: reply.revision.clone(),
        sections: [(
            SettingsSection::Desktop,
            legacy_desktop_section(legacy.theme),
        )]
        .into(),
        dry_run: false,
        client: SettingsClientSource::Desktop,
    };
    match save(data_dir, &migration).await {
        Ok(saved) => {
            // The unified file is authoritative now. If deleting the legacy file
            // fails, the next read sees [desktop] and will not import it again.
            let _ = retire_legacy(&legacy_path, saved.saved);
            let current =
                eliot_daemon::client_call_typed(data_dir, "settings.read", &request).await?;
            request.validate_response(&current)?;
            Ok(current)
        }
        Err(error) if error.code == "conflict" => {
            let current =
                eliot_daemon::client_call_typed(data_dir, "settings.read", &request).await?;
            request.validate_response(&current)?;
            Ok(current)
        }
        Err(error) => Err(error),
    }
}

fn retire_legacy(path: &Path, published: bool) -> std::io::Result<()> {
    if published {
        fs::remove_file(path)
    } else {
        Ok(())
    }
}

fn legacy_desktop_section(theme: ThemeChoice) -> serde_json::Value {
    serde_json::json!({"theme": theme})
}

#[derive(Deserialize)]
#[serde(default)]
struct LegacySettings {
    theme: ThemeChoice,
}

impl Default for LegacySettings {
    fn default() -> Self {
        Self {
            theme: ThemeChoice::System,
        }
    }
}

fn read_config_source(data_dir: &Path) -> eliot_core::Result<Option<String>> {
    match read_bounded(&data_dir.join("config.toml"), MAX_CONFIG_BYTES) {
        Ok(bytes) => String::from_utf8(bytes)
            .map(Some)
            .map_err(|_| eliot_core::Error::new("invalid", "unified settings are not valid UTF-8")),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(_) => Err(eliot_core::Error::new(
            "platform_io",
            "cannot inspect unified desktop settings",
        )),
    }
}

fn read_bounded(path: &Path, limit: usize) -> std::io::Result<Vec<u8>> {
    let file = fs::File::open(path)?;
    let mut bytes = Vec::with_capacity(limit + 1);
    file.take((limit + 1) as u64).read_to_end(&mut bytes)?;
    if bytes.len() > limit {
        return Err(std::io::Error::other("file exceeds its byte limit"));
    }
    Ok(bytes)
}

fn source_revision(source: &str) -> String {
    let digest = Sha256::digest(source.as_bytes());
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn needs_legacy_import(
    source: Option<&str>,
    observed_revision: Option<&str>,
) -> eliot_core::Result<bool> {
    if source.map(source_revision).as_deref() != observed_revision {
        return Ok(false);
    }
    config_has_desktop_section(source.unwrap_or_default()).map(|has_desktop| !has_desktop)
}

fn config_has_desktop_section(source: &str) -> eliot_core::Result<bool> {
    let value: toml::Value = toml::from_str(source)
        .map_err(|_| eliot_core::Error::new("invalid", "unified settings are invalid"))?;
    Ok(value.get("desktop").is_some())
}

#[allow(unsafe_code)]
fn legacy_path() -> Option<PathBuf> {
    #[cfg(windows)]
    {
        use std::ffi::c_void;
        use std::os::windows::ffi::OsStringExt;
        use windows_sys::Win32::System::Com::CoTaskMemFree;
        use windows_sys::Win32::UI::Shell::{
            FOLDERID_RoamingAppData, KF_FLAG_DEFAULT, SHGetKnownFolderPath,
        };

        let mut raw: *mut u16 = std::ptr::null_mut();
        // SAFETY: the GUID and out-pointer are valid; the shell-owned result is freed below.
        let result = unsafe {
            SHGetKnownFolderPath(
                &FOLDERID_RoamingAppData,
                KF_FLAG_DEFAULT as u32,
                std::ptr::null_mut(),
                &mut raw,
            )
        };
        let root = if result >= 0 && !raw.is_null() {
            // SAFETY: success returns a NUL-terminated UTF-16 string.
            let len = unsafe { (0..).take_while(|&index| *raw.add(index) != 0).count() };
            // SAFETY: the measured prefix is initialized UTF-16 data.
            let text = unsafe { std::slice::from_raw_parts(raw, len) };
            Some(PathBuf::from(std::ffi::OsString::from_wide(text)))
        } else {
            None
        };
        if !raw.is_null() {
            // SAFETY: SHGetKnownFolderPath allocates this pointer with CoTaskMemAlloc.
            unsafe { CoTaskMemFree(raw.cast::<c_void>()) };
        }
        root.map(|root| root.join("Eliot").join("swarm-desktop.json"))
    }
    #[cfg(not(windows))]
    {
        std::env::var_os("HOME")
            .filter(|home| !home.is_empty())
            .map(|home| {
                PathBuf::from(home)
                    .join(".config")
                    .join("eliot")
                    .join("swarm-desktop.json")
            })
    }
}

pub fn theme(reply: &SettingsReadResponse) -> ThemeChoice {
    reply
        .sections
        .get(&SettingsSection::Desktop)
        .and_then(|value| value.get("theme"))
        .and_then(|value| serde_json::from_value(value.clone()).ok())
        .unwrap_or_default()
}

pub fn window(reply: &SettingsReadResponse) -> Option<(u32, u32)> {
    let window = reply
        .sections
        .get(&SettingsSection::Desktop)?
        .get("window")?;
    Some((
        u32::try_from(window.get("width")?.as_u64()?).ok()?,
        u32::try_from(window.get("height")?.as_u64()?).ok()?,
    ))
}

pub async fn save(
    data_dir: &Path,
    request: &SettingsSaveRequest,
) -> eliot_core::Result<SettingsSaveResponse> {
    request.validate()?;
    let reply = eliot_daemon::client_call_typed(data_dir, "settings.save", request).await?;
    request.validate_response(&reply)?;
    Ok(reply)
}

/// Re-read the current desktop section so a theme change preserves window preferences.
pub async fn save_preferences(
    data_dir: &Path,
    choice: ThemeChoice,
    window: Option<(u32, u32)>,
) -> eliot_core::Result<()> {
    let read = read(data_dir).await?;
    let mut desktop = read
        .sections
        .get(&SettingsSection::Desktop)
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));
    desktop["theme"] = serde_json::to_value(choice).expect("theme serialization");
    if let Some((width, height)) = window {
        desktop["window"] = serde_json::json!({"width":width,"height":height});
    }
    let request = SettingsSaveRequest {
        expected_revision: read.revision,
        sections: [(SettingsSection::Desktop, desktop)].into(),
        dry_run: false,
        client: SettingsClientSource::Desktop,
    };
    save(data_dir, &request).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn theme_choices_match_wire_values() {
        for (choice, text) in [
            (ThemeChoice::System, "system"),
            (ThemeChoice::Light, "light"),
            (ThemeChoice::Dark, "dark"),
        ] {
            assert_eq!(serde_json::to_value(choice).unwrap(), text);
        }
        assert_eq!(ThemeChoice::Dark.next(), ThemeChoice::System);
    }

    #[test]
    fn legacy_preferences_preserve_supported_theme() {
        for (text, expected) in [
            (r#"{"theme":"light"}"#, ThemeChoice::Light),
            (r#"{"theme":"dark"}"#, ThemeChoice::Dark),
            (r#"{}"#, ThemeChoice::System),
        ] {
            let legacy: LegacySettings = serde_json::from_str(text).unwrap();
            assert_eq!(legacy.theme, expected);
        }
        assert_eq!(
            legacy_desktop_section(ThemeChoice::System),
            serde_json::json!({"theme":"system"})
        );
    }

    #[test]
    fn migration_runs_only_when_desktop_section_is_absent() {
        assert!(
            needs_legacy_import(Some("[profiles]\n"), Some(&source_revision("[profiles]\n")))
                .unwrap()
        );
        assert!(
            !needs_legacy_import(
                Some("[desktop]\ntheme = 'system'\n"),
                Some(&source_revision("[desktop]\ntheme = 'system'\n"))
            )
            .unwrap()
        );
        assert!(!needs_legacy_import(Some("[profiles]\n"), Some("stale-revision")).unwrap());
        assert!(!needs_legacy_import(None, Some("stale-revision")).unwrap());
    }

    #[test]
    fn legacy_file_is_retired_only_after_publication() {
        let path = std::env::temp_dir().join(format!(
            "swarm-desktop-legacy-{}-{}.json",
            std::process::id(),
            ulid::Ulid::new()
        ));
        fs::write(&path, br#"{"theme":"dark"}"#).unwrap();
        retire_legacy(&path, false).unwrap();
        assert!(path.exists());
        retire_legacy(&path, true).unwrap();
        assert!(!path.exists());

        fs::create_dir(&path).unwrap();
        fs::write(path.join("legacy.json"), b"keep").unwrap();
        assert!(retire_legacy(&path, true).is_err());
        assert!(path.join("legacy.json").exists());
        fs::remove_dir_all(path).unwrap();
    }
}
