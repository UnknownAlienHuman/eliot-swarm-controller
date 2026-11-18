//! Desktop preferences share the daemon settings source and revision guard.
//!
//! The theme, window size and what opens at start live in the daemon's
//! `[desktop]` section; the desktop keeps no file of its own. A legacy
//! per-user `swarm-desktop.json` is imported once when `[desktop]` is absent.
//! RPC goes through [`crate::config_client`].
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
    pub fn label(self) -> &'static str {
        match self {
            Self::System => "System",
            Self::Light => "Light",
            Self::Dark => "Dark",
        }
    }
}

/// What the window opens at start (`[desktop] startup`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Startup {
    /// The empty new-task view.
    #[default]
    NewTask,
    /// The most recently active Agent's conversation.
    LastConversation,
}

/// A launch access level: the Owner ad-hoc launch `access` and
/// `[desktop] last_access` (`docs/spec/configuration.md`, the access table).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Access {
    /// Every approval goes to the Owner as a pending request.
    Ask,
    /// The harness's automatic posture inside the workspace.
    Auto,
    /// The harness's full-access posture.
    Full,
}

impl Access {
    pub const ALL: [Self; 3] = [Self::Ask, Self::Auto, Self::Full];

    /// The launch `access` value.
    pub fn wire(self) -> &'static str {
        match self {
            Self::Ask => "ask",
            Self::Auto => "auto",
            Self::Full => "full",
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Ask => "Ask for approval",
            Self::Auto => "Approve for me",
            Self::Full => "Full access",
        }
    }

    /// One line on what the level lets the Agent do.
    pub fn description(self) -> &'static str {
        match self {
            Self::Ask => "Every command and file change waits for your approval",
            Self::Auto => "Works inside the project; asks only where the agent app must",
            Self::Full => "Unrestricted access to your files and the network, no approvals",
        }
    }
}

/// One model the picker remembers (`[desktop] favorite_models` and
/// `recent_models`): the harness, its native model id and an optional effort.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ModelRef {
    pub harness: String,
    pub model: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effort: Option<String>,
}

/// One Manage models choice (`[desktop] model_visibility`): a model, or a
/// whole provider group of one harness, shown or hidden in the picker.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Visibility {
    pub harness: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    pub visible: bool,
}

/// One owner-given display name (`[desktop] display_names`): `project:<key>`
/// or `agent:<id>`.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DisplayName {
    pub key: String,
    pub name: String,
}

/// `[desktop]` list bounds (the daemon's validation).
pub const MAX_FAVORITES: usize = 50;
pub const MAX_RECENTS: usize = 20;
pub const MAX_COLLAPSED: usize = 100;
pub const MAX_VISIBILITY: usize = 500;
pub const MAX_DISPLAY_NAMES: usize = 200;
pub const MAX_DISPLAY_NAME_CHARS: usize = 120;
/// The longest collapsed-project key `[desktop]` accepts.
pub const MAX_COLLAPSED_KEY: usize = 256;

/// The `[desktop]` preferences the window applies.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Preferences {
    pub theme: ThemeChoice,
    pub startup: Startup,
    /// Favorite models, in the order they were marked.
    pub favorites: Vec<ModelRef>,
    /// Models launched recently, most recent first.
    pub recents: Vec<ModelRef>,
    /// Section keys of collapsed sidebar projects.
    pub collapsed: Vec<String>,
    /// The access level new tasks start with (the last one chosen).
    pub last_access: Option<Access>,
    /// Manage models: models and provider groups shown or hidden.
    pub visibility: Vec<Visibility>,
    /// Collapsed picker groups (`<harness>` or `<harness>/<provider>`).
    pub collapsed_groups: Vec<String>,
    /// Owner-given names of sidebar projects and Agents.
    pub names: Vec<DisplayName>,
}

impl Preferences {
    pub fn from_reply(reply: &SettingsReadResponse) -> Self {
        let desktop = reply.sections.get(&SettingsSection::Desktop);
        let field = |name: &str| desktop.and_then(|value| value.get(name)).cloned();
        let models = |name: &str| -> Vec<ModelRef> {
            field(name)
                .and_then(|value| serde_json::from_value(value).ok())
                .unwrap_or_default()
        };
        Self {
            theme: theme(reply),
            startup: startup(reply),
            favorites: models("favorite_models"),
            recents: models("recent_models"),
            collapsed: field("collapsed_projects")
                .and_then(|value| serde_json::from_value(value).ok())
                .unwrap_or_default(),
            last_access: field("last_access").and_then(|value| serde_json::from_value(value).ok()),
            visibility: field("model_visibility")
                .and_then(|value| serde_json::from_value(value).ok())
                .unwrap_or_default(),
            collapsed_groups: field("collapsed_model_groups")
                .and_then(|value| serde_json::from_value(value).ok())
                .unwrap_or_default(),
            names: field("display_names")
                .and_then(|value| serde_json::from_value(value).ok())
                .unwrap_or_default(),
        }
    }

    /// Whether the picker shows `model` of `harness` under `provider`: its
    /// own choice, else its provider group's, else shown.
    pub fn model_visible(&self, harness: &str, provider: &str, model: &str) -> bool {
        let own = self
            .visibility
            .iter()
            .find(|entry| entry.harness == harness && entry.model.as_deref() == Some(model));
        own.or_else(|| self.group_entry(harness, provider))
            .is_none_or(|entry| entry.visible)
    }

    /// Whether a provider group is shown (its own entry, else shown).
    pub fn group_visible(&self, harness: &str, provider: &str) -> bool {
        self.group_entry(harness, provider)
            .is_none_or(|entry| entry.visible)
    }

    fn group_entry(&self, harness: &str, provider: &str) -> Option<&Visibility> {
        self.visibility
            .iter()
            .find(|entry| entry.harness == harness && entry.provider.as_deref() == Some(provider))
    }

    /// Show or hide one model; a choice equal to its group's drops out.
    pub fn set_model_visible(&mut self, harness: &str, provider: &str, model: &str, visible: bool) {
        self.visibility
            .retain(|entry| !(entry.harness == harness && entry.model.as_deref() == Some(model)));
        if self.group_visible(harness, provider) != visible
            && self.visibility.len() < MAX_VISIBILITY
        {
            self.visibility.push(Visibility {
                harness: harness.to_owned(),
                model: Some(model.to_owned()),
                provider: None,
                visible,
            });
        }
    }

    /// Show or hide a provider group: one entry, and its models' own
    /// choices drop out (`models` lists the group's model ids).
    pub fn set_group_visible(
        &mut self,
        harness: &str,
        provider: &str,
        models: &[&str],
        visible: bool,
    ) {
        self.visibility.retain(|entry| {
            entry.harness != harness
                || (entry.provider.as_deref() != Some(provider)
                    && !entry
                        .model
                        .as_deref()
                        .is_some_and(|model| models.contains(&model)))
        });
        if !visible && self.visibility.len() < MAX_VISIBILITY {
            self.visibility.push(Visibility {
                harness: harness.to_owned(),
                model: None,
                provider: Some(provider.to_owned()),
                visible,
            });
        }
    }

    /// Whether a picker group is collapsed.
    pub fn group_collapsed(&self, key: &str) -> bool {
        self.collapsed_groups.iter().any(|known| known == key)
    }

    /// Collapse or expand one picker group.
    pub fn toggle_group(&mut self, key: &str) {
        if self.group_collapsed(key) {
            self.collapsed_groups.retain(|known| known != key);
        } else if self.collapsed_groups.len() < MAX_COLLAPSED && key.len() <= MAX_COLLAPSED_KEY {
            self.collapsed_groups.push(key.to_owned());
        }
    }

    /// The owner's name for `key` (`project:<key>` or `agent:<id>`).
    pub fn display_name(&self, key: &str) -> Option<&str> {
        self.names
            .iter()
            .find(|entry| entry.key == key)
            .map(|entry| entry.name.as_str())
    }

    /// Set (or, with a blank name, clear) the display name of `key`: its
    /// words joined by single spaces, cut to the bound. False when the list
    /// is full or the key cannot be stored.
    pub fn rename(&mut self, key: &str, name: &str) -> bool {
        let name: String = name
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
            .chars()
            .take(MAX_DISPLAY_NAME_CHARS)
            .collect();
        let name = name.trim().to_owned();
        self.names.retain(|entry| entry.key != key);
        if name.is_empty() {
            return true;
        }
        if self.names.len() >= MAX_DISPLAY_NAMES
            || key.is_empty()
            || key.len() > MAX_COLLAPSED_KEY
            || key.chars().any(char::is_control)
            || key != key.trim()
        {
            return false;
        }
        self.names.push(DisplayName {
            key: key.to_owned(),
            name,
        });
        true
    }

    /// Put `model` first in Recents (once), within the bound.
    pub fn remember(&mut self, model: ModelRef) {
        self.recents.retain(|known| *known != model);
        self.recents.insert(0, model);
        self.recents.truncate(MAX_RECENTS);
    }

    /// Mark or unmark `model` (its harness and model id, any effort) as a
    /// favorite. Returns whether it is one now; a full list adds nothing.
    pub fn toggle_favorite(&mut self, model: ModelRef) -> bool {
        let same = |known: &ModelRef| known.harness == model.harness && known.model == model.model;
        if self.favorites.iter().any(same) {
            self.favorites.retain(|known| !same(known));
            false
        } else if self.favorites.len() < MAX_FAVORITES {
            self.favorites.push(model);
            true
        } else {
            false
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

pub fn startup(reply: &SettingsReadResponse) -> Startup {
    reply
        .sections
        .get(&SettingsSection::Desktop)
        .and_then(|value| value.get("startup"))
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

/// Re-read the current desktop section so a preference change keeps the
/// section's other fields.
pub async fn save_preferences(
    data_dir: &Path,
    preferences: Preferences,
    window: Option<(u32, u32)>,
) -> eliot_core::Result<SettingsSaveResponse> {
    let read = read(data_dir).await?;
    let desktop = crate::settings_model::desktop_section(
        read.sections.get(&SettingsSection::Desktop),
        &preferences,
        window,
    );
    let request = SettingsSaveRequest {
        expected_revision: read.revision,
        sections: [(SettingsSection::Desktop, desktop)].into(),
        dry_run: false,
        client: SettingsClientSource::Desktop,
    };
    save(data_dir, &request).await
}

/// Attempts for the one window-size write at close.
const CLOSE_SAVE_ATTEMPTS: usize = 4;

/// The window-size update for one settings read: the read's own revision
/// with its preferences carried over and only the size replaced. Built from
/// a single read so a concurrent preference write between two reads cannot
/// be silently overwritten: it reports `conflict` instead (#641).
pub fn window_close_request(read: &SettingsReadResponse, size: (u32, u32)) -> SettingsSaveRequest {
    let preferences = Preferences::from_reply(read);
    let desktop = crate::settings_model::desktop_section(
        read.sections.get(&SettingsSection::Desktop),
        &preferences,
        Some(size),
    );
    SettingsSaveRequest {
        expected_revision: read.revision.clone(),
        sections: [(SettingsSection::Desktop, desktop)].into(),
        dry_run: false,
        client: SettingsClientSource::Desktop,
    }
}

/// Persist the last observed window size into `[desktop]` once, at close: a
/// revision-guarded write only when it differs from the saved value, keeping
/// the saved theme and startup. A concurrent save (for example an in-flight
/// choice landing while the window closes) reports `conflict` instead of
/// clobbering, so retry with a fresh read a few times. Returns whether a
/// write landed.
pub async fn save_window_on_close(data_dir: &Path, size: (u32, u32)) -> eliot_core::Result<bool> {
    for _ in 0..CLOSE_SAVE_ATTEMPTS {
        let read = read(data_dir).await?;
        if window(&read) == Some(size) {
            return Ok(false);
        }
        // One read, one revision: never re-read inside `save_preferences`,
        // whose fresh revision would overwrite the concurrent change.
        let request = window_close_request(&read, size);
        match save(data_dir, &request).await {
            Ok(_) => return Ok(true),
            Err(error) if error.code == "conflict" => continue,
            Err(error) => return Err(error),
        }
    }
    Err(eliot_core::Error::new(
        "conflict",
        "settings changed while the window size was saved",
    ))
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
    }

    #[test]
    fn close_write_keeps_the_read_revision_and_preferences() {
        // #641: the close write must guard the exact revision its
        // preferences were read at, so a concurrent preference save
        // reports `conflict` instead of being silently overwritten.
        let read = SettingsReadResponse {
            revision: Some("rev-A".to_owned()),
            sections: [(
                SettingsSection::Desktop,
                serde_json::json!({
                    "theme": "dark",
                    "startup": "last_conversation",
                    "window": {"width": 800, "height": 600},
                }),
            )]
            .into(),
            restart_required: Vec::new(),
            history: Vec::new(),
        };
        let request = window_close_request(&read, (1024, 768));
        assert_eq!(request.expected_revision.as_deref(), Some("rev-A"));
        let desktop = request.sections.get(&SettingsSection::Desktop).unwrap();
        assert_eq!(
            desktop.get("window").unwrap(),
            &serde_json::json!({"width": 1024, "height": 768})
        );
        assert_eq!(desktop.get("theme").unwrap(), &serde_json::json!("dark"));
        assert_eq!(
            desktop.get("startup").unwrap(),
            &serde_json::json!("last_conversation")
        );
    }

    #[test]
    fn startup_choices_match_wire_values() {
        for (choice, text) in [
            (Startup::NewTask, "new_task"),
            (Startup::LastConversation, "last_conversation"),
        ] {
            assert_eq!(serde_json::to_value(choice).unwrap(), text);
        }
    }

    #[test]
    fn access_levels_match_launch_values() {
        for access in Access::ALL {
            assert_eq!(serde_json::to_value(access).unwrap(), access.wire());
        }
    }

    #[test]
    fn recents_stay_unique_most_recent_first_and_bounded() {
        let model = |id: &str| ModelRef {
            harness: "codex".to_owned(),
            model: id.to_owned(),
            effort: None,
        };
        let mut prefs = Preferences::default();
        for index in 0..(MAX_RECENTS + 5) {
            prefs.remember(model(&format!("m{index}")));
        }
        prefs.remember(model("m3"));
        assert_eq!(prefs.recents.len(), MAX_RECENTS);
        assert_eq!(prefs.recents[0], model("m3"));
        assert_eq!(prefs.recents.iter().filter(|m| m.model == "m3").count(), 1);
        assert!(prefs.toggle_favorite(model("a")));
        assert!(prefs.favorites.iter().any(|known| known.model == "a"));
        // The same model with another effort is the same favorite.
        assert!(!prefs.toggle_favorite(ModelRef {
            effort: Some("low".to_owned()),
            ..model("a")
        }));
        assert!(prefs.favorites.is_empty());
    }

    #[test]
    fn preferences_read_the_picker_lists() {
        let reply = SettingsReadResponse {
            revision: Some("r".to_owned()),
            sections: [(
                SettingsSection::Desktop,
                serde_json::json!({
                    "theme": "dark",
                    "favorite_models": [{"harness": "claude", "model": "claude-opus-5-5", "effort": "high"}],
                    "recent_models": [{"harness": "codex", "model": "gpt-6-luna"}],
                    "collapsed_projects": ["repo:c:/x/.git"],
                    "last_access": "full"
                }),
            )]
            .into(),
            restart_required: Vec::new(),
            history: Vec::new(),
        };
        let prefs = Preferences::from_reply(&reply);
        assert_eq!(prefs.theme, ThemeChoice::Dark);
        assert_eq!(prefs.favorites[0].effort.as_deref(), Some("high"));
        assert_eq!(prefs.recents[0].model, "gpt-6-luna");
        assert_eq!(prefs.collapsed, vec!["repo:c:/x/.git".to_owned()]);
        assert_eq!(prefs.last_access, Some(Access::Full));
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
