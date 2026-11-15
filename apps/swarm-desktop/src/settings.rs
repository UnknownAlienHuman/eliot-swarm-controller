//! Desktop preferences share the daemon settings source and revision guard.
use std::path::Path;

use eliot_core::api::{
    SettingsClientSource, SettingsReadRequest, SettingsReadResponse, SettingsSaveRequest,
    SettingsSaveResponse, SettingsSection,
};
use serde::{Deserialize, Serialize};

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
    let reply = eliot_daemon::client_call_typed(data_dir, "settings.read", &request).await?;
    request.validate_response(&reply)?;
    Ok(reply)
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
}
