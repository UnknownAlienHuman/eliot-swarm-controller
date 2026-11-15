//! Unified settings editor: the daemon validates every section and owns history.
use super::*;
use crate::theme::winui::Palette;
use eliot_core::api::{
    SettingsClientSource, SettingsReadResponse, SettingsSaveRequest, SettingsSaveResponse,
};

#[derive(Clone, Debug)]
pub(super) enum SettingsMsg {
    Loaded(Fallible<SettingsReadResponse>),
    Draft(String),
    Read,
    Save(bool),
    Saved(bool, Fallible<SettingsSaveResponse>),
}
#[derive(Default)]
pub(super) struct SettingsEditor {
    pub(super) loaded: bool,
    pub(super) busy: bool,
    revision: Option<String>,
    draft: String,
    base: String,
    history: String,
    status: String,
}
impl DesktopApp {
    pub(super) fn settings_update(&mut self, msg: SettingsMsg, context: &ComponentContext<Self>) {
        match msg {
            SettingsMsg::Loaded(result) => {
                self.unified.busy = false;
                match result {
                    Ok(read) => {
                        self.appearance.pref = crate::settings::theme(&read);
                        self.preferred_window = crate::settings::window(&read);
                        self.unified.loaded = true;
                        self.unified.revision = read.revision;
                        self.unified.draft =
                            serde_json::to_string_pretty(&read.sections).unwrap_or_default();
                        self.unified.base = self.unified.draft.clone();
                        self.unified.history =
                            serde_json::to_string_pretty(&read.history).unwrap_or_default();
                        self.unified.status = format!(
                            "Loaded. Restart required for changes to: {:?}",
                            read.restart_required
                        );
                    }
                    Err(error) => self.unified.status = format!("Settings read failed: {error}"),
                }
            }
            SettingsMsg::Draft(text) if !self.unified.busy => {
                self.unified.draft = profiles::normalize_newlines(&text);
            }
            SettingsMsg::Draft(_) => {}
            SettingsMsg::Read => self.load_settings(context),
            SettingsMsg::Save(dry_run) => {
                if self.unified.busy || !self.unified.loaded {
                    return;
                }
                let mut sections: std::collections::BTreeMap<
                    eliot_core::api::SettingsSection,
                    serde_json::Value,
                > = match serde_json::from_str(&self.unified.draft) {
                    Ok(sections) => sections,
                    Err(_) => {
                        self.unified.status = "Invalid settings JSON; draft kept.".to_owned();
                        return;
                    }
                };
                if let Ok(base) = serde_json::from_str::<
                    std::collections::BTreeMap<eliot_core::api::SettingsSection, serde_json::Value>,
                >(&self.unified.base)
                {
                    sections.retain(|section, value| base.get(section) != Some(value));
                }
                if sections.is_empty() {
                    self.unified.status = "No settings changed.".to_owned();
                    return;
                }
                let request = SettingsSaveRequest {
                    expected_revision: self.unified.revision.clone(),
                    sections,
                    dry_run,
                    client: SettingsClientSource::Desktop,
                };
                let Ok(data_dir) = self.data_dir.clone() else {
                    return;
                };
                self.unified.busy = true;
                self.unified.status = if dry_run {
                    "Validating…"
                } else {
                    "Saving…"
                }
                .to_owned();
                let _task = context.spawn_background(move |_| {
                    Message::Settings(SettingsMsg::Saved(
                        dry_run,
                        client::block_on(crate::settings::save(&data_dir, &request))
                            .map_err(|e| e.to_string()),
                    ))
                });
            }
            SettingsMsg::Saved(dry_run, result) => {
                self.unified.busy = false;
                match result {
                    Ok(reply) if dry_run => {
                        self.unified.status = format!(
                            "Valid. Changed: {:?}; restart: {:?}",
                            reply.changed_sections, reply.restart_required
                        )
                    }
                    Ok(reply) => {
                        self.unified.status = format!(
                            "Saved revision {:?}; restart: {:?}",
                            reply.revision, reply.restart_required
                        );
                        self.load_settings(context);
                        self.load_profile_list(context);
                    }
                    Err(error) => {
                        self.unified.status = format!(
                            "Save failed; draft kept. Reload before retrying an uncertain save: {error}"
                        )
                    }
                }
            }
        }
    }
    pub(super) fn load_settings(&mut self, context: &ComponentContext<Self>) {
        if self.unified.busy {
            return;
        }
        let Ok(data_dir) = self.data_dir.clone() else {
            return;
        };
        self.unified.busy = true;
        let _task = context.spawn_background(move |_| {
            Message::Settings(SettingsMsg::Loaded(
                client::block_on(crate::settings::read(&data_dir)).map_err(|e| e.to_string()),
            ))
        });
    }
    pub(super) fn settings_view(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        Grid::new()
            .rows([
                GridLength::Auto,
                GridLength::Auto,
                GridLength::STAR,
                GridLength::Auto,
                GridLength::Auto,
            ])
            .row_spacing(theme::spacing::SM)
            .children((
                m3::title("Settings", theme::type_scale::TITLE_LARGE),
                StackPanel::new()
                    .grid_row(1)
                    .orientation(Orientation::Horizontal)
                    .spacing(theme::spacing::SM)
                    .children((
                        m3::tonal(palette)
                            .is_enabled(!self.unified.busy)
                            .automation_name("Reload settings")
                            .on_click(context.message(Message::Settings(SettingsMsg::Read)))
                            .content(TextBlock::new().text("Reload (discard draft)")),
                        m3::tonal(palette)
                            .is_enabled(self.unified.loaded && !self.unified.busy)
                            .on_click(context.message(Message::Settings(SettingsMsg::Save(true))))
                            .content(TextBlock::new().text("Validate settings")),
                        m3::filled(palette)
                            .is_enabled(
                                self.unified.loaded
                                    && !self.unified.busy
                                    && self.unified.draft != self.unified.base,
                            )
                            .automation_name("Save settings")
                            .on_click(context.message(Message::Settings(SettingsMsg::Save(false))))
                            .content(TextBlock::new().text("Save settings")),
                    )),
                TextBox::new()
                    .grid_row(2)
                    .accepts_return(true)
                    .text_wrapping(TextWrapping::Wrap)
                    .automation_name("Settings sections JSON")
                    .text(self.primed(&self.unified.draft))
                    .is_enabled(!self.unified.busy)
                    .on_text_changed(
                        context.callback(|text| Message::Settings(SettingsMsg::Draft(text))),
                    ),
                TextBlock::new()
                    .grid_row(3)
                    .text(self.unified.status.clone())
                    .text_wrapping(TextWrapping::Wrap)
                    .is_text_selection_enabled(true),
                ScrollViewer::new().grid_row(4).max_height(160.0).content(
                    TextBlock::new()
                        .text(format!("Recent settings changes\n{}", self.unified.history))
                        .text_wrapping(TextWrapping::Wrap)
                        .is_text_selection_enabled(true),
                ),
            ))
    }
}
