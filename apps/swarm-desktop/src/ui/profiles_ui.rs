//! Profile editing: one draft for form and source over the `profiles`
//! settings section. Settings' save bar validates and saves it
//! (`settings.save`, revision-checked); see [`super::settings_ui`].

use super::*;
use crate::theme::winui::Palette;

#[derive(Clone, Debug)]
pub(super) enum ProfilesMsg {
    ProfilesDraftChanged(String),
    ProfileEditSelected(Option<usize>),
    ProfileFieldChanged(usize, String),
    /// A form dropdown (field index) picked a choice (None: items replaced).
    ProfileChoicePicked(usize, Option<usize>),
    DuplicateNameChanged(String),
    DuplicateRequested,
    /// Show or hide the TOML source editor.
    SourceToggled,
}

/// One document draft: the `profiles` section as `[profiles.*]` TOML.
#[derive(Clone, Debug, Default)]
pub(super) struct ProfilesEditor {
    pub(super) revision: Option<String>,
    pub(super) base: String,
    /// Line-feed text the form, dirty check and save use.
    pub(super) draft: String,
    /// The source TextBox's text exactly as reported (CR line breaks).
    pub(super) view: String,
    pub(super) editable: bool,
    pub(super) reason: Option<String>,
    pub(super) selected: Option<String>,
    pub(super) duplicate_name: String,
    pub(super) status: String,
    pub(super) loaded: bool,
    /// The TOML source editor is shown.
    pub(super) source_open: bool,
}

impl DesktopApp {
    pub(super) fn profiles_update(&mut self, msg: ProfilesMsg, context: &ComponentContext<Self>) {
        match msg {
            ProfilesMsg::ProfilesDraftChanged(text) => {
                if self.editor.editable {
                    self.editor.draft = profiles::normalize_newlines(&text);
                    self.editor.view = text;
                }
            }
            ProfilesMsg::ProfileEditSelected(index) => {
                let names = profiles::profile_names(&self.editor.draft);
                if let Some(name) = index.and_then(|index| names.get(index)) {
                    self.editor.selected = Some(name.clone());
                }
            }
            ProfilesMsg::ProfileFieldChanged(field, value) => {
                if let (Some(name), Some(key)) =
                    (self.editor.selected.clone(), FORM_FIELDS.get(field))
                {
                    let value = Some(value.trim()).filter(|value| !value.is_empty());
                    match profiles::set_field(&self.editor.draft, &name, key, value) {
                        Ok(draft) => {
                            self.editor.view = draft.clone();
                            self.editor.draft = draft;
                        }
                        Err(error) => self.editor.status = error,
                    }
                }
            }
            ProfilesMsg::ProfileChoicePicked(field, index) => {
                // A ComboBox reports None when its items are replaced.
                let (Some(index), Some(name), Some(key)) =
                    (index, self.editor.selected.clone(), FORM_FIELDS.get(field))
                else {
                    return;
                };
                let current = match profiles::field(&self.editor.draft, &name, key) {
                    profiles::Field::Simple(value) => Some(value),
                    _ => None,
                };
                let Some(choices) = profiles::choices(key, current.as_deref()) else {
                    return;
                };
                let value = profiles::choice_value(&choices, index);
                if value == current.as_deref() {
                    return;
                }
                match profiles::set_field(&self.editor.draft, &name, key, value) {
                    Ok(draft) => {
                        self.editor.view = draft.clone();
                        self.editor.draft = draft;
                    }
                    Err(error) => self.editor.status = error,
                }
            }
            ProfilesMsg::DuplicateNameChanged(text) => self.editor.duplicate_name = text,
            ProfilesMsg::DuplicateRequested => {
                if let Some(from) = self.editor.selected.clone() {
                    let to = self.editor.duplicate_name.trim().to_owned();
                    match profiles::duplicate(&self.editor.draft, &from, &to) {
                        Ok(draft) => {
                            self.editor.view = draft.clone();
                            self.editor.draft = draft;
                            self.editor.selected = Some(to.clone());
                            self.editor.duplicate_name.clear();
                            self.editor.status = format!(
                                "Duplicated {from} as {to} in the draft; Validate, then Save."
                            );
                        }
                        Err(error) => self.editor.status = error,
                    }
                }
            }
            ProfilesMsg::SourceToggled => {
                self.editor.source_open = !self.editor.source_open;
                // A new multi-line editor gets its text one render later.
                self.reprime_fields(context);
            }
        }
    }

    pub(super) fn profiles_view(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        let list = self.launch.profile_list.as_ref();
        let data = ProfilesData {
            file_valid: list.is_none_or(|list| list.valid),
            file_error: list.and_then(|list| list.error.as_deref()),
            editable: self.editor.editable,
            reason: self.editor.reason.as_deref(),
            draft: &self.editor.draft,
            view: self.primed(&self.editor.view),
            selected: self.editor.selected.as_deref(),
            duplicate_name: &self.editor.duplicate_name,
            status: &self.editor.status,
            busy: self.settings.reading,
            summaries: &self.launch.profile_summaries,
            source_open: self.editor.source_open,
        };
        let actions = ProfilesActions {
            draft_changed: context
                .callback(|value| Message::Profiles(ProfilesMsg::ProfilesDraftChanged(value))),
            select: context
                .callback(|value| Message::Profiles(ProfilesMsg::ProfileEditSelected(value))),
            field_changed: (0..FORM_FIELDS.len())
                .map(|index| {
                    context.callback(move |text: String| {
                        Message::Profiles(ProfilesMsg::ProfileFieldChanged(index, text))
                    })
                })
                .collect(),
            duplicate_name_changed: context
                .callback(|value| Message::Profiles(ProfilesMsg::DuplicateNameChanged(value))),
            duplicate: context.message(Message::Profiles(ProfilesMsg::DuplicateRequested)),
            toggle_source: context.message(Message::Profiles(ProfilesMsg::SourceToggled)),
            choice_picked: (0..FORM_FIELDS.len())
                .map(|field| {
                    context.callback(move |index: Option<usize>| {
                        Message::Profiles(ProfilesMsg::ProfileChoicePicked(field, index))
                    })
                })
                .collect(),
        };
        config_view::profiles_screen(data, actions, palette)
    }
}
