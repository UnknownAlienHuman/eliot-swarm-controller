//! Profile editing: one draft for form and source over the `profiles`
//! settings section. Settings' save bar validates and saves it
//! (`settings.save`, revision-checked); see [`super::settings_ui`].

use super::*;
use crate::theme::winui::Palette;

#[derive(Clone, Debug)]
pub(super) enum ProfilesMsg {
    ProfilesDraftChanged(String),
    ProfileEditSelected(Option<usize>),
    /// A form dropdown (its index in the selected profile's fields) picked
    /// a choice (None: items replaced).
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
                    if self.editor.selected.as_ref() != Some(name) {
                        self.close_popover();
                    }
                    self.editor.selected = Some(name.clone());
                }
            }
            ProfilesMsg::ProfileChoicePicked(field, index) => {
                // A ComboBox reports None when its items are replaced.
                let (Some(index), Some(name)) = (index, self.editor.selected.clone()) else {
                    return;
                };
                let Some(form) = self.preset_fields(&name).get(field).cloned() else {
                    return;
                };
                let key = form.key;
                let current = match profiles::field(&self.editor.draft, &name, key) {
                    profiles::Field::Simple(value) => Some(value),
                    _ => None,
                };
                let choices = profiles::choices(&form.values, current.as_deref());
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

    /// The harness a preset in the draft names.
    pub(super) fn preset_harness(&self, name: &str) -> Option<String> {
        match profiles::field(&self.editor.draft, name, "harness") {
            profiles::Field::Simple(harness) => Some(harness),
            _ => None,
        }
    }

    /// A preset's enumerated fields with its harness's values (effort from
    /// the catalogue of its model when the catalogue reports it).
    pub(super) fn preset_fields(&self, name: &str) -> Vec<profiles::FormField> {
        let Some(harness) = self.preset_harness(name) else {
            return Vec::new();
        };
        let model = match profiles::field(&self.editor.draft, name, "model") {
            profiles::Field::Simple(model) => Some(model),
            _ => None,
        };
        let efforts = match (self.launch.catalog.as_ref(), model.as_deref()) {
            (Some(Ok(reply)), Some(model)) => reply
                .models
                .iter()
                .find(|entry| entry.harness == harness && entry.id == model)
                .and_then(|entry| entry.efforts.clone()),
            _ => None,
        };
        profiles::form_fields(&harness, efforts.as_deref())
    }

    /// The selected preset's Model control: a field styled like the form's
    /// dropdowns (width, outline, chevron) naming its model, and, while
    /// open, the composer's catalogue picker filtered to its harness as a
    /// popover over the rows below it (with a layer a click on closes it).
    fn preset_model_control(
        &self,
        name: &str,
        context: &mut ViewContext<Self>,
        palette: Palette,
    ) -> Option<(View, Option<View>)> {
        let harness = self.preset_harness(name)?;
        let model = match profiles::field(&self.editor.draft, name, "model") {
            profiles::Field::Simple(model) => Some(model),
            profiles::Field::Absent => None,
            profiles::Field::Complex => {
                return Some((
                    m3::secondary("Complex value: edit it as TOML")
                        .vertical_alignment(VerticalAlignment::Center)
                        .into(),
                    None,
                ));
            }
        };
        let label = model
            .clone()
            .unwrap_or_else(|| format!("{} default", crate::picker::harness_label(&harness)));
        let open = self.launch.open == Some(launch_ui::Pop::Model)
            && self
                .launch
                .preset_target()
                .is_some_and(|(preset, _)| preset == name);
        let field = m3::field_button(palette)
            .automation_name(format!(
                "Profile Model: {label} ({})",
                if open {
                    "list open"
                } else {
                    "choose from the list"
                }
            ))
            .is_enabled(!self.settings.reading)
            .on_click(context.message(Message::Launch(LaunchMsg::PresetPicker(
                name.to_owned(),
                harness,
            ))))
            .content(m3::field_button_content(&label));
        let popover = open.then(|| {
            Grid::new().children((
                Border::new()
                    .background(Brush::Solid(Color::transparent()))
                    .on_pointer_pressed(
                        context.callback(|_: PointerEventInfo| Message::Launch(LaunchMsg::Close)),
                    ),
                Border::new()
                    .horizontal_alignment(HorizontalAlignment::Left)
                    .vertical_alignment(VerticalAlignment::Top)
                    .margin(Thickness::new(
                        tokens::layout::LABEL_WIDTH + tokens::spacing::SM,
                        tokens::height::FORM_ROW,
                        0.0,
                        0.0,
                    ))
                    .content(self.model_popover(context, palette)),
            ))
        });
        Some((field, popover))
    }

    pub(super) fn profiles_view(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        let list = self.launch.profile_list.as_ref();
        let selected = self
            .editor
            .selected
            .clone()
            .filter(|_| self.editor.editable);
        let fields = selected
            .as_deref()
            .map(|name| self.preset_fields(name))
            .unwrap_or_default();
        let (model, model_popover) = selected
            .as_deref()
            .and_then(|name| self.preset_model_control(name, context, palette))
            .map_or((None, None), |(field, popover)| (Some(field), popover));
        let count = fields.len();
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
            fields,
            model,
            model_popover,
        };
        let actions = ProfilesActions {
            draft_changed: context
                .callback(|value| Message::Profiles(ProfilesMsg::ProfilesDraftChanged(value))),
            select: context
                .callback(|value| Message::Profiles(ProfilesMsg::ProfileEditSelected(value))),
            duplicate_name_changed: context
                .callback(|value| Message::Profiles(ProfilesMsg::DuplicateNameChanged(value))),
            duplicate: context.message(Message::Profiles(ProfilesMsg::DuplicateRequested)),
            toggle_source: context.message(Message::Profiles(ProfilesMsg::SourceToggled)),
            choice_picked: (0..count)
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
