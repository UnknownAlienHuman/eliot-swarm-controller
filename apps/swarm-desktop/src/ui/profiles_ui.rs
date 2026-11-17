//! Profile editing: one draft for form and source over the `profiles`
//! settings section. A form change (a dropdown, the Model picker,
//! Duplicate) saves at once through Settings' revision-checked writer;
//! typed TOML keeps Settings' save bar (`settings.save`); see
//! [`super::settings_ui`].

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
    /// The draft holds typed TOML not saved yet: form changes then wait
    /// for the save bar instead of saving it unseen.
    pub(super) raw_dirty: bool,
    /// The draft the running save sent.
    pub(super) sending: Option<String>,
    /// The draft the last save wrote, until a read shows it (the read's
    /// TOML is formatted anew, so a draft equal to it follows the read).
    pub(super) sent: Option<String>,
}

impl DesktopApp {
    pub(super) fn profiles_update(&mut self, msg: ProfilesMsg, context: &ComponentContext<Self>) {
        match msg {
            ProfilesMsg::ProfilesDraftChanged(text) => {
                if self.editor.editable {
                    let draft = profiles::normalize_newlines(&text);
                    if draft != self.editor.draft {
                        self.editor.raw_dirty = draft != self.editor.base;
                        self.editor.draft = draft;
                    }
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
                        self.autosave_profiles(context);
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
                            self.editor.status = format!("Duplicated {from} as {to}.");
                            self.autosave_profiles(context);
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

    /// Save a form change of the presets at once, unless typed TOML is
    /// waiting for the save bar.
    pub(super) fn autosave_profiles(&mut self, context: &ComponentContext<Self>) {
        if !self.editor.raw_dirty {
            self.autosave(settings_ui::Page::Models, context);
        }
    }

    /// A model's name as the catalogue gives it ("GPT-6.1 Sol"), else its
    /// id (a model the catalogue does not list, such as Claude's aliases).
    pub(super) fn model_name(&self, harness: &str, model: &str) -> String {
        match self.launch.catalog.as_ref() {
            Some(Ok(reply)) => reply
                .models
                .iter()
                .find(|entry| entry.harness == harness && entry.id == model)
                .map(|entry| {
                    if harness == "opencode" {
                        crate::picker::short_model(&entry.display_name).to_owned()
                    } else {
                        entry.display_name.clone()
                    }
                })
                .filter(|name| !name.trim().is_empty())
                .unwrap_or_else(|| model.to_owned()),
            _ => model.to_owned(),
        }
    }

    /// A preset's line in the presets list: its app, model name and
    /// effort ("Codex · GPT-6.1 Sol · xhigh"), and whether it can launch.
    pub(super) fn preset_detail(&self, profile: &eliot_core::api::ProfileSummary) -> String {
        let mut line = format!(
            "{} · {} · {}",
            crate::picker::harness_label(&profile.harness),
            profile.model.as_deref().map_or_else(
                || "Default model".to_owned(),
                |model| self.model_name(&profile.harness, model)
            ),
            profile.effort.as_deref().map_or_else(
                || "default effort".to_owned(),
                |effort| format!("{effort} effort")
            )
        );
        if !profile.launchable {
            line.push_str(" · cannot launch now");
        }
        line
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
        // The model's name, not its id; "Default" leaves the choice to the
        // app.
        let label = model.as_deref().map_or_else(
            || "Default".to_owned(),
            |model| self.model_name(&harness, model),
        );
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
        // Anchored to the field: just under it, at its left edge (the form
        // row's label column and gap). Where the field's column is
        // narrower than the list, the list ends at the card's right edge
        // instead, so it is never cut off. The layer behind it closes it.
        let field_left = tokens::layout::LABEL_WIDTH + tokens::spacing::SM;
        let left = match self.settings_column_width() {
            Some(column) => {
                let inner = column - 2.0 * tokens::spacing::MD;
                if inner - field_left < tokens::layout::PICKER_WIDTH {
                    (inner - tokens::layout::PICKER_WIDTH).max(0.0)
                } else {
                    field_left
                }
            }
            None => field_left,
        };
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
                    .margin(Thickness::new(left, tokens::height::FORM_ROW, 0.0, 0.0))
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
            detail: &|profile| self.preset_detail(profile),
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
