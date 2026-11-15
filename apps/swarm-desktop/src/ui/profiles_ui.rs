//! Profile editing: one draft for form and source, validate, revision-checked save.

use super::*;
use crate::theme::winui::Palette;

#[derive(Clone, Debug)]
pub(super) enum ProfilesMsg {
    ProfilesSourceLoaded(Fallible<ProfilesReadResponse>),
    ProfilesDraftChanged(String),
    ProfileEditSelected(Option<usize>),
    ProfileFieldChanged(usize, String),
    DuplicateNameChanged(String),
    DuplicateRequested,
    ProfilesValidateRequested,
    ProfilesSaveRequested,
    ProfilesSaved(bool, Fallible<ProfilesSaveResponse>),
    ProfilesReloadRequested,
}

/// Profiles screen: one document draft over the daemon's profile source.
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
    pub(super) busy: bool,
    pub(super) loaded: bool,
}

impl DesktopApp {
    pub(super) fn profiles_update(&mut self, msg: ProfilesMsg, context: &ComponentContext<Self>) {
        match msg {
            ProfilesMsg::ProfilesSourceLoaded(result) => {
                self.editor.busy = false;
                self.editor.loaded = true;
                match result {
                    Ok(read) => {
                        // The TextBox reports LF-normalized text; compare
                        // drafts in that form (the daemon keeps CRLF files CRLF).
                        let source = profiles::normalize_newlines(&read.source.unwrap_or_default());
                        self.editor.revision = read.revision;
                        self.editor.base = source.clone();
                        self.editor.view = source.clone();
                        self.editor.draft = source;
                        self.editor.editable = read.editable;
                        self.editor.reason = read.reason;
                        let names = profiles::profile_names(&self.editor.draft);
                        if self
                            .editor
                            .selected
                            .as_ref()
                            .is_none_or(|name| !names.contains(name))
                        {
                            self.editor.selected = names.first().cloned();
                        }
                    }
                    Err(error) => {
                        self.editor.editable = false;
                        self.editor.reason = Some(format!("config.profiles.read failed: {error}"));
                    }
                }
            }
            ProfilesMsg::ProfilesDraftChanged(text) => {
                if self.editor.editable && !self.editor.busy {
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
            ProfilesMsg::ProfilesValidateRequested => self.save_profiles(context, true),
            ProfilesMsg::ProfilesSaveRequested => self.save_profiles(context, false),
            ProfilesMsg::ProfilesSaved(dry_run, result) => {
                self.editor.busy = false;
                match result {
                    Ok(reply) if dry_run => {
                        self.editor.status = format!(
                            "Valid: {} profile(s) parse and resolve for launch ({}). Not saved yet.",
                            reply.profiles.len(),
                            reply.profiles.join(", ")
                        );
                    }
                    Ok(reply) => {
                        self.editor.status = format!(
                            "Saved revision {}; the next launch uses it ({}).",
                            reply
                                .revision
                                .as_deref()
                                .map_or("?", |r| &r[..12.min(r.len())]),
                            reply.profiles.join(", ")
                        );
                        self.editor.loaded = false;
                        self.load_profile_source(context);
                        self.load_profile_list(context);
                    }
                    Err(error) if error.starts_with("unknown:") => {
                        self.editor.status = format!(
                            "Save outcome uncertain; Reload to see the current file (the draft is kept): {error}"
                        );
                    }
                    Err(error) if error.starts_with("conflict:") => {
                        // The file changed on disk under this draft: nothing
                        // was written by this save, and Reload would discard
                        // the draft, so say both.
                        self.editor.status = format!(
                            "Not saved: config.toml changed on disk after this draft was read. Copy what you need from the draft, then Reload to edit the current file. ({error})"
                        );
                    }
                    Err(error) => {
                        self.editor.status = format!(
                            "{} The draft is kept and the saved file is unchanged: {error}",
                            if dry_run { "Invalid." } else { "Not saved." }
                        );
                    }
                }
            }
            ProfilesMsg::ProfilesReloadRequested => {
                self.editor.status = "Reloaded the saved profile sections.".to_owned();
                self.load_profile_source(context);
                self.load_profile_list(context);
            }
        }
    }

    pub(super) fn load_profile_source(&mut self, context: &ComponentContext<Self>) {
        let Ok(data_dir) = self.data_dir.clone() else {
            return;
        };
        self.editor.busy = true;
        let _task = context.spawn_background(move |_| {
            Message::Profiles(ProfilesMsg::ProfilesSourceLoaded(client::block_on(async {
                config_client::read_profiles_source(&data_dir)
                    .await
                    .map_err(|error| error.to_string())
            })))
        });
    }

    pub(super) fn save_profiles(&mut self, context: &ComponentContext<Self>, dry_run: bool) {
        let Ok(data_dir) = self.data_dir.clone() else {
            return;
        };
        if self.editor.busy || !self.editor.editable {
            return;
        }
        self.editor.busy = true;
        self.editor.status = if dry_run {
            "Validating with the daemon parser and launch resolution…".to_owned()
        } else {
            "Saving…".to_owned()
        };
        let revision = self.editor.revision.clone();
        let draft = self.editor.draft.clone();
        let _task = context.spawn_background(move |_| {
            Message::Profiles(ProfilesMsg::ProfilesSaved(
                dry_run,
                client::block_on(async {
                    config_client::save_profiles(&data_dir, revision, draft, dry_run)
                        .await
                        .map_err(|error| error.to_string())
                }),
            ))
        });
    }

    pub(super) fn profiles_view(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        let list = self.launch.profile_list.as_ref();
        let data = ProfilesData {
            revision: self.editor.revision.as_deref(),
            file_valid: list.is_none_or(|list| list.valid),
            file_error: list.and_then(|list| list.error.as_deref()),
            editable: self.editor.editable,
            reason: self.editor.reason.as_deref(),
            draft: &self.editor.draft,
            view: self.primed(&self.editor.view),
            dirty: self.editor.draft != self.editor.base,
            selected: self.editor.selected.as_deref(),
            duplicate_name: &self.editor.duplicate_name,
            status: &self.editor.status,
            busy: self.editor.busy,
            summaries: &self.launch.profile_summaries,
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
            validate: context.message(Message::Profiles(ProfilesMsg::ProfilesValidateRequested)),
            save: context.message(Message::Profiles(ProfilesMsg::ProfilesSaveRequested)),
            reload: context.message(Message::Profiles(ProfilesMsg::ProfilesReloadRequested)),
        };
        config_view::profiles_screen(data, actions, palette)
    }
}
