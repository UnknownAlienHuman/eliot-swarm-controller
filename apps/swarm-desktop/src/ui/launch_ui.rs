//! The one launch picker: configured profile or ad-hoc harness/model/effort, workspace and the first message.

use super::*;
use crate::theme::winui::Palette;

#[derive(Clone, Debug)]
pub(super) enum LaunchMsg {
    WorkspaceChanged(String),
    ProfileTextChanged(String),
    ProfileSelected(Option<usize>),
    LaunchRequested,
    LaunchSubmitted(Fallible<LaunchOutcome>),
    ProfilesListed(Fallible<ProfileListResponse>),
    PickerModeSelected(Option<usize>),
    AdhocHarnessSelected(Option<usize>),
    AdhocModelChanged(String),
    AdhocEffortChanged(String),
    HarnessModelsRequested,
    HarnessModelsLoaded(String, Fallible<String>),
}

/// The launch picker and the new-conversation draft.
pub(super) struct LaunchState {
    pub(super) profiles: Vec<String>,
    pub(super) profile_idx: Option<usize>,
    pub(super) profile_text: String,
    pub(super) workspace: String,
    pub(super) launch_text: String,
    pub(super) launch_rev: u64,
    /// Daemon `profile.list` summaries for the launch picker.
    pub(super) profile_summaries: Vec<ProfileSummary>,
    pub(super) profile_list: Option<ProfileListResponse>,
    pub(super) picker_adhoc: bool,
    pub(super) adhoc_harness_idx: Option<usize>,
    pub(super) adhoc_model: String,
    pub(super) adhoc_effort: String,
    pub(super) harness_hint: String,
}

impl DesktopApp {
    pub(super) fn launch_update(&mut self, msg: LaunchMsg, context: &ComponentContext<Self>) {
        match msg {
            LaunchMsg::WorkspaceChanged(text) => self.launch.workspace = text,
            LaunchMsg::ProfileTextChanged(text) => self.launch.profile_text = text,
            LaunchMsg::ProfileSelected(index) => {
                self.launch.profile_idx = index;
                self.nav.selection.profile = index
                    .and_then(|index| self.picker_profiles().get(index).map(|p| p.name.clone()));
            }
            LaunchMsg::LaunchRequested => {
                if self.submitting() {
                    return;
                }
                let Ok(data_dir) = self.data_dir.clone() else {
                    return;
                };
                let target = match self.launch_target() {
                    Ok(target) => target,
                    Err(error) => {
                        self.set_status(format!("Cannot launch: {error}."));
                        return;
                    }
                };
                let text = profiles::normalize_newlines(&self.launch.launch_text);
                if text.trim().is_empty() || self.launch.workspace.trim().is_empty() {
                    self.set_status("Type the workspace and the first message before launching.");
                    return;
                }
                let key = format!(
                    "launch:{target:?}:{}:{}",
                    self.launch.workspace, self.launch.launch_rev
                );
                let mutation_id = self.ledger.id_for(&key);
                self.in_flight_key = Some(key);
                let workspace = self.launch.workspace.clone();
                let launch_rev = self.launch.launch_rev;
                self.set_status(format!("Launching ({mutation_id})…"));
                let _task = context.spawn_background(move |_| {
                    Message::Launch(LaunchMsg::LaunchSubmitted(client::block_on(async {
                        let reply = client::submit_launch(
                            &data_dir,
                            mutation_id,
                            target,
                            &workspace,
                            Some(text.clone()),
                        )
                        .await
                        .map_err(|error| error.to_string())?;
                        Ok(LaunchOutcome {
                            agent: reply
                                .agent
                                .clone()
                                .ok_or_else(|| "launch admitted no agent".to_owned())?,
                            generation: reply.generation.unwrap_or_default(),
                            command_id: reply.command_id.clone(),
                            linked_command: reply.linked_command.clone(),
                            text,
                            launch_rev,
                        })
                    })))
                });
            }
            LaunchMsg::LaunchSubmitted(outcome) => {
                // Keyboard focus goes back to the composer (no-op while it is unbound).
                let _ = self.chat.composer_ref.request_focus();
                self.settle_mutation(outcome.is_ok());
                match outcome {
                    Ok(outcome) => {
                        // The first prompt rode the launch admission; it is
                        // never sent again. Clear only the submitted draft.
                        if self.launch.launch_rev == outcome.launch_rev {
                            self.launch.launch_text.clear();
                        }
                        if let Some(linked) = outcome.linked_command.clone() {
                            self.chat.pending_local =
                                Some((outcome.agent.clone(), linked, outcome.text.clone()));
                        }
                        self.set_status(format!(
                            "Launched {} ({}); its first input is {}.",
                            outcome.agent,
                            outcome.command_id,
                            outcome.linked_command.as_deref().unwrap_or("not linked")
                        ));
                        self.nav.pending_select = Some(UiAgent {
                            id: outcome.agent.clone(),
                            generation: outcome.generation,
                            harness: String::new(),
                            state: "created".to_owned(),
                        });
                        self.nav.selection.select_agent(Some(outcome.agent));
                        self.load_agents(context, None);
                    }
                    Err(error) => {
                        self.set_status(format!(
                            "Launch not confirmed (Launch again reuses its id): {error}"
                        ));
                    }
                }
            }
            LaunchMsg::ProfilesListed(result) => match result {
                Ok(list) => {
                    self.launch.profile_summaries = list.profiles.clone();
                    // Keep the chosen profile by name: a refreshed list may
                    // reorder, add or drop profiles.
                    self.launch.profile_idx =
                        self.nav.selection.profile.as_ref().and_then(|name| {
                            self.picker_profiles()
                                .iter()
                                .position(|profile| &profile.name == name)
                        });
                    if self.launch.profile_idx.is_none() {
                        self.nav.selection.profile = None;
                    }
                    self.launch.profile_list = Some(list);
                }
                Err(error) => self.set_status(format!("profile.list failed: {error}")),
            },
            LaunchMsg::PickerModeSelected(index) => self.launch.picker_adhoc = index == Some(1),
            LaunchMsg::AdhocHarnessSelected(index) => {
                self.launch.adhoc_harness_idx = index;
                self.launch.harness_hint.clear();
            }
            LaunchMsg::AdhocModelChanged(text) => self.launch.adhoc_model = text,
            LaunchMsg::AdhocEffortChanged(text) => self.launch.adhoc_effort = text,
            LaunchMsg::HarnessModelsRequested => {
                let (Ok(data_dir), Some(harness)) = (
                    self.data_dir.clone(),
                    self.launch
                        .adhoc_harness_idx
                        .and_then(|index| ADHOC_HARNESSES.get(index)),
                ) else {
                    return;
                };
                let harness = (*harness).to_owned();
                self.launch.harness_hint = format!("Reading the {harness} catalogue…");
                let _task = context.spawn_background(move |_| {
                    let hint = client::block_on(async {
                        client::harness_models(&data_dir, &harness, None)
                            .await
                            .map(|reply| {
                                let default = reply
                                    .models
                                    .iter()
                                    .find(|model| model.default)
                                    .or_else(|| reply.models.first());
                                let mut hint = format!(
                                    "Unset model: {} · unset effort: {}",
                                    reply.default.as_deref().unwrap_or("the native default"),
                                    default
                                        .and_then(|model| model.default_effort.as_deref())
                                        .unwrap_or("the native default"),
                                );
                                if !reply.models.is_empty() {
                                    let names: Vec<&str> = reply
                                        .models
                                        .iter()
                                        .take(8)
                                        .map(|m| m.id.as_str())
                                        .collect();
                                    hint.push_str(&format!(
                                        " · {} model(s): {}{}",
                                        reply.models.len(),
                                        names.join(", "),
                                        if reply.models.len() > 8 { ", …" } else { "" }
                                    ));
                                }
                                if let Some(note) = reply.note {
                                    hint.push_str(&format!(" · {note}"));
                                }
                                hint
                            })
                            .map_err(|error| error.to_string())
                    });
                    Message::Launch(LaunchMsg::HarnessModelsLoaded(harness, hint))
                });
            }
            LaunchMsg::HarnessModelsLoaded(harness, hint) => {
                self.launch.harness_hint = match hint {
                    Ok(hint) => hint,
                    Err(error) => format!("{harness} catalogue unavailable: {error}"),
                };
            }
        }
    }

    /// Launch-picker profiles: the daemon's summaries, narrowed to the
    /// selected project's allowlist when one is selected.
    pub(super) fn picker_profiles(&self) -> Vec<&ProfileSummary> {
        self.launch
            .profile_summaries
            .iter()
            .filter(|profile| {
                self.launch.profiles.is_empty() || self.launch.profiles.contains(&profile.name)
            })
            .collect()
    }

    pub(super) fn launch_profile(&self) -> Option<String> {
        if self.launch.profile_summaries.is_empty() {
            Some(self.launch.profile_text.trim().to_owned()).filter(|profile| !profile.is_empty())
        } else {
            self.launch
                .profile_idx
                .and_then(|index| self.picker_profiles().get(index).map(|p| p.name.clone()))
        }
    }

    pub(super) fn launch_target(&self) -> Result<LaunchTarget, String> {
        if self.launch.picker_adhoc {
            let harness = self
                .launch
                .adhoc_harness_idx
                .and_then(|index| ADHOC_HARNESSES.get(index))
                .ok_or("choose a harness")?;
            Ok(LaunchTarget::Adhoc {
                harness: (*harness).to_owned(),
                model: Some(self.launch.adhoc_model.trim().to_owned()).filter(|v| !v.is_empty()),
                effort: Some(self.launch.adhoc_effort.trim().to_owned()).filter(|v| !v.is_empty()),
            })
        } else {
            self.launch_profile()
                .map(LaunchTarget::Profile)
                .ok_or_else(|| "choose or type a configured profile".to_owned())
        }
    }

    pub(super) fn load_profile_list(&mut self, context: &ComponentContext<Self>) {
        let Ok(data_dir) = self.data_dir.clone() else {
            return;
        };
        let _task = context.spawn_background(move |_| {
            Message::Launch(LaunchMsg::ProfilesListed(client::block_on(async {
                client::list_profiles(&data_dir)
                    .await
                    .map_err(|error| error.to_string())
            })))
        });
    }

    /// The one launch picker beside the first message (issue #407 §1): a
    /// configured profile with its requested summary, or an ad-hoc
    /// harness/model/effort, plus the workspace. Requested settings only;
    /// the Agent detail shows the effective readback after launch.
    pub(super) fn launch_card(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        let adhoc = self.launch.picker_adhoc;
        let mode = |label: &str, name: &str, index: usize, selected: bool| {
            m3::segment(palette, selected)
                .automation_name(if selected {
                    format!("{name} (selected)")
                } else {
                    name.to_owned()
                })
                .on_click(
                    context.message(Message::Launch(LaunchMsg::PickerModeSelected(Some(index)))),
                )
                .content(TextBlock::new().text(label.to_owned()))
        };
        let modes = StackPanel::new()
            .orientation(Orientation::Horizontal)
            .spacing(theme::spacing::XS)
            .children((
                mode("Profile", "Launch mode: configured profile", 0, !adhoc),
                mode("Ad-hoc harness", "Launch mode: ad-hoc harness", 1, adhoc),
            ));

        let picker_profiles = self.picker_profiles();
        let selected_summary = self
            .launch
            .profile_idx
            .and_then(|index| picker_profiles.get(index).copied());
        let field_width = 220.0;
        let (inputs, note): (View, String) = if adhoc {
            (
                StackPanel::new()
                    .orientation(Orientation::Horizontal)
                    .spacing(theme::spacing::SM)
                    .children((
                        ComboBox::new()
                            .items_source(ADHOC_HARNESSES.map(str::to_owned).to_vec())
                            .selected_index(self.launch.adhoc_harness_idx)
                            .automation_name("Ad-hoc harness")
                            .on_selection_changed(context.callback(|value| {
                                Message::Launch(LaunchMsg::AdhocHarnessSelected(value))
                            })),
                        m3::field(palette)
                            .text(self.launch.adhoc_model.clone())
                            .placeholder_text("Model (harness default)")
                            .automation_name("Ad-hoc model")
                            .width(field_width)
                            .on_text_changed(context.callback(|value| {
                                Message::Launch(LaunchMsg::AdhocModelChanged(value))
                            })),
                        m3::field(palette)
                            .text(self.launch.adhoc_effort.clone())
                            .placeholder_text("Effort (harness default)")
                            .automation_name("Ad-hoc effort")
                            .width(160.0)
                            .on_text_changed(context.callback(|value| {
                                Message::Launch(LaunchMsg::AdhocEffortChanged(value))
                            })),
                        m3::text_button(palette)
                            .on_click(
                                context.message(Message::Launch(LaunchMsg::HarnessModelsRequested)),
                            )
                            .content(TextBlock::new().text("Harness defaults")),
                    )),
                self.launch.harness_hint.clone(),
            )
        } else if !self.launch.profile_summaries.is_empty() {
            (
                ComboBox::new()
                    .items_source(
                        picker_profiles
                            .iter()
                            .map(|profile| profile.name.clone())
                            .collect::<Vec<_>>(),
                    )
                    .selected_index(self.launch.profile_idx)
                    .placeholder_text("Profile")
                    .automation_name("Profile")
                    .width(field_width)
                    .on_selection_changed(
                        context
                            .callback(|value| Message::Launch(LaunchMsg::ProfileSelected(value))),
                    )
                    .into(),
                selected_summary.map_or_else(
                    || {
                        "Pick a profile: its requested harness, model and route appear here."
                            .to_owned()
                    },
                    config_view::summary_line,
                ),
            )
        } else if self.launch.profiles.is_empty() {
            (
                m3::field(palette)
                    .text(self.launch.profile_text.clone())
                    .placeholder_text("Configured profile name")
                    .automation_name("Profile")
                    .width(field_width)
                    .on_text_changed(
                        context.callback(|value| {
                            Message::Launch(LaunchMsg::ProfileTextChanged(value))
                        }),
                    )
                    .into(),
                String::new(),
            )
        } else {
            (
                ComboBox::new()
                    .items_source(self.launch.profiles.clone())
                    .selected_index(self.launch.profile_idx)
                    .placeholder_text("Profile")
                    .automation_name("Profile")
                    .width(field_width)
                    .on_selection_changed(
                        context
                            .callback(|value| Message::Launch(LaunchMsg::ProfileSelected(value))),
                    )
                    .into(),
                String::new(),
            )
        };
        let workspace = m3::field(palette)
            .text(self.launch.workspace.clone())
            .placeholder_text("Workspace path")
            .automation_name("Workspace")
            .on_text_changed(
                context.callback(|value| Message::Launch(LaunchMsg::WorkspaceChanged(value))),
            );

        m3::card(palette).content(
            StackPanel::new().spacing(theme::spacing::SM).children((
                Grid::new()
                    .columns([GridLength::Auto, GridLength::Auto, GridLength::STAR])
                    .column_spacing(theme::spacing::MD)
                    .children((
                        modes,
                        Border::new().grid_column(1).content(inputs),
                        workspace.grid_column(2),
                    )),
                m3::muted(note, theme::type_scale::BODY_SMALL).text_wrapping(TextWrapping::Wrap),
            )),
        )
    }
}
