//! A new task (owner review 2026-11-16, the Codex, OpenCode and Antigravity
//! composers as references). The composer's top row is the project chip;
//! the bottom row is the model chip (`Harness · Model · effort`), the
//! access chip and Send. There are no profiles in it: the model chip opens
//! the picker ([`crate::picker`], fed by `models.catalog`) with Auto, the
//! favorites, Recent, every harness's models and the saved presets; the
//! access chip opens the Codex access menu (Ask for approval / Approve for
//! me / Full access → launch `access` ask / auto / full, remembered in
//! `[desktop] last_access`).
//!
//! "Auto" is the daemon's routing: the launch names no profile and admission
//! picks the first available candidate of the default role. Before launch
//! the picker shows `routing.preview`'s pick and why; after launch the
//! Agent details show the recorded decision. A model choice is an Owner
//! ad-hoc launch (`harness`, `model`, `effort`, `access`), which the daemon
//! validates and freezes; a preset launches its profile as configured.
//!
//! The popovers are drawn in the window above the composer, not as WinUI
//! flyouts: Reactor 0.100.0 cannot close a `Flyout` it opened, and a menu
//! whose callback captures data is replaced (dropping the click) on every
//! render. Their keys come from the window's keyboard hook
//! ([`crate::shortcuts`]) while one is open.

use super::*;
use crate::picker::{self, Choice, Item, Pick, Row};
use crate::settings::Access;
use crate::shortcuts::{Popover as HookPopover, PopoverKey};
use eliot_core::api::ModelsCatalogResponse;

/// A popover above the composer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Pop {
    /// The model picker: a new task's, a preset's, or the open Agent's
    /// (its own route's models, applied through `configure`).
    Model,
    /// The picker's Manage models panel (OpenCode's).
    Manage,
    Access,
    Project,
    /// The open Agent's effort steps and the way into its model picker
    /// (Codex's model-and-effort popover).
    Effort,
    /// The usage ring's popover: the tightest limits and Manage usage.
    Usage,
}

/// The project popover's row that shows the folder field.
pub(super) const OTHER_FOLDER: &str = "other-folder";
/// The project popover's "No project (chat)" row: a chat launch (`chat:
/// true`, no workspace; the daemon makes its private chat directory).
pub(super) const CHAT_PROJECT: &str = "no-project-chat";
/// Focus attempts for a popover's search field, and the wait between them.
const FIELD_FOCUS_TRIES: u8 = 5;
const FIELD_FOCUS_RETRY_MS: u64 = 50;
/// Re-check for the render that bound the highlighted row, one frame later.
const REVEAL_RETRY_MS: u64 = 16;

#[derive(Clone, Debug)]
pub(super) enum LaunchMsg {
    WorkspaceChanged(String),
    LaunchRequested,
    LaunchSubmitted(MutationResult<LaunchOutcome>),
    ProfilesListed(Fallible<ProfileListResponse>),
    /// `routing.preview` for the default role.
    PreviewLoaded(Box<Fallible<eliot_core::RoutingDecision>>),
    /// `models.catalog`, every page.
    CatalogLoaded(Box<Fallible<ModelsCatalogResponse>>),
    /// A failed harness's Retry: read the catalogue again.
    CatalogRetry,
    /// Open a popover, or close it when it is the one open.
    Toggle(Pop),
    OpenModel,
    /// Close the open popover (a click outside it, Esc).
    Close,
    /// The composer's + button: choose files to attach.
    AttachRequested,
    /// Remove one attached file (its path) from the composer.
    AttachmentRemoved(String),
    /// The New worktree chip: ask for a new worktree for this task.
    WorktreeToggled,
    /// The popover's first frame is drawn: let it settle in.
    Entered,
    /// The pointer entered (true) or left the composer's message field,
    /// and whether a button was already down as it entered.
    ComposerPointer {
        over: bool,
        pressed: bool,
    },
    /// The model search field's text.
    PickerQuery(String),
    /// A picker row was clicked (its key).
    PickerChosen(String),
    /// An effort on the highlighted row was clicked: choose the row with it.
    PickerEffort(String, String),
    /// The pointer entered a picker row: highlight it.
    PickerHover(String),
    /// The highlighted row's star.
    PickerFavorite(String),
    AccessChosen(Access),
    /// The pointer entered an access row.
    AccessHover(Access),
    /// The project search field's text.
    ProjectQuery(String),
    /// A project row (its checkout) or [`OTHER_FOLDER`].
    ProjectChosen(String),
    /// The picker's sliders button: open Manage models.
    ManageOpened,
    /// Manage models' back arrow: the picker again.
    ManageClosed,
    /// Manage models' search field.
    ManageQuery(String),
    /// One model's toggle in Manage models.
    ManageModel {
        harness: String,
        provider: String,
        model: String,
        visible: bool,
    },
    /// A provider group's toggle in Manage models (its key, shown).
    ManageGroup(String, bool),
    /// Collapse or expand a picker or Manage models group (its key).
    GroupToggled(String),
    /// A preset's Model field: open the picker for that preset and harness.
    PresetPicker(String, String),
    /// An effort step of the open Agent's effort popover: configure it for
    /// the next turn.
    EffortChosen(String),
    /// A popover key from the keyboard hook.
    Key(PopoverKey),
    /// After the render that bound the highlighted row: bring it into view.
    Reveal(u64),
    /// Put the keyboard in the open popover's search field (tries left).
    FocusField(u8),
    /// The capacity notice's Queue it: submit the refused launch as a
    /// queued intent (`launch.enqueue`).
    QueueRequested,
    /// `launch.enqueue` settled.
    Queued(MutationResult<eliot_core::api::LaunchIntentView>),
}

/// A launch the daemon refused for full capacity, kept so the notice's
/// Queue it submits exactly that launch as a queued intent.
#[derive(Clone, Debug)]
pub(super) struct QueueOffer {
    /// The notice it belongs to: Queue it shows only beside this text.
    pub(super) alert: String,
    target: client::LaunchTarget,
    place: client::LaunchPlace,
    text: String,
    launch_rev: u64,
    /// The intent id of a Queue it whose outcome is not known yet (in
    /// flight, or its reply was lost): the intent may exist, so the draft
    /// is released only after that id resolves.
    intent: Option<String>,
}

impl QueueOffer {
    /// The ledger key holding Queue it's mutation id (the intent id), so a
    /// retry after a lost reply reuses it.
    fn ledger_key(&self) -> String {
        format!(
            "enqueue:{:?}:{:?}:{}",
            self.target, self.place, self.launch_rev
        )
    }
}

/// What dismissing the queue notice did with its draft.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum Dismissed {
    /// No queue notice was open.
    Nothing,
    /// No intent can exist for the draft: it goes back to the composer.
    Returned(String),
    /// Queue it may have committed this intent: the draft waits, watched,
    /// until the intent reads back or the daemon reports it missing.
    Resolving { intent: String, key: String },
}

/// The launch being submitted, for a Queue it after a capacity refusal.
#[derive(Clone, Debug)]
pub(super) struct Submission {
    target: client::LaunchTarget,
    place: client::LaunchPlace,
    text: String,
    launch_rev: u64,
}

/// The new-task draft and its context.
/// How long the popover's first frame stays before it settles in.
const ENTRANCE_FRAME_MS: u64 = 16;

#[derive(Default)]
pub(super) struct LaunchState {
    /// The open popover's first frame: drawn clear and lowered, it then
    /// settles in (the design's 250 ms entrance).
    pub(super) entering: bool,
    /// Files attached in the composer, for the composer they were attached
    /// in (`NEW_TASK` or the open Agent's id).
    pub(super) attachments: Option<(String, Vec<String>)>,
    /// The next task asks for a new worktree (where the project would
    /// otherwise share its checkout).
    pub(super) worktree: bool,
    /// The selected project's profile allowlist (empty: any profile).
    pub(super) profiles: Vec<String>,
    pub(super) workspace: String,
    /// The folder field is shown (a path outside the registered projects).
    pub(super) custom_workspace: bool,
    /// "No project (chat)": the next task launches as a chat.
    pub(super) chat: bool,
    /// The start being submitted: its text and what it says while the
    /// Agent starts (shown at once, until the conversation opens).
    pub(super) starting: Option<(String, String)>,
    /// The picker chooses for this preset (`name`, `harness`) instead of
    /// the composer: Settings › Models & presets' Model field.
    pub(super) preset: Option<(String, String)>,
    /// Manage models' search.
    pub(super) manage_query: String,
    /// Daemon `profile.list` summaries (the presets).
    pub(super) profile_summaries: Vec<ProfileSummary>,
    pub(super) profile_list: Option<ProfileListResponse>,
    pub(super) choice: Choice,
    /// The owner chose in this window (a later preference read keeps it).
    pub(super) chosen: bool,
    pub(super) access: Option<Access>,
    /// The unified catalogue, once read.
    pub(super) catalog: Option<Fallible<ModelsCatalogResponse>>,
    pub(super) catalog_loading: bool,
    /// What routing would pick for a new task now (advisory).
    pub(super) preview: Option<Fallible<eliot_core::RoutingDecision>>,
    pub(super) preview_loading: bool,
    /// A refresh was asked for while one ran: read again when it lands.
    pub(super) preview_again: bool,
    /// The open popover.
    pub(super) open: Option<Pop>,
    pub(super) query: String,
    pub(super) project_query: String,
    /// The highlighted picker or project row (its key).
    pub(super) highlight: Option<String>,
    pub(super) access_highlight: Option<Access>,
    /// The effort step arrow keys moved to in the effort popover (Enter
    /// applies it).
    pub(super) effort_highlight: Option<String>,
    /// Efforts chosen on picker rows with ←/→ (row key → effort).
    pub(super) efforts: HashMap<String, String>,
    /// The open popover's search field.
    pub(super) field_ref: ElementRef<TextBox>,
    /// The highlighted picker row (brought into view by focusing it).
    pub(super) row_ref: ElementRef<Button>,
    pub(super) reveal_epoch: u64,
    pub(super) reveal_after: u64,
    /// The launch in flight (its target, place and text).
    pub(super) submission: Option<Submission>,
    /// A capacity refusal's Queue it offer.
    pub(super) queue_offer: Option<QueueOffer>,
    /// Intents this window queued, with their text, until the arbiter
    /// admits or refuses them.
    pub(super) watched: Vec<(String, String)>,
}

impl LaunchState {
    /// The preset the open picker chooses for, if any.
    pub(super) fn preset_target(&self) -> Option<&(String, String)> {
        self.preset.as_ref()
    }

    /// Presets a new task may use: the daemon's summaries, narrowed to the
    /// project's allowlist when it has one.
    pub(super) fn allowed(&self) -> Vec<&ProfileSummary> {
        self.profile_summaries
            .iter()
            .filter(|profile| self.profiles.is_empty() || self.profiles.contains(&profile.name))
            .collect()
    }

    /// A preset the project no longer allows falls back to Auto.
    pub(super) fn reconcile_choice(&mut self) {
        if let Choice::Preset(name) = &self.choice
            && !self.profile_summaries.is_empty()
            && !self.allowed().iter().any(|profile| &profile.name == name)
        {
            self.choice = Choice::Auto;
        }
    }

    /// The profile routing would pick now, from the last preview.
    pub(super) fn auto_profile(&self) -> Option<&str> {
        self.preview.as_ref()?.as_ref().ok()?.profile.as_deref()
    }

    /// The preset (or Auto's previewed profile) this task launches, if any.
    pub(super) fn profile(&self) -> Option<&ProfileSummary> {
        let name = match &self.choice {
            Choice::Auto => self.auto_profile()?,
            Choice::Preset(name) => name.as_str(),
            Choice::Model(_) => return None,
        };
        self.profile_summaries
            .iter()
            .find(|profile| profile.name == name)
    }

    /// The access level a model choice launches with.
    pub(super) fn access(&self) -> Access {
        self.access.unwrap_or(Access::Ask)
    }

    /// Why a fresh send is refused while the refused launch waits in the
    /// queue offer: sending now could launch the same draft twice. Queue it
    /// (the same-id retry) or dismiss its notice first.
    pub(super) fn send_blocked(&self) -> Option<&'static str> {
        self.queue_offer
            .as_ref()
            .map(|_| "Queue the refused launch or dismiss its notice before starting another.")
    }

    /// Dismiss the queue notice. A draft no intent can exist for returns
    /// to an empty composer. A draft whose Queue it is unconfirmed moves
    /// to the watched intents instead: it returns only when the daemon
    /// reports that intent missing, so it can never be sent beside a
    /// committed intent.
    pub(super) fn dismiss_queue_offer(&mut self) -> Dismissed {
        let Some(offer) = self.queue_offer.take() else {
            return Dismissed::Nothing;
        };
        match offer.intent.clone() {
            Some(intent) => {
                let key = offer.ledger_key();
                self.watched.push((intent.clone(), offer.text));
                Dismissed::Resolving { intent, key }
            }
            None => Dismissed::Returned(offer.text),
        }
    }

    /// Auto's line in the picker: what Swarm picks now, and why.
    pub(super) fn auto_detail(&self) -> String {
        match &self.preview {
            Some(Ok(decision)) => {
                let (headline, skipped) = crate::settings_model::decision_lines(decision);
                match decision.profile.as_deref() {
                    Some(profile) => format!(
                        "Swarm picks {profile}: {}",
                        headline
                            .strip_prefix("Routing picks ")
                            .and_then(|rest| rest.split_once(": ").map(|(_, why)| why))
                            .unwrap_or(&headline)
                    ),
                    None if skipped.is_empty() => headline,
                    None => format!("{headline} ({})", skipped.join("; ")),
                }
            }
            Some(Err(error)) => format!("Swarm's pick is unavailable: {error}"),
            None => "Swarm picks when the task starts".to_owned(),
        }
    }
}

impl DesktopApp {
    /// The picker's rows for the current state.
    pub(super) fn picker_rows(&self) -> Vec<Row> {
        if let Some(detail) = self.popover_agent() {
            // Preview this harness's launch catalogue until the Agent's
            // exact route is known. Only the route's choices can apply.
            let route = self.agent_catalog();
            let preview = self.agent_launch_catalog(&detail.harness);
            let catalog = route.map(|read| {
                read.as_ref()
                    .map(crate::configure::as_catalog)
                    .map_err(Clone::clone)
            });
            let current = Choice::Model(Pick {
                harness: detail.harness.clone(),
                model: detail.settings.next_model().map(str::to_owned),
                effort: detail.settings.next_effort().map(str::to_owned),
            });
            let effort_locked = matches!(
                route,
                Some(Ok(reply)) if reply.configure.effort_refusal.is_some()
            );
            let mut rows = picker::rows(&picker::Inputs {
                catalog: catalog.as_ref().or(preview),
                query: &self.launch.query,
                prefs: &self.prefs,
                only_harness: Some(&detail.harness),
                presets: &[],
                current: &current,
                auto_detail: "",
                efforts: &self.launch.efforts,
                claude_efforts: if effort_locked {
                    &[]
                } else {
                    eliot_daemon::CLAUDE_EFFORT_LEVELS
                },
            });
            if preview.is_some() {
                for row in &mut rows {
                    if let Row::Choice(item) = row {
                        item.disabled.get_or_insert_with(|| {
                            "Checking which models this task can use…".to_owned()
                        });
                    }
                }
            }
            return rows;
        }
        let presets = self.launch.allowed();
        let auto_detail = self.launch.auto_detail();
        let preset_current = self.preset_choice();
        picker::rows(&picker::Inputs {
            catalog: self.launch.catalog.as_ref(),
            query: &self.launch.query,
            prefs: &self.prefs,
            only_harness: self
                .launch
                .preset_target()
                .map(|(_, harness)| harness.as_str()),
            presets: &presets,
            current: preset_current.as_ref().unwrap_or(&self.launch.choice),
            auto_detail: &auto_detail,
            efforts: &self.launch.efforts,
            claude_efforts: eliot_daemon::CLAUDE_EFFORT_LEVELS,
        })
    }

    /// The preset picker's current choice: the preset's model and effort.
    fn preset_choice(&self) -> Option<Choice> {
        let (name, harness) = self.launch.preset_target()?;
        let field = |key: &str| match profiles::field(&self.editor.draft, name, key) {
            profiles::Field::Simple(value) => Some(value),
            _ => None,
        };
        Some(Choice::Model(Pick {
            harness: harness.clone(),
            model: field("model"),
            effort: field("effort"),
        }))
    }

    /// The project popover's rows: `(checkout, name, path)`, filtered;
    /// "No project (chat)" first.
    pub(super) fn project_rows(&self) -> Vec<(String, String, String)> {
        let query = self.launch.project_query.trim().to_lowercase();
        let chat = "no project (chat)";
        let mut rows: Vec<(String, String, String)> = Vec::new();
        if query.is_empty() || chat.contains(&query) {
            rows.push((
                CHAT_PROJECT.to_owned(),
                "No project (chat)".to_owned(),
                "A chat in its own private folder".to_owned(),
            ));
        }
        // Each project, then its subprojects ("app" < "app › x"), as the
        // sidebar lists them. The list comes from daemon registrations and
        // verified checkouts, and an absent checkout cannot start a task.
        let checkouts = self.checkouts();
        let mut seen = HashSet::new();
        let mut projects: Vec<(String, String, String)> = checkouts
            .iter()
            .filter(|checkout| !checkout.missing && seen.insert(nav::path_key(&checkout.path)))
            .map(|checkout| (checkout, self.place_title_in(&checkout.path, &checkouts)))
            .filter(|(checkout, label)| {
                query.is_empty()
                    || label.to_lowercase().contains(&query)
                    || checkout.path.to_lowercase().contains(&query)
            })
            .map(|(checkout, label)| {
                (
                    checkout.path.clone(),
                    label,
                    nav::display_path(&checkout.path).to_owned(),
                )
            })
            .collect();
        projects.sort_by(|a, b| a.1.cmp(&b.1));
        rows.extend(projects);
        rows.push((
            OTHER_FOLDER.to_owned(),
            "Other folder…".to_owned(),
            "Type a folder path".to_owned(),
        ));
        rows
    }

    pub(super) fn launch_update(&mut self, msg: LaunchMsg, context: &ComponentContext<Self>) {
        match msg {
            LaunchMsg::WorkspaceChanged(text) => self.launch.workspace = text,
            LaunchMsg::CatalogLoaded(reply) => {
                self.launch.catalog_loading = false;
                // Stale while revalidating: a failed refresh keeps the last
                // catalogue it read and says why.
                match (*reply, self.launch.catalog.as_ref()) {
                    (Err(error), Some(Ok(_))) => {
                        self.set_status(format!("Models not refreshed: {error}"));
                    }
                    (reply, _) => self.launch.catalog = Some(reply),
                }
                self.keep_highlight();
            }
            LaunchMsg::CatalogRetry => {
                // The Agent picker's Retry re-reads live state: opencode
                // re-opens the live picker (#706); any other open Agent
                // re-reads its own route (#681); only the new-task picker
                // re-reads the unified catalogue.
                if self
                    .popover_agent()
                    .is_some_and(|detail| detail.harness == "opencode")
                {
                    self.open_popover(Pop::Model, context);
                } else if self.popover_agent().is_some() {
                    self.retry_catalog(context);
                } else {
                    self.load_catalog(context);
                }
            }
            LaunchMsg::Toggle(pop) => {
                if self.launch.open == Some(pop) {
                    self.close_popover();
                } else {
                    self.open_popover(pop, context);
                }
            }
            LaunchMsg::OpenModel => self.open_popover(Pop::Model, context),
            LaunchMsg::Close => self.close_popover(),
            LaunchMsg::AttachRequested => {
                self.close_popover();
                match crate::file_dialog::pick_files("Attach files") {
                    Ok(picked) if !picked.is_empty() => {
                        let target = self.composer_target();
                        let mut files = self
                            .launch
                            .attachments
                            .take()
                            .filter(|(owner, _)| *owner == target)
                            .map(|(_, files)| files)
                            .unwrap_or_default();
                        for path in picked {
                            if !files.contains(&path) {
                                files.push(path);
                            }
                        }
                        if files.len() > crate::attachments::MAX {
                            files.truncate(crate::attachments::MAX);
                            self.set_status(format!(
                                "A message carries at most {} files.",
                                crate::attachments::MAX
                            ));
                        }
                        self.launch.attachments = Some((target, files));
                    }
                    Ok(_) => {}
                    Err(error) => self.alert(error),
                }
            }
            LaunchMsg::AttachmentRemoved(path) => {
                if let Some((_, files)) = self.launch.attachments.as_mut() {
                    files.retain(|file| *file != path);
                    if files.is_empty() {
                        self.launch.attachments = None;
                    }
                }
            }
            LaunchMsg::WorktreeToggled => self.launch.worktree = !self.launch.worktree,
            LaunchMsg::Entered => {
                self.launch.entering = false;
                crate::motion::popover_entered();
            }
            LaunchMsg::ComposerPointer { over, pressed } => {
                crate::shortcuts::set_composer_over(over);
                if over && pressed {
                    self.composer_pressed();
                }
            }
            LaunchMsg::PickerQuery(text) => {
                if self.launch.query != text {
                    self.launch.query = text;
                    self.launch.highlight = picker::moved(&self.picker_rows(), None, 0);
                }
            }
            LaunchMsg::PickerChosen(key) => {
                let rows = self.picker_rows();
                if let Some(item) = picker::find(&rows, Some(&key)).cloned() {
                    self.choose(item.choice(), context);
                }
            }
            LaunchMsg::PickerEffort(key, effort) => {
                let rows = self.picker_rows();
                if let Some(item) = picker::find(&rows, Some(&key)).cloned() {
                    self.launch.efforts.insert(key, effort.clone());
                    self.choose(item.choice_with(Some(effort)), context);
                }
            }
            LaunchMsg::PickerHover(key) => {
                if self.launch.highlight.as_deref() != Some(key.as_str()) {
                    self.launch.highlight = Some(key);
                }
            }
            LaunchMsg::PickerFavorite(key) => self.toggle_favorite(&key, context),
            LaunchMsg::AccessChosen(access) => self.choose_access(access, context),
            LaunchMsg::AccessHover(access) => self.launch.access_highlight = Some(access),
            LaunchMsg::ProjectQuery(text) => {
                if self.launch.project_query != text {
                    self.launch.project_query = text;
                    self.launch.highlight = self.project_rows().first().map(|row| row.0.clone());
                }
            }
            LaunchMsg::ProjectChosen(checkout) => {
                self.close_popover();
                self.launch.chat = checkout == CHAT_PROJECT;
                if self.launch.chat {
                    self.launch.custom_workspace = false;
                    self.launch.profiles.clear();
                    self.nav.selection.project = None;
                } else if checkout == OTHER_FOLDER {
                    self.launch.custom_workspace = true;
                    self.launch.workspace.clear();
                    self.launch.profiles.clear();
                    self.nav.selection.project = None;
                } else {
                    self.pick_project(&checkout);
                }
            }
            LaunchMsg::ManageOpened => {
                self.launch.open = Some(Pop::Manage);
                self.launch.manage_query.clear();
                crate::shortcuts::set_popover(HookPopover::Panel);
                self.load_catalog(context);
                self.post_focus_field(FIELD_FOCUS_TRIES, 0, context);
            }
            LaunchMsg::ManageClosed => {
                self.launch.open = Some(Pop::Model);
                self.launch.highlight = picker::initial(&self.picker_rows());
                crate::shortcuts::set_popover(HookPopover::Picker);
                self.post_focus_field(FIELD_FOCUS_TRIES, 0, context);
            }
            LaunchMsg::ManageQuery(text) => self.launch.manage_query = text,
            LaunchMsg::ManageModel {
                harness,
                provider,
                model,
                visible,
            } => {
                if self.prefs.model_visible(&harness, &provider, &model) != visible {
                    self.prefs
                        .set_model_visible(&harness, &provider, &model, visible);
                    self.prefs_rev += 1;
                    self.save_preferences(context);
                }
            }
            LaunchMsg::ManageGroup(key, visible) => {
                let Some(Ok(reply)) = self.launch.catalog.as_ref() else {
                    return;
                };
                let Some((harness, provider)) = key.split_once('/') else {
                    return;
                };
                let models: Vec<String> = reply
                    .models
                    .iter()
                    .filter(|model| model.harness == harness && model.provider == provider)
                    .map(|model| model.id.clone())
                    .collect();
                let ids: Vec<&str> = models.iter().map(String::as_str).collect();
                let already = self.prefs.group_visible(harness, provider)
                    && ids
                        .iter()
                        .all(|id| self.prefs.model_visible(harness, provider, id) == visible);
                if visible && already {
                    return;
                }
                let (harness, provider) = (harness.to_owned(), provider.to_owned());
                self.prefs
                    .set_group_visible(&harness, &provider, &ids, visible);
                self.prefs_rev += 1;
                self.save_preferences(context);
            }
            LaunchMsg::GroupToggled(key) => {
                self.prefs.toggle_group(&key);
                self.prefs_rev += 1;
                self.save_preferences(context);
                self.keep_highlight();
                // The keyboard stays in the search field, so typing filters.
                self.post_focus_field(FIELD_FOCUS_TRIES, 0, context);
            }
            LaunchMsg::PresetPicker(name, harness) => {
                let same = self.launch.preset.as_ref() == Some(&(name.clone(), harness.clone()));
                if same && self.launch.open == Some(Pop::Model) {
                    self.close_popover();
                } else {
                    self.close_popover();
                    self.launch.preset = Some((name, harness));
                    self.open_popover(Pop::Model, context);
                }
            }
            LaunchMsg::EffortChosen(effort) => {
                if self.submitting() || !self.agent_efforts().contains(&effort) {
                    return;
                }
                self.close_popover();
                self.configure_update(
                    configure_ui::ConfigureMsg::Chosen {
                        model: None,
                        effort: Some(effort),
                    },
                    context,
                );
            }
            LaunchMsg::Key(key) => self.popover_key(key, context),
            LaunchMsg::Reveal(epoch) => {
                if epoch != self.launch.reveal_epoch || self.launch.open != Some(Pop::Model) {
                    return;
                }
                // The highlighted row's reference binds when it renders.
                if self.renders.get() <= self.launch.reveal_after {
                    self.post_reveal(epoch, REVEAL_RETRY_MS, context);
                    return;
                }
                // Focusing the row brings it into view (WinUI scrolls a
                // focused element in); the keyboard then goes back to the
                // search field, so typing still filters.
                let later = context.sender();
                let accepted = self.launch.row_ref.request_focus_result(move |_| {
                    let _ = later.send(Message::Launch(LaunchMsg::FocusField(1)));
                });
                if !accepted {
                    self.focus_field(1, context);
                }
            }
            LaunchMsg::FocusField(tries) => self.focus_field(tries, context),
            LaunchMsg::LaunchRequested => self.request_launch(context),
            LaunchMsg::LaunchSubmitted(outcome) => self.launch_submitted(outcome, context),
            LaunchMsg::QueueRequested => self.request_queue(context),
            LaunchMsg::Queued(outcome) => self.queued(outcome, context),
            LaunchMsg::ProfilesListed(result) => match result {
                Ok(list) => {
                    self.launch.profile_summaries = list.profiles.clone();
                    self.launch.profile_list = Some(list);
                    self.launch.reconcile_choice();
                }
                Err(error) => self.alert(format!("Presets not read: {error}")),
            },
            LaunchMsg::PreviewLoaded(result) => {
                self.launch.preview_loading = false;
                self.launch.preview = Some(*result);
                if std::mem::take(&mut self.launch.preview_again) {
                    self.load_routing_preview(context);
                }
            }
        }
    }

    /// The `[desktop]` preferences arrived: a new task starts with the most
    /// recent model and the last access level, unless the owner already
    /// chose in this window.
    pub(super) fn launch_preferences_loaded(&mut self) {
        if self.launch.chosen {
            return;
        }
        self.launch.access = self.prefs.last_access;
        if let Some(recent) = self.prefs.recents.first() {
            self.launch.choice = Choice::Model(Pick {
                harness: recent.harness.clone(),
                model: Some(recent.model.clone()),
                effort: recent.effort.clone(),
            });
        }
    }

    fn open_popover(&mut self, pop: Pop, context: &ComponentContext<Self>) {
        // The composer for a new task, a preset's Model field in Settings,
        // or the open conversation's composer (its effort popover, model
        // picker and usage).
        let preset = self.launch.preset.is_some() && self.main == Main::Settings;
        if !preset && self.main != Main::Conversation {
            return;
        }
        let agent = !preset && self.selected_agent_id().is_some();
        let pop = match (agent, pop) {
            (true, Pop::Effort | Pop::Usage) => pop,
            // A model the harness keeps opens the effort popover, which
            // says so.
            (true, Pop::Model)
                if self.agent_model_allowed()
                    || self.popover_agent().is_some_and(|detail| {
                        detail.harness == "opencode"
                            || self.agent_launch_catalog(&detail.harness).is_some()
                    }) =>
            {
                pop
            }
            (true, Pop::Model) => Pop::Effort,
            (true, _) | (false, Pop::Effort) => return,
            (false, _) => pop,
        };
        if !preset {
            self.launch.preset = None;
        }
        self.launch.open = Some(pop);
        // The entrance: one frame clear and lowered, then the transition.
        self.launch.entering = true;
        let _task = context.spawn_background(|cancel| {
            let cancelled = || cancel.is_cancelled();
            let _ = crate::feed::backoff(ENTRANCE_FRAME_MS, &cancelled);
            Message::Launch(LaunchMsg::Entered)
        });
        match pop {
            Pop::Model => {
                self.launch.query.clear();
                if agent {
                    if self
                        .popover_agent()
                        .is_some_and(|detail| detail.harness == "opencode")
                    {
                        self.configure.catalog = None;
                    }
                    self.ensure_catalog(context);
                } else {
                    // Every open revalidates (the daemon serves a complete
                    // catalogue from its cache and reads a failed harness
                    // again); the rows show the last read meanwhile.
                    self.load_catalog(context);
                }
                self.launch.highlight = picker::initial(&self.picker_rows());
                crate::shortcuts::set_popover(HookPopover::Picker);
                self.post_focus_field(FIELD_FOCUS_TRIES, 0, context);
                self.reveal(context);
            }
            Pop::Manage => self.launch_update(LaunchMsg::ManageOpened, context),
            Pop::Access => {
                self.launch.access_highlight = Some(self.launch.access());
                crate::shortcuts::set_popover(HookPopover::Menu);
            }
            Pop::Project => {
                self.launch.project_query.clear();
                self.launch.highlight = Some(
                    self.selected_project()
                        .map_or_else(|| OTHER_FOLDER.to_owned(), |p| p.workspace.clone()),
                );
                crate::shortcuts::set_popover(HookPopover::Menu);
                self.post_focus_field(FIELD_FOCUS_TRIES, 0, context);
            }
            Pop::Effort => {
                self.launch.effort_highlight = self
                    .chat
                    .detail
                    .as_ref()
                    .and_then(|detail| detail.settings.next_effort())
                    .map(str::to_owned);
                self.ensure_catalog(context);
                // Left and right step the effort, Enter applies it.
                crate::shortcuts::set_popover(HookPopover::Picker);
            }
            Pop::Usage => crate::shortcuts::set_popover(HookPopover::Menu),
        }
    }

    /// A press on the composer's message field (the pointer hook's report,
    /// or the field's `PointerEntered` with a button already down: touch,
    /// or a press without a move first) closes the open popover, so its
    /// keys stop taking the composer's Enter; the press itself reached the
    /// field. A preset's picker in Settings has no composer.
    pub(super) fn composer_pressed(&mut self) {
        if self.main == Main::Conversation && self.launch.preset.is_none() {
            self.close_popover();
        }
    }

    /// Close the open popover; its keys go back to the focused control.
    pub(super) fn close_popover(&mut self) {
        if self.launch.open.take().is_some() {
            crate::shortcuts::set_popover(HookPopover::None);
            if self.launch.preset.take().is_none() {
                // Keyboard focus goes back to the composer (no-op while unbound).
                take_focus(&self.chat.composer_ref);
            }
        }
    }

    /// A choice from the picker: it sets the launch's harness, model and
    /// effort (or Auto, or a preset) and closes the picker.
    fn choose(&mut self, choice: Choice, context: &ComponentContext<Self>) {
        if self.popover_agent().is_some() {
            // The open Agent's picker: the row's model and effort for its
            // next turn.
            if !self.agent_model_allowed() || self.submitting() {
                return;
            }
            self.close_popover();
            if let Choice::Model(pick) = choice {
                self.configure_update(
                    configure_ui::ConfigureMsg::Chosen {
                        model: pick.model,
                        effort: pick.effort,
                    },
                    context,
                );
            }
            return;
        }
        if let Some((name, _)) = self.launch.preset.clone() {
            // A preset's Model field: the model (and an effort chosen on the
            // row) go into the preset's draft, saved at once.
            if let Choice::Model(pick) = choice {
                self.set_preset_model(&name, pick.model.as_deref(), pick.effort.as_deref());
                self.autosave_profiles(context);
            }
            self.close_popover();
            return;
        }
        self.nav.selection.profile = match &choice {
            Choice::Preset(name) => Some(name.clone()),
            _ => None,
        };
        self.launch.choice = choice;
        self.launch.chosen = true;
        self.close_popover();
        if self.launch.choice == Choice::Auto {
            self.load_routing_preview(context);
        }
    }

    fn choose_access(&mut self, access: Access, context: &ComponentContext<Self>) {
        if access == Access::Full && self.launch.choice.harness() == Some("opencode") {
            // The daemon refuses it (no full-access mapping); say so here.
            return;
        }
        self.launch.access = Some(access);
        self.launch.chosen = true;
        self.close_popover();
        if self.prefs.last_access != Some(access) {
            self.prefs.last_access = Some(access);
            self.prefs_rev += 1;
            self.save_preferences(context);
        }
    }

    fn toggle_favorite(&mut self, key: &str, context: &ComponentContext<Self>) {
        let rows = self.picker_rows();
        let Some(model) = picker::find(&rows, Some(key)).and_then(Item::model_ref) else {
            return;
        };
        self.prefs.toggle_favorite(model);
        self.prefs_rev += 1;
        self.save_preferences(context);
        self.keep_highlight();
    }

    /// A row the highlight names may be gone (a read, a favorite): keep it
    /// when shown, else the first row.
    pub(super) fn keep_highlight(&mut self) {
        if self.launch.open != Some(Pop::Model) {
            return;
        }
        let rows = self.picker_rows();
        if picker::find(&rows, self.launch.highlight.as_deref()).is_none() {
            self.launch.highlight = picker::initial(&rows);
        }
    }

    fn popover_key(&mut self, key: PopoverKey, context: &ComponentContext<Self>) {
        let Some(pop) = self.launch.open else {
            return;
        };
        match key {
            PopoverKey::Escape if pop == Pop::Manage => {
                self.launch_update(LaunchMsg::ManageClosed, context);
                return;
            }
            // Tab returns the keyboard to the composer: the popover would
            // otherwise keep taking its Enter.
            PopoverKey::Escape | PopoverKey::Tab => {
                self.close_popover();
                return;
            }
            _ => {}
        }
        match pop {
            Pop::Manage | Pop::Usage => {}
            Pop::Effort => {
                let efforts = self.agent_efforts();
                let at = self
                    .launch
                    .effort_highlight
                    .as_deref()
                    .and_then(|effort| efforts.iter().position(|known| known == effort));
                match key {
                    PopoverKey::Left | PopoverKey::Right if !efforts.is_empty() => {
                        let next = match (at, key == PopoverKey::Right) {
                            (None, _) => 0,
                            (Some(at), true) => (at + 1).min(efforts.len() - 1),
                            (Some(at), false) => at.saturating_sub(1),
                        };
                        self.launch.effort_highlight = efforts.get(next).cloned();
                    }
                    PopoverKey::Enter => {
                        if let Some(effort) = self.launch.effort_highlight.clone() {
                            self.launch_update(LaunchMsg::EffortChosen(effort), context);
                        }
                    }
                    _ => {}
                }
            }
            Pop::Model => {
                let rows = self.picker_rows();
                let highlight = self.launch.highlight.clone();
                match key {
                    PopoverKey::Up | PopoverKey::Down => {
                        let step = if key == PopoverKey::Up { -1 } else { 1 };
                        self.launch.highlight = picker::moved(&rows, highlight.as_deref(), step);
                        self.reveal(context);
                    }
                    PopoverKey::Left | PopoverKey::Right => {
                        if let Some(item) = picker::find(&rows, highlight.as_deref())
                            && let Some(effort) = item.step_effort(key == PopoverKey::Right)
                        {
                            self.launch.efforts.insert(item.key.clone(), effort);
                        }
                    }
                    PopoverKey::Enter => {
                        if let Some(item) = picker::find(&rows, highlight.as_deref()).cloned() {
                            self.choose(item.choice(), context);
                        }
                    }
                    PopoverKey::Favorite => {
                        if let Some(key) = highlight {
                            self.toggle_favorite(&key, context);
                        }
                    }
                    PopoverKey::Escape | PopoverKey::Tab => {}
                }
            }
            Pop::Access => {
                let levels = self.access_levels();
                let at = self
                    .launch
                    .access_highlight
                    .and_then(|access| levels.iter().position(|(level, _)| *level == access))
                    .unwrap_or(0);
                match key {
                    PopoverKey::Up => {
                        self.launch.access_highlight = Some(levels[at.saturating_sub(1)].0);
                    }
                    PopoverKey::Down => {
                        self.launch.access_highlight =
                            Some(levels[(at + 1).min(levels.len() - 1)].0);
                    }
                    PopoverKey::Enter => {
                        if let Some((access, None)) = levels.get(at).cloned() {
                            self.choose_access(access, context);
                        }
                    }
                    _ => {}
                }
            }
            Pop::Project => {
                let rows = self.project_rows();
                let at = self
                    .launch
                    .highlight
                    .as_deref()
                    .and_then(|key| rows.iter().position(|row| row.0 == key))
                    .unwrap_or(0);
                match key {
                    PopoverKey::Up => {
                        self.launch.highlight = Some(rows[at.saturating_sub(1)].0.clone());
                    }
                    PopoverKey::Down => {
                        self.launch.highlight = Some(rows[(at + 1).min(rows.len() - 1)].0.clone());
                    }
                    PopoverKey::Enter => {
                        let chosen = rows[at].0.clone();
                        self.launch_update(LaunchMsg::ProjectChosen(chosen), context);
                    }
                    _ => {}
                }
            }
        }
    }

    /// The access levels and, for each, why it cannot be chosen now.
    pub(super) fn access_levels(&self) -> Vec<(Access, Option<&'static str>)> {
        Access::ALL
            .into_iter()
            .map(|access| {
                let refused = (access == Access::Full
                    && self.launch.choice.harness() == Some("opencode"))
                .then_some("OpenCode has no full-access mode");
                (access, refused)
            })
            .collect()
    }

    /// After the next render, bring the highlighted picker row into view.
    fn reveal(&mut self, context: &ComponentContext<Self>) {
        self.launch.reveal_epoch += 1;
        self.launch.reveal_after = self.renders.get();
        self.post_reveal(self.launch.reveal_epoch, 0, context);
    }

    fn post_reveal(&self, epoch: u64, delay_ms: u64, context: &ComponentContext<Self>) {
        let _task = context.spawn_background(move |cancel| {
            let cancelled = || cancel.is_cancelled();
            if delay_ms > 0 {
                let _ = feed::backoff(delay_ms, &cancelled);
            }
            Message::Launch(LaunchMsg::Reveal(epoch))
        });
    }

    fn post_focus_field(&self, tries: u8, delay_ms: u64, context: &ComponentContext<Self>) {
        let _task = context.spawn_background(move |cancel| {
            let cancelled = || cancel.is_cancelled();
            if delay_ms > 0 {
                let _ = feed::backoff(delay_ms, &cancelled);
            }
            Message::Launch(LaunchMsg::FocusField(tries))
        });
    }

    /// Focus the open popover's search field; a refused or unbound focus
    /// is tried again, `tries` times in all.
    fn focus_field(&mut self, tries: u8, context: &ComponentContext<Self>) {
        if !matches!(
            self.launch.open,
            Some(Pop::Model | Pop::Manage | Pop::Project)
        ) {
            return;
        }
        let Some(left) = tries.checked_sub(1) else {
            return;
        };
        let later = context.sender();
        let accepted = self.launch.field_ref.request_focus_result(move |result| {
            if !matches!(result, Ok(true)) && left > 0 {
                let _ = later.send(Message::Launch(LaunchMsg::FocusField(left)));
            }
        });
        if !accepted && left > 0 {
            self.post_focus_field(left, FIELD_FOCUS_RETRY_MS, context);
        }
    }

    /// Read the unified model catalogue (every page) on a worker.
    pub(super) fn load_catalog(&mut self, context: &ComponentContext<Self>) {
        if self.launch.catalog_loading {
            return;
        }
        let Ok(data_dir) = self.data_dir.clone() else {
            return;
        };
        self.launch.catalog_loading = true;
        let _task = context.spawn_background(move |_| {
            Message::Launch(LaunchMsg::CatalogLoaded(Box::new(
                client::block_on(config_client::models_catalog(&data_dir))
                    .map_err(|error| error.to_string()),
            )))
        });
    }

    fn request_launch(&mut self, context: &ComponentContext<Self>) {
        if self.submitting() {
            return;
        }
        // The refused draft waits in the queue offer: it is queued (the
        // same-id retry) or dismissed before another send. Its notice, with
        // Queue it, stays in view; the reason goes to the status line.
        if let Some(reason) = self.launch.send_blocked() {
            if let Some(alert) = self.launch.queue_offer.as_ref().map(|o| o.alert.clone()) {
                self.alert(alert);
            }
            self.set_status(reason);
            return;
        }
        let Ok(data_dir) = self.data_dir.clone() else {
            return;
        };
        let target = match self.launch_target() {
            Ok(target) => target,
            Err(error) => {
                self.alert(format!("Cannot start: {error}."));
                return;
            }
        };
        let text = profiles::normalize_newlines(self.chat.drafts.borrow().text(None));
        if text.trim().is_empty() {
            self.alert("Describe the task first.");
            return;
        }
        let text = crate::attachments::compose(&text, self.attached_to(NEW_TASK_COMPOSER));
        if !self.launch.chat && self.launch.workspace.trim().is_empty() {
            self.alert("Choose a project, a folder, or No project (chat).");
            return;
        }
        self.close_popover();
        let place = if self.launch.chat {
            client::LaunchPlace::Chat
        } else if self.worktree_chip() == WorktreeChip::Toggle(true) {
            client::LaunchPlace::Worktree(self.launch.workspace.clone())
        } else {
            client::LaunchPlace::Workspace(self.launch.workspace.clone())
        };
        // Bound now: the picker stays editable while the launch is in flight.
        let submitted = Submitted {
            choice: self.launch.choice.clone(),
            harness: self.starting_harness(),
            access: self.launch.access(),
            chat: self.launch.chat,
            workspace: self.launch.workspace.clone(),
            project: self.launch_project_id(),
        };
        let launch_rev = self.chat.drafts.borrow().launch_rev();
        let key = format!("launch:{target:?}:{place:?}:{launch_rev}");
        let mutation_id = self.ledger.id_for(&key);
        self.in_flight_key = Some(key);
        self.alert = None;
        self.launch.queue_offer = None;
        self.launch.submission = Some(Submission {
            target: target.clone(),
            place: place.clone(),
            text: text.clone(),
            launch_rev,
        });
        // Shown at once: the message and what happens now, until the
        // conversation opens (no empty pause after Send).
        self.launch.starting = Some((text.clone(), starting_label(submitted.harness.as_deref())));
        // The composer empties at once; the revision stays, so a retry of
        // an unconfirmed start reuses its mutation id.
        self.chat.drafts.borrow_mut().clear_launch();
        self.clear_attached(NEW_TASK_COMPOSER);
        self.set_status(format!("Launching ({mutation_id})…"));
        let _task = context.spawn_background(move |_| {
            Message::Launch(LaunchMsg::LaunchSubmitted(client::block_on(async {
                let reply = client::submit_launch(
                    &data_dir,
                    mutation_id,
                    target,
                    &place,
                    Some(text.clone()),
                )
                .await?;
                Ok(LaunchOutcome {
                    // Admitted without an Agent id: the outcome is
                    // unknown, so the id is kept for a same-id retry.
                    agent: reply.agent.clone().ok_or_else(|| {
                        eliot_core::Error::new(
                            "unknown",
                            "the launch was admitted without an Agent id",
                        )
                    })?,
                    generation: reply.generation.unwrap_or_default(),
                    command_id: reply.command_id.clone(),
                    linked_command: reply.linked_command.clone(),
                    text,
                    launch_rev,
                    submitted,
                })
            })))
        });
    }

    fn launch_submitted(
        &mut self,
        outcome: MutationResult<LaunchOutcome>,
        context: &ComponentContext<Self>,
    ) {
        // Keyboard focus goes back to the composer (no-op while it is unbound).
        take_focus(&self.chat.composer_ref);
        self.settle_mutation(
            outcome
                .as_ref()
                .err()
                .is_none_or(|error| !crate::state::uncertain(error)),
        );
        let starting = self.launch.starting.take();
        match outcome {
            Ok(outcome) => {
                let label = starting.map(|(_, label)| label);
                // The first prompt rode the launch admission; it is never
                // sent again. Clear only the submitted draft.
                let mut drafts = self.chat.drafts.borrow_mut();
                if drafts.launch_rev() == outcome.launch_rev {
                    drafts.clear_launch();
                }
                drop(drafts);
                if let Some(linked) = outcome.linked_command.clone() {
                    self.chat.pending_local =
                        Some((outcome.agent.clone(), linked.clone(), outcome.text.clone()));
                    self.chat
                        .launches
                        .insert(outcome.agent.clone(), (linked, outcome.submitted.clone()));
                }
                self.chat.waiting_label = label;
                self.set_status(format!(
                    "Launched {} ({}); its first input is {}.",
                    outcome.agent,
                    outcome.command_id,
                    outcome.linked_command.as_deref().unwrap_or("not linked")
                ));
                // The submitted model goes first in Recent, whatever the
                // picker shows now.
                if let Some(model) = outcome.submitted.recent() {
                    self.prefs.remember(model);
                    self.prefs_rev += 1;
                    self.save_preferences(context);
                }
                // Until the list reads it back, the title is the first
                // message, known now.
                self.nav.pending_select = Some(outcome.pending_agent(crate::handoff::now_ms()));
                self.nav.selection.select_agent(Some(outcome.agent));
                self.load_agents(context, None);
                // Capacity changed: the next Auto pick may differ.
                self.load_routing_preview(context);
            }
            Err(error) if crate::state::uncertain(&error) => {
                self.restore_launch_text(starting);
                self.alert(format!(
                    "Start not confirmed (sending again reuses its id): {}",
                    crate::settings_model::launch_error(&error)
                ));
            }
            Err(error) => {
                let text = format!(
                    "Start refused: {}",
                    crate::settings_model::launch_error(&error)
                );
                // Full capacity is not a dead end: the notice offers the
                // same launch as a queued intent the arbiter admits when a
                // slot frees (`launch.enqueue`); nothing retries by itself.
                let offer = (error.code == "capacity_unavailable")
                    .then(|| self.launch.submission.take())
                    .flatten()
                    .map(|submission| QueueOffer {
                        alert: format!("{text}. Queue it to start when a slot frees."),
                        target: submission.target,
                        place: submission.place,
                        text: submission.text,
                        launch_rev: submission.launch_rev,
                        intent: None,
                    });
                match offer {
                    Some(offer) => {
                        // The draft waits in the offer, not the composer:
                        // no restore, so it cannot be sent as a fresh
                        // launch too. Dismissing returns it.
                        self.alert(offer.alert.clone());
                        self.launch.queue_offer = Some(offer);
                    }
                    None => {
                        self.restore_launch_text(starting);
                        self.alert(text);
                    }
                }
            }
        }
        self.launch.submission = None;
    }

    /// Queue it: the refused launch as a durable intent with its own
    /// ledger-held id (a retry after an unconfirmed reply reads back).
    fn request_queue(&mut self, context: &ComponentContext<Self>) {
        if self.submitting() {
            self.alert("Waiting for the last action to settle.");
            return;
        }
        let Ok(data_dir) = self.data_dir.clone() else {
            return;
        };
        let Some(offer) = self.launch.queue_offer.as_mut() else {
            return;
        };
        let key = offer.ledger_key();
        let mutation_id = self.ledger.id_for(&key);
        // From here the intent may exist until its outcome reads back.
        offer.intent = Some(mutation_id.clone());
        let offer = offer.clone();
        self.in_flight_key = Some(key);
        self.set_status(format!("Queueing the launch ({mutation_id})…"));
        let _task = context.spawn_background(move |_| {
            Message::Launch(LaunchMsg::Queued(client::block_on(client::enqueue_launch(
                &data_dir,
                mutation_id,
                offer.target,
                &offer.place,
                Some(offer.text),
            ))))
        });
    }

    fn queued(
        &mut self,
        outcome: MutationResult<eliot_core::api::LaunchIntentView>,
        context: &ComponentContext<Self>,
    ) {
        self.settle_mutation(
            outcome
                .as_ref()
                .err()
                .is_none_or(|error| !crate::state::uncertain(error)),
        );
        match outcome {
            Ok(intent) => {
                let offer = self.launch.queue_offer.take();
                // The queued intent carries the text now; it comes back if
                // the arbiter refuses the intent.
                if let Some(offer) = offer {
                    let mut drafts = self.chat.drafts.borrow_mut();
                    if offer.launch_rev == drafts.launch_rev() {
                        drafts.clear_launch();
                    }
                    drop(drafts);
                    self.launch.watched.push((intent.id.clone(), offer.text));
                }
                self.alert = None;
                let wait = intent
                    .reason
                    .as_ref()
                    .map_or_else(|| "waiting for its turn".to_owned(), nav::wait_reason_words);
                self.set_status(format!("Queued launch {}: {wait}.", intent.id));
                // The project's queued launches show their wait reason.
                self.load_organization(context);
                self.load_queue(context);
            }
            Err(error) if crate::state::uncertain(&error) => {
                // The intent may be committed. The offer keeps its id and
                // the notice names the lost reply with Queue it beside it
                // (the same-id retry reads the intent back); the draft
                // stays in the offer, never back in the composer.
                let text = format!(
                    "Queueing not confirmed: {}. Queue it again reuses its id.",
                    error.user_message()
                );
                if let Some(offer) = self.launch.queue_offer.as_mut() {
                    offer.alert = text.clone();
                }
                self.set_status(text.clone());
                self.alert(text);
            }
            Err(error) => {
                // A definite refusal: no intent exists, so the draft goes
                // back to an empty composer.
                if let Some(offer) = self.launch.queue_offer.take() {
                    self.chat.drafts.borrow_mut().restore_launch(offer.text);
                }
                self.alert(format!("Not queued: {}", error.user_message()));
            }
        }
    }

    /// Dismiss the alert's queue notice: a draft no intent can exist for
    /// returns to the composer; an unconfirmed Queue it resolves its intent
    /// id first (the queue poll reads it back or reports it missing).
    pub(super) fn dismiss_queue_notice(&mut self, context: &ComponentContext<Self>) {
        match self.launch.dismiss_queue_offer() {
            Dismissed::Nothing => {}
            Dismissed::Returned(text) => {
                self.chat.drafts.borrow_mut().restore_launch(text);
                self.set_status("The refused launch is back in the composer.");
            }
            Dismissed::Resolving { intent, key } => {
                // The watched entry carries the id now; a later Queue it of
                // a returned draft takes a fresh one.
                self.ledger.complete(&key);
                self.set_status(format!(
                    "Checking queued launch {intent}: its text comes back if it was never queued."
                ));
                self.load_queue(context);
            }
        }
    }

    /// Report what became of this window's queued launches: an admitted
    /// one lists with the Agents, a refused one says why and gives its
    /// text back to an empty composer, and one the daemon does not know
    /// (an unconfirmed Queue it that never committed) gives its text back
    /// too. A queued one stays watched.
    pub(super) fn settle_queued_launches(&mut self, context: &ComponentContext<Self>) {
        use eliot_core::api::LaunchIntentState;
        for id in self.nav.organization.missing_intents.clone() {
            let Some(index) = self
                .launch
                .watched
                .iter()
                .position(|(watched, _)| *watched == id)
            else {
                continue;
            };
            let (_, text) = self.launch.watched.remove(index);
            self.chat.drafts.borrow_mut().restore_launch(text);
            self.alert(format!(
                "Launch {id} was never queued; its text is back in the composer."
            ));
        }
        let intents = self.nav.organization.intents.clone();
        let mut admitted = false;
        for intent in intents {
            let Some(index) = self
                .launch
                .watched
                .iter()
                .position(|(id, _)| *id == intent.id)
            else {
                continue;
            };
            match intent.state {
                LaunchIntentState::Queued => {}
                LaunchIntentState::Admitted => {
                    self.launch.watched.remove(index);
                    admitted = true;
                    self.set_status(format!(
                        "Queued launch {} started ({}).",
                        intent.id,
                        intent
                            .command_id
                            .as_deref()
                            .unwrap_or("command not read back")
                    ));
                }
                LaunchIntentState::Rejected => {
                    let (_, text) = self.launch.watched.remove(index);
                    self.chat.drafts.borrow_mut().restore_launch(text);
                    let reason = intent
                        .reason
                        .as_ref()
                        .map_or_else(|| "no reason read back".to_owned(), nav::wait_reason_words);
                    self.alert(format!("Queued launch {} refused: {reason}.", intent.id));
                }
            }
        }
        if admitted {
            self.load_agents(context, None);
        }
    }

    /// The registration ID the pending launch row carries: the explicitly
    /// chosen registered project's ID, when the launch still runs in its
    /// checkout. "" for chats and for every other folder: the desktop
    /// never re-implements the daemon's project resolution, so those rows
    /// stay unknown until the daemon Agent row arrives. Never the
    /// workspace path: the daemon's project label is a registration ID,
    /// not a directory.
    fn launch_project_id(&self) -> String {
        if self.launch.chat {
            return String::new();
        }
        chosen_project_id(
            &self.nav.projects,
            self.nav.selection.project.as_deref(),
            &self.launch.workspace,
        )
    }

    /// A start that did not go through gives its text back to the composer
    /// (with the same draft revision, so a retry reuses the mutation id),
    /// unless the owner already typed something new.
    fn restore_launch_text(&mut self, starting: Option<(String, String)>) {
        if let Some((text, _)) = starting {
            self.chat.drafts.borrow_mut().restore_launch(text);
        }
    }

    /// The harness a launch of the current choice starts: the chosen
    /// model's, or Swarm's pick for Auto and presets.
    fn starting_harness(&self) -> Option<String> {
        match &self.launch.choice {
            Choice::Model(pick) => Some(pick.harness.clone()),
            _ => self.launch.profile().map(|profile| profile.harness.clone()),
        }
    }

    /// Write the preset's model (and an effort chosen in the picker) into
    /// the profiles draft.
    fn set_preset_model(&mut self, name: &str, model: Option<&str>, effort: Option<&str>) {
        let mut draft = self.editor.draft.clone();
        for (key, value) in [("model", model), ("effort", effort)] {
            let current = match profiles::field(&draft, name, key) {
                profiles::Field::Simple(value) => Some(value),
                _ => None,
            };
            if key == "effort" && value.is_none() {
                continue;
            }
            if current.as_deref() == value {
                continue;
            }
            match profiles::set_field(&draft, name, key, value) {
                Ok(next) => draft = next,
                Err(error) => {
                    self.editor.status = error;
                    return;
                }
            }
        }
        self.editor.view = draft.clone();
        self.editor.draft = draft;
    }

    /// Ask routing what it would pick for a new task now.
    pub(super) fn load_routing_preview(&mut self, context: &ComponentContext<Self>) {
        if self.launch.preview_loading {
            // The running read may predate the change that asked.
            self.launch.preview_again = true;
            return;
        }
        let Ok(data_dir) = self.data_dir.clone() else {
            return;
        };
        self.launch.preview_loading = true;
        let _task = context.spawn_background(move |_| {
            Message::Launch(LaunchMsg::PreviewLoaded(Box::new(
                client::block_on(config_client::routing_preview(&data_dir, None))
                    .map_err(|error| error.to_string()),
            )))
        });
    }

    pub(super) fn launch_target(&self) -> Result<LaunchTarget, String> {
        match &self.launch.choice {
            Choice::Model(pick) => {
                let access = self.launch.access();
                if access == Access::Full && pick.harness == "opencode" {
                    return Err(
                        "OpenCode has no full-access mode; choose Ask for approval or Approve for me"
                            .to_owned(),
                    );
                }
                Ok(LaunchTarget::Adhoc {
                    harness: pick.harness.clone(),
                    model: pick.model.clone(),
                    effort: pick.effort.clone(),
                    access: Some(access.wire().to_owned()),
                })
            }
            Choice::Preset(name) => Ok(LaunchTarget::Profile(name.clone())),
            // Admission routes; a miss is refused there with its reasons.
            Choice::Auto => Ok(LaunchTarget::Routed),
        }
    }

    pub(super) fn load_profile_list(&mut self, context: &ComponentContext<Self>) {
        let Ok(data_dir) = self.data_dir.clone() else {
            return;
        };
        let _task = context.spawn_background(move |_| {
            Message::Launch(LaunchMsg::ProfilesListed(client::block_on(async {
                config_client::list_profiles(&data_dir)
                    .await
                    .map_err(|error| error.to_string())
            })))
        });
    }
}

/// The pending row's project ID: the explicitly chosen registration's
/// ID, when the launch still runs in that registration's checkout. Every
/// other folder (a typed path, a subfolder, a linked checkout the daemon
/// alone resolves) yields none: the row stays unknown until the daemon
/// Agent row arrives.
fn chosen_project_id(projects: &[UiProject], selected: Option<&str>, workspace: &str) -> String {
    let Some(selected) = selected else {
        return String::new();
    };
    let key = nav::path_key(workspace);
    projects
        .iter()
        .find(|project| project.id == selected && nav::path_key(&project.workspace) == key)
        .map(|project| project.id.clone())
        .unwrap_or_default()
}

impl Submitted {
    /// The Recent entry a launch records: the submitted model and effort.
    fn recent(&self) -> Option<crate::settings::ModelRef> {
        match &self.choice {
            Choice::Model(pick) => pick.model_ref(),
            _ => None,
        }
    }
}

impl LaunchOutcome {
    /// The new Agent's row until the list reads it back: the submitted
    /// harness and place, titled by the first message. Its scope stays
    /// unknown until the daemon row arrives: the submitted ID must not
    /// stand in for the scope, and pickers never offer this row meanwhile.
    fn pending_agent(&self, now: i64) -> UiAgent {
        UiAgent {
            id: self.agent.clone(),
            generation: self.generation,
            harness: self.submitted.harness.clone().unwrap_or_default(),
            state: eliot_core::AgentState::Created,
            status_line: None,
            project_id: if self.submitted.chat {
                String::new()
            } else {
                self.submitted.project.clone()
            },
            project_scope: None,
            pending: true,
            repo_common_dir: None,
            workspace: if self.submitted.chat {
                None
            } else {
                Some(self.submitted.workspace.clone())
            },
            parent: None,
            updated_at: now,
            title: nav::title(&self.text).unwrap_or_else(|| self.agent.clone()),
            native_child: false,
            tokens: None,
            chat: self.submitted.chat,
            archived: false,
            // The open row renders above the list; the daemon's group
            // arrives with the list read.
            organization: None,
            tags: Vec::new(),
            organization_revision: None,
            model: None,
            work: None,
        }
    }
}

/// What the conversation says while a new task starts: "Starting Codex…".
pub(super) fn starting_label(harness: Option<&str>) -> String {
    match harness {
        Some(harness) => format!("Starting {}…", picker::harness_label(harness)),
        None => "Starting…".to_owned(),
    }
}

/// A running Agent's access chip: its frozen access level when the launch
/// named one, else its profile's permissions.
pub(super) fn agent_access_label(settings: &crate::configure::Settings) -> String {
    match settings.access.as_deref() {
        Some("ask") => Access::Ask.label().to_owned(),
        Some("auto") => Access::Auto.label().to_owned(),
        Some("full") => Access::Full.label().to_owned(),
        _ => access_label(settings.permissions.as_deref()),
    }
}

/// The access chip's words for a profile's permissions.
pub(super) fn access_label(permissions: Option<&str>) -> String {
    use eliot_core::api::native_profile::CodexPermissions;
    let Some(value) = permissions else {
        return "Default access".to_owned();
    };
    match CodexPermissions::parse(value) {
        Some(CodexPermissions::DangerFullAccess) => "Full access".to_owned(),
        Some(CodexPermissions::ReadOnly) => "Read only".to_owned(),
        Some(CodexPermissions::WorkspaceWrite) => "Workspace write".to_owned(),
        None => value.replace(['-', '_'], " "),
    }
}

/// The new task composer's key for its attached files.
pub(super) const NEW_TASK_COMPOSER: &str = "new-task";

/// How the composer shows where a new task works.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum WorktreeChip {
    /// A chat, or no folder yet.
    Hidden,
    /// The project prepares a worktree for every task (its `workspace =
    /// "worktree"`, the default for a registered project): a fixed fact.
    Fixed,
    /// The folder is shared unless a new worktree is asked for (on).
    Toggle(bool),
}

impl DesktopApp {
    /// The composer files attach to: the open Agent's, else the new task's.
    pub(super) fn composer_target(&self) -> String {
        self.selected_agent_id()
            .unwrap_or_else(|| NEW_TASK_COMPOSER.to_owned())
    }

    /// The files attached in `target`'s composer.
    pub(super) fn attached_to(&self, target: &str) -> &[String] {
        match &self.launch.attachments {
            Some((owner, files)) if owner == target => files,
            _ => &[],
        }
    }

    /// The message carrying `target`'s attachments went out.
    pub(super) fn clear_attached(&mut self, target: &str) {
        if self
            .launch
            .attachments
            .as_ref()
            .is_some_and(|(owner, _)| owner == target)
        {
            self.launch.attachments = None;
        }
    }

    /// Where the next task works, as the daemon decides it: a registered
    /// project prepares a worktree per top-level task unless its settings
    /// share the checkout; any other folder is shared unless asked.
    pub(super) fn worktree_chip(&self) -> WorktreeChip {
        if self.launch.chat || self.launch.workspace.trim().is_empty() {
            return WorktreeChip::Hidden;
        }
        let key = nav::path_key(&self.launch.workspace);
        let registered = self
            .nav
            .projects
            .iter()
            .any(|project| nav::path_key(&project.workspace) == key);
        if registered && !self.settings.shares_checkout(&self.launch.workspace) {
            WorktreeChip::Fixed
        } else {
            WorktreeChip::Toggle(self.launch.worktree)
        }
    }
}
