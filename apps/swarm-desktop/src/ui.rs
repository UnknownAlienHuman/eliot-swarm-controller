//! Desktop screen: one native WinUI 3 window through Reactor.
//!
//! Donor: `windows-reactor` 0.100.0 (Microsoft windows-rs,
//! https://github.com/microsoft/windows-rs), MIT OR Apache-2.0, rust-version
//! 1.95. Used as a direct dependency; no donor source is copied.
//!
//! Data path: every read and mutation runs on a Reactor background worker
//! and calls [`crate::client`] / [`crate::feed`], which use the existing
//! typed daemon RPC and its validators. The UI thread never performs RPC.
//! Each control binds its Agent, generation and turn/request at click time
//! (see [`crate::state::BoundAction`]); later selection changes cannot
//! retarget an in-flight submission. Results carry the selection epoch and
//! are dropped when the conversation they were read for is no longer shown.
//!
//! Reactor 0.100.0 integration gaps, stated rather than papered over:
//! `ScrollViewer` exposes no offset, `ViewChanged`, `ChangeView` or anchor
//! ratio, so the transcript cannot detect "at the bottom" or auto-follow.
//! A "N new" jump button cannot know it is already at the bottom, so there
//! is none (k98: it stayed shown at the bottom); opening a conversation
//! focuses the end anchor, which WinUI brings into view. There is no
//! font-family property (code is not monospace), no
//! clipboard (see [`crate::clipboard`]) and no key events on `TextBox`: Enter
//! sends through the window's thread keyboard hook, which marks the line
//! break that Enter without Shift typed into the composer
//! ([`crate::shortcuts::take_plain_enter`]); Shift+Enter keeps it.
//!
//! Checked against windows-rs master `f37e2f9046cf9710fe00285436c4d22ad6053d72`
//! (2026-11-14, unreleased `windows-reactor` 0.101.0, MIT OR Apache-2.0):
//! it adds `TextBlock::font_family`, `Border::on_preview_key_down` /
//! `on_key_up` / `on_character_received` (a `RoutedCallback<KeyEventInfo>`
//! whose `bool` sets `Handled`), and `ElementRef::request_notification`.
//! It still has no `ScrollViewer`/`ScrollView` view-changed event, offset,
//! extent or viewport readback, `ChangeView`/`ScrollTo`/`StartBringIntoView`
//! or `VerticalAnchorRatio`, no `ElementRef` for scroll viewers, no `TextBox`
//! key events, no Shift `AcceleratorModifiers` variant and no IME
//! composition events. It is not pinned: conditional bottom-follow stays
//! impossible there, and a Git pin makes every lockfile resolution
//! (including both uncached CI jobs) fetch the 1.48 GB windows-rs
//! repository. Monospace code waits for a published release that carries
//! `font_family`.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::{Arc, Mutex};

use eliot_core::Command;
use eliot_core::api::{
    AgentGetResponse, AgentListRequest, CommandSubmitResponse, ProfileListResponse, ProfileSummary,
    ReparentResponse, RuleListEntry, ScriptListResponse, ScriptReadResponse, ScriptSaveResponse,
};
use windows_reactor::*;

mod automation_ui;
mod chat_ui;
mod configure_ui;
mod handoff_ui;
mod launch_ui;
mod menu_ui;
mod monitor_ui;
mod picker_ui;
mod profiles_ui;
mod scripts_ui;
mod settings_ui;
mod shell_ui;
pub(crate) mod tokens;

use automation_ui::AutomationMsg;
use chat_ui::ChatMsg;
use configure_ui::ConfigureMsg;
use launch_ui::LaunchMsg;
use menu_ui::MenuMsg;
use monitor_ui::MonitorMsg;
use profiles_ui::ProfilesMsg;
use scripts_ui::ScriptsMsg;
use settings_ui::{Page, SettingsMsg};
use shell_ui::{Main, Panel, ShellMsg};

use crate::chat::{Conversation, Invalidation};
use crate::client::{self, LaunchTarget};
use crate::config_client;
use crate::config_view::{self, ProfilesActions, ProfilesData, RuleNamespace};
use crate::configure;
use crate::feed::{self, Delivery, Depth, Reread, Snapshot};
use crate::live::{self, LiveStatus, Next};
use crate::m3;
use crate::markdown::Doc;
use crate::monitor::{self, Monitor, MonitorSnapshot};
use crate::monitor_view;
use crate::nav::{self, UiAgent};
use crate::profiles;
use crate::reparent;
use crate::scripts;
use crate::state::{BoundAction, DraftStore, MutationLedger, Selection};
use crate::theme;
use crate::views::{self, RenderCache, RenderedItem, RowCallbacks};

/// Agent rows per `agent.list` page in the navigation list.
const AGENT_PAGE: usize = 100;
/// Harnesses an Owner ad-hoc launch accepts (the daemon validates again).
const ADHOC_HARNESSES: [&str; 5] = ["codex", "claude", "muse", "opencode", "antigravity"];

/// UI copy of one project registration.
#[derive(Clone, Debug)]
struct UiProject {
    id: String,
    /// What the owner reads: the id, or the folder of a discovered project.
    name: String,
    profiles: Vec<String>,
    workspace: String,
    /// The Git common directory registered with the checkout: its
    /// repository, which the sidebar groups by.
    common_dir: String,
}

/// One page of the navigation list with its honest counters.
#[derive(Clone, Debug)]
struct AgentPage {
    rows: Vec<UiAgent>,
    next: Option<String>,
    total: usize,
    /// The archived Agents the default list leaves out (first page only).
    archived: nav::Archived,
}

/// UI copy of one pending request with its exact control binding.
#[derive(Clone, Debug)]
struct UiPending {
    binding: BoundAction,
    kind: String,
    summary: String,
}

/// Readback for the selected Agent.
#[derive(Clone, Debug)]
struct UiDetail {
    generation: i64,
    active_turn: Option<String>,
    pending: Vec<UiPending>,
    harness: String,
    settings: configure::Settings,
    state: String,
    /// Observe-only Agents reject native control verbs.
    observe_only: bool,
    /// The project label (its checkout).
    project: String,
    /// The routing decision recorded at admission (None: an explicit
    /// profile or harness).
    routing: Option<eliot_core::RoutingDecision>,
}

/// Admission of one draft submission (Send, Steer or Resume): bound Agent,
/// draft revision and submitted text.
#[derive(Clone, Debug)]
struct SendOutcome {
    what: &'static str,
    agent: String,
    command_id: String,
    /// A resume's linked `<id>/send`, when it carried text.
    linked: Option<String>,
    draft_rev: u64,
    text: String,
    state: String,
    recovered: bool,
    /// The send that followed a steer the ended turn refused.
    fallback: bool,
}

/// Admission of one launch-with-text: the new Agent and its linked input,
/// with the choice that was actually submitted (the picker stays editable
/// while the launch is in flight, so completion never reads it again).
#[derive(Clone, Debug)]
struct LaunchOutcome {
    agent: String,
    generation: i64,
    command_id: String,
    linked_command: Option<String>,
    text: String,
    launch_rev: u64,
    submitted: Submitted,
}

/// What one launch submitted: the picker choice (harness, model, effort),
/// its access level and where it runs.
#[derive(Clone, Debug, PartialEq)]
struct Submitted {
    choice: crate::picker::Choice,
    /// The harness that starts: the chosen model's, or the preset's.
    harness: Option<String>,
    access: crate::settings::Access,
    chat: bool,
    workspace: String,
}

type Fallible<T> = Result<T, String>;
/// One rule namespace read: its rules and whether more remain.
type RulePage = (Vec<RuleListEntry>, bool);

/// Messages are plain owned data so background workers can return them.
#[derive(Clone, Debug)]
enum Message {
    Shell(ShellMsg),
    Launch(LaunchMsg),
    Chat(ChatMsg),
    Configure(ConfigureMsg),
    Monitor(MonitorMsg),
    Profiles(ProfilesMsg),
    Automation(AutomationMsg),
    Scripts(ScriptsMsg),
    Handoff(handoff_ui::HandoffMsg),
    Settings(SettingsMsg),
    Menu(MenuMsg),
}

/// The input to the single desktop window: the workspace plus the shared
/// slot that remembers the last observed window size for the one `[desktop]`
/// write after the window closes (windows-reactor 0.100.0 exposes no close
/// hook: `ViewContext` offers only `on_color_scheme`, `on_window_size`,
/// `window_title` and `window_visuals`).
#[derive(Clone)]
struct DesktopInput {
    workspace: PathBuf,
    close_window: CloseWindow,
}

impl PartialEq for DesktopInput {
    fn eq(&self, other: &Self) -> bool {
        self.workspace == other.workspace
    }
}

/// What the one write after the window closes carries: the data directory
/// it belongs to and the last observed size.
type ClosePayload = Option<(PathBuf, (u32, u32))>;

/// The last observed window size and the data directory it belongs to, for
/// the one write after the window closes. Shared with [`run`], which
/// outlives the component.
#[derive(Clone, Default)]
struct CloseWindow(Arc<Mutex<ClosePayload>>);

impl CloseWindow {
    fn store(&self, data_dir: PathBuf, window: (u32, u32)) {
        if let Ok(mut slot) = self.0.lock() {
            *slot = Some((data_dir, window));
        }
    }

    fn load(&self) -> Option<(PathBuf, (u32, u32))> {
        self.0.lock().ok().and_then(|slot| slot.clone())
    }
}

struct DesktopApp {
    /// The checkout the window was opened for (`--workspace` or the working
    /// directory); it is registered through `project.discover` at start.
    opened_workspace: PathBuf,
    data_dir: Fallible<PathBuf>,
    /// The latest status, shown in the details panel.
    status: String,
    /// A notice for the owner above the composer, until dismissed.
    alert: Option<String>,
    appearance: shell_ui::Appearance,
    /// The palette the title bar was last colored in (`None`: not yet).
    title_bar: Option<theme::winui::Palette>,
    /// The `[desktop]` preferences (theme, what opens at start).
    prefs: crate::settings::Preferences,
    /// A preferences write is running (see `save_preferences`).
    prefs_saving: bool,
    /// Counts local preference choices; a read issued before the latest
    /// choice does not overwrite it.
    prefs_rev: u64,
    /// The startup conversation was considered (once per window).
    startup_done: bool,
    /// The window's client width in DIPs, once observed.
    window_width: Option<f64>,
    /// The client size for the one `[desktop]` write at close, once at
    /// least 640 x 480.
    window_size: Option<(u32, u32)>,
    /// The saved window size, applied to the window's first size.
    preferred_window: Option<(u32, u32)>,
    /// The last observed size for the one `[desktop]` write at close.
    close_window: CloseWindow,
    /// `[desktop]` was read.
    preferences_loaded: bool,
    /// False for the one render after the screen tree is re-created: the
    /// multi-line editors are then created empty (see `reprime_fields`).
    fields_primed: bool,
    /// Tags one re-prime; a `FieldsPrimed` for an older one is ignored.
    prime_epoch: u64,
    /// Renders composed so far, and the count when the last re-prime began:
    /// the editors get their text only after a render without it.
    renders: std::cell::Cell<u64>,
    prime_after: u64,
    ledger: MutationLedger,
    /// Ledger key of the single unresolved submission; blocks duplicates.
    in_flight_key: Option<String>,
    main: Main,
    panel: Panel,
    settings_page: Page,
    configure: configure_ui::ConfigureState,
    editor: profiles_ui::ProfilesEditor,
    /// The unified settings: the read sections, the page's drafts and
    /// each section's last save result.
    settings: settings_ui::SettingsState,
    /// The daemon could not start in the data directory (the fresh
    /// private directory remedy is offered).
    startup_error: bool,
    scripts: scripts_ui::ScriptsEditor,
    handoff: handoff_ui::DialogState,
    nav: shell_ui::ShellState,
    launch: launch_ui::LaunchState,
    chat: chat_ui::ChatState,
    monitor: monitor_ui::MonitorState,
    automation: automation_ui::AutomationState,
    /// The sidebar's right-click menu, rename and confirmation.
    menu: menu_ui::MenuState,
}

impl DesktopApp {
    /// The active M3 palette (system colors under High Contrast).
    fn palette(&self) -> theme::winui::Palette {
        self.appearance.palette()
    }

    fn submitting(&self) -> bool {
        self.in_flight_key.is_some()
    }

    fn set_status(&mut self, status: impl Into<String>) {
        self.status = status.into();
    }

    /// End the in-flight submission. A definite admission also releases the
    /// ledger entry; an uncertain outcome keeps the entry, so the next click
    /// of the same action reuses the same mutation id.
    fn settle_mutation(&mut self, admitted: bool) {
        if let Some(key) = self.in_flight_key.take()
            && admitted
        {
            self.ledger.complete(&key);
        }
    }
}

/// Load project registrations on a worker; decode to picker rows.
fn load_projects_blocking(data_dir: &Path) -> Fallible<Vec<UiProject>> {
    client::block_on(async {
        let registrations = config_client::list_projects(data_dir)
            .await
            .map_err(|error| error.to_string())?;
        let names = nav::project_names(
            &registrations
                .iter()
                .map(|registration| (registration.id.clone(), registration.checkout.clone()))
                .collect::<Vec<_>>(),
        );
        Ok(registrations
            .into_iter()
            .zip(names)
            .map(|(registration, name)| UiProject {
                workspace: registration.checkout.clone(),
                common_dir: registration.repo_common_dir.clone(),
                profiles: registration.profiles.clone(),
                id: registration.id.clone(),
                name,
            })
            .collect())
    })
}

/// One validated page of Agent rows on a worker, with each row's chat and
/// archived flags; `archived` is the list filter (None: the default).
fn load_agents_blocking(
    data_dir: &Path,
    request: &AgentListRequest,
    archived: Option<&str>,
) -> Fallible<AgentPage> {
    client::block_on(async {
        let (reply, flags) = client::list_agents_flagged(data_dir, request, archived)
            .await
            .map_err(|error| {
                if archived.is_some() {
                    client::archived_filter_error(&error)
                } else {
                    error.to_string()
                }
            })?;
        // The default list leaves archived Agents out; the fleet line still
        // counts the live ones (an archived chat may be idle, not stopped).
        let archived = if archived.is_none() && request.after.is_none() {
            let only = AgentListRequest {
                limit: request.limit,
                ..AgentListRequest::default()
            };
            match client::list_agents_flagged(data_dir, &only, Some("only")).await {
                Ok((page, _)) => {
                    let rows: Vec<UiAgent> = page
                        .agents
                        .iter()
                        .map(|row| UiAgent::from_summary(&row.summary))
                        .collect();
                    nav::Archived::of(&rows, page.total)
                }
                Err(_) => nav::Archived::default(),
            }
        } else {
            nav::Archived::default()
        };
        Ok(AgentPage {
            rows: reply
                .agents
                .iter()
                .map(|row| {
                    let mut agent = UiAgent::from_summary(&row.summary);
                    if let Some((chat, archived)) = flags.get(&agent.id) {
                        agent.chat = *chat;
                        agent.archived = *archived;
                    }
                    agent
                })
                .collect(),
            next: reply.next,
            total: reply.total,
            archived,
        })
    })
}

fn detail_from(reply: &AgentGetResponse) -> UiDetail {
    let pending = reply
        .agent
        .pending
        .iter()
        .map(|request| UiPending {
            binding: BoundAction::request_control(
                &reply.agent.id,
                reply.agent.generation,
                &request.id,
            ),
            kind: request.kind.clone(),
            summary: request_summary(&request.details),
        })
        .collect();
    UiDetail {
        generation: reply.agent.generation,
        active_turn: reply.agent.active_turn.clone(),
        pending,
        harness: reply.agent.harness.clone(),
        settings: configure::Settings::from_reply(reply),
        state: eliot_core::api::agent_state_name(reply.agent.state).to_owned(),
        observe_only: reply.agent.observe_only,
        project: reply.agent.project.clone(),
        routing: reply.agent.profile_choice.clone(),
    }
}

fn request_summary(details: &serde_json::Value) -> String {
    let text = details.to_string();
    if text.chars().count() <= 160 {
        text
    } else {
        format!("{}…", text.chars().take(160).collect::<String>())
    }
}

/// Poll one retained command (bounded, cancellable) until it is terminal.
/// The outcome stays the daemon's; this only reads it.
fn settle_command(
    data_dir: &Path,
    id: &str,
    max_polls: usize,
    cancelled: &dyn Fn() -> bool,
) -> Fallible<Command> {
    const POLL_MS: u64 = 1_000;
    let mut command =
        client::block_on(client::read_command(data_dir, id)).map_err(|error| error.to_string())?;
    for _ in 0..max_polls {
        if scripts::is_terminal(command.state) {
            return Ok(command);
        }
        if !feed::backoff(POLL_MS, cancelled) {
            return Err(format!(
                "stopped waiting; command {id} continues in the daemon"
            ));
        }
        command = client::block_on(client::read_command(data_dir, id))
            .map_err(|error| error.to_string())?;
    }
    if scripts::is_terminal(command.state) {
        return Ok(command);
    }
    Err(format!(
        "command {id} still {} after {max_polls}s; its outcome stays with the daemon",
        format!("{:?}", command.state).to_lowercase()
    ))
}

/// Follow a `timer.run` command to its Run child and wait (bounded,
/// cancellable) until that command is terminal. The timeout, custody and
/// unknown outcome are the daemon's; this only reads them.
fn settle_run(data_dir: &Path, parent: Command, cancelled: &dyn Fn() -> bool) -> Fallible<Command> {
    const POLL_MS: u64 = 1_000;
    const MAX_POLLS: usize = 15 * 60;
    let mut watched = parent;
    for _ in 0..MAX_POLLS {
        if cancelled() {
            return Err(
                "stopped waiting (window closed or cancelled); the Run continues in the daemon"
                    .to_owned(),
            );
        }
        if watched.verb != "run" {
            let children = client::block_on(client::command_children(data_dir, &watched.id))
                .map_err(|error| error.to_string())?;
            if let Some(run) = children.into_iter().find(|child| child.verb == "run") {
                watched = run;
                continue;
            }
        }
        if scripts::is_terminal(watched.state) && watched.verb == "run" {
            return Ok(watched);
        }
        if scripts::is_terminal(watched.state) && watched.verb != "run" {
            // A refused or settled timer occurrence without a Run child.
            return Ok(watched);
        }
        if !feed::backoff(POLL_MS, cancelled) {
            continue;
        }
        watched = client::block_on(client::read_command(data_dir, &watched.id))
            .map_err(|error| error.to_string())?;
    }
    Err(format!(
        "still {} after 15 minutes; the outcome stays with the daemon (command {})",
        format!("{:?}", watched.state).to_lowercase(),
        watched.id
    ))
}

fn submit_note(reply: &CommandSubmitResponse) -> String {
    format!("{:?}", reply.state).to_lowercase()
}

impl Component for DesktopApp {
    type Input = DesktopInput;
    type Message = Message;

    fn create(input: &DesktopInput, context: &ComponentContext<Self>) -> Self {
        let data_dir = client::data_dir().map_err(|error| error.to_string());
        let mut app = Self {
            opened_workspace: input.workspace.clone(),
            data_dir,
            status: "Connecting to the daemon…".to_owned(),
            alert: None,
            appearance: shell_ui::Appearance::new(),
            title_bar: None,
            prefs: crate::settings::Preferences::default(),
            prefs_saving: false,
            prefs_rev: 0,
            startup_done: false,
            window_width: None,
            window_size: None,
            close_window: input.close_window.clone(),
            preferred_window: None,
            preferences_loaded: false,
            fields_primed: true,
            prime_epoch: 0,
            renders: std::cell::Cell::new(0),
            prime_after: 0,
            ledger: MutationLedger::default(),
            in_flight_key: None,
            main: Main::Conversation,
            panel: Panel::Closed,
            settings_page: Page::Appearance,
            configure: configure_ui::ConfigureState::default(),
            editor: profiles_ui::ProfilesEditor::default(),
            settings: settings_ui::SettingsState::default(),
            startup_error: false,
            scripts: scripts_ui::ScriptsEditor::default(),
            handoff: handoff_ui::DialogState::default(),
            nav: shell_ui::ShellState {
                selection: Selection::default(),
                projects: Vec::new(),
                agents: Vec::new(),
                branches: HashMap::new(),
                expanded: HashSet::new(),
                tree_epoch: 0,
                agents_next: None,
                agents_total: 0,
                archived: nav::Archived::default(),
                agents_loading: false,
                list_epoch: 0,
                pending_select: None,
                collapsed: HashSet::new(),
                unfolded: HashSet::new(),
                show_archived: false,
                search: shell_ui::Search::default(),
                search_ref: ElementRef::new(),
                shortcuts: None,
            },
            launch: launch_ui::LaunchState {
                workspace: input.workspace.to_string_lossy().into_owned(),
                ..Default::default()
            },
            chat: chat_ui::ChatState {
                convo: None,
                detail: None,
                epoch: 0,
                depth: Depth::default(),
                live: LiveStatus::Idle,
                live_task: None,
                failures: 0,
                reread_inflight: false,
                invalidated: Invalidation::default(),
                docs: HashMap::new(),
                render_cache: RenderCache::default(),
                items: Rc::new(Vec::new()),
                items_revision: 0,
                expanded: HashSet::new(),
                end_anchor: ElementRef::new(),
                composer_ref: ElementRef::new(),
                pending_local: None,
                waiting_label: None,
                inputs_checking: false,
                launches: HashMap::new(),
                opened_from_search: false,
                drafts: DraftStore::default(),
                answers: HashMap::new(),
                confirm_force: None,
                loss: None,
            },
            monitor: monitor_ui::MonitorState::default(),
            automation: automation_ui::AutomationState {
                rules: Vec::new(),
                status: String::new(),
                epoch: 0,
            },
            menu: menu_ui::MenuState::default(),
        };
        // Before the first controls exist (see `app_resources::install`).
        if let Err(error) = crate::app_resources::install(app.palette().scheme()) {
            app.status = format!("WinUI M3 resources not applied: {error}");
        }
        if let Ok(data_dir) = app.data_dir.clone() {
            app.watch_high_contrast(context);
            app.start_connection(data_dir, context);
        }
        // On the UI thread, before its message loop runs.
        match crate::shortcuts::install() {
            Ok(chords) => {
                app.nav.shortcuts = Some(Arc::new(Mutex::new(chords)));
                app.watch_shortcuts(context);
            }
            Err(error) => app.status = error,
        }
        app
    }

    fn update(&mut self, message: Message, context: &ComponentContext<Self>) {
        match message {
            Message::Shell(msg) => self.shell_update(msg, context),
            Message::Launch(msg) => self.launch_update(msg, context),
            Message::Chat(msg) => self.chat_update(msg, context),
            Message::Configure(msg) => self.configure_update(msg, context),
            Message::Monitor(msg) => self.monitor_update(msg, context),
            Message::Profiles(msg) => self.profiles_update(msg, context),
            Message::Settings(msg) => self.settings_update(msg, context),
            Message::Automation(msg) => self.automation_update(msg, context),
            Message::Scripts(msg) => self.scripts_update(msg, context),
            Message::Handoff(msg) => self.handoff_update(msg, context),
            Message::Menu(msg) => self.menu_update(msg, context),
        }
    }

    fn view(&self, _input: &DesktopInput, context: &mut ViewContext<Self>) -> View {
        self.renders.set(self.renders.get() + 1);
        self.shell_view(context)
    }
}

/// Run the single desktop window. Blocks while the WinUI message loop runs;
/// closing the window disconnects the UI, never the Agent. Afterwards the
/// last observed window size persists once into `[desktop]`, only when it
/// differs from the saved value.
pub fn run(workspace: PathBuf) -> Result<(), String> {
    let close_window = CloseWindow::default();
    let result = App::run_component::<DesktopApp>(DesktopInput {
        workspace,
        close_window: close_window.clone(),
    })
    .map_err(|error| error.to_string());
    if let Some((data_dir, window)) = close_window.load()
        && let Err(error) =
            client::block_on(crate::settings::save_window_on_close(&data_dir, window))
    {
        eprintln!("swarm-desktop: window size not saved: {error}");
    }
    result
}
