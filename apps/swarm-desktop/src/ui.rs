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
//! ratio, so the transcript cannot detect "at the bottom" or auto-follow;
//! "Jump to latest" moves focus to the end anchor, which WinUI brings into
//! view. There is no font-family property (code is not monospace), no
//! clipboard (see [`crate::clipboard`]) and no key events on `TextBox`, so
//! Enter cannot send. A multiline `TextBox` consumes Ctrl+Enter as a newline
//! before any `KeyboardAccelerator` sees it, and Reactor's Button/Grid
//! accelerators (Ctrl+R, Ctrl+Enter) did not fire under real keystrokes in
//! this window, so none is offered: the keyboard path is Tab to the action
//! button, then Enter or Space.
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
//! repository. Monospace code and Enter-to-send wait for a published
//! release that carries those two APIs.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::rc::Rc;

use eliot_core::Command;
use eliot_core::api::{
    AgentGetResponse, AgentListRequest, CommandSubmitResponse, ProfileListResponse, ProfileSummary,
    ProfilesReadResponse, ProfilesSaveResponse, ReparentResponse, RuleListEntry,
    ScriptListResponse, ScriptReadResponse, ScriptSaveResponse,
};
use windows_reactor::*;

mod automation_ui;
mod chat_ui;
mod configure_ui;
mod handoff_ui;
mod launch_ui;
mod monitor_ui;
mod profiles_ui;
mod scripts_ui;
mod shell_ui;

use automation_ui::AutomationMsg;
use chat_ui::ChatMsg;
use configure_ui::ConfigureMsg;
use launch_ui::LaunchMsg;
use monitor_ui::MonitorMsg;
use profiles_ui::ProfilesMsg;
use scripts_ui::ScriptsMsg;
use shell_ui::ShellMsg;

use crate::chat::{Conversation, Invalidation};
use crate::client::{self, LaunchTarget};
use crate::config_view::{self, ProfilesActions, ProfilesData, RuleNamespace};
use crate::configure;
use crate::feed::{self, Delivery, Depth, Reread, Snapshot};
use crate::live::{self, LiveStatus, Next};
use crate::m3;
use crate::markdown::Doc;
use crate::monitor::{self, Monitor, MonitorSnapshot};
use crate::monitor_view::{self, MonitorActions};
use crate::nav::{self, UiAgent};
use crate::profiles::{self, FORM_FIELDS};
use crate::reparent;
use crate::scripts;
use crate::state::{BoundAction, DraftStore, MutationLedger, Selection};
use crate::theme;
use crate::views::{self, RenderCache, RenderedItem, RowCallbacks};

/// Agent rows per `agent.list` page in the navigation list.
const AGENT_PAGE: usize = 100;
/// Harnesses an Owner ad-hoc launch accepts (the daemon validates again).
const ADHOC_HARNESSES: [&str; 5] = ["codex", "claude", "muse", "opencode", "antigravity"];

/// The three screens of the main column.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Screen {
    Chat,
    Profiles,
    Automation,
    Scripts,
}

/// Index 0 of the project picker lists every Agent in scope.
const ALL_PROJECTS: &str = "All projects (every Agent in scope)";

/// UI copy of one project registration.
#[derive(Clone, Debug)]
struct UiProject {
    id: String,
    profiles: Vec<String>,
    workspace: String,
}

/// One page of the navigation list with its honest counters.
#[derive(Clone, Debug)]
struct AgentPage {
    rows: Vec<UiAgent>,
    next: Option<String>,
    total: usize,
}

/// UI copy of one pending request with its exact control binding.
#[derive(Clone, Debug)]
struct UiPending {
    binding: BoundAction,
    turn_id: String,
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
}

/// Admission of one launch-with-text: the new Agent and its linked input.
#[derive(Clone, Debug)]
struct LaunchOutcome {
    agent: String,
    generation: i64,
    command_id: String,
    linked_command: Option<String>,
    text: String,
    launch_rev: u64,
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
}

struct DesktopApp {
    data_dir: Fallible<PathBuf>,
    status: String,
    appearance: shell_ui::Appearance,
    /// The window's client width in DIPs, once observed.
    window_width: Option<f64>,
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
    screen: Screen,
    configure: configure_ui::ConfigureState,
    editor: profiles_ui::ProfilesEditor,
    scripts: scripts_ui::ScriptsEditor,
    handoff: handoff_ui::DialogState,
    nav: shell_ui::ShellState,
    launch: launch_ui::LaunchState,
    chat: chat_ui::ChatState,
    monitor: monitor_ui::MonitorState,
    automation: automation_ui::AutomationState,
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
        let registrations = client::list_projects(data_dir)
            .await
            .map_err(|error| error.to_string())?;
        Ok(registrations
            .into_iter()
            .map(|registration| UiProject {
                workspace: registration.checkout.clone(),
                profiles: registration.profiles.clone(),
                id: registration.id.clone(),
            })
            .collect())
    })
}

/// One validated page of Agent rows on a worker.
fn load_agents_blocking(data_dir: &Path, request: &AgentListRequest) -> Fallible<AgentPage> {
    client::block_on(async {
        let reply = client::list_agents(data_dir, request)
            .await
            .map_err(|error| error.to_string())?;
        Ok(AgentPage {
            rows: reply
                .agents
                .into_iter()
                .map(|row| UiAgent {
                    id: row.summary.id,
                    generation: row.summary.generation,
                    harness: row.summary.harness,
                    state: format!("{:?}", row.summary.state).to_lowercase(),
                    project: row.summary.project,
                    parent: row.summary.parent,
                })
                .collect(),
            next: reply.next,
            total: reply.total,
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
            turn_id: request.turn_id.clone(),
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
        state: format!("{:?}", reply.agent.state).to_lowercase(),
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
    type Input = ();
    type Message = Message;

    fn create(_input: &(), context: &ComponentContext<Self>) -> Self {
        let data_dir = client::data_dir().map_err(|error| error.to_string());
        let mut app = Self {
            data_dir,
            status: "Connecting to the daemon…".to_owned(),
            appearance: shell_ui::Appearance::new(),
            window_width: None,
            fields_primed: true,
            prime_epoch: 0,
            renders: std::cell::Cell::new(0),
            prime_after: 0,
            ledger: MutationLedger::default(),
            in_flight_key: None,
            screen: Screen::Chat,
            configure: configure_ui::ConfigureState::default(),
            editor: profiles_ui::ProfilesEditor::default(),
            scripts: scripts_ui::ScriptsEditor::default(),
            handoff: handoff_ui::DialogState::default(),
            nav: shell_ui::ShellState {
                selection: Selection::default(),
                projects: Vec::new(),
                project_idx: Some(0),
                agents: Vec::new(),
                branches: HashMap::new(),
                expanded: HashSet::new(),
                tree_epoch: 0,
                rows: Vec::new(),
                agent_idx: None,
                agents_next: None,
                agents_total: 0,
                agents_loading: false,
                list_epoch: 0,
                pending_select: None,
            },
            launch: launch_ui::LaunchState {
                profiles: Vec::new(),
                profile_idx: None,
                profile_text: String::new(),
                workspace: String::new(),
                launch_text: String::new(),
                launch_rev: 0,
                profile_summaries: Vec::new(),
                profile_list: None,
                picker_adhoc: false,
                adhoc_harness_idx: Some(0),
                adhoc_model: String::new(),
                adhoc_effort: String::new(),
                harness_hint: String::new(),
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
                unseen: 0,
                end_anchor: ElementRef::new(),
                composer_ref: ElementRef::new(),
                pending_local: None,
                drafts: DraftStore::default(),
                answers: HashMap::new(),
            },
            monitor: monitor_ui::MonitorState {
                data: Monitor::default(),
                open: false,
                epoch: 0,
                task: None,
                selected: None,
                published: 0,
                reparent_status: String::new(),
                confirm_force: None,
                on_screen: Vec::new(),
                applied_at: 0,
            },
            automation: automation_ui::AutomationState {
                rules: Vec::new(),
                status: String::new(),
                epoch: 0,
            },
        };
        // Before the first controls exist (see `app_resources::install`).
        if let Err(error) = crate::app_resources::install() {
            app.status = format!("WinUI accent resources not applied: {error}");
        }
        if let Ok(data_dir) = app.data_dir.clone() {
            let _task = context.spawn_background(move |_| {
                Message::Shell(ShellMsg::ProjectsLoaded(load_projects_blocking(&data_dir)))
            });
            app.load_agents(context, None);
            app.watch_high_contrast(context);
            app.load_profile_list(context);
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
            Message::Automation(msg) => self.automation_update(msg, context),
            Message::Scripts(msg) => self.scripts_update(msg, context),
            Message::Handoff(msg) => self.handoff_update(msg, context),
        }
    }

    fn view(&self, _input: &(), context: &mut ViewContext<Self>) -> View {
        self.renders.set(self.renders.get() + 1);
        self.shell_view(context)
    }
}

/// Run the single desktop window. Blocks while the WinUI message loop runs;
/// closing the window disconnects the UI, never the Agent.
pub fn run() -> Result<(), String> {
    App::run_component::<DesktopApp>(()).map_err(|error| error.to_string())
}
