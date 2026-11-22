//! The daemon link (#826). The daemon alone says whether it runs: a read
//! that fails asks it (`daemon.status`), and when no daemon answers and its
//! Owner did not stop it, the desktop starts it again through the startup
//! path it opened with ([`DesktopApp::start_connection`], whose
//! `ensure_running` starts one only when none is reachable, and the data
//! directory's lock admits one daemon). Only a start that may succeed when
//! tried again is retried, with growing pauses up to a cap; a calm notice
//! above the composer counts the attempts. A daemon its Owner stopped
//! (`daemon.shutdown`, which leaves the daemon's owner-stop marker) is not
//! started again: the main area says so and offers Start. After the last
//! attempt, or a start that cannot succeed by repeating it, the main area
//! shows the daemon's own failure and the owner's choices.

use super::*;
use crate::theme::winui::Palette;

/// Restart attempts before the failure view. The pauses before attempts
/// 2 to 5 grow as [`live::backoff_ms`]: 1, 2, 4 and 8 s.
const RESTART_ATTEMPTS: u32 = 5;
/// A probe with no answer this soon leaves the daemon to its readers: it
/// is busy, not gone.
const PROBE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// What a `daemon.status` probe found.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Probe {
    /// The daemon answered, refused or was too busy to answer in time: it
    /// is there, and the failed read speaks for itself.
    Running,
    /// Nothing answers and its Owner did not stop it: it failed.
    Gone,
    /// Nothing answers because its Owner stopped it.
    OwnerStopped,
}

#[derive(Debug, Default)]
pub(super) struct LinkState {
    /// A `daemon.status` probe is in flight.
    probing: bool,
    /// The restart attempt in flight (from 1) and why the previous one
    /// failed.
    restarting: Option<(u32, Option<String>)>,
    /// The owner's Retry, Start or data-directory choice is starting the
    /// daemon: no restart chain may start beside it.
    starting: bool,
    /// The restarts ended without a daemon: the failure view says Swarm
    /// stopped.
    gave_up: bool,
    /// The daemon's Owner stopped it: nothing restarts it; the main area
    /// offers Start, and the list's cadence watches for a start elsewhere.
    stopped: bool,
    /// A failed read's alert, held until the probe answers: shown when the
    /// daemon runs, dropped when it is gone (the notice says why once).
    held: Option<String>,
    /// Probes in a row that found the daemon running after a failed read,
    /// and the earliest next one: repeated failing reads ask it with the
    /// same growing pauses as every other retry.
    strikes: u32,
    quiet_until: Option<std::time::Instant>,
    /// The start in flight: superseded starts are cancelled, and the
    /// window's close cancels it before it starts a daemon.
    task: Option<ComponentTask>,
    /// The data directory being started is the owner's new choice: it is
    /// remembered only once its daemon answers.
    candidate: bool,
    /// The confirmation of a new empty data directory is open.
    pub(super) confirm_fresh: bool,
}

impl DesktopApp {
    /// No daemon answers now: it is being started, its Owner stopped it, or
    /// it could not start. The polls read nothing meanwhile.
    pub(super) fn daemon_down(&self) -> bool {
        self.link.restarting.is_some()
            || self.link.starting
            || self.link.stopped
            || self.startup_error.is_some()
    }

    /// The daemon is being asked whether it runs, or is down: a read's
    /// transport error is not the owner's news yet.
    pub(super) fn link_unsure(&self) -> bool {
        self.link.probing || self.daemon_down()
    }

    /// The main area shows the link's state (could not start, or stopped by
    /// its Owner) instead of the conversation.
    pub(super) fn link_view_shown(&self) -> bool {
        self.startup_error.is_some() || self.link.stopped
    }

    /// A list read failed with `text`: shown when the daemon still runs,
    /// otherwise the restart notice speaks for it.
    pub(super) fn read_failed(&mut self, text: String, context: &ComponentContext<Self>) {
        if self.daemon_down() {
            return;
        }
        if !self.preferences_loaded {
            self.alert(text);
            return;
        }
        self.link.held = Some(text);
        self.link_suspect(context);
        // No probe started (none needed this soon): the read's own error
        // is the news.
        if !self.link.probing
            && let Some(text) = self.link.held.take()
        {
            self.alert(text);
        }
    }

    /// A read failed: ask the daemon whether it runs. One probe at a time,
    /// only once connected and not while it is down, and after a probe that
    /// found it running, not again before its growing pause ends.
    pub(super) fn link_suspect(&mut self, context: &ComponentContext<Self>) {
        let resting = self
            .link
            .quiet_until
            .is_some_and(|until| std::time::Instant::now() < until);
        if !self.preferences_loaded || self.daemon_down() || self.link.probing || resting {
            return;
        }
        self.spawn_probe(context);
    }

    /// While its Owner keeps it stopped, the list's cadence asks whether a
    /// daemon was started elsewhere (CLI, update) and reconnects to it.
    /// Asking never starts one.
    pub(super) fn link_watch(&mut self, context: &ComponentContext<Self>) {
        if self.link.stopped && !self.link.probing {
            self.spawn_probe(context);
        }
    }

    fn spawn_probe(&mut self, context: &ComponentContext<Self>) {
        let Ok(data_dir) = self.data_dir.clone() else {
            return;
        };
        self.link.probing = true;
        let _task = context.spawn_background(move |_| {
            let reachable = client::block_on(async {
                tokio::time::timeout(PROBE_TIMEOUT, eliot_daemon::startup::reachable(&data_dir))
                    .await
            });
            let probe = match reachable {
                Ok(Ok(false)) if eliot_daemon::owner_stopped(&data_dir) => Probe::OwnerStopped,
                Ok(Ok(false)) => Probe::Gone,
                _ => Probe::Running,
            };
            Message::Shell(ShellMsg::LinkProbed(probe))
        });
    }

    pub(super) fn link_probed(&mut self, probe: Probe, context: &ComponentContext<Self>) {
        self.link.probing = false;
        let held = self.link.held.take();
        if self.link.stopped {
            if probe == Probe::Running {
                self.retry_startup(context);
            }
            return;
        }
        if self.daemon_down() {
            return;
        }
        match probe {
            Probe::Running => {
                self.link.strikes = self.link.strikes.saturating_add(1);
                self.link.quiet_until = Some(
                    std::time::Instant::now()
                        + std::time::Duration::from_millis(live::backoff_ms(self.link.strikes)),
                );
                if let Some(text) = held {
                    self.alert(text);
                }
            }
            Probe::Gone => self.restart_daemon(1, None, context),
            Probe::OwnerStopped => {
                self.link.stopped = true;
                self.set_status("Swarm was stopped by its owner.");
                self.park_reads("Swarm was stopped", context);
            }
        }
    }

    /// No daemon serves the open conversation for now: its feed stops, and
    /// the main area shows the link (from Settings too).
    fn park_reads(&mut self, live_reason: &str, context: &ComponentContext<Self>) {
        if let Some(task) = self.chat.live_task.take() {
            task.cancel();
        }
        if self.chat.convo.is_some() {
            self.chat.live = LiveStatus::Stopped(live_reason.to_owned());
        }
        self.close_popover();
        if self.main != Main::Conversation {
            self.main = Main::Conversation;
            self.reprime_fields(context);
        }
    }

    /// Start attempt `attempt` after its pause; `reason` is why the one
    /// before failed.
    fn restart_daemon(
        &mut self,
        attempt: u32,
        reason: Option<String>,
        context: &ComponentContext<Self>,
    ) {
        let Ok(data_dir) = self.data_dir.clone() else {
            return;
        };
        // The open conversation's feed waits for the daemon; the reply that
        // reconnects starts it again (`DaemonReady`).
        if let Some(task) = self.chat.live_task.take() {
            task.cancel();
        }
        if self.chat.convo.is_some() {
            self.chat.live = LiveStatus::Reconnecting {
                attempt,
                reason: reason
                    .clone()
                    .unwrap_or_else(|| "Swarm stopped; starting it again".to_owned()),
            };
        }
        self.set_status(format!("Swarm stopped; reconnecting (attempt {attempt})"));
        self.link.restarting = Some((attempt, reason));
        let delay_ms = if attempt > 1 {
            live::backoff_ms(attempt - 1)
        } else {
            0
        };
        self.link.task = Some(self.start_connection(data_dir, delay_ms, context));
    }

    /// The daemon answers again (at launch, after Retry or Start, or after
    /// a restart): the link is up. A newly chosen data directory is
    /// remembered now. True when it was a restart.
    pub(super) fn link_ready(&mut self, context: &ComponentContext<Self>) -> bool {
        let link = std::mem::take(&mut self.link);
        if link.candidate
            && let Ok(root) = self.data_dir.clone()
        {
            let _task = context.spawn_background(move |_| {
                let remembered = eliot_daemon::persist_selected_root(&root)
                    .map_err(|error| error.user_message());
                Message::Shell(ShellMsg::RootRemembered(root, remembered))
            });
        }
        link.restarting.is_some()
    }

    /// The startup path failed with `error`. A restart tries again only
    /// when trying again may help (nothing answered yet, the platform could
    /// not start it, another start holds the root) and attempts remain;
    /// otherwise the failure view shows the daemon's reason at once.
    pub(super) fn link_failed(
        &mut self,
        error: eliot_core::Error,
        context: &ComponentContext<Self>,
    ) {
        self.link.starting = false;
        self.link.task = None;
        match self.link.restarting.take() {
            Some((attempt, _))
                if attempt < RESTART_ATTEMPTS && eliot_daemon::startup::retryable(&error.code) =>
            {
                self.restart_daemon(attempt + 1, Some(error.user_message()), context);
            }
            restarting => {
                self.link.gave_up = restarting.is_some();
                self.startup_error = Some(error);
                self.park_reads("Swarm is not running", context);
            }
        }
    }

    /// Retry, or Start after an Owner stop, on the current data directory
    /// (a new choice stays pending until its daemon answers).
    pub(super) fn retry_startup(&mut self, context: &ComponentContext<Self>) {
        let Ok(data_dir) = self.data_dir.clone() else {
            return;
        };
        if let Some(task) = self.link.task.take() {
            task.cancel();
        }
        self.startup_error = None;
        self.link = LinkState {
            starting: true,
            candidate: self.link.candidate,
            ..LinkState::default()
        };
        self.alert = None;
        self.set_status("Starting Swarm…");
        self.link.task = Some(self.start_connection(data_dir, 0, context));
    }

    /// Start on the owner's new data directory; it is remembered for the
    /// next launch only once its daemon answers ([`Self::link_ready`]).
    fn switch_data_dir(&mut self, candidate: PathBuf, context: &ComponentContext<Self>) {
        // An open conversation belongs to the directory left behind.
        if self.chat.convo.is_some() {
            self.new_task(context);
        }
        self.data_dir = Ok(candidate.clone());
        self.note_window_size();
        self.link.candidate = true;
        self.retry_startup(context);
        self.set_status(format!("Starting Swarm in {}", candidate.display()));
    }

    /// A data directory to start on, checked off the UI thread, or why not.
    pub(super) fn candidate_checked(
        &mut self,
        checked: Result<PathBuf, String>,
        context: &ComponentContext<Self>,
    ) {
        match checked {
            Ok(candidate) => self.switch_data_dir(candidate, context),
            Err(error) => self.alert(error),
        }
    }

    /// "Use another data directory…": a Swarm data folder the owner already
    /// has. The folder dialog runs on the UI thread; the daemon library's
    /// check of the root runs on a worker.
    pub(super) fn choose_data_dir(&mut self, context: &ComponentContext<Self>) {
        let picked = match crate::file_dialog::pick_folder("Use another Swarm data directory") {
            Ok(Some(picked)) => PathBuf::from(picked),
            Ok(None) => return,
            Err(error) => return self.alert(error),
        };
        let _task = context.spawn_background(move |_| {
            let eligible = match eliot_daemon::DataDir::open_existing(&picked) {
                Ok(root) => Ok(root.path().to_path_buf()),
                Err(error) if eliot_daemon::startup::repairable_data_dir_acl(&error) => {
                    eliot_daemon::startup::check_acl_repair_target(&picked, error)
                        .map(|()| picked.clone())
                }
                Err(error) => Err(error),
            };
            let checked = eligible.map_err(|error| {
                format!(
                    "{} cannot be used: {}",
                    picked.display(),
                    error.user_message()
                )
            });
            Message::Shell(ShellMsg::CandidateChecked(checked))
        });
    }

    /// The last choice, once confirmed: a new, empty private data directory
    /// beside the current one, which keeps its chats and settings.
    pub(super) fn fresh_confirmed(&mut self, confirmed: bool, context: &ComponentContext<Self>) {
        self.link.confirm_fresh = false;
        if !confirmed {
            return;
        }
        let _task = context.spawn_background(move |_| {
            let checked = client::data_dir()
                .and_then(|original| {
                    eliot_daemon::bootstrap::ensure_private_data_dir(
                        original
                            .with_file_name(format!("Eliot-private-{}", client::new_mutation_id())),
                    )
                })
                .map_err(|error| {
                    format!(
                        "A new data directory was not created: {}",
                        error.user_message()
                    )
                });
            Message::Shell(ShellMsg::CandidateChecked(checked))
        });
    }

    /// The new data directory runs and is remembered, or runs for this
    /// session only.
    pub(super) fn root_remembered(&mut self, root: PathBuf, remembered: Result<(), String>) {
        match remembered {
            Ok(()) => self.set_status(format!("Swarm uses {} from now on", root.display())),
            Err(error) => self.alert(format!(
                "Swarm runs in {}, but it was not remembered for the next start: {error}",
                root.display()
            )),
        }
    }

    /// Open a file of the data directory (its path parts), or show the
    /// folder itself (no parts); say why not when that fails.
    pub(super) fn open_in_data_dir(&mut self, parts: &[&str]) {
        let Ok(data_dir) = self.data_dir.clone() else {
            return;
        };
        let opened = if parts.is_empty() {
            super::reveal::reveal(&data_dir.to_string_lossy())
        } else {
            let path = parts.iter().fold(data_dir, |path, part| path.join(part));
            super::reveal::open_with_default_app(&path)
        };
        if let Err(error) = opened {
            self.alert(error);
        }
    }

    /// The restart in progress, above the composer: calm, not an alert.
    pub(super) fn reconnect_notice(&self, column: Option<f64>, palette: Palette) -> Option<View> {
        let (attempt, reason) = self.link.restarting.as_ref()?;
        let text = format!("Swarm stopped. Reconnecting… attempt {attempt} of {RESTART_ATTEMPTS}");
        let reason = reason.clone().unwrap_or_default();
        Some(
            Border::new()
                .grid_row(4)
                .width(column)
                .max_width(tokens::layout::READING_WIDTH)
                .horizontal_alignment(HorizontalAlignment::Center)
                .margin(Thickness::new(0.0, tokens::spacing::SM, 0.0, 0.0))
                .background(palette.tonal())
                .corner_radius(tokens::radius::CONTAINER)
                .padding(Thickness::xy(tokens::spacing::MD, tokens::spacing::SM))
                .automation_name(if reason.is_empty() {
                    text.clone()
                } else {
                    format!("{text}. {reason}")
                })
                .content(
                    Grid::new()
                        .columns([GridLength::Auto, GridLength::Auto, GridLength::STAR])
                        .column_spacing(tokens::spacing::SM)
                        .children((
                            m3::status_mark(
                                palette,
                                crate::theme::StatusTone::Pending,
                                tokens::layout::DOT,
                            ),
                            m3::body(text)
                                .grid_column(1)
                                .vertical_alignment(VerticalAlignment::Center),
                            m3::secondary(reason)
                                .grid_column(2)
                                .vertical_alignment(VerticalAlignment::Center)
                                .text_trimming(TextTrimming::CharacterEllipsis),
                        )),
                ),
        )
    }

    /// The daemon could not start, or stopped and could not be started
    /// again: its own reason, then the owner's choices, the safe ones first.
    /// Nothing is moved; a new empty directory is the last, named choice.
    /// A daemon its Owner stopped gets Start instead.
    pub(super) fn startup_error_view(
        &self,
        context: &mut ViewContext<Self>,
        palette: Palette,
    ) -> View {
        if self.link.stopped {
            return self.stopped_view(context, palette);
        }
        let reason = self
            .startup_error
            .as_ref()
            .map_or_else(String::new, eliot_core::Error::user_message);
        let config = self.startup_error.as_ref().is_some_and(|error| {
            error.failure_where.as_deref() == Some(eliot_daemon::startup::CONFIG_STARTUP_LOCATION)
        });
        let title = if self.link.gave_up {
            "Swarm stopped and could not be restarted"
        } else {
            "Swarm could not start"
        };
        let data_dir = self
            .data_dir
            .as_ref()
            .map_or_else(|_| String::new(), |dir| dir.display().to_string());
        let shell = |msg: ShellMsg| context.message(Message::Shell(msg));
        let choice = |key: &str, glyph: &str, title: &str, note: &str, msg: ShellMsg| {
            KeyedView::new(
                key,
                m3::row(palette, false)
                    .automation_name(title.to_owned())
                    .on_click(shell(msg))
                    .content(m3::row_content(Some(glyph), title, note, View::empty())),
            )
        };
        let notice: View = match &self.alert {
            Some(text) => StackPanel::new()
                .orientation(Orientation::Horizontal)
                .spacing(tokens::spacing::SM)
                .automation_name(text.clone())
                .children((
                    m3::status_mark(
                        palette,
                        crate::theme::StatusTone::Attention,
                        tokens::layout::DOT,
                    ),
                    m3::body(text.clone())
                        .text_wrapping(TextWrapping::Wrap)
                        .is_text_selection_enabled(true),
                )),
            None => View::empty(),
        };
        StackPanel::new()
            .max_width(tokens::layout::READING_WIDTH)
            .horizontal_alignment(HorizontalAlignment::Center)
            .vertical_alignment(VerticalAlignment::Center)
            .spacing(tokens::spacing::MD)
            .margin(Thickness::uniform(tokens::spacing::LG))
            .children((
                m3::page_title(title),
                m3::body(reason)
                    .text_wrapping(TextWrapping::Wrap)
                    .is_text_selection_enabled(true),
                // One filled action: Retry on the same data directory.
                StackPanel::new()
                    .orientation(Orientation::Horizontal)
                    .spacing(tokens::spacing::SM)
                    .children((
                        m3::filled(palette)
                            .automation_name("Retry")
                            .on_click(shell(ShellMsg::RetryStartup))
                            .content(m3::label("Retry")),
                        m3::outlined(palette)
                            .automation_name("Repair settings")
                            .on_click(shell(ShellMsg::RepairSettings))
                            .content(m3::label("Repair settings")),
                        if config {
                            m3::text_button(palette)
                                .automation_name("Open config.toml")
                                .on_click(shell(ShellMsg::OpenStartupConfig))
                                .content(m3::link_content("Open config.toml"))
                        } else {
                            View::empty()
                        },
                    )),
                m3::list(
                    palette,
                    vec![
                        choice(
                            "log",
                            m3::glyph::FILE,
                            "Open log",
                            "The daemon's own record of what failed",
                            ShellMsg::OpenStartupLog,
                        ),
                        choice(
                            "folder",
                            m3::glyph::OPEN_FOLDER,
                            "Open the data folder",
                            &data_dir,
                            ShellMsg::OpenDataFolder,
                        ),
                        choice(
                            "another",
                            m3::glyph::FOLDER,
                            "Use another data directory…",
                            "A Swarm data folder you already have",
                            ShellMsg::ChooseDataDirectory,
                        ),
                        choice(
                            "fresh",
                            m3::glyph::NEW_TASK,
                            "Start over in a new empty data directory",
                            "No chats or settings; the current folder is kept",
                            ShellMsg::UseFreshDirectory,
                        ),
                    ],
                ),
                notice,
            ))
    }

    /// The daemon's Owner stopped it: nothing starts it again on its own.
    fn stopped_view(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        StackPanel::new()
            .max_width(tokens::layout::READING_WIDTH)
            .horizontal_alignment(HorizontalAlignment::Center)
            .vertical_alignment(VerticalAlignment::Center)
            .spacing(tokens::spacing::MD)
            .margin(Thickness::uniform(tokens::spacing::LG))
            .children((
                m3::page_title("Swarm was stopped"),
                m3::body(
                    "Its owner stopped it, so it is not started again on its own. \
                     This window reconnects when Swarm runs again.",
                )
                .text_wrapping(TextWrapping::Wrap),
                m3::filled(palette)
                    .automation_name("Start Swarm")
                    .on_click(context.message(Message::Shell(ShellMsg::RetryStartup)))
                    .content(m3::label("Start")),
            ))
    }

    /// Confirm the last choice: a new, empty data directory.
    pub(super) fn fresh_confirmation(
        &self,
        context: &mut ViewContext<Self>,
        palette: Palette,
    ) -> View {
        let current = self
            .data_dir
            .as_ref()
            .map_or_else(|_| String::new(), |dir| dir.display().to_string());
        super::dialog_ui::surface(
            context,
            self.dialog.epoch,
            palette,
            "Start over in a new empty data directory?".to_owned(),
            m3::secondary(format!(
                "Swarm creates a new private data directory with no chats, Agents or settings \
                 and starts there. {current} stays as it is; use “Use another data directory…” \
                 to return to it. The new directory is remembered once Swarm runs in it."
            ))
            .text_wrapping(TextWrapping::Wrap)
            .into(),
            super::dialog_ui::actions(
                m3::text_button(palette)
                    .element_ref(&self.dialog.close)
                    .automation_name("Cancel")
                    .on_click(context.message(Message::Shell(ShellMsg::FreshConfirmed(false))))
                    .content(m3::label("Cancel")),
                m3::filled(palette)
                    .element_ref(&self.dialog.primary)
                    .automation_name("Create empty data directory")
                    .on_click(context.message(Message::Shell(ShellMsg::FreshConfirmed(true))))
                    .content(m3::label("Create and start")),
            ),
            tokens::layout::DIALOG_WIDTH,
        )
    }
}
