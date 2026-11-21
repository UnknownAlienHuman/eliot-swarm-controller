//! The daemon link (#826). The daemon alone says whether it runs: a read
//! that fails asks it (`daemon.status`), and when no daemon answers the
//! desktop starts it again through the startup path it opened with
//! ([`DesktopApp::start_connection`], whose `ensure_running` starts one
//! only when none is reachable, and the data directory's lock admits one
//! daemon). The attempts pause longer each time and stop at a cap; a calm
//! notice above the composer counts them meanwhile. After the last one,
//! or when the first start fails, the main area shows the daemon's own
//! failure and the owner's choices.

use super::*;
use crate::theme::winui::Palette;

/// Restart attempts before the failure view. The pauses before attempts
/// 2 to 5 grow as [`live::backoff_ms`]: 1, 2, 4 and 8 s.
const RESTART_ATTEMPTS: u32 = 5;
/// A probe with no answer this soon leaves the daemon to its readers: it
/// is busy, not gone.
const PROBE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

#[derive(Debug, Default)]
pub(super) struct LinkState {
    /// A `daemon.status` probe is in flight.
    probing: bool,
    /// The restart attempt in flight (from 1) and why the previous one
    /// failed.
    restarting: Option<(u32, Option<String>)>,
    /// The owner's Retry or data-directory choice is starting the daemon:
    /// no restart chain may start beside it.
    starting: bool,
    /// The restarts reached their cap: the failure view says Swarm stopped.
    gave_up: bool,
    /// A failed read's alert, held until the probe answers: shown when the
    /// daemon runs, dropped when it is gone (the notice says why once).
    held: Option<String>,
}

impl DesktopApp {
    /// No daemon answers now: it is being started again, or it could not
    /// start. The polls read nothing meanwhile.
    pub(super) fn daemon_down(&self) -> bool {
        self.link.restarting.is_some() || self.link.starting || self.startup_error.is_some()
    }

    /// The daemon is being asked whether it runs, or is down: a read's
    /// transport error is not the owner's news yet.
    pub(super) fn link_unsure(&self) -> bool {
        self.link.probing || self.daemon_down()
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
        // No probe could start: the read's own error is the news.
        if !self.link.probing
            && let Some(text) = self.link.held.take()
        {
            self.alert(text);
        }
    }

    /// A read failed: ask the daemon whether it runs (one probe at a time,
    /// only once connected and not while it is being started).
    pub(super) fn link_suspect(&mut self, context: &ComponentContext<Self>) {
        if !self.preferences_loaded || self.daemon_down() || self.link.probing {
            return;
        }
        let Ok(data_dir) = self.data_dir.clone() else {
            return;
        };
        self.link.probing = true;
        let _task = context.spawn_background(move |_| {
            let probed = client::block_on(async {
                tokio::time::timeout(PROBE_TIMEOUT, eliot_daemon::startup::reachable(&data_dir))
                    .await
            });
            Message::Shell(ShellMsg::LinkProbed(probed.unwrap_or(Ok(true))))
        });
    }

    pub(super) fn link_probed(
        &mut self,
        probed: MutationResult<bool>,
        context: &ComponentContext<Self>,
    ) {
        self.link.probing = false;
        let held = self.link.held.take();
        // A refusal other than "nothing answers" is the read's own error.
        if probed.unwrap_or(true) || self.daemon_down() {
            if let Some(text) = held {
                self.alert(text);
            }
            return;
        }
        self.restart_daemon(1, None, context);
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
        self.start_connection(data_dir, delay_ms, context);
    }

    /// The daemon answers again (at launch, after Retry, or after a
    /// restart): the restart is over. True when it was a restart.
    pub(super) fn link_ready(&mut self) -> bool {
        std::mem::take(&mut self.link).restarting.is_some()
    }

    /// The startup path failed with `error`: the next restart attempt, or
    /// the failure view once the attempts are spent (or at the first start).
    pub(super) fn link_failed(
        &mut self,
        error: eliot_core::Error,
        context: &ComponentContext<Self>,
    ) {
        self.link.starting = false;
        match self.link.restarting.take() {
            Some((attempt, _)) if attempt < RESTART_ATTEMPTS => {
                self.restart_daemon(attempt + 1, Some(error.user_message()), context);
            }
            restarting => {
                self.link.gave_up = restarting.is_some();
                self.startup_error = Some(error);
                if self.chat.convo.is_some() {
                    self.chat.live = LiveStatus::Stopped("Swarm is not running".to_owned());
                }
                // The failure and its choices take the main area, from
                // Settings too.
                self.close_popover();
                if self.main != Main::Conversation {
                    self.main = Main::Conversation;
                    self.reprime_fields(context);
                }
            }
        }
    }

    /// Retry on the same data directory.
    pub(super) fn retry_startup(&mut self, context: &ComponentContext<Self>) {
        if let Ok(data_dir) = self.data_dir.clone() {
            self.startup_error = None;
            self.link = LinkState {
                starting: true,
                ..LinkState::default()
            };
            self.alert = None;
            self.set_status("Starting Swarm…");
            self.start_connection(data_dir, 0, context);
        }
    }

    /// Use `selected`, already validated and remembered as the data root by
    /// the daemon's own selection, from now on.
    pub(super) fn switch_data_dir(&mut self, selected: PathBuf, context: &ComponentContext<Self>) {
        // An open conversation belongs to the directory left behind.
        if self.chat.convo.is_some() {
            self.new_task(context);
        }
        self.data_dir = Ok(selected.clone());
        self.note_window_size();
        self.set_status(format!("Starting Swarm in {}", selected.display()));
        self.retry_startup(context);
    }

    /// "Use another data directory…": a Swarm data folder the owner already
    /// has. The daemon's selection validates it and remembers it.
    pub(super) fn choose_data_dir(&mut self, context: &ComponentContext<Self>) {
        let picked = match crate::file_dialog::pick_folder("Use another Swarm data directory") {
            Ok(Some(picked)) => PathBuf::from(picked),
            Ok(None) => return,
            Err(error) => return self.alert(error),
        };
        match eliot_daemon::DataDir::open_existing(&picked)
            .map(|root| root.path().to_path_buf())
            .and_then(|selected| eliot_daemon::persist_selected_root(&selected).map(|()| selected))
        {
            Ok(selected) => self.switch_data_dir(selected, context),
            Err(error) => self.alert(format!(
                "{} cannot be used: {}",
                picked.display(),
                error.user_message()
            )),
        }
    }

    /// The last choice: a new, empty private data directory beside the
    /// current one, which keeps its chats and settings.
    pub(super) fn use_fresh_data_dir(&mut self, context: &ComponentContext<Self>) {
        let Ok(original) = client::data_dir() else {
            return;
        };
        let fresh = original.with_file_name(format!("Eliot-private-{}", client::new_mutation_id()));
        match eliot_daemon::bootstrap::ensure_private_data_dir(&fresh)
            .and_then(|selected| eliot_daemon::persist_selected_root(&selected).map(|()| selected))
        {
            Ok(selected) => self.switch_data_dir(selected, context),
            Err(error) => self.alert(format!(
                "A new data directory was not created: {}",
                error.user_message()
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
    pub(super) fn startup_error_view(
        &self,
        context: &mut ViewContext<Self>,
        palette: Palette,
    ) -> View {
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
}
