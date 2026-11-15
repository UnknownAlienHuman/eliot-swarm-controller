//! Script entries: native editing, revision-checked save, editor launch and Run through a timer.

use super::*;
use crate::theme::winui::Palette;

#[derive(Clone, Debug)]
pub(super) enum ScriptsMsg {
    ScriptsRefreshRequested,
    ScriptsListed(Fallible<ScriptListResponse>),
    ScriptSelected(Option<usize>),
    ScriptLoaded(u64, Fallible<ScriptReadResponse>),
    ScriptDraftChanged(String),
    ScriptValidateRequested,
    ScriptSaveRequested,
    ScriptSaved(u64, bool, Fallible<ScriptSaveResponse>),
    ScriptReloadRequested,
    EditorPathChanged(String),
    OpenInEditorRequested,
    ScriptRunRequested(String),
    ScriptRunSubmitted(Box<Fallible<Command>>),
    ScriptRunSettled(Box<Fallible<Command>>),
}

/// Scripts screen: one text draft over the daemon's `script.read`.
#[derive(Clone, Debug, Default)]
pub(super) struct ScriptsEditor {
    /// Flattened entries: (project, relative path, size).
    pub(super) entries: Vec<(Option<String>, String, u64)>,
    pub(super) unavailable: Vec<String>,
    pub(super) listed: bool,
    pub(super) selected: Option<usize>,
    pub(super) load_epoch: u64,
    pub(super) revision: Option<String>,
    pub(super) display_path: Option<String>,
    pub(super) base: String,
    /// Line-feed text the dirty check and save use.
    pub(super) draft: String,
    /// The editor TextBox's text exactly as reported (CR line breaks).
    pub(super) view: String,
    pub(super) editable: bool,
    pub(super) reason: Option<String>,
    pub(super) status: String,
    pub(super) busy: bool,
    pub(super) editor_exe: String,
    pub(super) run_status: String,
    pub(super) run_busy: bool,
    /// Distinguishes one Run click from the next in the mutation ledger.
    pub(super) run_seq: u64,
}

impl DesktopApp {
    pub(super) fn scripts_update(&mut self, msg: ScriptsMsg, context: &ComponentContext<Self>) {
        match msg {
            ScriptsMsg::ScriptsRefreshRequested => {
                self.load_scripts(context);
                self.load_rules(context);
            }
            ScriptsMsg::ScriptsListed(result) => {
                self.scripts.listed = true;
                match result {
                    Ok(list) => {
                        let previous = self.selected_script();
                        self.scripts.unavailable = list
                            .roots
                            .iter()
                            .filter_map(|root| {
                                root.reason.as_ref().map(|reason| {
                                    format!(
                                        "{}: {reason}",
                                        root.project.as_deref().unwrap_or("global")
                                    )
                                })
                            })
                            .collect();
                        self.scripts.entries = list
                            .roots
                            .into_iter()
                            .flat_map(|root| {
                                let project = root.project;
                                root.entries
                                    .into_iter()
                                    .map(move |entry| (project.clone(), entry.path, entry.size))
                            })
                            .collect();
                        // Keep the selection by identity, not by index.
                        self.scripts.selected = previous.and_then(|(project, path)| {
                            self.scripts
                                .entries
                                .iter()
                                .position(|(p, e, _)| *p == project && *e == path)
                        });
                        if self.scripts.selected.is_none() {
                            self.clear_script_editor();
                        }
                        self.scripts.status =
                            format!("{} script entr(ies) listed.", self.scripts.entries.len());
                    }
                    Err(error) => self.scripts.status = format!("script.list failed: {error}"),
                }
            }
            ScriptsMsg::ScriptSelected(index) => {
                if index.is_some() && index != self.scripts.selected {
                    if self.scripts.draft != self.scripts.base {
                        self.scripts.status =
                            "The draft has unsaved edits: Save or Reload it before opening another entry.".to_owned();
                        return;
                    }
                    self.scripts.selected = index;
                    self.clear_script_editor();
                    self.load_script(context);
                }
            }
            ScriptsMsg::ScriptLoaded(epoch, result) => {
                if epoch != self.scripts.load_epoch {
                    return;
                }
                self.scripts.busy = false;
                match result {
                    Ok(read) => {
                        let text = profiles::normalize_newlines(&read.content.unwrap_or_default());
                        self.scripts.revision = Some(read.revision);
                        self.scripts.display_path = Some(read.display_path);
                        self.scripts.base = text.clone();
                        self.scripts.view = text.clone();
                        self.scripts.draft = text;
                        self.scripts.editable = read.editable;
                        self.scripts.reason = read.reason;
                    }
                    Err(error) => {
                        self.scripts.editable = false;
                        self.scripts.reason = Some(format!("script.read failed: {error}"));
                    }
                }
            }
            ScriptsMsg::ScriptDraftChanged(text) => {
                if self.scripts.editable && !self.scripts.busy {
                    self.scripts.draft = profiles::normalize_newlines(&text);
                    self.scripts.view = text;
                }
            }
            ScriptsMsg::ScriptValidateRequested => self.save_script(context, true),
            ScriptsMsg::ScriptSaveRequested => self.save_script(context, false),
            ScriptsMsg::ScriptSaved(epoch, dry_run, result) => {
                // A save for an entry that is no longer loaded changes nothing here.
                if epoch != self.scripts.load_epoch {
                    return;
                }
                self.scripts.busy = false;
                match result {
                    Ok(_) if dry_run => {
                        self.scripts.status =
                            "Valid: the daemon accepts this text for the entry. Not saved yet; nothing ran.".to_owned();
                    }
                    Ok(reply) => {
                        self.scripts.status = format!(
                            "Saved revision {}; nothing ran. A Run admitted before this save fails its digest check instead of running the new text.",
                            &reply.revision[..12]
                        );
                        self.load_script(context);
                    }
                    Err(error) if error.starts_with("conflict:") => {
                        self.scripts.status = format!(
                            "Not saved: the script changed on disk after it was read. Copy what you need from the draft, then Reload. ({error})"
                        );
                    }
                    Err(error) if error.starts_with("unknown:") => {
                        self.scripts.status = format!(
                            "Save outcome uncertain; Reload to see the current file (the draft is kept): {error}"
                        );
                    }
                    Err(error) => {
                        self.scripts.status = format!(
                            "{} The draft is kept and the file is unchanged: {error}",
                            if dry_run { "Invalid." } else { "Not saved." }
                        );
                    }
                }
            }
            ScriptsMsg::ScriptReloadRequested => {
                self.scripts.status = "Reloaded the saved script.".to_owned();
                self.load_script(context);
            }
            ScriptsMsg::EditorPathChanged(text) => self.scripts.editor_exe = text,
            ScriptsMsg::OpenInEditorRequested => {
                let path = self.scripts.display_path.clone();
                match (scripts::editor_executable(&self.scripts.editor_exe), path) {
                    (Ok(editor), Some(path)) => {
                        // Literal argv, no shell and no file association; the
                        // editor gets no Eliot credentials from this window.
                        let mut command = scripts::editor_command(&editor, &path);
                        self.scripts.status = match command.spawn() {
                            Ok(child) => format!(
                                "Opened {path} in {} (pid {}). Reload here after saving in the editor.",
                                editor.display(),
                                child.id()
                            ),
                            Err(error) => format!("The editor did not start: {error}"),
                        };
                    }
                    (Err(error), _) => self.scripts.status = format!("Open in editor: {error}."),
                    (_, None) => self.scripts.status = "Select a script first.".to_owned(),
                }
            }
            ScriptsMsg::ScriptRunRequested(rule) => {
                if self.submitting() || self.scripts.run_busy {
                    return;
                }
                let (Ok(data_dir), Some((project, _))) =
                    (self.data_dir.clone(), self.selected_script())
                else {
                    return;
                };
                // One ULID per Run click; an uncertain reply keeps the key so
                // a retry reuses the same id and the daemon deduplicates it.
                let key = format!("timer.run:{project:?}:{rule}:{}", self.scripts.run_seq);
                let mutation_id = self.ledger.id_for(&key);
                self.in_flight_key = Some(key);
                self.scripts.run_busy = true;
                self.scripts.run_status = format!("Submitting timer {rule} ({mutation_id})…");
                let _task = context.spawn_background(move |_| {
                    Message::Scripts(ScriptsMsg::ScriptRunSubmitted(Box::new(client::block_on(
                        async {
                            client::run_timer(&data_dir, mutation_id, project, &rule)
                                .await
                                .map_err(|error| error.to_string())
                        },
                    ))))
                });
            }
            ScriptsMsg::ScriptRunSubmitted(result) => {
                let result = *result;
                // Only an uncertain reply keeps the key (so a retry reuses the id);
                // a definite refusal ends that action.
                let uncertain = result
                    .as_ref()
                    .err()
                    .is_some_and(|error| crate::state::uncertain(error));
                self.settle_mutation(!uncertain);
                if result.is_err() && !uncertain {
                    self.scripts.run_seq += 1;
                }
                match result {
                    Ok(command) => {
                        self.scripts.run_seq += 1;
                        self.scripts.run_status = format!(
                            "Admitted {} ({}); waiting for the daemon's outcome…",
                            command.id,
                            format!("{:?}", command.state).to_lowercase()
                        );
                        let Ok(data_dir) = self.data_dir.clone() else {
                            return;
                        };
                        let _task = context.spawn_background(move |cancel| {
                            let cancelled = || cancel.is_cancelled();
                            Message::Scripts(ScriptsMsg::ScriptRunSettled(Box::new(settle_run(
                                &data_dir, command, &cancelled,
                            ))))
                        });
                    }
                    Err(error) => {
                        self.scripts.run_busy = false;
                        self.scripts.run_status = if uncertain {
                            format!(
                                "Run outcome uncertain (Run again reuses its id, so it cannot run twice): {error}"
                            )
                        } else {
                            format!("Run refused: {error}")
                        };
                    }
                }
            }
            ScriptsMsg::ScriptRunSettled(result) => {
                self.scripts.run_busy = false;
                self.scripts.run_status = match *result {
                    Ok(command) => scripts::outcome_line(&command),
                    Err(error) => format!("Run outcome not read: {error}"),
                };
            }
        }
    }

    pub(super) fn selected_script(&self) -> Option<(Option<String>, String)> {
        self.scripts
            .selected
            .and_then(|index| self.scripts.entries.get(index))
            .map(|(project, path, _)| (project.clone(), path.clone()))
    }

    pub(super) fn clear_script_editor(&mut self) {
        let editor = &mut self.scripts;
        editor.revision = None;
        editor.display_path = None;
        editor.base.clear();
        editor.draft.clear();
        editor.view.clear();
        editor.editable = false;
        editor.reason = None;
    }

    pub(super) fn load_scripts(&mut self, context: &ComponentContext<Self>) {
        let Ok(data_dir) = self.data_dir.clone() else {
            return;
        };
        let _task = context.spawn_background(move |_| {
            Message::Scripts(ScriptsMsg::ScriptsListed(client::block_on(async {
                client::list_scripts(&data_dir)
                    .await
                    .map_err(|error| error.to_string())
            })))
        });
    }

    pub(super) fn load_script(&mut self, context: &ComponentContext<Self>) {
        let (Ok(data_dir), Some((project, path))) = (self.data_dir.clone(), self.selected_script())
        else {
            return;
        };
        self.scripts.load_epoch += 1;
        let epoch = self.scripts.load_epoch;
        self.scripts.busy = true;
        let _task = context.spawn_background(move |_| {
            Message::Scripts(ScriptsMsg::ScriptLoaded(
                epoch,
                client::block_on(async {
                    client::read_script(&data_dir, project, path)
                        .await
                        .map_err(|error| error.to_string())
                }),
            ))
        });
    }

    pub(super) fn save_script(&mut self, context: &ComponentContext<Self>, dry_run: bool) {
        let (Ok(data_dir), Some((project, path)), Some(revision)) = (
            self.data_dir.clone(),
            self.selected_script(),
            self.scripts.revision.clone(),
        ) else {
            return;
        };
        if self.scripts.busy || !self.scripts.editable {
            return;
        }
        self.scripts.busy = true;
        self.scripts.status = if dry_run {
            "Validating…"
        } else {
            "Saving…"
        }
        .to_owned();
        let draft = self.scripts.draft.clone();
        let epoch = self.scripts.load_epoch;
        let _task = context.spawn_background(move |_| {
            Message::Scripts(ScriptsMsg::ScriptSaved(
                epoch,
                dry_run,
                client::block_on(async {
                    client::save_script(&data_dir, project, path, revision, draft, dry_run)
                        .await
                        .map_err(|error| error.to_string())
                }),
            ))
        });
    }

    /// Timers whose run action names the selected script.
    pub(super) fn script_timers(&self) -> Vec<String> {
        let Some((project, path)) = self.selected_script() else {
            return Vec::new();
        };
        self.automation
            .rules
            .iter()
            .filter(|namespace| namespace.project == project)
            .filter_map(|namespace| namespace.rules.as_ref().ok())
            .flat_map(|rules| scripts::running_timers(rules, project.as_deref(), &path))
            .collect()
    }

    pub(super) fn scripts_view(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        let timers = self.script_timers();
        let editor = &self.scripts;
        let data = config_view::ScriptsData {
            labels: editor
                .entries
                .iter()
                .map(|(project, path, size)| {
                    format!(
                        "{}: {path} ({size} B)",
                        project.as_deref().unwrap_or("global")
                    )
                })
                .collect(),
            unavailable: &editor.unavailable,
            selected: editor.selected,
            display_path: editor.display_path.as_deref(),
            revision: editor.revision.as_deref(),
            editable: editor.editable,
            reason: editor.reason.as_deref(),
            view: self.primed(&editor.view),
            dirty: editor.draft != editor.base,
            timers: &timers,
            editor_exe: &editor.editor_exe,
            status: &editor.status,
            run_status: &editor.run_status,
            busy: editor.busy,
            run_busy: editor.run_busy || self.submitting(),
        };
        let actions = config_view::ScriptsActions {
            select: context.callback(|value| Message::Scripts(ScriptsMsg::ScriptSelected(value))),
            draft_changed: context
                .callback(|value| Message::Scripts(ScriptsMsg::ScriptDraftChanged(value))),
            validate: context.message(Message::Scripts(ScriptsMsg::ScriptValidateRequested)),
            save: context.message(Message::Scripts(ScriptsMsg::ScriptSaveRequested)),
            reload: context.message(Message::Scripts(ScriptsMsg::ScriptReloadRequested)),
            refresh: context.message(Message::Scripts(ScriptsMsg::ScriptsRefreshRequested)),
            editor_changed: context
                .callback(|value| Message::Scripts(ScriptsMsg::EditorPathChanged(value))),
            open_editor: context.message(Message::Scripts(ScriptsMsg::OpenInEditorRequested)),
            run: timers
                .iter()
                .map(|timer| {
                    context.message(Message::Scripts(ScriptsMsg::ScriptRunRequested(
                        timer.clone(),
                    )))
                })
                .collect(),
        };
        config_view::scripts_screen(data, actions, palette)
    }
}
