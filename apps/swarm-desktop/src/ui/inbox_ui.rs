//! The Owner inbox (#870) as a page (`Inbox.dc.html`): Swarm runs Agents
//! by itself and the owner takes part through this one decision queue.
//! A group filter with counts, Select with Apply recommended and Snooze
//! (one `owner.decide` per item, a stop confirmed first, then "Applied N ·
//! skipped N · failed N"), restore commands with Copy. `owner.inbox` items grouped by
//! kind, each with its subject, age and reason, the recommended action as
//! the filled button, the other offered choices and Snooze; every choice
//! goes to `owner.decide` bound to the revision shown, so a stale choice
//! comes back refused (`stale_item`), says why and reads the inbox again.
//! The per-kind counts in the navigation are `daemon.sidebar` `owner_inbox`,
//! read on the sidebar's cadence. Wire shapes:
//! [`eliot_core::api::OwnerInboxItem`] and friends; nothing here derives a
//! recommendation or a choice.

use std::collections::BTreeMap;

use super::*;
use crate::theme::winui::Palette;
use eliot_core::api::{
    OwnerDecideRequest, OwnerDecideResponse, OwnerInboxItem, OwnerInboxItemRef, OwnerInboxKind,
    OwnerInboxResponse, ProfileCommandIssue,
    auth_login::{AuthLoginResponse, AuthLoginStatus},
};

/// The choice that asks the daemon for its recommended action.
const RECOMMENDED: &str = "recommended";
const SNOOZE: &str = "snooze";
/// An inspection result is shown up to this many characters.
const MAX_RESULT_CHARS: usize = 4_000;

#[derive(Clone, Debug)]
pub(super) enum InboxMsg {
    /// Owner counts and executable diagnostics from the sidebar's status read.
    Status(Fallible<eliot_core::api::DaemonSidebarResponse>),
    /// One `owner.inbox` read (its sequence).
    Loaded(u64, Fallible<OwnerInboxResponse>),
    /// Decide an item: its reference as shown, the choice (`recommended`,
    /// an offered choice or `snooze`) and the button's words.
    Decide(OwnerInboxItemRef, String, String),
    /// Edit the reason for abandoning a retained reservation.
    AbandonReason(OwnerInboxItemRef, String),
    /// Queue the shown harness sign-in directly through `auth.login`.
    AuthLogin(String, String),
    /// The direct sign-in result for one inbox revision.
    AuthLoginResult(String, Box<MutationResult<AuthLoginResponse>>),
    /// A decision's outcome: its ledger key, item cursor, choice and words.
    Decided(
        String,
        String,
        String,
        String,
        Box<MutationResult<OwnerDecideResponse>>,
    ),
    /// Open an item's Agent (an answer is given in its question form).
    OpenAgent(String),
    Refresh,
    NextPage,
    /// Show one group only (`None`: every group).
    Filter(Option<OwnerInboxKind>),
    /// Select several items (or stop selecting).
    SelectToggled,
    /// Check or uncheck one item (its cursor and revision).
    Selected(String, bool),
    /// Check or uncheck every item shown.
    SelectAll(bool),
    /// Apply the recommended action to the selected items (after a
    /// confirmation when one of them stops an Agent).
    ApplyRecommended,
    /// The stop confirmation's answer.
    BulkConfirmed(bool),
    /// Snooze the selected items.
    BulkSnooze,
    /// One bulk pass: how many were skipped, and each decision's ledger
    /// key, item cursor, choice and outcome.
    BulkFinished(
        usize,
        Vec<(String, String, String, MutationResult<OwnerDecideResponse>)>,
    ),
    /// A choice from an item's menu: its cursor and revision, a unit
    /// separator, the choice.
    MenuPicked(String),
    /// Copy an item's restore commands.
    CopyCommands(String),
}

/// The last outcome of a decision on one item, in words.
#[derive(Clone, Debug)]
struct Outcome {
    text: String,
    error: bool,
}

#[derive(Default)]
pub(super) struct InboxState {
    /// Visible items by kind (`daemon.sidebar`).
    pub(super) counts: BTreeMap<OwnerInboxKind, usize>,
    pub(super) profile_command_issues: Vec<ProfileCommandIssue>,
    items: Vec<OwnerInboxItem>,
    /// The server cursor for the next page in blocker order.
    next: Option<String>,
    error: Option<String>,
    loaded: bool,
    /// Tags reads: an older read's reply is dropped.
    seq: u64,
    reading: bool,
    /// One mutation id per item revision and choice, kept after an
    /// uncertain outcome so choosing it again retries the same decision.
    ids: HashMap<String, String>,
    /// Reason entered for each exact revision, frozen while its decision ID is retained.
    abandon_reasons: HashMap<String, String>,
    /// Items (cursors) whose decision is running.
    deciding: HashSet<String>,
    /// Harness and revision pairs whose direct sign-in call is running.
    auth_signing: HashSet<String>,
    /// The last outcome per item cursor.
    outcomes: HashMap<String, Outcome>,
    /// The last direct sign-in outcome per harness and exact revision.
    auth_outcomes: HashMap<String, Outcome>,
    /// The last refusal, with its item's cursor: shown above the items
    /// once a read no longer lists that item (a stale choice), until the
    /// next decision.
    refusal: Option<(String, String)>,
    /// The group shown (`None`: all).
    filter: Option<OwnerInboxKind>,
    /// Several items are being selected, and which (cursor@revision).
    selecting: bool,
    selected: std::collections::BTreeSet<String>,
    /// The Agents Apply recommended would stop, waiting for the owner's
    /// confirmation.
    pub(super) confirm: Option<Vec<String>>,
    bulk_running: bool,
    /// "Applied N · skipped N · failed N" after a bulk pass.
    bulk_result: Option<String>,
}

impl InboxState {
    /// Every visible item.
    pub(super) fn total(&self) -> usize {
        self.counts.values().sum()
    }
}

/// A kind's group title.
fn kind_title(kind: OwnerInboxKind) -> &'static str {
    match kind {
        OwnerInboxKind::HarnessRequest => "Questions and approvals",
        OwnerInboxKind::UnknownCommand => "Unconfirmed commands",
        OwnerInboxKind::UnknownTurn => "Unconfirmed turns",
        OwnerInboxKind::Hold => "Provider holds",
        OwnerInboxKind::HarnessCheck => "Harness checks",
        OwnerInboxKind::Agent => "Failed or lost Agents",
        OwnerInboxKind::BlockedGoal => "Blocked Goals",
        OwnerInboxKind::LaunchQueue => "Waiting launches",
        OwnerInboxKind::Stuck => "Stuck",
        OwnerInboxKind::Handoff => "Handoffs to check",
        OwnerInboxKind::WorkNotInGit => "Work not in Git",
        OwnerInboxKind::HarnessAuth => "Harness sign-in",
        OwnerInboxKind::ArtifactCapacity => "Artifact capacity",
    }
}

/// An item's kind in a word or two, after its task ("· Request").
fn kind_word(kind: OwnerInboxKind) -> &'static str {
    match kind {
        OwnerInboxKind::HarnessRequest => "Request",
        OwnerInboxKind::UnknownCommand | OwnerInboxKind::UnknownTurn => "Outcome unknown",
        OwnerInboxKind::Hold => "Provider hold",
        OwnerInboxKind::HarnessCheck => "Harness check",
        OwnerInboxKind::Agent => "Failed or lost",
        OwnerInboxKind::BlockedGoal => "Goal blocked",
        OwnerInboxKind::LaunchQueue => "Waiting launch",
        OwnerInboxKind::Stuck => "Stuck",
        OwnerInboxKind::Handoff => "Handoff to check",
        OwnerInboxKind::WorkNotInGit => "Not in Git",
        OwnerInboxKind::HarnessAuth => "Sign-in",
        OwnerInboxKind::ArtifactCapacity => "Capacity",
    }
}

/// A kind's count in the navigation ("2 requests").
fn kind_count(kind: OwnerInboxKind, count: usize) -> String {
    let (one, many) = match kind {
        OwnerInboxKind::HarnessRequest => ("request", "requests"),
        OwnerInboxKind::UnknownCommand => ("unconfirmed command", "unconfirmed commands"),
        OwnerInboxKind::UnknownTurn => ("unconfirmed turn", "unconfirmed turns"),
        OwnerInboxKind::Hold => ("hold", "holds"),
        OwnerInboxKind::HarnessCheck => ("harness check", "harness checks"),
        OwnerInboxKind::Agent => ("failed or lost Agent", "failed or lost Agents"),
        OwnerInboxKind::BlockedGoal => ("blocked Goal", "blocked Goals"),
        OwnerInboxKind::LaunchQueue => ("waiting launch", "waiting launches"),
        OwnerInboxKind::Stuck => ("stuck", "stuck"),
        OwnerInboxKind::Handoff => ("handoff to check", "handoffs to check"),
        OwnerInboxKind::WorkNotInGit => ("item not in Git", "items not in Git"),
        OwnerInboxKind::HarnessAuth => ("sign-in", "sign-ins"),
        OwnerInboxKind::ArtifactCapacity => ("capacity warning", "capacity warnings"),
    };
    eliot_core::count_words(count as u64, one, many)
}

/// The navigation's counter: every kind with items, "Nothing waiting" else.
pub(super) fn counts_line(counts: &BTreeMap<OwnerInboxKind, usize>) -> String {
    let parts: Vec<String> = counts
        .iter()
        .filter(|(_, count)| **count > 0)
        .map(|(kind, count)| kind_count(*kind, *count))
        .collect();
    if parts.is_empty() {
        "Nothing waiting".to_owned()
    } else {
        parts.join(" · ")
    }
}

/// A choice's button words ("resume" → "Resume", "clear_hold" → "Clear hold").
fn choice_words(choice: &str) -> String {
    let words = choice.replace(['_', '.'], " ");
    let mut chars = words.chars();
    match chars.next() {
        Some(first) => first.to_uppercase().chain(chars).collect(),
        None => words,
    }
}

/// The Agent an item is about, from its subject (`a12`, `a12:g1:…`).
fn subject_agent(subject: &str) -> Option<&str> {
    // A kind may lead its subject with a word (`waiting:a4:g1`).
    subject.split(':').take(2).find(|part| {
        part.strip_prefix('a').is_some_and(|digits| {
            !digits.is_empty() && digits.chars().all(|ch| ch.is_ascii_digit())
        })
    })
}

fn cursor(item: &OwnerInboxItemRef) -> String {
    format!("{}/{}", item.kind.as_str(), item.subject)
}

impl DesktopApp {
    pub(super) fn inbox_update(&mut self, msg: InboxMsg, context: &ComponentContext<Self>) {
        match msg {
            InboxMsg::Status(Ok(status)) => {
                self.inbox.counts = status.owner_inbox;
                self.inbox.profile_command_issues = status.profile_command_issues;
            }
            // The last counts stay; the daemon's own notice speaks for it.
            InboxMsg::Status(Err(_)) => {}
            InboxMsg::Loaded(seq, read) => {
                if seq != self.inbox.seq {
                    return;
                }
                let inbox = &mut self.inbox;
                inbox.reading = false;
                match read {
                    Ok(read) => {
                        inbox.next = read.next;
                        inbox.items = read.items;
                        inbox.error = None;
                        inbox.loaded = true;
                        let shown: HashSet<String> =
                            inbox.items.iter().map(OwnerInboxItem::cursor).collect();
                        inbox.outcomes.retain(|key, _| shown.contains(key));
                        let shown_auth: HashSet<String> = inbox
                            .items
                            .iter()
                            .filter(|item| item.kind == OwnerInboxKind::HarnessAuth)
                            .map(|item| format!("{}@{}", item.subject, item.revision))
                            .collect();
                        inbox
                            .auth_outcomes
                            .retain(|key, _| shown_auth.contains(key));
                    }
                    Err(error) => inbox.error = Some(error),
                }
            }
            InboxMsg::Decide(item, choice, words) => {
                let key = format!("{}@{}:{choice}", cursor(&item), item.revision);
                let at = cursor(&item);
                if self.inbox.deciding.contains(&at) {
                    return;
                }
                let Ok(data_dir) = self.data_dir.clone() else {
                    return;
                };
                let args = if choice == "abandon_reservation" {
                    let Some(reason) = self
                        .inbox
                        .abandon_reasons
                        .get(&key)
                        .filter(|reason| !reason.trim().is_empty() && reason.len() <= 1024)
                    else {
                        return;
                    };
                    Some(serde_json::json!({ "confirm": true, "reason": reason }))
                } else {
                    None
                };
                let id = self
                    .inbox
                    .ids
                    .entry(key.clone())
                    .or_insert_with(client::new_mutation_id)
                    .clone();
                self.inbox.deciding.insert(at.clone());
                self.inbox.outcomes.remove(&at);
                self.inbox.refusal = None;
                let _task = context.spawn_background(move |_| {
                    let request = OwnerDecideRequest {
                        id,
                        item,
                        choice: choice.clone(),
                        args,
                    };
                    let result = client::block_on(client::owner_decide(&data_dir, &request));
                    Message::Inbox(InboxMsg::Decided(key, at, choice, words, Box::new(result)))
                });
            }
            InboxMsg::AbandonReason(item, reason) => {
                let at = cursor(&item);
                let key = format!("{at}@{}:abandon_reservation", item.revision);
                if !self.inbox.ids.contains_key(&key) {
                    self.inbox.abandon_reasons.insert(key, reason);
                }
            }
            InboxMsg::AuthLogin(harness, revision) => {
                let task = format!("{harness}@{revision}");
                if self.inbox.auth_signing.contains(&task) {
                    return;
                }
                let Ok(data_dir) = self.data_dir.clone() else {
                    return;
                };
                self.inbox.auth_signing.insert(task.clone());
                self.inbox.auth_outcomes.remove(&task);
                let _task = context.spawn_background(move |_| {
                    let result = client::block_on(client::auth_login(
                        &data_dir,
                        &harness,
                        Some(revision.clone()),
                    ));
                    Message::Inbox(InboxMsg::AuthLoginResult(task, Box::new(result)))
                });
            }
            InboxMsg::AuthLoginResult(task, result) => {
                self.inbox.auth_signing.remove(&task);
                let outcome = match *result {
                    Ok(response) => Outcome {
                        text: match response.status {
                            AuthLoginStatus::Queued => "Sign-in requested.".to_owned(),
                            AuthLoginStatus::Open => "A sign-in window is already open.".to_owned(),
                            AuthLoginStatus::Cooldown => {
                                "Sign-in is cooling down. Try again later.".to_owned()
                            }
                            AuthLoginStatus::Unknown => {
                                "Sign-in outcome unknown. Complete sign-in with the vendor."
                                    .to_owned()
                            }
                        },
                        error: false,
                    },
                    Err(error) => Outcome {
                        text: error.user_message(),
                        error: true,
                    },
                };
                self.inbox.auth_outcomes.insert(task, outcome);
                self.read_inbox(context);
                self.read_inbox_counts(context);
            }
            InboxMsg::Decided(key, at, choice, words, result) => {
                let subject = at
                    .split_once('/')
                    .map_or(at.as_str(), |(_, subject)| subject);
                let subject = match subject_agent(subject) {
                    Some(agent) => self.agent_title(agent),
                    None => subject.to_owned(),
                };
                let inbox = &mut self.inbox;
                inbox.deciding.remove(&at);
                let outcome = match *result {
                    Ok(reply) => {
                        inbox.ids.remove(&key);
                        inbox.abandon_reasons.remove(&key);
                        let text = if reply.snoozed {
                            "Snoozed until something new happens on it.".to_owned()
                        } else {
                            match reply.result {
                                Some(result) if choice == "inspect" => {
                                    let text =
                                        serde_json::to_string_pretty(&result).unwrap_or_default();
                                    if text.chars().count() > MAX_RESULT_CHARS {
                                        let shown: String =
                                            text.chars().take(MAX_RESULT_CHARS).collect();
                                        format!("{shown}\n…")
                                    } else {
                                        text
                                    }
                                }
                                _ => format!("{words}: done."),
                            }
                        };
                        Outcome { text, error: false }
                    }
                    // Uncertain: never a new decision on our own; the same
                    // choice retries this one (same mutation id).
                    Err(error) if crate::state::uncertain(&error) => Outcome {
                        text: format!(
                            "Not confirmed ({}). Choosing it again retries the same decision.",
                            error.user_message()
                        ),
                        error: true,
                    },
                    Err(error) => {
                        inbox.ids.remove(&key);
                        Outcome {
                            text: error.user_message(),
                            error: true,
                        }
                    }
                };
                if outcome.error {
                    inbox.refusal = Some((
                        at.clone(),
                        format!("{words} on {subject}: {}", outcome.text),
                    ));
                }
                inbox.outcomes.insert(at, outcome);
                // A decision or a refusal changes the queue: read it again.
                self.read_inbox(context);
                self.read_inbox_counts(context);
            }
            InboxMsg::OpenAgent(agent) => {
                self.monitor_update(MonitorMsg::RowOpened(agent), context);
            }
            InboxMsg::Refresh => {
                self.read_inbox(context);
                self.read_inbox_counts(context);
            }
            InboxMsg::NextPage => {
                if let Some(after) = self.inbox.next.clone().filter(|_| !self.inbox.reading) {
                    self.inbox.selected.clear();
                    self.read_inbox_page(Some(after), context);
                }
            }
            InboxMsg::Filter(kind) => self.inbox.filter = kind,
            InboxMsg::SelectToggled => {
                let inbox = &mut self.inbox;
                inbox.selecting = !inbox.selecting;
                if !inbox.selecting {
                    inbox.selected.clear();
                }
            }
            InboxMsg::Selected(key, on) => {
                if on {
                    self.inbox.selected.insert(key);
                } else {
                    self.inbox.selected.remove(&key);
                }
            }
            InboxMsg::SelectAll(on) => {
                let inbox = &mut self.inbox;
                let keys: Vec<String> = inbox
                    .items
                    .iter()
                    .filter(|item| inbox.filter.is_none_or(|kind| item.kind == kind))
                    .map(selection_key)
                    .collect();
                for key in keys {
                    if on {
                        inbox.selected.insert(key);
                    } else {
                        inbox.selected.remove(&key);
                    }
                }
            }
            InboxMsg::ApplyRecommended => {
                if self.inbox.bulk_running {
                    return;
                }
                let stops: Vec<String> = self
                    .inbox
                    .items
                    .iter()
                    .filter(|item| self.inbox.selected.contains(&selection_key(item)))
                    .filter(|item| {
                        item.recommended
                            .as_ref()
                            .is_some_and(|action| action.choice.contains("stop"))
                    })
                    .map(|item| match subject_agent(&item.subject) {
                        Some(agent) => self.agent_title(agent),
                        None => item.subject.clone(),
                    })
                    .collect();
                if stops.is_empty() {
                    self.run_bulk(false, context);
                } else {
                    self.inbox.confirm = Some(stops);
                }
            }
            InboxMsg::BulkConfirmed(apply) => {
                self.inbox.confirm = None;
                if apply {
                    self.run_bulk(false, context);
                }
            }
            InboxMsg::BulkSnooze => {
                if !self.inbox.bulk_running {
                    self.run_bulk(true, context);
                }
            }
            InboxMsg::BulkFinished(skipped, results) => {
                let inbox = &mut self.inbox;
                inbox.bulk_running = false;
                let (mut applied, mut failed) = (0usize, 0usize);
                for (key, at, _choice, result) in results {
                    inbox.deciding.remove(&at);
                    let outcome = match result {
                        Ok(reply) => {
                            applied += 1;
                            inbox.ids.remove(&key);
                            let prefix = format!("{at}@");
                            inbox
                                .selected
                                .retain(|selected| !selected.starts_with(&prefix));
                            Outcome {
                                text: if reply.snoozed {
                                    "Snoozed until something new happens on it.".to_owned()
                                } else {
                                    "Done.".to_owned()
                                },
                                error: false,
                            }
                        }
                        // Uncertain: never a new decision on our own; the
                        // same choice retries this one (same mutation id).
                        Err(error) if crate::state::uncertain(&error) => {
                            failed += 1;
                            Outcome {
                                text: format!(
                                    "Outcome unknown ({}). Choosing it again retries the same decision.",
                                    error.user_message()
                                ),
                                error: true,
                            }
                        }
                        Err(error) => {
                            failed += 1;
                            inbox.ids.remove(&key);
                            Outcome {
                                text: error.user_message(),
                                error: true,
                            }
                        }
                    };
                    inbox.outcomes.insert(at, outcome);
                }
                inbox.bulk_result = Some(format!(
                    "Applied {applied} · skipped {skipped} · failed {failed}"
                ));
                self.read_inbox(context);
                self.read_inbox_counts(context);
            }
            InboxMsg::MenuPicked(key) => {
                let Some((item_key, choice)) = key.split_once('\u{1f}') else {
                    return;
                };
                let Some(item) = self
                    .inbox
                    .items
                    .iter()
                    .find(|item| selection_key(item) == item_key)
                    .cloned()
                else {
                    return;
                };
                let agent = subject_agent(&item.subject).map(str::to_owned);
                let message = match (item.kind, choice, agent) {
                    (OwnerInboxKind::HarnessAuth, "sign_in", _) => {
                        InboxMsg::AuthLogin(item.subject, item.revision)
                    }
                    (_, "answer", Some(agent)) => InboxMsg::OpenAgent(agent),
                    _ => InboxMsg::Decide(
                        OwnerInboxItemRef {
                            kind: item.kind,
                            subject: item.subject,
                            revision: item.revision,
                        },
                        choice.to_owned(),
                        choice_words(choice),
                    ),
                };
                self.inbox_update(message, context);
            }
            InboxMsg::CopyCommands(text) => match crate::clipboard::set_text(&text) {
                Ok(()) => self.set_status("Restore commands copied.".to_owned()),
                Err(error) => self.alert(format!("Not copied: {error}")),
            },
        }
    }

    /// Read the first inbox page (in the sidebar's batch when one is being
    /// collected).
    pub(super) fn read_inbox(&mut self, context: &ComponentContext<Self>) {
        self.read_inbox_page(None, context);
    }

    fn read_inbox_page(&mut self, after: Option<String>, context: &ComponentContext<Self>) {
        let Ok(data_dir) = self.data_dir.clone() else {
            return;
        };
        self.inbox.seq += 1;
        self.inbox.reading = true;
        let seq = self.inbox.seq;
        self.spawn_read(context, move || {
            let read = client::block_on(client::owner_inbox(&data_dir, after))
                .map_err(|error| error.user_message());
            ShellMsg::Inbox(InboxMsg::Loaded(seq, read))
        });
    }

    /// Read the per-kind counts for the navigation.
    pub(super) fn read_inbox_counts(&mut self, context: &ComponentContext<Self>) {
        let Ok(data_dir) = self.data_dir.clone() else {
            return;
        };
        self.spawn_read(context, move || {
            let read = client::block_on(client::sidebar_status(&data_dir))
                .map_err(|error| error.user_message());
            ShellMsg::Inbox(InboxMsg::Status(read))
        });
    }

    /// Decide every selected item in one pass (`owner.decide` per item,
    /// each with its own retained mutation id, one at a time): Apply
    /// recommended (`snooze` false) or Snooze. An item without a
    /// recommended action, or whose recommendation needs its own form
    /// (an answer, a sign-in, a reason), is skipped.
    fn run_bulk(&mut self, snooze: bool, context: &ComponentContext<Self>) {
        let Ok(data_dir) = self.data_dir.clone() else {
            return;
        };
        let inbox = &mut self.inbox;
        let mut jobs: Vec<(String, String, String, OwnerDecideRequest)> = Vec::new();
        let mut skipped = 0usize;
        for item in inbox
            .items
            .iter()
            .filter(|item| inbox.selected.contains(&selection_key(item)))
        {
            let at = item.cursor();
            let choice = if snooze {
                if item.has_unknown_outcome() {
                    skipped += 1;
                    continue;
                }
                SNOOZE
            } else {
                match item
                    .recommended
                    .as_ref()
                    .map(|action| action.choice.as_str())
                {
                    Some("answer" | "sign_in" | "abandon_reservation") | None => {
                        skipped += 1;
                        continue;
                    }
                    Some(_) => RECOMMENDED,
                }
            };
            if inbox.deciding.contains(&at) {
                skipped += 1;
                continue;
            }
            let key = format!("{at}@{}:{choice}", item.revision);
            let id = inbox
                .ids
                .entry(key.clone())
                .or_insert_with(client::new_mutation_id)
                .clone();
            inbox.deciding.insert(at.clone());
            inbox.outcomes.remove(&at);
            jobs.push((
                key,
                at,
                choice.to_owned(),
                OwnerDecideRequest {
                    id,
                    item: OwnerInboxItemRef {
                        kind: item.kind,
                        subject: item.subject.clone(),
                        revision: item.revision.clone(),
                    },
                    choice: choice.to_owned(),
                    args: None,
                },
            ));
        }
        inbox.bulk_running = true;
        inbox.bulk_result = None;
        inbox.refusal = None;
        let _task = context.spawn_background(move |_| {
            let results = jobs
                .into_iter()
                .map(|(key, at, choice, request)| {
                    let result = client::block_on(client::owner_decide(&data_dir, &request));
                    (key, at, choice, result)
                })
                .collect();
            Message::Inbox(InboxMsg::BulkFinished(skipped, results))
        });
    }

    /// The Inbox page (`Inbox.dc.html`): the title and Select, the group
    /// filter with counts, the selection bar, then the items by group.
    pub(super) fn inbox_view(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        let inbox = &self.inbox;
        let mut items: Vec<KeyedView> = Vec::new();
        let selecting = inbox.selecting;
        items.push(KeyedView::new(
            format!("title:{selecting}"),
            page_ui::toolbar(
                page_ui::title("Inbox"),
                vec![KeyedView::new(
                    "select",
                    m3::chip(palette)
                        .resource_overrides(page_ui::selected_chip(palette, selecting))
                        .automation_name(if selecting {
                            "Select several items (on)"
                        } else {
                            "Select several items"
                        })
                        .on_click(context.message(Message::Inbox(InboxMsg::SelectToggled)))
                        .content(m3::chip_content(
                            Some(m3::glyph::CHECKLIST),
                            "Select",
                            false,
                            tokens::layout::PROJECT_CHIP_WIDTH,
                        )),
                )],
            ),
        ));
        // The group filter: All and every group with items, with the
        // daemon's counts.
        let mut chips: Vec<KeyedView> = vec![KeyedView::new(
            format!("all:{}:{}", inbox.filter.is_none(), inbox.total()),
            page_ui::filter_chip(
                palette,
                "All",
                Some(inbox.total()),
                inbox.filter.is_none(),
                context.message(Message::Inbox(InboxMsg::Filter(None))),
            ),
        )];
        for kind in OwnerInboxKind::ALL {
            let count = inbox.counts.get(&kind).copied().unwrap_or(0);
            if count == 0 && inbox.filter != Some(kind) {
                continue;
            }
            let selected = inbox.filter == Some(kind);
            chips.push(KeyedView::new(
                format!("{}:{selected}:{count}", kind.as_str()),
                page_ui::filter_chip(
                    palette,
                    kind_title(kind),
                    Some(count),
                    selected,
                    context.message(Message::Inbox(InboxMsg::Filter(Some(kind)))),
                ),
            ));
        }
        items.push(KeyedView::new("filters", page_ui::chip_rows(chips, 5)));
        let shown: Vec<&OwnerInboxItem> = inbox
            .items
            .iter()
            .filter(|item| inbox.filter.is_none_or(|kind| item.kind == kind))
            .collect();
        if selecting {
            items.push(KeyedView::new(
                "bulk",
                self.inbox_bulk_bar(&shown, context, palette),
            ));
        }
        if let Some(result) = &inbox.bulk_result {
            items.push(KeyedView::new(
                format!("bulk-result:{result}"),
                m3::body(result.clone())
                    .automation_name(format!("Result: {result}"))
                    .is_text_selection_enabled(true),
            ));
        }
        if let Some(error) = &inbox.error {
            items.push(KeyedView::new(
                "error",
                page_ui::read_failed(
                    palette,
                    format!("The inbox could not be read: {error}"),
                    "Read again",
                    !inbox.reading,
                    context.message(Message::Inbox(InboxMsg::Refresh)),
                ),
            ));
        }
        // A refused choice whose item the inbox no longer lists (a stale
        // choice): the daemon's words stay until the next decision.
        if let Some((at, text)) = inbox
            .refusal
            .as_ref()
            .filter(|(at, _)| !inbox.items.iter().any(|item| item.cursor() == *at))
        {
            items.push(KeyedView::new(
                format!("refusal:{at}"),
                page_ui::read_failed(
                    palette,
                    format!("This item changed. {text}"),
                    "Read again",
                    !inbox.reading,
                    context.message(Message::Inbox(InboxMsg::Refresh)),
                ),
            ));
        }
        if !inbox.loaded && inbox.error.is_none() {
            items.push(KeyedView::new(
                "reading",
                page_ui::quiet("Reading the inbox…"),
            ));
        } else if inbox.loaded && shown.is_empty() {
            items.push(KeyedView::new(
                "empty",
                page_ui::quiet(if inbox.next.is_some() {
                    "No matching items on this page."
                } else {
                    "Nothing waits for you."
                }),
            ));
        }
        for (priority, title) in [
            (0, "Blocking work"),
            (1, "Unresolved outcomes and work at risk"),
            (2, "Recommended actions"),
            (3, "Other decisions"),
        ] {
            let group: Vec<&OwnerInboxItem> = shown
                .iter()
                .copied()
                .filter(|item| item.page_key().0 == priority)
                .collect();
            if group.is_empty() {
                continue;
            }
            items.push(KeyedView::new(
                format!("priority:{priority}"),
                page_ui::group_heading(title),
            ));
            let rows: Vec<KeyedView> = group
                .into_iter()
                .map(|item| {
                    KeyedView::new(
                        format!("item:{}@{}", item.cursor(), item.revision),
                        self.inbox_item(item, context, palette),
                    )
                })
                .collect();
            items.push(KeyedView::new(
                format!("list:{priority}"),
                StackPanel::new()
                    .spacing(tokens::spacing::ROW_GAP)
                    .keyed_children(rows),
            ));
        }
        if inbox.next.is_some() {
            items.push(KeyedView::new(
                "more",
                m3::text_button(palette)
                    .is_enabled(!inbox.reading)
                    .on_click(context.message(Message::Inbox(InboxMsg::NextPage)))
                    .content(m3::label("Next page")),
            ));
        }
        page_ui::page("Owner inbox", self.page_width(tokens::page::WIDTH), items)
    }

    /// The selection bar (`.bulk`): select all, the count, Snooze and
    /// Apply recommended.
    fn inbox_bulk_bar(
        &self,
        shown: &[&OwnerInboxItem],
        context: &mut ViewContext<Self>,
        palette: Palette,
    ) -> View {
        let inbox = &self.inbox;
        let selected = shown
            .iter()
            .filter(|item| inbox.selected.contains(&selection_key(item)))
            .count();
        let all = selected > 0 && selected == shown.len();
        let busy = inbox.bulk_running;
        let (fill, on) = palette.status_chip(crate::theme::StatusTone::Active);
        Border::new()
            .background(fill)
            .corner_radius(tokens::radius::CONTAINER)
            .min_height(tokens::height::CONTROL + tokens::spacing::MD)
            .padding(Thickness::new(
                tokens::spacing::INSET,
                0.0,
                tokens::spacing::SM,
                0.0,
            ))
            .automation_name(format!("{selected} selected"))
            .content(
                Grid::new()
                    .columns([
                        GridLength::Auto,
                        GridLength::STAR,
                        GridLength::Auto,
                        GridLength::Auto,
                    ])
                    .column_spacing(tokens::spacing::INSET)
                    .children((
                        Grid::new().keyed_children(vec![KeyedView::new(
                            format!("all:{all}"),
                            CheckBox::new()
                                .is_checked(all)
                                .min_width(0.0)
                                .vertical_alignment(VerticalAlignment::Center)
                                .automation_name(format!("Select all {}", shown.len()))
                                .on_is_checked_changed(crate::views::check_changes(
                                    context.callback(|on: bool| {
                                        Message::Inbox(InboxMsg::SelectAll(on))
                                    }),
                                )),
                        )]),
                        m3::text(
                            if busy {
                                format!("Deciding {selected}…")
                            } else {
                                format!("{selected} selected")
                            },
                            tokens::page::HEADING,
                        )
                        .grid_column(1)
                        .foreground(on)
                        .vertical_alignment(VerticalAlignment::Center),
                        m3::text_button(palette)
                            .grid_column(2)
                            .is_enabled(selected > 0 && !busy)
                            .automation_name(format!("Snooze {selected} selected"))
                            .on_click(context.message(Message::Inbox(InboxMsg::BulkSnooze)))
                            .content(m3::label("Snooze")),
                        m3::filled(palette)
                            .grid_column(3)
                            .is_enabled(selected > 0 && !busy)
                            .automation_name(format!(
                                "Apply the recommended action to {selected} selected"
                            ))
                            .on_click(context.message(Message::Inbox(InboxMsg::ApplyRecommended)))
                            .content(m3::label("Apply recommended")),
                    )),
            )
            .into()
    }

    /// The confirmation before Apply recommended stops Agents: which ones.
    pub(super) fn inbox_confirmation(
        &self,
        context: &mut ViewContext<Self>,
        palette: Palette,
    ) -> Option<ContentDialog> {
        let Some(names) = &self.inbox.confirm else {
            return None;
        };
        let lines: Vec<KeyedView> = names
            .iter()
            .enumerate()
            .map(|(index, name)| {
                KeyedView::new(
                    index,
                    m3::body(format!("• {name}")).text_wrapping(TextWrapping::Wrap),
                )
            })
            .collect();
        Some(super::dialog_ui::surface(
            context,
            self.dialog.epoch,
            palette,
            "Stop these Agents?".to_owned(),
            StackPanel::new()
                .spacing(tokens::spacing::SM)
                .children((
                    m3::body(
                        "The recommended action stops them. Their worktrees, branches and records stay.",
                    )
                    .text_wrapping(TextWrapping::Wrap),
                    StackPanel::new()
                        .spacing(tokens::spacing::XS)
                        .keyed_children(lines),
                )).into(),
            super::dialog_ui::actions(
                m3::text_button(palette)
                    .element_ref(&self.dialog.close)
                    .automation_name("Cancel")
                    .on_click(context.message(Message::Inbox(InboxMsg::BulkConfirmed(false))))
                    .content(m3::label("Cancel")).into(),
                View::from(m3::destructive(palette)
                    .element_ref(&self.dialog.primary)
                    .automation_name("Apply recommended and stop")
                    .on_click(context.message(Message::Inbox(InboxMsg::BulkConfirmed(true))))
                    .content(m3::label("Apply recommended"))),
            ),
            tokens::layout::DIALOG_WIDTH,
        ))
    }

    /// One item (`.ibx`): its check while selecting, the task and kind,
    /// the reason in one line, its age, the recommended action with the
    /// other choices in its menu, Snooze; then the restore commands with
    /// Copy, a reason field, and the last outcome.
    fn inbox_item(
        &self,
        item: &OwnerInboxItem,
        context: &mut ViewContext<Self>,
        palette: Palette,
    ) -> View {
        let inbox = &self.inbox;
        let at = item.cursor();
        let auth_task = format!("{}@{}", item.subject, item.revision);
        let busy = inbox.deciding.contains(&at)
            || (item.kind == OwnerInboxKind::HarnessAuth
                && inbox.auth_signing.contains(&auth_task));
        let agent = subject_agent(&item.subject);
        let mut subject = match agent {
            Some(agent) => self.agent_title(agent),
            None => item.subject.clone(),
        };
        if let Some(group) = item.group.as_ref().filter(|group| group.affected > 1) {
            subject.push_str(&format!(" · {} affected", group.affected));
        }
        let age = crate::nav::short_age(item.age_ms, 0);
        let reason = choice_words(&eliot_core::reason_words(&item.reason));
        let kind = kind_word(item.kind);
        let reference = OwnerInboxItemRef {
            kind: item.kind,
            subject: item.subject.clone(),
            revision: item.revision.clone(),
        };
        let decide = |choice: &str, words: &str| {
            let (reference, choice, words) =
                (reference.clone(), choice.to_owned(), words.to_owned());
            context.message(Message::Inbox(InboxMsg::Decide(reference, choice, words)))
        };
        let abandon_key = format!("{at}@{}:abandon_reservation", item.revision);
        let abandon_reason = inbox
            .abandon_reasons
            .get(&abandon_key)
            .cloned()
            .unwrap_or_default();
        let reason_ok = !abandon_reason.trim().is_empty() && abandon_reason.len() <= 1024;

        // The main action: the recommended one, else the restore commands'
        // Copy, else the first choice.
        let mut actions: Vec<KeyedView> = Vec::new();
        let recommended = item
            .recommended
            .as_ref()
            .map(|action| action.choice.as_str());
        let main_click = |choice: &str| -> (String, Callback<()>) {
            let words = choice_words(choice);
            match (item.kind, choice, agent) {
                (OwnerInboxKind::HarnessAuth, "sign_in", _) => (
                    "Sign in".to_owned(),
                    context.message(Message::Inbox(InboxMsg::AuthLogin(
                        item.subject.clone(),
                        item.revision.clone(),
                    ))),
                ),
                (_, "answer", Some(agent)) => (
                    format!("{words}…"),
                    context.message(Message::Inbox(InboxMsg::OpenAgent(agent.to_owned()))),
                ),
                (_, "abandon_reservation", _) => {
                    ("Confirm abandonment".to_owned(), decide(choice, &words))
                }
                _ if Some(choice) == recommended => (words.clone(), decide(RECOMMENDED, &words)),
                _ => (words.clone(), decide(choice, &words)),
            }
        };
        let others: Vec<&str> = item
            .choices
            .iter()
            .map(|choice| choice.choice.as_str())
            .filter(|choice| Some(*choice) != recommended)
            .collect();
        let primary = recommended.or_else(|| {
            item.instructions
                .is_none()
                .then(|| others.first().copied())
                .flatten()
        });
        if let Some(choice) = primary {
            let (label, click) = main_click(choice);
            let enabled = !busy && (choice != "abandon_reservation" || reason_ok);
            actions.push(KeyedView::new(
                format!("main:{choice}"),
                // The recommended action is tonal; without a recommendation
                // the first choice is only outlined, so it never reads as
                // Swarm's advice.
                m3::filled(palette)
                    .resource_overrides(if recommended == Some(choice) {
                        palette.segment(true)
                    } else {
                        palette.outlined()
                    })
                    .is_enabled(enabled)
                    .automation_name(format!(
                        "{label}{}: {subject}",
                        if recommended == Some(choice) {
                            " (recommended)"
                        } else {
                            ""
                        }
                    ))
                    .on_click(click)
                    .content(m3::label(&label)),
            ));
        }
        if let Some(commands) = &item.instructions {
            actions.push(KeyedView::new(
                "copy",
                m3::outlined(palette)
                    .automation_name(format!("Copy restore commands: {subject}"))
                    .on_click(
                        context.message(Message::Inbox(InboxMsg::CopyCommands(commands.clone()))),
                    )
                    .content(m3::icon_label(m3::glyph::COPY, "Copy restore commands")),
            ));
        }
        let menu_choices: Vec<&str> = others
            .iter()
            .copied()
            .filter(|choice| Some(*choice) != primary)
            .collect();
        if !menu_choices.is_empty() {
            let menu: Vec<MenuItem> = menu_choices
                .iter()
                .map(|choice| {
                    let (label, _) = main_click(choice);
                    let key = format!("{at}@{}\u{1f}{choice}", item.revision);
                    if busy || (*choice == "abandon_reservation" && !reason_ok) {
                        MenuItem::disabled(key, label)
                    } else {
                        MenuItem::item(key, label)
                    }
                })
                .collect();
            actions.push(KeyedView::new(
                format!("menu:{}:{busy}:{reason_ok}", menu_choices.join(",")),
                m3::icon_button(palette)
                    .automation_name(format!("Other options: {subject}"))
                    .content(m3::icon(m3::glyph::CHEVRON_DOWN))
                    .menu(Menu::new(
                        menu,
                        crate::views::menu_keys(
                            context
                                .callback(|key: String| Message::Inbox(InboxMsg::MenuPicked(key))),
                        ),
                    )),
            ));
        }
        actions.push(KeyedView::new(
            "snooze",
            m3::icon_button(palette)
                .is_enabled(!busy && !item.has_unknown_outcome())
                .automation_name(format!("Snooze: {subject}"))
                .on_click(decide(SNOOZE, "Snooze"))
                .content(m3::icon(m3::glyph::SNOOZE)),
        ));

        let mut columns = vec![GridLength::STAR, GridLength::Auto, GridLength::Auto];
        let mut cells: Vec<KeyedView> = Vec::new();
        let offset = if inbox.selecting {
            columns.insert(0, GridLength::Auto);
            let key = selection_key(item);
            let checked = inbox.selected.contains(&key);
            cells.push(KeyedView::new(
                format!("check:{checked}"),
                CheckBox::new()
                    .is_checked(checked)
                    .min_width(0.0)
                    .vertical_alignment(VerticalAlignment::Center)
                    .automation_name(format!("Select {subject} · {kind}"))
                    .on_is_checked_changed(crate::views::check_changes(context.callback(
                        move |on: bool| Message::Inbox(InboxMsg::Selected(key.clone(), on)),
                    ))),
            ));
            1
        } else {
            0
        };
        let title: View = match agent {
            Some(agent) => m3::text_button(palette)
                .horizontal_alignment(HorizontalAlignment::Left)
                .margin(Thickness::new(-tokens::spacing::ROW, 0.0, 0.0, 0.0))
                .min_height(0.0)
                .height(tokens::spacing::LG - tokens::spacing::XS)
                .automation_name(format!("Open {subject}"))
                .on_click(context.message(Message::Inbox(InboxMsg::OpenAgent(agent.to_owned()))))
                .content(
                    m3::text(format!("{subject} · {kind}"), tokens::page::HEADING)
                        .text_trimming(TextTrimming::CharacterEllipsis),
                )
                .into(),
            None => m3::text(format!("{subject} · {kind}"), tokens::page::HEADING)
                .text_trimming(TextTrimming::CharacterEllipsis)
                .into(),
        };
        let mut text: Vec<KeyedView> = vec![
            KeyedView::new("title", title),
            KeyedView::new(
                "reason",
                m3::secondary(reason.clone())
                    .text_trimming(TextTrimming::CharacterEllipsis)
                    .tooltip(match &item.detail {
                        Some(detail) => format!("{reason}\n{detail}"),
                        None => reason.clone(),
                    }),
            ),
        ];
        if let Some(commands) = &item.instructions {
            text.push(KeyedView::new(
                "commands",
                m3::secondary(commands.clone())
                    .text_wrapping(TextWrapping::Wrap)
                    .is_text_selection_enabled(true)
                    .automation_name(format!("Commands to run: {subject}")),
            ));
        }
        if item
            .choices
            .iter()
            .any(|choice| choice.choice == "abandon_reservation")
        {
            text.push(KeyedView::new(
                "abandon",
                m3::field()
                    .text(abandon_reason)
                    .is_enabled(!inbox.ids.contains_key(&abandon_key))
                    .placeholder_text("Reason for releasing the reservation")
                    .automation_name(format!("Reason to abandon reservation: {subject}"))
                    .on_text_changed(crate::views::text_edits(context.callback({
                        let reference = reference.clone();
                        move |text| Message::Inbox(InboxMsg::AbandonReason(reference.clone(), text))
                    }))),
            ));
        }
        let visible_outcome = if item.kind == OwnerInboxKind::HarnessAuth {
            inbox.auth_outcomes.get(&auth_task)
        } else {
            inbox.outcomes.get(&at)
        };
        match visible_outcome {
            Some(outcome) => {
                let line = m3::secondary(outcome.text.clone())
                    .text_wrapping(TextWrapping::Wrap)
                    .is_text_selection_enabled(true)
                    .automation_name(format!("Outcome: {}", outcome.text));
                text.push(KeyedView::new(
                    "outcome",
                    if outcome.error {
                        line.foreground(palette.attention())
                    } else {
                        line
                    },
                ));
            }
            None if busy && item.kind == OwnerInboxKind::HarnessAuth => {
                text.push(KeyedView::new(
                    "outcome",
                    m3::secondary("Requesting sign-in…"),
                ));
            }
            None if busy => text.push(KeyedView::new("outcome", m3::secondary("Deciding…"))),
            None => {}
        }
        cells.push(KeyedView::new(
            "text",
            StackPanel::new()
                .grid_column(offset)
                .vertical_alignment(VerticalAlignment::Center)
                .keyed_children(text),
        ));
        cells.push(KeyedView::new(
            "age",
            page_ui::age(age.clone()).grid_column(offset + 1),
        ));
        cells.push(KeyedView::new(
            "actions",
            StackPanel::new()
                .grid_column(offset + 2)
                .orientation(Orientation::Horizontal)
                .spacing(tokens::spacing::XS)
                .vertical_alignment(VerticalAlignment::Center)
                .keyed_children(actions),
        ));
        let selected = inbox.selecting && inbox.selected.contains(&selection_key(item));
        Border::new()
            .background(if selected {
                palette.steer()
            } else {
                palette.tonal()
            })
            .corner_radius(tokens::radius::CONTAINER)
            .min_height(tokens::height::CONTROL + tokens::spacing::MD)
            .padding(Thickness::new(
                tokens::spacing::INSET,
                tokens::spacing::CHIP_GAP,
                tokens::spacing::SM,
                tokens::spacing::CHIP_GAP,
            ))
            .automation_name(format!("{subject} · {kind}: {reason}, {age}"))
            .content(
                Grid::new()
                    .columns(columns)
                    .column_spacing(tokens::spacing::INSET)
                    .keyed_children(cells),
            )
            .into()
    }
}

/// An item's selection key: its cursor and the revision shown.
fn selection_key(item: &OwnerInboxItem) -> String {
    format!("{}@{}", item.cursor(), item.revision)
}
