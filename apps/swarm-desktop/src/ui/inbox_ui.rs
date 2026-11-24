//! The Owner inbox (#870): Swarm runs Agents by itself and the owner takes
//! part through this one decision queue. `owner.inbox` items grouped by
//! kind, each with its subject, age and reason, the recommended action as
//! the filled button, the other offered choices and Snooze; every choice
//! goes to `owner.decide` bound to the revision shown, so a stale choice
//! comes back refused (`stale_item`), says why and reads the inbox again.
//! The per-kind counts in the navigation are `daemon.status` `owner_inbox`,
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
}

/// The last outcome of a decision on one item, in words.
#[derive(Clone, Debug)]
struct Outcome {
    text: String,
    error: bool,
}

#[derive(Default)]
pub(super) struct InboxState {
    /// Visible items by kind (`daemon.status`).
    pub(super) counts: BTreeMap<OwnerInboxKind, usize>,
    pub(super) profile_command_issues: Vec<ProfileCommandIssue>,
    items: Vec<OwnerInboxItem>,
    /// More items than the first page.
    more: bool,
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
    let first = subject.split(':').next()?;
    let digits = first.strip_prefix('a')?;
    (!digits.is_empty() && digits.chars().all(|ch| ch.is_ascii_digit())).then_some(first)
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
                        inbox.more = read.next.is_some();
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
        }
    }

    /// Read the first inbox page (in the sidebar's batch when one is being
    /// collected).
    pub(super) fn read_inbox(&mut self, context: &ComponentContext<Self>) {
        let Ok(data_dir) = self.data_dir.clone() else {
            return;
        };
        self.inbox.seq += 1;
        self.inbox.reading = true;
        let seq = self.inbox.seq;
        self.spawn_read(context, move || {
            let read = client::block_on(client::owner_inbox(&data_dir, None))
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

    /// The Inbox pane: the pane's tabs, then the items by kind.
    pub(super) fn inbox_view(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        let inbox = &self.inbox;
        let mut items: Vec<KeyedView> = Vec::new();
        if let Some(error) = &inbox.error {
            items.push(KeyedView::new(
                "error",
                StackPanel::new().spacing(tokens::spacing::XS).children((
                    m3::secondary(format!("The inbox could not be read: {error}"))
                        .foreground(palette.attention())
                        .text_wrapping(TextWrapping::Wrap)
                        .is_text_selection_enabled(true),
                    m3::text_button(palette)
                        .horizontal_alignment(HorizontalAlignment::Left)
                        .is_enabled(!inbox.reading)
                        .automation_name("Read the inbox again")
                        .on_click(context.message(Message::Inbox(InboxMsg::Refresh)))
                        .content(m3::label("Read again")),
                )),
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
                m3::secondary(format!("Not done. {text}"))
                    .foreground(palette.attention())
                    .text_wrapping(TextWrapping::Wrap)
                    .is_text_selection_enabled(true)
                    .automation_name(format!("Refused: {text}")),
            ));
        }
        if !inbox.loaded && inbox.error.is_none() {
            items.push(KeyedView::new(
                "reading",
                m3::secondary("Reading the inbox…"),
            ));
        } else if inbox.loaded && inbox.items.is_empty() {
            items.push(KeyedView::new(
                "empty",
                m3::secondary("Nothing waits for you.").text_wrapping(TextWrapping::Wrap),
            ));
        }
        for kind in OwnerInboxKind::ALL {
            let group: Vec<&OwnerInboxItem> = inbox
                .items
                .iter()
                .filter(|item| item.kind == kind)
                .collect();
            if group.is_empty() {
                continue;
            }
            items.push(KeyedView::new(
                format!("kind:{}", kind.as_str()),
                m3::muted(
                    format!("{} · {}", kind_title(kind), group.len()),
                    tokens::text::LABEL,
                )
                .automation_heading_level(AutomationHeadingLevel::Level3)
                .margin(Thickness::new(tokens::spacing::ROW, 6.0, 0.0, 0.0)),
            ));
            for item in group {
                items.push(KeyedView::new(
                    format!("item:{}@{}", item.cursor(), item.revision),
                    self.inbox_item(item, context, palette),
                ));
            }
        }
        if inbox.more {
            items.push(KeyedView::new(
                "more",
                m3::secondary("More items wait beyond these; they show as these are decided.")
                    .text_wrapping(TextWrapping::Wrap),
            ));
        }
        Grid::new()
            .rows([GridLength::Auto, GridLength::STAR])
            .children((
                self.panel_header("Inbox", context, palette),
                ScrollViewer::new()
                    .grid_row(1)
                    .vertical_scroll_bar_visibility(ScrollBarVisibility::Auto)
                    .automation_name("Owner inbox")
                    .content(
                        StackPanel::new()
                            .spacing(tokens::spacing::ROW)
                            .margin(Thickness::new(
                                tokens::spacing::ROW,
                                tokens::spacing::ROW,
                                tokens::spacing::ROW,
                                tokens::spacing::INSET,
                            ))
                            .keyed_children(items),
                    ),
            ))
    }

    /// One item: its subject and age, its reason, the recommended action
    /// (filled), the other choices (outlined), Snooze, and the last outcome.
    fn inbox_item(
        &self,
        item: &OwnerInboxItem,
        context: &mut ViewContext<Self>,
        palette: Palette,
    ) -> View {
        let at = item.cursor();
        let auth_task = format!("{}@{}", item.subject, item.revision);
        let busy = self.inbox.deciding.contains(&at)
            || (item.kind == OwnerInboxKind::HarnessAuth
                && self.inbox.auth_signing.contains(&auth_task));
        let agent = subject_agent(&item.subject);
        let subject = match agent {
            Some(agent) => self.agent_title(agent),
            None => item.subject.clone(),
        };
        let age = crate::nav::short_age(item.age_ms, 0);
        let reason = choice_words(&eliot_core::reason_words(&item.reason));
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
        let abandon_reason = self
            .inbox
            .abandon_reasons
            .get(&abandon_key)
            .cloned()
            .unwrap_or_default();
        let mut buttons: Vec<KeyedView> = Vec::new();
        if let Some(recommended) = &item.recommended {
            let words = choice_words(&recommended.choice);
            let click =
                if item.kind == OwnerInboxKind::HarnessAuth && recommended.choice == "sign_in" {
                    context.message(Message::Inbox(InboxMsg::AuthLogin(
                        item.subject.clone(),
                        item.revision.clone(),
                    )))
                } else {
                    decide(RECOMMENDED, &words)
                };
            buttons.push(KeyedView::new(
                "recommended",
                m3::filled(palette)
                    .is_enabled(!busy)
                    .automation_name(format!("{words} (recommended): {subject}"))
                    .on_click(click)
                    .content(m3::label(&words)),
            ));
        }
        for choice in &item.choices {
            if item
                .recommended
                .as_ref()
                .is_some_and(|recommended| recommended.choice == choice.choice)
            {
                continue;
            }
            let words = choice_words(&choice.choice);
            let enabled = !busy
                && (choice.choice != "abandon_reservation"
                    || (!abandon_reason.trim().is_empty() && abandon_reason.len() <= 1024));
            // Sign-in is a direct auth.login operation; an answer needs the
            // question's own form, so open its Agent.
            let (label, click) = match (item.kind, choice.choice.as_str(), agent) {
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
                (_, "abandon_reservation", _) => (
                    "Confirm abandonment".to_owned(),
                    decide(&choice.choice, &words),
                ),
                _ => (words.clone(), decide(&choice.choice, &words)),
            };
            buttons.push(KeyedView::new(
                format!("choice:{}", choice.choice),
                m3::outlined(palette)
                    .is_enabled(enabled)
                    .automation_name(format!("{label}: {subject}"))
                    .on_click(click)
                    .content(m3::label(&label)),
            ));
        }
        let abandon_input: View = if item
            .choices
            .iter()
            .any(|choice| choice.choice == "abandon_reservation")
        {
            m3::field()
                .text(abandon_reason)
                .is_enabled(!self.inbox.ids.contains_key(&abandon_key))
                .placeholder_text("Reason for releasing the reservation")
                .automation_name(format!("Reason to abandon reservation: {subject}"))
                .on_text_changed(context.callback({
                    let reference = reference.clone();
                    move |text| Message::Inbox(InboxMsg::AbandonReason(reference.clone(), text))
                }))
                .into()
        } else {
            View::empty()
        };
        buttons.push(KeyedView::new(
            "snooze",
            m3::text_button(palette)
                .is_enabled(!busy)
                .automation_name(format!("Snooze: {subject}"))
                .on_click(decide(SNOOZE, "Snooze"))
                .content(m3::label("Snooze")),
        ));
        let visible_outcome = if item.kind == OwnerInboxKind::HarnessAuth {
            self.inbox.auth_outcomes.get(&auth_task)
        } else {
            self.inbox.outcomes.get(&at)
        };
        let outcome: View = match visible_outcome {
            Some(outcome) => {
                let text = m3::secondary(outcome.text.clone())
                    .text_wrapping(TextWrapping::Wrap)
                    .is_text_selection_enabled(true)
                    .automation_name(format!("Outcome: {}", outcome.text));
                if outcome.error {
                    text.foreground(palette.attention()).into()
                } else {
                    text.into()
                }
            }
            None if busy && item.kind == OwnerInboxKind::HarnessAuth => {
                m3::secondary("Requesting sign-in…").into()
            }
            None if busy => m3::secondary("Deciding…").into(),
            None => View::empty(),
        };
        let instructions: View = match &item.instructions {
            Some(text) => m3::secondary(text.clone())
                .text_wrapping(TextWrapping::Wrap)
                .is_text_selection_enabled(true)
                .automation_name(format!("Commands to run: {subject}"))
                .into(),
            None => View::empty(),
        };
        m3::card(palette)
            .automation_name(format!("{subject}: {reason}, {age}"))
            .content(
                StackPanel::new().spacing(tokens::spacing::SM).children((
                    Grid::new()
                        .columns([GridLength::STAR, GridLength::Auto])
                        .column_spacing(tokens::spacing::SM)
                        .children((
                            m3::body(subject.clone())
                                .vertical_alignment(VerticalAlignment::Center)
                                .text_trimming(TextTrimming::CharacterEllipsis),
                            m3::secondary(age)
                                .grid_column(1)
                                .vertical_alignment(VerticalAlignment::Center),
                        )),
                    m3::secondary(reason).text_wrapping(TextWrapping::Wrap),
                    instructions,
                    abandon_input,
                    StackPanel::new()
                        .orientation(Orientation::Horizontal)
                        .spacing(tokens::spacing::SM)
                        .keyed_children(buttons),
                    outcome,
                )),
            )
    }
}
