//! Model and effort of a live Agent: pickers fed by `harness.models` for the
//! Agent's own frozen route, applied through `configure`, and the requested
//! versus effective readback with the native verification marks.
//!
//! The catalogue and the per-field refusals come from the daemon
//! (`harness.models` with `agent`, whose `configure` member is the same table
//! `configure` admission uses), so a disabled picker shows the daemon's own
//! reason. `effective` values are the last natively admitted ones; a value
//! is marked verified only when a turn proved the harness carried it
//! (`model_verified`/`effort_verified`), never from the request.

use eliot_core::Command;
use eliot_core::api::{AgentConfigView, AgentGetResponse, HarnessModelsResponse};
use serde_json::Value;
use windows_reactor::*;

use crate::theme;

/// First entry of each picker: submit nothing for that field.
pub const KEEP: &str = "Keep current";

/// Settings facts of one Agent generation, read from `agent.get`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Settings {
    pub profile: String,
    /// Frozen profile-snapshot values (absent: the harness native default).
    pub profile_model: Option<String>,
    pub profile_effort: Option<String>,
    /// Latest retained `configure` revision of this generation.
    pub config: Option<AgentConfigView>,
    /// Last natively admitted values.
    pub effective_model: Option<String>,
    pub effective_effort: Option<String>,
    pub model_verified: bool,
    pub effort_verified: bool,
}

impl Settings {
    pub fn from_reply(reply: &AgentGetResponse) -> Self {
        let text = |value: Option<&Value>, key: &str| {
            value
                .and_then(|value| value.get(key))
                .and_then(Value::as_str)
                .map(str::to_owned)
        };
        let flag = |key: &str| {
            reply
                .agent
                .effective
                .as_ref()
                .and_then(|value| value.get(key))
                .and_then(Value::as_bool)
                .unwrap_or(false)
        };
        let snapshot = Some(&reply.agent.profile_snapshot);
        let effective = reply.agent.effective.as_ref();
        Self {
            profile: reply.agent.profile.clone(),
            profile_model: text(snapshot, "model"),
            profile_effort: text(snapshot, "effort"),
            config: reply.config.clone(),
            effective_model: text(effective, "model"),
            effective_effort: text(effective, "effort"),
            model_verified: flag("model_verified"),
            effort_verified: flag("effort_verified"),
        }
    }

    /// The model the next turn carries: the overlay's, else the admitted
    /// readback, else the frozen profile value.
    pub fn next_model(&self) -> Option<&str> {
        self.config
            .as_ref()
            .and_then(|config| config.model.as_deref())
            .or(self.effective_model.as_deref())
            .or(self.profile_model.as_deref())
    }

    /// One-line summary for the detail header.
    pub fn summary(&self) -> String {
        format!(
            "{} · {}",
            self.effective_model
                .as_deref()
                .unwrap_or("model not read back"),
            self.effective_effort
                .as_deref()
                .unwrap_or("effort not read back"),
        )
    }

    /// Requested, next-turn and effective values, kept apart: a request is
    /// not proof of what the harness runs.
    pub fn lines(&self) -> Vec<String> {
        let or_default = |value: Option<&str>| value.unwrap_or("native default").to_owned();
        let mut lines = Vec::new();
        match &self.config {
            Some(config) => {
                lines.push(format!(
                    "Requested (configure rev {}): model {} · effort {}",
                    config.revision,
                    config.requested_model.as_deref().unwrap_or("unchanged"),
                    config.requested_effort.as_deref().unwrap_or("unchanged"),
                ));
                lines.push(format!(
                    "Next turn carries: model {} · effort {} · applies at {} · revision {} ({}); profile {} had {} / {}",
                    or_default(config.model.as_deref()),
                    or_default(config.effort.as_deref()),
                    config.application.replace('_', " "),
                    config.revision,
                    config.provenance.replace('_', " "),
                    config.profile,
                    or_default(config.original_model.as_deref()),
                    or_default(config.original_effort.as_deref()),
                ));
            }
            None => lines.push(format!(
                "Requested (profile {}): model {} · effort {} · no configure revision",
                self.profile,
                or_default(self.profile_model.as_deref()),
                or_default(self.profile_effort.as_deref()),
            )),
        }
        let mark = |verified: bool| {
            if verified {
                "✓ verified by a turn"
            } else {
                "admission readback, not turn-verified"
            }
        };
        lines.push(format!(
            "Effective: model {} ({}) · effort {} ({})",
            self.effective_model.as_deref().unwrap_or("not read back"),
            mark(self.model_verified),
            self.effective_effort.as_deref().unwrap_or("not read back"),
            mark(self.effort_verified),
        ));
        lines
    }
}

/// The catalogue read for one exact Agent generation.
#[derive(Clone, Debug)]
pub struct Catalog {
    pub agent: String,
    pub generation: i64,
    pub reply: Result<HarnessModelsResponse, String>,
}

impl Catalog {
    pub fn is_for(&self, agent: &str, generation: i64) -> bool {
        self.agent == agent && self.generation == generation
    }

    pub fn models(&self) -> Vec<String> {
        let mut options = vec![KEEP.to_owned()];
        if let Ok(reply) = &self.reply {
            options.extend(reply.models.iter().map(|model| model.id.clone()));
        }
        options
    }

    /// Efforts the catalogue lists for `model` (the picked one, or the model
    /// the next turn carries). No listed efforts leaves only [`KEEP`].
    pub fn efforts(&self, model: Option<&str>) -> Vec<String> {
        let mut options = vec![KEEP.to_owned()];
        if let (Ok(reply), Some(model)) = (&self.reply, model)
            && let Some(entry) = reply.models.iter().find(|entry| entry.id == model)
        {
            options.extend(entry.efforts.iter().flatten().cloned());
        }
        options
    }

    pub fn model_refusal(&self) -> Option<&str> {
        self.reply
            .as_ref()
            .ok()
            .and_then(|reply| reply.configure.model_refusal.as_deref())
    }

    pub fn effort_refusal(&self) -> Option<&str> {
        self.reply
            .as_ref()
            .ok()
            .and_then(|reply| reply.configure.effort_refusal.as_deref())
    }

    pub fn line(&self) -> String {
        match &self.reply {
            Ok(reply) => {
                let mut line = format!(
                    "Catalogue ({}): {} model(s) under {} · credential {}",
                    reply.catalogue,
                    reply.models.len(),
                    reply.scope,
                    reply.credential,
                );
                if let Some(note) = &reply.note {
                    line.push_str(&format!(" · {note}"));
                }
                line
            }
            Err(error) => format!("Catalogue unavailable: {error}"),
        }
    }
}

/// The option at `pick`, unless it is [`KEEP`] or out of range.
pub fn picked(options: &[String], pick: Option<usize>) -> Option<String> {
    pick.filter(|index| *index > 0)
        .and_then(|index| options.get(index))
        .cloned()
}

/// The Apply button text for the values that would be submitted.
pub fn apply_label(model: Option<&str>, effort: Option<&str>) -> String {
    match (model, effort) {
        (Some(model), Some(effort)) => {
            format!("Apply model {model} and effort {effort} for the next turn")
        }
        (Some(model), None) => format!("Apply model {model} for the next turn"),
        (None, Some(effort)) => format!("Apply effort {effort} for the next turn"),
        (None, None) => "Pick a model or an effort to apply".to_owned(),
    }
}

/// Settled `configure` command in one line: state, the daemon's code and
/// message, and the applied revision when the outcome names one.
pub fn outcome_line(command: &Command) -> String {
    let state = format!("{:?}", command.state).to_lowercase();
    let outcome = command.outcome.as_ref();
    let text = |key: &str| {
        outcome
            .and_then(|value| value.get(key))
            .and_then(Value::as_str)
    };
    let mut line = format!("configure {} {state}", command.id);
    if let Some(code) = text("code") {
        line.push_str(&format!(" · {code}"));
    }
    if let Some(message) = text("message") {
        line.push_str(&format!(": {message}"));
    }
    if let Some(revision) = outcome
        .and_then(|value| value.get("config_revision"))
        .and_then(Value::as_i64)
    {
        line.push_str(&format!(" · revision {revision}"));
    }
    line
}

pub struct ConfigureData<'a> {
    pub settings: &'a Settings,
    pub catalog_line: String,
    pub models: Vec<String>,
    pub model_pick: Option<usize>,
    pub model_refusal: Option<&'a str>,
    pub efforts: Vec<String>,
    pub effort_pick: Option<usize>,
    pub effort_refusal: Option<&'a str>,
    pub can_apply: bool,
    /// What Apply submits, e.g. "Apply effort high for the next turn".
    pub apply_label: String,
    pub status: &'a str,
    pub expanded: bool,
}

pub struct ConfigureActions {
    pub model: Callback<Option<usize>>,
    pub effort: Callback<Option<usize>>,
    pub apply: Callback<()>,
    pub refresh: Callback<()>,
    pub expanded: Callback<bool>,
}

fn small(text: impl Into<String>) -> TextBlock {
    TextBlock::new()
        .text(text.into())
        .font_size(theme::type_scale::LABEL)
        .text_wrapping(TextWrapping::Wrap)
        .is_text_selection_enabled(true)
}

/// Collapsible "Model and effort" section of the Agent detail.
pub fn panel(data: ConfigureData<'_>, actions: ConfigureActions) -> View {
    let refusal = |reason: Option<&str>, field: &str| -> View {
        match reason {
            Some(reason) => small(format!("{field} cannot change: {reason}"))
                .opacity(theme::MUTED_OPACITY)
                .into(),
            None => View::empty(),
        }
    };
    let model_picker = ComboBox::new()
        .items_source(data.models)
        .selected_index(data.model_pick)
        .is_enabled(data.model_refusal.is_none())
        .automation_name("Configure model")
        .on_selection_changed(actions.model);
    // The effort options follow the model. A ComboBox whose items change in
    // place can report a selection index that differs from the item it
    // shows, so a new option list gets a new control.
    let effort_key = format!("effort:{}", data.efforts.join("\u{1f}"));
    let effort_picker = ComboBox::new()
        .items_source(data.efforts)
        .selected_index(data.effort_pick)
        .is_enabled(data.effort_refusal.is_none())
        .automation_name("Configure effort")
        .on_selection_changed(actions.effort);
    let lines = data.settings.lines().join("\n");
    // The button says exactly what it submits.
    let apply = Button::new()
        .is_enabled(data.can_apply)
        .automation_name(data.apply_label.clone())
        .on_click(actions.apply)
        .content(TextBlock::new().text(data.apply_label));
    let content = StackPanel::new().spacing(theme::spacing::XS).children((
        small(lines),
        StackPanel::new()
            .orientation(Orientation::Horizontal)
            .spacing(theme::spacing::XS)
            .keyed_children(vec![
                KeyedView::new("model", model_picker),
                KeyedView::new(effort_key, effort_picker),
                KeyedView::new("apply", apply),
                KeyedView::new(
                    "reread",
                    Button::new()
                        .automation_name("Reread catalogue")
                        .on_click(actions.refresh)
                        .content(TextBlock::new().text("Reread catalogue")),
                ),
            ]),
        refusal(data.model_refusal, "Model"),
        refusal(data.effort_refusal, "Effort"),
        small(data.catalog_line).opacity(theme::MUTED_OPACITY),
        small(data.status.to_owned()),
    ));
    Expander::new()
        .is_expanded(data.expanded)
        .on_is_expanded_changed(actions.expanded)
        .automation_name("Model and effort")
        .slots([
            SlotView::new(
                ExpanderSlot::Header,
                TextBlock::new().text(format!("Model and effort · {}", data.settings.summary())),
            ),
            SlotView::new(ExpanderSlot::Content, content),
        ])
}

#[cfg(test)]
mod tests {
    use super::*;
    use eliot_core::api::{ConfigureSupport, HarnessModelEntry};

    fn catalog(configure: ConfigureSupport) -> Catalog {
        Catalog {
            agent: "a1".to_owned(),
            generation: 1,
            reply: Ok(HarnessModelsResponse {
                harness: "codex".to_owned(),
                catalogue: "native".to_owned(),
                models: vec![
                    HarnessModelEntry {
                        id: "m1".to_owned(),
                        default: true,
                        efforts: Some(vec!["low".to_owned(), "high".to_owned()]),
                        default_effort: Some("low".to_owned()),
                    },
                    HarnessModelEntry {
                        id: "m2".to_owned(),
                        default: false,
                        efforts: None,
                        default_effort: None,
                    },
                ],
                default: Some("m1".to_owned()),
                note: None,
                scope: "profile:p@a1g1 harness:codex".to_owned(),
                fetched_at_ms: 1,
                ttl_ms: 1,
                credential: "ambient".to_owned(),
                configure,
            }),
        }
    }

    #[test]
    fn apply_names_exactly_what_it_submits() {
        assert_eq!(
            apply_label(None, Some("high")),
            "Apply effort high for the next turn"
        );
        assert_eq!(
            apply_label(Some("m2"), Some("low")),
            "Apply model m2 and effort low for the next turn"
        );
        assert_eq!(
            apply_label(None, None),
            "Pick a model or an effort to apply"
        );
    }

    #[test]
    fn efforts_follow_the_model_and_keep_comes_first() {
        let catalog = catalog(ConfigureSupport::default());
        assert_eq!(catalog.models(), vec![KEEP, "m1", "m2"]);
        assert_eq!(catalog.efforts(Some("m1")), vec![KEEP, "low", "high"]);
        assert_eq!(catalog.efforts(Some("m2")), vec![KEEP]);
        assert_eq!(catalog.efforts(None), vec![KEEP]);
        assert_eq!(picked(&catalog.models(), Some(0)), None);
        assert_eq!(picked(&catalog.models(), Some(2)), Some("m2".to_owned()));
        assert_eq!(picked(&catalog.models(), Some(9)), None);
    }

    #[test]
    fn refusals_are_the_daemon_reasons() {
        let catalog = catalog(ConfigureSupport::for_harness("opencode"));
        assert!(catalog.model_refusal().is_none());
        assert!(
            catalog
                .effort_refusal()
                .is_some_and(|reason| reason.contains("no native effort knob"))
        );
    }

    #[test]
    fn verified_marks_come_only_from_the_turn_flags() {
        let mut settings = Settings {
            profile: "p".to_owned(),
            effective_model: Some("m1".to_owned()),
            effective_effort: Some("high".to_owned()),
            ..Settings::default()
        };
        let effective = settings.lines().pop().unwrap();
        assert!(effective.contains("high (admission readback, not turn-verified)"));
        settings.effort_verified = true;
        let effective = settings.lines().pop().unwrap();
        assert!(effective.contains("high (✓ verified by a turn)"));
        assert!(effective.contains("m1 (admission readback"));
    }

    #[test]
    fn overlay_lines_show_revision_and_application_point() {
        let settings = Settings {
            profile: "p".to_owned(),
            config: Some(AgentConfigView {
                revision: 2,
                application: "next_turn".to_owned(),
                provenance: "configure".to_owned(),
                profile: "p".to_owned(),
                profile_inherited: false,
                requested_model: None,
                requested_effort: Some("high".to_owned()),
                model: Some("m1".to_owned()),
                effort: Some("high".to_owned()),
                original_model: Some("m1".to_owned()),
                original_effort: Some("low".to_owned()),
            }),
            ..Settings::default()
        };
        let lines = settings.lines();
        assert!(lines[0].contains("configure rev 2"));
        assert!(lines[0].contains("model unchanged · effort high"));
        assert!(lines[1].contains("applies at next turn · revision 2"));
        assert!(lines[1].contains("profile p had m1 / low"));
        assert_eq!(settings.next_model(), Some("m1"));
    }
}
