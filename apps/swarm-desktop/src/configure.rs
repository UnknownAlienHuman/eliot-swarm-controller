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
use windows_reactor::MenuItem;

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
    /// The frozen profile's permissions (absent: the native default).
    pub permissions: Option<String>,
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
            permissions: text(snapshot, "permissions"),
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

    /// The effort the next turn carries, like [`Self::next_model`].
    pub fn next_effort(&self) -> Option<&str> {
        self.config
            .as_ref()
            .and_then(|config| config.effort.as_deref())
            .or(self.effective_effort.as_deref())
            .or(self.profile_effort.as_deref())
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
}

/// The option at `pick`, unless it is the first (default) entry or out of range.
pub fn picked(options: &[String], pick: Option<usize>) -> Option<String> {
    pick.filter(|index| *index > 0)
        .and_then(|index| options.get(index))
        .cloned()
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

/// The "Default model" entry of an ad-hoc launch's model menu.
pub const DEFAULT_MODEL: &str = "Default model";
/// The "Default effort" entry of an ad-hoc launch's effort menu.
pub const DEFAULT_EFFORT: &str = "Default effort";

/// What one entry of the model-and-effort menu picks.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Pick {
    /// A model; `None` is the harness default (ad-hoc launch only).
    Model(Option<String>),
    /// An effort; `None` is the harness default (ad-hoc launch only).
    Effort(Option<String>),
}

/// The one model-and-effort menu: a "Model" and an "Effort" submenu. With
/// `defaults` each starts with its "Default …" entry (an ad-hoc launch may
/// leave either to the harness). A refusal replaces that submenu's entries
/// with the daemon's reason, disabled.
pub fn menu_items(
    models: &[String],
    efforts: &[String],
    defaults: bool,
    model_refusal: Option<&str>,
    effort_refusal: Option<&str>,
) -> Vec<MenuItem> {
    let entries = |prefix: &str, default: &str, values: &[String], refusal: Option<&str>| {
        if let Some(reason) = refusal {
            return vec![MenuItem::disabled(format!("{prefix}:refused"), reason)];
        }
        let mut items = Vec::new();
        if defaults {
            items.push(MenuItem::item(format!("{prefix}:default"), default));
        }
        items.extend(
            values
                .iter()
                .map(|value| MenuItem::item(format!("{prefix}:{value}"), value.clone())),
        );
        if items.is_empty() {
            items.push(MenuItem::disabled(
                format!("{prefix}:none"),
                "None listed by the harness",
            ));
        }
        items
    };
    vec![
        MenuItem::submenu(
            "model",
            "Model",
            entries("m", DEFAULT_MODEL, models, model_refusal),
        ),
        MenuItem::submenu(
            "effort",
            "Effort",
            entries("e", DEFAULT_EFFORT, efforts, effort_refusal),
        ),
    ]
}

/// The pick a clicked menu label names. Reactor reports only the label: a
/// label that is both a model id and an effort name is taken as the model.
pub fn resolve(label: &str, models: &[String], efforts: &[String]) -> Option<Pick> {
    if label == DEFAULT_MODEL {
        Some(Pick::Model(None))
    } else if label == DEFAULT_EFFORT {
        Some(Pick::Effort(None))
    } else if models.iter().any(|model| model == label) {
        Some(Pick::Model(Some(label.to_owned())))
    } else if efforts.iter().any(|effort| effort == label) {
        Some(Pick::Effort(Some(label.to_owned())))
    } else {
        None
    }
}

/// Models and efforts of a catalogue reply, the efforts for `model` (else
/// the catalogue's default model).
pub fn options(reply: &HarnessModelsResponse, model: Option<&str>) -> (Vec<String>, Vec<String>) {
    let models = reply.models.iter().map(|entry| entry.id.clone()).collect();
    let model = model.or(reply.default.as_deref()).or_else(|| {
        reply
            .models
            .iter()
            .find(|entry| entry.default)
            .map(|entry| entry.id.as_str())
    });
    let efforts = model
        .and_then(|model| reply.models.iter().find(|entry| entry.id == model))
        .map(|entry| entry.efforts.iter().flatten().cloned().collect())
        .unwrap_or_default();
    (models, efforts)
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

    #[test]
    fn picks_skip_the_harness_default_entry_and_out_of_range() {
        let options = vec!["Harness default".to_owned(), "m1".to_owned()];
        assert_eq!(picked(&options, Some(0)), None);
        assert_eq!(picked(&options, Some(1)), Some("m1".to_owned()));
        assert_eq!(picked(&options, Some(9)), None);
    }

    #[test]
    fn menu_labels_resolve_to_their_pick() {
        let models = vec!["m1".to_owned(), "m2".to_owned()];
        let efforts = vec!["low".to_owned(), "high".to_owned()];
        assert_eq!(
            resolve("m2", &models, &efforts),
            Some(Pick::Model(Some("m2".to_owned())))
        );
        assert_eq!(
            resolve("high", &models, &efforts),
            Some(Pick::Effort(Some("high".to_owned())))
        );
        assert_eq!(
            resolve(DEFAULT_MODEL, &models, &efforts),
            Some(Pick::Model(None))
        );
        assert_eq!(
            resolve(DEFAULT_EFFORT, &models, &efforts),
            Some(Pick::Effort(None))
        );
        assert_eq!(resolve("other", &models, &efforts), None);
    }

    #[test]
    fn a_refusal_replaces_the_submenu_with_the_reason() {
        let items = menu_items(
            &["m1".to_owned()],
            &[],
            false,
            None,
            Some("fixed by the harness"),
        );
        let MenuItem::Submenu { items: model, .. } = &items[0] else {
            panic!("model submenu");
        };
        assert_eq!(model.len(), 1);
        let MenuItem::Submenu { items: effort, .. } = &items[1] else {
            panic!("effort submenu");
        };
        assert!(matches!(
            &effort[0],
            MenuItem::Item { label, enabled: false, .. } if label == "fixed by the harness"
        ));
    }

    #[test]
    fn options_follow_the_chosen_model_or_the_default() {
        let Ok(reply) = catalog(ConfigureSupport::default()).reply else {
            panic!("catalogue");
        };
        assert_eq!(options(&reply, None).1, ["low", "high"]);
        assert!(options(&reply, Some("m2")).1.is_empty());
        assert_eq!(options(&reply, None).0, ["m1", "m2"]);
    }
}
