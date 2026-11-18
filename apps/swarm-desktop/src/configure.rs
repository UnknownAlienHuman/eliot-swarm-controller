//! Model and effort of a live Agent: the composer's effort popover and its
//! model picker, fed by `harness.models` for the Agent's own frozen route,
//! applied through `configure`, and the requested versus effective readback
//! with the native verification marks.
//!
//! The catalogue and the per-field refusals come from the daemon
//! (`harness.models` with `agent`, whose `configure` member is the same table
//! `configure` admission uses), so the popover hides what the harness keeps
//! and says so in plain words. `effective` values are the last natively admitted ones; a value
//! is marked verified only when a turn proved the harness carried it
//! (`model_verified`/`effort_verified`), never from the request.

use eliot_core::Command;
use eliot_core::api::{
    AgentConfigView, AgentGetResponse, HarnessEntry, HarnessModelsResponse, ModelsCatalogEntry,
    ModelsCatalogResponse,
};
use serde_json::Value;

use crate::picker::{effort_label, effort_name};

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
    /// The frozen ad-hoc access level (`ask`/`auto`/`full`), when the
    /// launch named one: it decides what the Agent may do, whatever the
    /// harness's own permission fields say.
    pub access: Option<String>,
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
            access: text(snapshot, "access"),
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

    /// The model the next turn carries, in words: plain once the harness
    /// confirmed carrying it (a turn, or the native switch, proved it) and
    /// "Requested model X" until then, as in [`Self::lines`]. A model asked
    /// for the next turn stays requested until a turn reports it.
    pub fn next_model_words(&self) -> String {
        match self.next_model() {
            None => "Default model".to_owned(),
            Some(model)
                if self.model_verified && self.effective_model.as_deref() == Some(model) =>
            {
                model.to_owned()
            }
            Some(model) => format!("Requested model {model}"),
        }
    }

    /// The effort the next turn carries, like [`Self::next_model`].
    pub fn next_effort(&self) -> Option<&str> {
        self.config
            .as_ref()
            .and_then(|config| config.effort.as_deref())
            .or(self.effective_effort.as_deref())
            .or(self.profile_effort.as_deref())
    }

    /// What the Agent runs with, in words. The model and effort the
    /// harness reports, plain once the harness admitted it (its launch
    /// readback or a turn that carried it) and "Requested" until then (a
    /// request is not proof); a change for the next turn when one was asked
    /// for. Rows with nothing to say are left out. Efforts read short
    /// (`med`), as everywhere in the desktop.
    pub fn lines(&self) -> Vec<String> {
        self.lines_with(|effort| effort_label(effort).to_owned())
    }

    /// [`Self::lines`] with efforts in full words ("medium"): what a screen
    /// reader says for them.
    pub fn spoken_lines(&self) -> Vec<String> {
        self.lines_with(effort_name)
    }

    fn lines_with(&self, words: impl Fn(&str) -> String) -> Vec<String> {
        let mut lines = Vec::new();
        let said = |what: &str, value: &str, verified: bool| {
            if verified {
                format!("{what} {value}")
            } else {
                format!("Requested {} {value}", what.to_lowercase())
            }
        };
        match (&self.effective_model, &self.effective_effort) {
            (Some(model), effort) => {
                lines.push(said("Model", model, self.model_verified));
                if let Some(effort) = effort {
                    lines.push(said("Effort", &words(effort), self.effort_verified));
                }
            }
            _ => match (&self.profile_model, &self.profile_effort) {
                (Some(model), Some(effort)) => lines.push(format!(
                    "Requested model {model} · {} effort",
                    words(effort)
                )),
                (Some(model), None) => lines.push(format!("Requested model {model}")),
                _ => lines.push("Default model".to_owned()),
            },
        }
        if let Some(config) = &self.config {
            let model = config.model.as_deref().unwrap_or("the same model");
            let effort = config
                .effort
                .as_deref()
                .map_or_else(|| "the same effort".to_owned(), &words);
            lines.push(format!("Next turn: {model} · {effort}"));
        }
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

/// A settled `configure` command in the owner's words: when it applies,
/// or why it did not (no command id or revision).
pub fn outcome_words(command: &Command) -> String {
    let outcome = command.outcome.as_ref();
    let text = |key: &str| {
        outcome
            .and_then(|value| value.get(key))
            .and_then(Value::as_str)
            .unwrap_or("")
    };
    match command.state {
        eliot_core::CommandState::Applied => "Applies from the next turn.".to_owned(),
        _ => {
            let code = text("code");
            let code = if code.is_empty() {
                format!("{:?}", command.state).to_lowercase()
            } else {
                code.to_owned()
            };
            let message = text("message");
            eliot_core::reason_message(
                &code,
                if message.is_empty() {
                    text("reason")
                } else {
                    message
                },
            )
        }
    }
}

pub fn failure_alert(command: &Command, detail: &str) -> String {
    let prefix = if command.state == eliot_core::CommandState::Unknown {
        "Model or effort change is uncertain"
    } else {
        "Model or effort not changed"
    };
    format!("{prefix}: {detail}")
}

/// What the harness keeps for the whole task, in one plain sentence, or
/// `None` when both model and effort can change.
pub fn locked_words(harness: &str, model_locked: bool, effort_locked: bool) -> Option<String> {
    match (model_locked, effort_locked) {
        (true, true) => Some(format!(
            "{harness} keeps the model and effort it started with."
        )),
        (true, false) => Some(format!(
            "{harness} can't switch models during a task; start a new task for another model."
        )),
        (false, true) => Some(format!("{harness} keeps the effort it started with.")),
        (false, false) => None,
    }
}

/// The `configure` a choice asks for, checked against the catalogue of the
/// exact Agent generation: only the fields that change, each one the
/// daemon accepts for this route (a refused field, an unknown model or an
/// effort the model does not list never goes out). `None`: nothing valid
/// to change.
pub fn change(
    reply: &HarnessModelsResponse,
    settings: &Settings,
    model: Option<&str>,
    effort: Option<&str>,
) -> Option<(Option<String>, Option<String>)> {
    let current_model = settings.next_model();
    let model = match model {
        Some(model) if Some(model) != current_model => {
            let known = reply.models.iter().any(|entry| entry.id == model);
            if reply.configure.model_refusal.is_some() || !known {
                return None;
            }
            Some(model.to_owned())
        }
        _ => None,
    };
    let target = model.as_deref().or(current_model);
    let effort = match effort {
        Some(effort) if model.is_some() || Some(effort) != settings.next_effort() => {
            let (_, efforts) = options(reply, target);
            if reply.configure.effort_refusal.is_some() || !efforts.iter().any(|e| e == effort) {
                return None;
            }
            Some(effort.to_owned())
        }
        _ => None,
    };
    (model.is_some() || effort.is_some()).then_some((model, effort))
}

/// The Agent's route catalogue in the model picker's shape (one installed
/// harness), so the picker lists exactly what `configure` admission checks
/// for this Agent, not the unified catalogue of new tasks.
pub fn as_catalog(reply: &HarnessModelsResponse) -> ModelsCatalogResponse {
    ModelsCatalogResponse {
        harnesses: vec![HarnessEntry {
            harness: reply.harness.clone(),
            installed: true,
            cmd: None,
            version: None,
            code: None,
            error: None,
            candidates: None,
        }],
        models: reply
            .models
            .iter()
            .map(|entry| ModelsCatalogEntry {
                harness: reply.harness.clone(),
                id: entry.id.clone(),
                display_name: entry.display_name().to_owned(),
                provider: eliot_core::api::catalog_provider(&reply.harness, &entry.id),
                efforts: entry
                    .efforts
                    .clone()
                    .filter(|_| reply.configure.effort_refusal.is_none()),
                default_effort: entry
                    .default_effort
                    .clone()
                    .filter(|_| reply.configure.effort_refusal.is_none()),
                default: entry.default || reply.default.as_deref() == Some(entry.id.as_str()),
                availability: eliot_core::api::harness::CATALOG_AVAILABLE.to_owned(),
                source: entry.source.clone(),
            })
            .collect(),
        next: None,
        fetched_at_ms: reply.fetched_at_ms,
        ttl_ms: reply.ttl_ms,
        failures: Vec::new(),
    }
}

/// The launch catalogue can preview one harness's efforts while the
/// Agent's route is checked. An unknown explicit model has no preview.
pub fn launch_model<'a>(
    catalog: &'a ModelsCatalogResponse,
    harness: &str,
    model: Option<&str>,
) -> Option<&'a ModelsCatalogEntry> {
    catalog.models.iter().find(|entry| {
        entry.harness == harness && model.map_or(entry.default, |model| entry.id == model)
    })
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

    fn reply(configure: ConfigureSupport) -> HarnessModelsResponse {
        HarnessModelsResponse {
            harness: "codex".to_owned(),
            catalogue: "native".to_owned(),
            models: vec![
                HarnessModelEntry {
                    id: "m1".to_owned(),
                    display_name: Some("Model One".to_owned()),
                    default: true,
                    efforts: Some(vec!["low".to_owned(), "high".to_owned()]),
                    default_effort: Some("low".to_owned()),
                    source: None,
                },
                HarnessModelEntry {
                    id: "m2".to_owned(),
                    display_name: None,
                    default: false,
                    efforts: Some(vec!["medium".to_owned(), "xhigh".to_owned()]),
                    default_effort: None,
                    source: None,
                },
            ],
            default: Some("m1".to_owned()),
            note: None,
            scope: "profile:p@a1g1 harness:codex".to_owned(),
            fetched_at_ms: 1,
            ttl_ms: 1,
            credential: "ambient".to_owned(),
            configure,
        }
    }

    fn running(model: &str, effort: &str) -> Settings {
        Settings {
            profile: "p".to_owned(),
            effective_model: Some(model.to_owned()),
            effective_effort: Some(effort.to_owned()),
            ..Settings::default()
        }
    }

    #[test]
    fn requested_until_a_turn_confirms_it() {
        let mut settings = running("m1", "xhigh");
        let lines = settings.lines();
        assert_eq!(lines[0], "Requested model m1");
        assert_eq!(lines[1], "Requested effort xhigh");
        settings.effort_verified = true;
        let lines = settings.lines();
        assert_eq!(lines[1], "Effort xhigh");
        // Details use the desktop's short effort words; screen readers
        // hear the full ones.
        assert_eq!(running("m1", "medium").lines()[1], "Requested effort med");
        assert_eq!(
            running("m1", "medium").spoken_lines()[1],
            "Requested effort medium"
        );
        assert_eq!(settings.spoken_lines()[1], "Effort extra high");
        assert_eq!(lines[0], "Requested model m1");
        settings.model_verified = true;
        assert_eq!(settings.lines()[0], "Model m1");
    }

    #[test]
    fn a_requested_change_reads_as_the_next_turn() {
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
        assert_eq!(lines[0], "Default model");
        assert_eq!(lines[1], "Next turn: m1 · high");
        assert_eq!(settings.next_model(), Some("m1"));
    }

    #[test]
    fn a_requested_model_reads_requested_until_a_turn_carries_it() {
        let mut settings = running("m1", "low");
        assert_eq!(settings.next_model_words(), "Requested model m1");
        settings.model_verified = true;
        assert_eq!(settings.next_model_words(), "m1");
        // Codex: a model switch for the next turn, not carried yet.
        settings.config = Some(AgentConfigView {
            revision: 1,
            application: "next_turn".to_owned(),
            provenance: "configure".to_owned(),
            profile: "p".to_owned(),
            profile_inherited: false,
            requested_model: Some("m2".to_owned()),
            requested_effort: None,
            model: Some("m2".to_owned()),
            effort: Some("low".to_owned()),
            original_model: Some("m1".to_owned()),
            original_effort: Some("low".to_owned()),
        });
        assert_eq!(settings.next_model_words(), "Requested model m2");
        // The next turn reported carrying it.
        settings.effective_model = Some("m2".to_owned());
        assert_eq!(settings.next_model_words(), "m2");
        assert_eq!(Settings::default().next_model_words(), "Default model");
    }

    #[test]
    fn picks_skip_the_harness_default_entry_and_out_of_range() {
        let options = vec!["Harness default".to_owned(), "m1".to_owned()];
        assert_eq!(picked(&options, Some(0)), None);
        assert_eq!(picked(&options, Some(1)), Some("m1".to_owned()));
        assert_eq!(picked(&options, Some(9)), None);
    }

    #[test]
    fn a_change_sends_only_what_differs_and_what_the_route_lists() {
        let reply = reply(ConfigureSupport::default());
        let settings = running("m1", "low");
        let catalog = as_catalog(&reply);
        assert_eq!(catalog.models[0].display_name, "Model One");
        assert_eq!(catalog.models[1].display_name, "m2");
        assert_eq!(change(&reply, &settings, Some("Model One"), None), None);
        // The same effort changes nothing; another listed one changes it.
        assert_eq!(change(&reply, &settings, None, Some("low")), None);
        assert_eq!(
            change(&reply, &settings, None, Some("high")),
            Some((None, Some("high".to_owned())))
        );
        // An effort the model does not list never goes out.
        assert_eq!(change(&reply, &settings, None, Some("xhigh")), None);
        // A model change carries the row's effort, checked against that model.
        assert_eq!(
            change(&reply, &settings, Some("m2"), Some("xhigh")),
            Some((Some("m2".to_owned()), Some("xhigh".to_owned())))
        );
        assert_eq!(change(&reply, &settings, Some("m2"), Some("low")), None);
        assert_eq!(change(&reply, &settings, Some("m9"), None), None);
        assert_eq!(change(&reply, &settings, Some("m1"), Some("low")), None);
    }

    #[test]
    fn configure_alert_preserves_refusal_reason_and_reports_uncertainty() {
        let command = serde_json::from_value(serde_json::json!({
            "id":"configure", "principal":"owner", "method":"command.submit",
            "verb":"configure", "args":{}, "state":"rejected", "created_at":0, "updated_at":0,
            "outcome":{"reason":"Claude already has an active turn"}
        }))
        .unwrap();
        assert!(outcome_words(&command).contains("Claude already has an active turn"));
        let unknown = Command {
            state: eliot_core::CommandState::Unknown,
            ..command
        };
        assert!(
            failure_alert(&unknown, &outcome_words(&unknown))
                .starts_with("Model or effort change is uncertain")
        );
        assert!(
            ConfigureSupport::for_harness("claude")
                .effort_refusal
                .unwrap()
                .contains("launch_only")
        );
    }

    #[test]
    fn a_refused_field_never_changes() {
        let reply = reply(ConfigureSupport {
            model_refusal: Some("model changes need a new session".to_owned()),
            effort_refusal: None,
        });
        let settings = running("m1", "low");
        assert_eq!(change(&reply, &settings, Some("m2"), None), None);
        assert_eq!(
            change(&reply, &settings, None, Some("high")),
            Some((None, Some("high".to_owned())))
        );
        let locked = self::reply(ConfigureSupport::for_harness("claude"));
        assert_eq!(change(&locked, &settings, None, Some("high")), None);
        assert_eq!(
            locked_words("Claude Code", true, true).as_deref(),
            Some("Claude Code keeps the model and effort it started with.")
        );
        assert_eq!(locked_words("Codex", false, false), None);
    }

    #[test]
    fn launch_preview_stays_with_the_harness_and_confirmed_options_remove_locked_effort() {
        let launch = as_catalog(&reply(ConfigureSupport::default()));
        assert_eq!(launch_model(&launch, "codex", None).unwrap().id, "m1");
        assert_eq!(
            launch_model(&launch, "codex", Some("m2"))
                .unwrap()
                .efforts
                .as_deref()
                .unwrap(),
            ["medium", "xhigh"]
        );
        assert!(launch_model(&launch, "claude", Some("m2")).is_none());
        assert!(launch_model(&launch, "codex", Some("missing")).is_none());
        let locked = as_catalog(&reply(ConfigureSupport {
            model_refusal: None,
            effort_refusal: Some("fixed effort".to_owned()),
        }));
        assert!(
            locked
                .models
                .iter()
                .all(|model| { model.efforts.is_none() && model.default_effort.is_none() })
        );
    }

    #[test]
    fn options_follow_the_chosen_model_or_the_default() {
        let reply = reply(ConfigureSupport::default());
        assert_eq!(options(&reply, None).1, ["low", "high"]);
        assert_eq!(options(&reply, Some("m2")).1, ["medium", "xhigh"]);
        assert_eq!(options(&reply, None).0, ["m1", "m2"]);
    }
}
