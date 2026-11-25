//! Pure command error classification and state checks. Store still proves exact
//! command/generation/turn bindings and authoritative outcomes before committing.

use crate::CommandState;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Command dispatch identity. The persisted and public envelopes retain the
/// original verb and argument shape used for same-ID replay.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CommandVerb {
    Launch,
    Attach,
    Detach,
    Send,
    Steer,
    Stop,
    Interrupt,
    Resume,
    Compact,
    Inject,
    Configure,
    Approve,
    Deny,
    Answer,
    Switch,
    Run,
    GitFetch,
    GitIntegrate,
    GitPublish,
    Notify,
    Emit,
    AttentionEmit,
    MailSend,
    VarSet,
    WorktreeRemove,
    Other(String),
}

impl CommandVerb {
    fn known(verb: &str) -> Option<Self> {
        Some(match verb {
            "launch" => Self::Launch,
            "attach" => Self::Attach,
            "detach" => Self::Detach,
            "send" => Self::Send,
            "steer" => Self::Steer,
            "stop" => Self::Stop,
            "interrupt" => Self::Interrupt,
            "resume" => Self::Resume,
            "compact" => Self::Compact,
            "inject" => Self::Inject,
            "configure" => Self::Configure,
            "approve" => Self::Approve,
            "deny" => Self::Deny,
            "answer" => Self::Answer,
            "switch" => Self::Switch,
            "run" => Self::Run,
            "git.fetch" => Self::GitFetch,
            "git.integrate" => Self::GitIntegrate,
            "git.publish" => Self::GitPublish,
            "notify" => Self::Notify,
            "emit" => Self::Emit,
            "attention.emit" => Self::AttentionEmit,
            "mail.send" => Self::MailSend,
            "var.set" => Self::VarSet,
            "worktree.remove" => Self::WorktreeRemove,
            _ => return None,
        })
    }

    pub fn as_str(&self) -> &str {
        match self {
            Self::Launch => "launch",
            Self::Attach => "attach",
            Self::Detach => "detach",
            Self::Send => "send",
            Self::Steer => "steer",
            Self::Stop => "stop",
            Self::Interrupt => "interrupt",
            Self::Resume => "resume",
            Self::Compact => "compact",
            Self::Inject => "inject",
            Self::Configure => "configure",
            Self::Approve => "approve",
            Self::Deny => "deny",
            Self::Answer => "answer",
            Self::Switch => "switch",
            Self::Run => "run",
            Self::GitFetch => "git.fetch",
            Self::GitIntegrate => "git.integrate",
            Self::GitPublish => "git.publish",
            Self::Notify => "notify",
            Self::Emit => "emit",
            Self::AttentionEmit => "attention.emit",
            Self::MailSend => "mail.send",
            Self::VarSet => "var.set",
            Self::WorktreeRemove => "worktree.remove",
            Self::Other(verb) => verb,
        }
    }

    pub fn is_native_control(&self) -> bool {
        matches!(
            self,
            Self::Send
                | Self::Steer
                | Self::Interrupt
                | Self::Compact
                | Self::Inject
                | Self::Configure
                | Self::Approve
                | Self::Deny
                | Self::Answer
        )
    }
}

impl From<&str> for CommandVerb {
    fn from(verb: &str) -> Self {
        Self::known(verb).unwrap_or_else(|| Self::Other(verb.to_owned()))
    }
}

impl serde::Serialize for CommandVerb {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> serde::Deserialize<'de> for CommandVerb {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let verb = String::deserialize(deserializer)?;
        Ok(Self::known(&verb).unwrap_or(Self::Other(verb)))
    }
}

/// Common text payload used by SEND and STEER. The public SEND envelope can
/// also carry source references that the daemon resolves before dispatch.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct CommandText<'a> {
    pub text: &'a str,
}

impl<'a> CommandText<'a> {
    pub fn read(args: &'a Value) -> Option<Self> {
        args.as_object()?;
        Self::deserialize(args).ok()
    }

    pub fn required(args: &'a Value) -> crate::Result<Self> {
        Self::read(args)
            .filter(|input| !input.text.trim().is_empty())
            .ok_or_else(|| crate::Error::new("invalid", "text must be a non-empty string"))
    }
}

/// Ordinary Resume carries optional new input. Its session comes from Store.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
pub struct ResumeCommandArgs<'a> {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text: Option<&'a str>,
}

/// A configure request selects an optional model, effort, or both.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ConfigureCommandArgs {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effort: Option<String>,
}

impl<'a> ResumeCommandArgs<'a> {
    pub fn read(args: &'a Value) -> crate::Result<Self> {
        Self::deserialize(args).map_err(|_| crate::Error::new("invalid", "text must be a string"))
    }
}

/// An Owner continuation binds Resume to a retained Stop instead of taking text.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResumeContinuationArgs<'a> {
    pub continue_from_stop: &'a str,
}

impl<'a> ResumeContinuationArgs<'a> {
    pub fn read(args: &'a Value) -> crate::Result<Self> {
        args.as_object()
            .and_then(|_| Self::deserialize(args).ok())
            .ok_or_else(|| {
                crate::Error::new(
                    "invalid",
                    "continuation Resume requires an exact Stop command ID",
                )
            })
    }
}

/// Stop's force bit and the daemon's typed escalation reason. Unknown or
/// malformed fields in retained rows keep their historical false default.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
pub struct StopCommandArgs {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub force: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interrupt_runtime_unavailable: Option<bool>,
}

impl StopCommandArgs {
    pub fn retained(args: &Value) -> Self {
        Self {
            force: args.get("force").and_then(Value::as_bool),
            interrupt_runtime_unavailable: args
                .get("interrupt_runtime_unavailable")
                .and_then(Value::as_bool),
        }
    }

    pub fn read(args: &Value) -> crate::Result<Self> {
        let stop = Self::retained(args);
        if args.get("force").is_some() && stop.force.is_none() {
            return Err(crate::Error::new("invalid", "stop force must be a boolean"));
        }
        Ok(stop)
    }
}

/// Validate an exact retained command identity, including bounded derived IDs.
/// External mutation IDs have a separate ULID contract; command references are
/// opaque and must not be trimmed, normalized, or parsed as mutation IDs.
pub fn validate_command_id(id: &str) -> crate::Result<()> {
    if id.trim().is_empty() || id.len() > crate::limits::MAX_COMMAND_ID_BYTES {
        return Err(crate::Error::new(
            "invalid",
            "command ID must be nonblank and at most 256 bytes",
        ));
    }
    Ok(())
}

/// Whether a code is a deterministic refusal before an external effect is
/// dispatched. After dispatch, callers must use `classify_dispatched_error`;
/// these codes alone do not prove that native I/O did not occur.
/// `version_conflict` is the Store's compare-and-swap guard: it returns
/// before any write, so a stale expectation is a confirmed pre-effect
/// refusal, not an uncertain outcome.
pub fn is_deterministic_pre_admission_rejection(code: &str) -> bool {
    matches!(
        code,
        "assignment_occupied"
            | "brief_settling"
            | "brief_requires_include"
            | "bad_state"
            | "capacity_unavailable"
            | "conflict"
            | "daemon_update_in_progress"
            | "frame_too_large"
            | "handed_off"
            | "invalid"
            | "not_allowed"
            | "not_found"
            | "provider_held"
            | "quota_reserve"
            | "quota_unknown"
            | "routing_unavailable"
            | "secret_in_payload"
            | "stale_generation"
            | "stale_assignment"
            | "transfer_in_progress"
            | "unsupported"
            | "version_conflict"
    )
}

/// Classify an error after command dispatch without inventing a native receipt.
/// Only `native_not_dispatched` explicitly proves that native I/O did not start.
/// Generic validation, framing and transport codes can also arrive after I/O.
/// Authoritative plugin result states and typed workspace failures are settled
/// separately; an error code alone never establishes `Failed`.
pub fn classify_dispatched_error(code: &str) -> CommandState {
    match code {
        "native_not_dispatched" => CommandState::Rejected,
        _ => CommandState::Unknown,
    }
}

/// Ordinary command edges, not permission to repeat external work. A same-state
/// acknowledgement still requires Store's payload/outcome idempotency checks;
/// leaving Unknown requires exact readback or another authoritative result.
pub fn command_transition_allowed(from: CommandState, to: CommandState) -> bool {
    use CommandState::{
        Accepted, Applied, Cancelled, Dispatched, Failed, Queued, Rejected, Unknown,
    };

    if from == to {
        return true;
    }
    match from {
        Accepted => matches!(to, Queued | Dispatched | Rejected | Cancelled),
        Queued => matches!(to, Dispatched | Rejected | Cancelled),
        Dispatched | Unknown => matches!(to, Applied | Rejected | Failed | Unknown),
        Applied | Rejected | Failed | Cancelled => false,
    }
}

/// Narrow Owner assertion for a failed handoff brief. Store proves the brief
/// identity and Owner principal; this never authorizes another native SEND.
pub fn handoff_brief_resolution_allowed(from: CommandState, to: CommandState) -> bool {
    command_transition_allowed(from, to)
        || (from == CommandState::Failed && to == CommandState::Applied)
}

/// Existing pre-native workspace exceptions. The caller must prove the exact
/// preparation marker, command verb and generation before using these edges.
/// Unknown -> Accepted additionally needs verified creation readback proving the
/// remaining steps/native dispatch unperformed; it never authorizes Git replay.
/// Unknown -> Cancelled requires confirmed preparation cancellation/cleanup.
pub fn workspace_preparation_transition_allowed(from: CommandState, to: CommandState) -> bool {
    use CommandState::{Accepted, Cancelled, Failed, Queued, Unknown};

    command_transition_allowed(from, to)
        || matches!(
            (from, to),
            (Accepted | Queued, Failed | Unknown) | (Unknown, Accepted | Cancelled)
        )
}

/// Verbs that start an Agent generation: first launch or resume.
pub fn is_start_verb(verb: &str) -> bool {
    matches!(verb, "launch" | "resume")
}

/// Verbs that attach or detach an observer of an exact native subject.
pub fn is_attachment_verb(verb: &str) -> bool {
    matches!(verb, "attach" | "detach")
}

/// Verbs that answer a pending Agent request.
pub fn is_request_reply_verb(verb: &str) -> bool {
    matches!(verb, "approve" | "deny" | "answer")
}

/// Targetless verbs that record a user-visible event.
pub fn is_user_event_verb(verb: &str) -> bool {
    matches!(verb, "notify" | "emit")
}

/// Targetless verbs that move coordination data between Agents.
pub fn is_coordination_verb(verb: &str) -> bool {
    matches!(verb, "mail.send" | "var.set")
}
