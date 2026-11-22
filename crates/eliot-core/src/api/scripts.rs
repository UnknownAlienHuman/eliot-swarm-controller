//! Serializable script contracts used by the automation-to-runner path.
//!
//! This module contains data and pure validation only. The daemon owns config
//! resolution, duration parsing, file reads, process custody and admission.

use super::{
    EmitDefinitionArgs, TimerSchedule, mutations::CommandSubmitRequest, reads::CommandGetRequest,
};
use crate::{
    Error, Event, EventSubject, FrozenSetupFile, Result, StderrDiagnostic, StderrSnapshot,
    StderrSource, SubmitCommand, bound_stderr_diagnostics, limits::MAX_EVENT_BYTES, parse_agent_id,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{collections::BTreeMap, path::Path};

/// Owner-configured interpreter executable and fixed argv prefix.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScriptInterpreter {
    pub program: String,
    pub argv_prefix: Vec<String>,
}

impl ScriptInterpreter {
    /// Pure string validation; path existence and executable checks belong to the daemon.
    pub fn validate(&self) -> Result<()> {
        if self.program.trim().is_empty()
            || self.program.contains('\0')
            || !Path::new(&self.program).is_absolute()
        {
            return Err(invalid(
                "script interpreter program must be an absolute nonempty path",
            ));
        }
        if self.argv_prefix.iter().any(|value| value.contains('\0')) {
            return Err(invalid("script interpreter argv_prefix cannot contain NUL"));
        }
        Ok(())
    }
}

/// Strict, flat stdout record emitted by a script.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum ScriptActionRecord {
    Stop {
        target: String,
        expected_generation: i64,
    },
    Send {
        target: String,
        expected_generation: i64,
        text: String,
    },
    Interrupt {
        target: String,
        expected_generation: i64,
        expected_turn_id: String,
    },
    Emit {
        topic: String,
        data: Value,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        key: Option<String>,
    },
}

impl ScriptActionRecord {
    /// Reuse the canonical Agent-id parser and the event/action text storage bound.
    pub fn validate(&self) -> Result<()> {
        match self {
            Self::Stop {
                target,
                expected_generation,
            } => validate_target_generation(target, *expected_generation),
            Self::Send {
                target,
                expected_generation,
                text,
            } => {
                validate_target_generation(target, *expected_generation)?;
                validate_send_text(text)
            }
            Self::Interrupt {
                target,
                expected_generation,
                expected_turn_id,
            } => {
                validate_target_generation(target, *expected_generation)?;
                CommandGetRequest::new(expected_turn_id.clone()).validate()
            }
            Self::Emit { topic, data, key } => {
                EmitDefinitionArgs {
                    topic: topic.clone(),
                    data: data.clone(),
                    key: key.clone(),
                }
                .validate()?;
                Ok(())
            }
        }
    }

    /// Convert one admitted record to the existing command DTO.
    ///
    /// The id remains empty until Store assigns it in the atomic child admission.
    pub fn into_submit_command(self) -> Result<SubmitCommand> {
        self.validate()?;
        let (verb, target, generation, expected_turn_id, args) = match self {
            Self::Stop {
                target,
                expected_generation,
            } => (
                "stop",
                Some(target),
                Some(expected_generation),
                None,
                json!({}),
            ),
            Self::Send {
                target,
                expected_generation,
                text,
            } => (
                "send",
                Some(target),
                Some(expected_generation),
                None,
                json!({ "text": text }),
            ),
            Self::Interrupt {
                target,
                expected_generation,
                expected_turn_id,
            } => (
                "interrupt",
                Some(target),
                Some(expected_generation),
                Some(expected_turn_id),
                json!({}),
            ),
            Self::Emit { topic, data, key } => {
                let definition = EmitDefinitionArgs { topic, data, key };
                let args = serde_json::to_value(definition)
                    .map_err(|_| invalid("script emit definition cannot be encoded"))?;
                ("emit", None, None, None, args)
            }
        };

        let mut command = CommandSubmitRequest::new(String::new(), verb);
        command.target = target;
        command.args = args;
        command.expected_generation = generation;
        command.expected_turn_id = expected_turn_id;
        command.validate()?;
        Ok(command)
    }
}

fn validate_target_generation(target: &str, generation: i64) -> Result<()> {
    parse_agent_id(target)?;
    if generation < 0 {
        return Err(invalid("expected_generation must be nonnegative"));
    }
    Ok(())
}

fn validate_send_text(text: &str) -> Result<()> {
    if text.len() > MAX_EVENT_BYTES {
        return Err(invalid("send text exceeds the 64 KiB persisted-text bound"));
    }
    Ok(())
}

/// Raw rule-authored run arguments. Timeout parsing and path resolution stay in the daemon.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunDefinitionArgs {
    pub cmd: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout: Option<String>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
}

impl RunDefinitionArgs {
    /// Check only argv structure and process-string NUL constraints.
    pub fn validate(&self) -> Result<()> {
        if self.cmd.len() < 2 || self.cmd[0].trim().is_empty() || self.cmd[1].trim().is_empty() {
            return Err(invalid(
                "run cmd requires a literal interpreter reference and script entry",
            ));
        }
        if self.cmd.iter().any(|value| value.contains('\0'))
            || self.cwd.as_ref().is_some_and(|value| value.contains('\0'))
            || self
                .timeout
                .as_ref()
                .is_some_and(|value| value.contains('\0'))
            || self
                .env
                .iter()
                .any(|(name, value)| name.contains('\0') || value.contains('\0'))
        {
            return Err(invalid("run arguments cannot contain NUL"));
        }
        Ok(())
    }
}

/// Frozen owner-selected interpreter reference used by an admitted run.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FrozenScriptInterpreter {
    pub reference: String,
    pub executable: FrozenSetupFile,
    pub argv_prefix: Vec<String>,
}

/// Why the owner-authorized timer invocation was admitted.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ScriptTimerInvocation {
    Scheduled { scheduled_at_ms: i64 },
    Manual { parent_command_id: String },
}

impl ScriptTimerInvocation {
    pub fn validate(&self) -> Result<()> {
        match self {
            Self::Scheduled { .. } => Ok(()),
            Self::Manual { parent_command_id } => {
                CommandGetRequest::new(parent_command_id.clone()).validate()
            }
        }
    }
}

/// Timer schedule snapshot and invocation metadata captured for a script Run.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScriptTimerContext {
    pub schedule_revision: i64,
    pub schedule: TimerSchedule,
    pub anchor: i64,
    pub invocation: ScriptTimerInvocation,
}

impl ScriptTimerContext {
    pub fn validate(&self) -> Result<()> {
        if self.schedule_revision < 0 {
            return Err(invalid(
                "script timer schedule_revision must be nonnegative",
            ));
        }
        self.schedule.validate()?;
        let invalid_anchor = match &self.schedule {
            TimerSchedule::Interval {
                anchor: Some(schedule_anchor),
                ..
            } => self.anchor != *schedule_anchor,
            _ => false,
        };
        if invalid_anchor {
            return Err(invalid(
                "script timer anchor does not match its interval schedule",
            ));
        }
        self.invocation.validate()
    }
}

/// Frozen event or timer context stored with the ordinary Run command args.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged, deny_unknown_fields)]
pub enum FrozenScriptContext {
    Event {
        event: Box<Event>,
        subject: Option<EventSubject>,
        rule: ScriptRuleContext,
    },
    Timer {
        timer: ScriptTimerContext,
        rule: ScriptRuleContext,
    },
}

/// Rule identity captured when the event rule matched.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScriptRuleContext {
    pub id: String,
    pub revision: i64,
}

impl ScriptRuleContext {
    pub fn validate(&self) -> Result<()> {
        if self.id.trim().is_empty() || self.revision < 0 {
            return Err(invalid(
                "script rule context requires an id and nonnegative revision",
            ));
        }
        Ok(())
    }
}

impl FrozenScriptContext {
    pub fn rule(&self) -> &ScriptRuleContext {
        match self {
            Self::Event { rule, .. } | Self::Timer { rule, .. } => rule,
        }
    }

    pub fn event(&self) -> Option<&Event> {
        match self {
            Self::Event { event, .. } => Some(event.as_ref()),
            Self::Timer { .. } => None,
        }
    }

    pub fn subject(&self) -> Option<&EventSubject> {
        match self {
            Self::Event { subject, .. } => subject.as_ref(),
            Self::Timer { .. } => None,
        }
    }

    pub fn timer(&self) -> Option<&ScriptTimerContext> {
        match self {
            Self::Event { .. } => None,
            Self::Timer { timer, .. } => Some(timer),
        }
    }

    pub fn validate(&self) -> Result<()> {
        self.rule().validate()?;
        if let Some(timer) = self.timer() {
            timer.validate()?;
        }
        Ok(())
    }
}

impl FrozenScriptInterpreter {
    pub fn validate(&self) -> Result<()> {
        if self.reference.trim().is_empty()
            || !Path::new(&self.executable.canonical_path).is_absolute()
            || self.argv_prefix.iter().any(|value| value.contains('\0'))
        {
            return Err(invalid("frozen script interpreter is malformed"));
        }
        validate_frozen_file(&self.executable)
    }
}

fn validate_frozen_file(file: &FrozenSetupFile) -> Result<()> {
    if !Path::new(&file.canonical_path).is_absolute()
        || file.canonical_path.contains('\0')
        || file.sha256.len() != 64
        || !file.sha256.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(invalid("frozen script file receipt is malformed"));
    }
    Ok(())
}

/// Frozen semantic run inputs stored in the ordinary command args.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FrozenScriptRun {
    pub definition: RunDefinitionArgs,
    pub interpreter: FrozenScriptInterpreter,
    pub entry: FrozenSetupFile,
    pub context: FrozenScriptContext,
}

impl FrozenScriptRun {
    pub fn validate(&self) -> Result<()> {
        self.definition.validate()?;
        self.context.validate()?;
        self.interpreter.validate()?;
        validate_frozen_file(&self.entry)?;
        if self.interpreter.reference != self.definition.cmd[0] {
            return Err(invalid(
                "frozen interpreter reference must match run cmd[0]",
            ));
        }
        Ok(())
    }
}

/// Minimal command member of the script stdin envelope.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScriptCommandContext {
    pub id: String,
    pub verb: String,
}

/// Runtime stdin envelope assembled after Store assigns the run command id.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged, deny_unknown_fields)]
pub enum ScriptInputContext {
    Event {
        event: Box<Event>,
        subject: Option<EventSubject>,
        rule: ScriptRuleContext,
        command: ScriptCommandContext,
    },
    Timer {
        timer: ScriptTimerContext,
        rule: ScriptRuleContext,
        command: ScriptCommandContext,
    },
}

impl ScriptInputContext {
    /// Assemble stdin without embedding command args or creating a serialization cycle.
    pub fn for_run_command(
        context: &FrozenScriptContext,
        command_id: impl Into<String>,
    ) -> Result<Self> {
        let command = ScriptCommandContext {
            id: command_id.into(),
            verb: "run".to_owned(),
        };
        let input = match context {
            FrozenScriptContext::Event {
                event,
                subject,
                rule,
            } => Self::Event {
                event: event.clone(),
                subject: subject.clone(),
                rule: rule.clone(),
                command,
            },
            FrozenScriptContext::Timer { timer, rule } => Self::Timer {
                timer: timer.clone(),
                rule: rule.clone(),
                command,
            },
        };
        input.validate()?;
        Ok(input)
    }

    pub fn rule(&self) -> &ScriptRuleContext {
        match self {
            Self::Event { rule, .. } | Self::Timer { rule, .. } => rule,
        }
    }

    pub fn event(&self) -> Option<&Event> {
        match self {
            Self::Event { event, .. } => Some(event.as_ref()),
            Self::Timer { .. } => None,
        }
    }

    pub fn subject(&self) -> Option<&EventSubject> {
        match self {
            Self::Event { subject, .. } => subject.as_ref(),
            Self::Timer { .. } => None,
        }
    }

    pub fn timer(&self) -> Option<&ScriptTimerContext> {
        match self {
            Self::Event { .. } => None,
            Self::Timer { timer, .. } => Some(timer),
        }
    }

    pub fn validate(&self) -> Result<()> {
        self.rule().validate()?;
        if let Some(timer) = self.timer() {
            timer.validate()?;
        }
        let command = match self {
            Self::Event { command, .. } | Self::Timer { command, .. } => command,
        };
        CommandGetRequest::new(command.id.clone()).validate()?;
        if command.verb != "run" {
            return Err(invalid("script stdin command verb must be run"));
        }
        Ok(())
    }
}

/// Custody guarantee observed for one script process family.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScriptCustody {
    Exact,
    Degraded,
    Unknown,
}

/// Direct-exit observation. Exited with no code represents a known signal exit.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub enum ScriptDirectExit {
    Unknown,
    Exited {
        success: bool,
        exit_code: Option<i32>,
    },
}

impl ScriptDirectExit {
    fn is_successful_zero(&self) -> bool {
        matches!(
            self,
            Self::Exited {
                success: true,
                exit_code: Some(0)
            }
        )
    }
}

/// Reusable process metadata for applied runs and failed-capture diagnostics.
/// Stderr is an optional bounded diagnostic and does not determine the primary result.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScriptProcessResult {
    pub custody: ScriptCustody,
    pub quiescent: bool,
    pub direct_exit: ScriptDirectExit,
    pub stderr: Option<StderrSnapshot>,
}

impl ScriptProcessResult {
    pub fn validate(&self) -> Result<()> {
        if let ScriptDirectExit::Exited {
            success: true,
            exit_code,
        } = &self.direct_exit
            && *exit_code != Some(0)
        {
            return Err(invalid(
                "successful script exit must carry a known zero exit code",
            ));
        }
        if let ScriptDirectExit::Exited {
            success: false,
            exit_code: Some(0),
        } = &self.direct_exit
        {
            return Err(invalid(
                "unsuccessful script exit cannot carry a zero exit code",
            ));
        }
        Ok(())
    }
}

/// Applied run outcome carries admitted child IDs rather than recursive Command objects.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScriptRunApplied {
    pub children: Vec<String>,
    pub process: ScriptProcessResult,
}

impl ScriptRunApplied {
    pub fn validate(&self) -> Result<()> {
        self.process.validate()?;
        if !self.process.direct_exit.is_successful_zero()
            || !self.process.quiescent
            || self.process.custody == ScriptCustody::Unknown
        {
            return Err(invalid(
                "applied script run requires known zero exit and quiescent custody",
            ));
        }
        for child_id in &self.children {
            CommandGetRequest::new(child_id.clone()).validate()?;
        }
        Ok(())
    }
}

/// Bound only optional process stderr in a Run outcome using the supplied
/// budget for the complete encoded outcome value. The existing process DTO owns
/// its shape; callers own Run outcome identity and shape.
pub fn bound_script_outcome_stderr(outcome: &mut Value, max_encoded_bytes: usize) {
    let Some(process_value) = outcome.get("process") else {
        return;
    };
    let Ok(process_result) = serde_json::from_value::<ScriptProcessResult>(process_value.clone())
    else {
        return;
    };
    let Some(snapshot) = process_result.stderr else {
        return;
    };

    let mut without_stderr = outcome.clone();
    let Some(process) = without_stderr
        .get_mut("process")
        .and_then(Value::as_object_mut)
    else {
        return;
    };
    process.insert("stderr".to_owned(), Value::Null);
    let Ok(base_bytes) = serde_json::to_vec(&without_stderr) else {
        return;
    };
    if base_bytes.len() > max_encoded_bytes {
        // Leave an oversized primary outcome for its existing semantic guard.
        *outcome = without_stderr;
        return;
    }

    let diagnostic = StderrDiagnostic {
        // This diagnostic is only a temporary carrier for the shared encoded
        // tail bounder; only its snapshot is written back to the Run outcome.
        source: StderrSource::Harness,
        snapshot,
    };
    let snapshot_bytes = serde_json::to_vec(&diagnostic.snapshot).map_or(0, |bytes| bytes.len());
    let diagnostic_bytes = serde_json::to_vec(std::slice::from_ref(&diagnostic))
        .map_or(snapshot_bytes, |bytes| bytes.len());
    let wrapper_bytes = diagnostic_bytes.saturating_sub(snapshot_bytes);
    let null_bytes = serde_json::to_vec(&Value::Null).map_or(0, |bytes| bytes.len());
    let snapshot_budget = max_encoded_bytes
        .saturating_sub(base_bytes.len())
        .saturating_add(null_bytes);
    let mut diagnostics = vec![diagnostic];
    bound_stderr_diagnostics(
        &mut diagnostics,
        snapshot_budget.saturating_add(wrapper_bytes),
    );

    let Some(diagnostic) = diagnostics.pop() else {
        *outcome = without_stderr;
        return;
    };
    let Ok(stderr) = serde_json::to_value(diagnostic.snapshot) else {
        *outcome = without_stderr;
        return;
    };
    let Some(process) = without_stderr
        .get_mut("process")
        .and_then(Value::as_object_mut)
    else {
        *outcome = without_stderr;
        return;
    };
    process.insert("stderr".to_owned(), stderr);
    if serde_json::to_vec(&without_stderr).is_ok_and(|bytes| bytes.len() <= max_encoded_bytes) {
        *outcome = without_stderr;
    } else {
        *outcome = {
            let mut primary = outcome.clone();
            if let Some(process) = primary.get_mut("process").and_then(Value::as_object_mut) {
                process.insert("stderr".to_owned(), Value::Null);
            }
            primary
        };
    }
}

fn invalid(message: &'static str) -> Error {
    Error::new("invalid", message)
}
