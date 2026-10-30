//! Serializable script contracts used by the automation-to-runner path.
//!
//! This module contains data and pure validation only. The daemon owns config
//! resolution, duration parsing, file reads, process custody and admission.

use super::{mutations::CommandSubmitRequest, reads::CommandGetRequest};
use crate::{
    Error, Event, EventSubject, FrozenSetupFile, Result, StderrDiagnostic, StderrSnapshot,
    StderrSource, SubmitCommand, bound_stderr_diagnostics, limits::MAX_EVENT_BYTES, parse_agent_id,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{collections::BTreeMap, path::Path};

/// Owner-configured interpreter executable and fixed argv prefix.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
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
            } => ("stop", target, expected_generation, None, json!({})),
            Self::Send {
                target,
                expected_generation,
                text,
            } => (
                "send",
                target,
                expected_generation,
                None,
                json!({ "text": text }),
            ),
            Self::Interrupt {
                target,
                expected_generation,
                expected_turn_id,
            } => (
                "interrupt",
                target,
                expected_generation,
                Some(expected_turn_id),
                json!({}),
            ),
        };

        let mut command = CommandSubmitRequest::new(String::new(), verb);
        command.target = Some(target);
        command.args = args;
        command.expected_generation = Some(generation);
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
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
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

/// Frozen project/event context stored with the ordinary run command args.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FrozenScriptContext {
    pub event: Event,
    pub subject: Option<EventSubject>,
    pub rule: ScriptRuleContext,
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
    pub fn validate(&self) -> Result<()> {
        self.rule.validate()
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
#[serde(deny_unknown_fields)]
pub struct ScriptInputContext {
    pub event: Event,
    pub subject: Option<EventSubject>,
    pub rule: ScriptRuleContext,
    pub command: ScriptCommandContext,
}

impl ScriptInputContext {
    /// Assemble stdin without embedding command args or creating a serialization cycle.
    pub fn for_run_command(
        context: &FrozenScriptContext,
        command_id: impl Into<String>,
    ) -> Result<Self> {
        let input = Self {
            event: context.event.clone(),
            subject: context.subject.clone(),
            rule: context.rule.clone(),
            command: ScriptCommandContext {
                id: command_id.into(),
                verb: "run".to_owned(),
            },
        };
        input.validate()?;
        Ok(input)
    }

    pub fn validate(&self) -> Result<()> {
        self.rule.validate()?;
        CommandGetRequest::new(self.command.id.clone()).validate()?;
        if self.command.verb != "run" {
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn script_interpreter_and_run_args_are_strict_and_keep_timeout_raw() {
        let program = std::env::temp_dir().join("pwsh.exe").display().to_string();
        let interpreter: ScriptInterpreter = serde_json::from_value(json!({
            "program": program,
            "argv_prefix": ["-NoProfile", "-NonInteractive", "-File"]
        }))
        .unwrap();
        interpreter.validate().unwrap();
        assert!(
            serde_json::from_value::<ScriptInterpreter>(json!({
                "program": "pwsh",
                "argv_prefix": [],
                "credential": "forbidden"
            }))
            .is_err()
        );

        let args: RunDefinitionArgs = serde_json::from_value(json!({
            "cmd": ["pwsh", ".eliot/scripts/check.ps1", "-Changed"],
            "timeout": "10m"
        }))
        .unwrap();
        args.validate().unwrap();
        assert_eq!(args.timeout.as_deref(), Some("10m"));
        assert!(args.env.is_empty());
        assert!(
            serde_json::from_value::<RunDefinitionArgs>(json!({
                "cmd": ["pwsh", ".eliot/scripts/check.ps1"],
                "token": "forbidden"
            }))
            .is_err()
        );
    }

    #[test]
    fn script_action_records_reject_authority_and_extra_fields() {
        let base = json!({
            "action": "stop",
            "target": "a7",
            "expected_generation": 4
        });
        assert!(serde_json::from_value::<ScriptActionRecord>(base.clone()).is_ok());
        for field in [
            "id",
            "principal",
            "parent_command",
            "ordinal",
            "executable",
            "env",
            "cmd",
        ] {
            let mut record = base.clone();
            record[field] = json!("forbidden");
            assert!(
                serde_json::from_value::<ScriptActionRecord>(record).is_err(),
                "field {field} must not be accepted"
            );
        }
        for record in [
            json!({"action":"stop","target":null,"expected_generation":4}),
            json!({"action":"stop","target":"a7","expected_generation":"4"}),
            json!({"action":"stop","target":"not-an-agent","expected_generation":4}),
            json!({"action":"stop","target":"a7","expected_generation":-1}),
            json!({"action":"interrupt","target":"a7","expected_generation":4}),
            json!({"action":"run","target":"a7","expected_generation":4}),
        ] {
            if let Ok(parsed) = serde_json::from_value::<ScriptActionRecord>(record.clone()) {
                assert!(parsed.validate().is_err());
            }
        }
    }

    #[test]
    fn script_send_text_uses_the_utf8_persisted_event_byte_limit() {
        let within = "é".repeat(MAX_EVENT_BYTES / 2);
        assert_eq!(within.len(), MAX_EVENT_BYTES);
        ScriptActionRecord::Send {
            target: "a7".to_owned(),
            expected_generation: 4,
            text: within,
        }
        .validate()
        .unwrap();

        let over = "é".repeat(MAX_EVENT_BYTES / 2 + 1);
        assert_eq!(over.len(), MAX_EVENT_BYTES + 2);
        assert!(
            ScriptActionRecord::Send {
                target: "a7".to_owned(),
                expected_generation: 4,
                text: over,
            }
            .validate()
            .is_err()
        );
    }

    #[test]
    fn action_records_convert_to_existing_submit_bindings_with_store_owned_ids() {
        let stop = ScriptActionRecord::Stop {
            target: "a7".to_owned(),
            expected_generation: 4,
        }
        .into_submit_command()
        .unwrap();
        assert_eq!(stop.id, "");
        assert_eq!(stop.verb, "stop");
        assert_eq!(stop.target.as_deref(), Some("a7"));
        assert_eq!(stop.expected_generation, Some(4));
        assert_eq!(stop.expected_turn_id, None);
        assert_eq!(stop.request_id, None);
        assert!(!stop.probe);
        assert_eq!(stop.args, json!({}));
        stop.validate().unwrap();

        let send = ScriptActionRecord::Send {
            target: "a7".to_owned(),
            expected_generation: 5,
            text: "done".to_owned(),
        }
        .into_submit_command()
        .unwrap();
        assert_eq!(send.verb, "send");
        assert_eq!(send.expected_generation, Some(5));
        assert_eq!(send.args, json!({"text":"done"}));
        send.validate().unwrap();

        let interrupt = ScriptActionRecord::Interrupt {
            target: "a7".to_owned(),
            expected_generation: 6,
            expected_turn_id: "turn-6".to_owned(),
        }
        .into_submit_command()
        .unwrap();
        assert_eq!(interrupt.verb, "interrupt");
        assert_eq!(interrupt.expected_generation, Some(6));
        assert_eq!(interrupt.expected_turn_id.as_deref(), Some("turn-6"));
        assert_eq!(interrupt.args, json!({}));
        interrupt.validate().unwrap();
    }

    #[test]
    fn frozen_run_context_assembles_runtime_id_without_recursive_command_args() {
        let event = Event {
            seq: 8,
            ts: 10,
            kind: "turn.completed".to_owned(),
            agent: Some("a7".to_owned()),
            subject: None,
            generation: Some(4),
            source: "fake".to_owned(),
            native_id: None,
            data: json!({"result":"ok"}),
            cause: None,
        };
        let snapshot = FrozenScriptRun {
            definition: RunDefinitionArgs {
                cmd: vec!["pwsh".to_owned(), ".eliot/scripts/check.ps1".to_owned()],
                cwd: Some(".".to_owned()),
                timeout: Some("10m".to_owned()),
                env: BTreeMap::from([("CI".to_owned(), "true".to_owned())]),
            },
            interpreter: FrozenScriptInterpreter {
                reference: "pwsh".to_owned(),
                executable: FrozenSetupFile {
                    canonical_path: std::env::temp_dir().join("pwsh.exe").display().to_string(),
                    size: 123,
                    sha256: "ab".repeat(32),
                },
                argv_prefix: vec![
                    "-NoProfile".to_owned(),
                    "-NonInteractive".to_owned(),
                    "-File".to_owned(),
                ],
            },
            entry: FrozenSetupFile {
                canonical_path: std::env::temp_dir()
                    .join(".eliot")
                    .join("scripts")
                    .join("check.ps1")
                    .display()
                    .to_string(),
                size: 321,
                sha256: "cd".repeat(32),
            },
            context: FrozenScriptContext {
                event,
                subject: None,
                rule: ScriptRuleContext {
                    id: "check-completed-turn".to_owned(),
                    revision: 2,
                },
            },
        };
        snapshot.validate().unwrap();
        let stored = serde_json::to_value(&snapshot).unwrap();
        assert!(stored.get("command").is_none());
        assert_eq!(stored["entry"]["sha256"], "cd".repeat(32));

        let input = ScriptInputContext::for_run_command(&snapshot.context, "run-1").unwrap();
        let wire = serde_json::to_value(input).unwrap();
        assert_eq!(wire["command"], json!({"id":"run-1","verb":"run"}));
        assert_eq!(
            wire["rule"],
            json!({"id":"check-completed-turn","revision":2})
        );
        assert!(wire["command"].get("args").is_none());
    }

    #[test]
    fn process_result_keeps_signal_exit_distinct_from_unknown() {
        let signaled = ScriptProcessResult {
            custody: ScriptCustody::Degraded,
            quiescent: true,
            direct_exit: ScriptDirectExit::Exited {
                success: false,
                exit_code: None,
            },
            stderr: None,
        };
        signaled.validate().unwrap();

        let unknown = ScriptProcessResult {
            custody: ScriptCustody::Unknown,
            quiescent: false,
            direct_exit: ScriptDirectExit::Unknown,
            stderr: None,
        };
        unknown.validate().unwrap();
        assert_ne!(
            serde_json::to_value(signaled).unwrap(),
            serde_json::to_value(unknown).unwrap()
        );
    }

    #[test]
    fn applied_run_carries_child_ids_and_accepts_degraded_quiescence_with_stderr_diagnostic() {
        let applied = ScriptRunApplied {
            children: vec!["child-command-id".to_owned()],
            process: ScriptProcessResult {
                custody: ScriptCustody::Degraded,
                quiescent: true,
                direct_exit: ScriptDirectExit::Exited {
                    success: true,
                    exit_code: Some(0),
                },
                stderr: Some(StderrSnapshot {
                    text: "diagnostic tail".to_owned(),
                    truncated: true,
                    eof: false,
                    read_error: true,
                }),
            },
        };
        applied.validate().unwrap();
        let wire = serde_json::to_value(&applied).unwrap();
        assert_eq!(wire["children"], json!(["child-command-id"]));
        assert!(wire["children"][0].is_string());
        assert_eq!(wire["process"]["custody"], "degraded");
        assert_eq!(wire["process"]["stderr"]["read_error"], true);

        let mut unproven = applied;
        unproven.process.custody = ScriptCustody::Unknown;
        assert!(unproven.validate().is_err());
    }

    #[test]
    fn run_outcome_stderr_trims_escaped_nuls_to_the_whole_value_budget() {
        let mut outcome = json!({
            "children": ["child-one", "child-two"],
            "process": {
                "custody": "degraded",
                "quiescent": true,
                "direct_exit": {
                    "state": "exited",
                    "success": true,
                    "exit_code": 0
                },
                "stderr": {
                    "text": "\0".repeat(crate::MAX_STDERR_BYTES),
                    "truncated": false,
                    "eof": true,
                    "read_error": true
                }
            }
        });
        let mut primary = outcome.clone();
        primary["process"]["stderr"] = Value::Null;
        let max_encoded_bytes = 8 * 1024;

        bound_script_outcome_stderr(&mut outcome, max_encoded_bytes);

        assert!(
            serde_json::to_vec(&outcome).unwrap().len() <= max_encoded_bytes,
            "the complete Run outcome must fit the supplied encoded budget"
        );
        let mut retained_primary = outcome.clone();
        retained_primary["process"]["stderr"] = Value::Null;
        assert_eq!(retained_primary, primary);
        let stderr: StderrSnapshot =
            serde_json::from_value(outcome["process"]["stderr"].clone()).unwrap();
        assert!(!stderr.text.is_empty());
        assert!(stderr.text.len() < crate::MAX_STDERR_BYTES);
        assert!(stderr.text.chars().all(|character| character == '\0'));
        assert!(stderr.truncated);
        assert!(stderr.eof);
        assert!(stderr.read_error);
    }

    #[test]
    fn run_outcome_stderr_is_omitted_when_no_envelope_room_remains() {
        let original = json!({
            "code": "failed",
            "cause": "script_output_invalid",
            "process": {
                "custody": "degraded",
                "quiescent": true,
                "direct_exit": {
                    "state": "exited",
                    "success": false,
                    "exit_code": 7
                },
                "stderr": {
                    "text": "diagnostic tail",
                    "truncated": false,
                    "eof": true,
                    "read_error": false
                }
            }
        });
        let mut primary = original.clone();
        primary["process"]["stderr"] = Value::Null;
        let primary_bytes = serde_json::to_vec(&primary).unwrap().len();

        let mut exact_room = original.clone();
        bound_script_outcome_stderr(&mut exact_room, primary_bytes);
        assert_eq!(exact_room, primary);

        let max_encoded_bytes = primary_bytes - 1;
        let mut undersized = original;
        bound_script_outcome_stderr(&mut undersized, max_encoded_bytes);
        assert_eq!(undersized, primary);
        assert!(serde_json::to_vec(&undersized).unwrap().len() > max_encoded_bytes);
    }
}
