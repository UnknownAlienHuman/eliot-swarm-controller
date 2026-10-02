use super::{
    Options, Service, diagnostic,
    http::{Data, decode},
    input_id, root_id, valid_id,
};
use crate::{
    error::{Error, Result},
    model,
    runtime::{EffectOutcome, RuntimeCommand, RuntimeOutcome},
};
use serde_json::{Value, json};
use std::{collections::BTreeSet, path::Path};

pub(super) fn outcome(
    command: &RuntimeCommand,
    state: EffectOutcome,
    options: &Options,
    details: Value,
) -> RuntimeOutcome {
    RuntimeOutcome {
        operation_id: command.operation_id.clone(),
        outcome: state,
        native_scope_key: Some(options.scope()),
        native_root_id: command.native_root_id.clone(),
        turn_id: None,
        native_input_id: None,
        details,
    }
}
pub(super) fn failed(
    command: &RuntimeCommand,
    options: &Options,
    error: &Error,
    sent: bool,
) -> RuntimeOutcome {
    outcome(
        command,
        if !sent || error.code == "NATIVE_REJECTED" {
            EffectOutcome::Rejected
        } else {
            EffectOutcome::Unknown
        },
        options,
        diagnostic(error),
    )
}
fn marker(command: &RuntimeCommand) -> Value {
    json!({"binding":command.binding_id,"generation":command.generation,"operation":command.operation_id})
}
fn prompt(command: &RuntimeCommand) -> Result<String> {
    let text = model::text(&command.input, "text")?;
    if command.method == "task.dispatch" {
        if !command.input["task_snapshot"].is_object() {
            return Err(Error::invalid("immutable task snapshot is required"));
        }
        Ok(format!(
            "{text}\n\nELIOT immutable task snapshot:\n{}",
            model::canonical(&command.input["task_snapshot"])?
        ))
    } else {
        Ok(text.to_owned())
    }
}
/// A server fallback to its default workspace must not redirect an assignment.
/// Native canonicalization (case, separators, symlinks) is allowed only when the
/// two paths actually resolve to the same local directory. No I/O runs in Store.
pub(super) async fn verify_directory(expected: &Path, observed: &Value) -> Result<()> {
    let observed = Path::new(model::text(observed, "directory")?);
    if !observed.is_absolute() {
        return Err(Error::new(
            "NATIVE_LOCATION_MISMATCH",
            "native workspace is not an absolute directory",
        ));
    }
    if observed == expected {
        return Ok(());
    }
    let expected = expected.to_owned();
    let observed = observed.to_owned();
    let same = tokio::task::spawn_blocking(move || {
        match (
            std::fs::canonicalize(&expected),
            std::fs::canonicalize(&observed),
        ) {
            (Ok(expected), Ok(observed)) => {
                expected == observed && std::fs::metadata(&expected).is_ok_and(|m| m.is_dir())
            }
            _ => false,
        }
    })
    .await
    .map_err(|_| {
        Error::new(
            "NATIVE_LOCATION_UNAVAILABLE",
            "workspace verification stopped",
        )
    })?;
    if !same {
        return Err(Error::new(
            "NATIVE_LOCATION_MISMATCH",
            "native workspace differs from the explicitly selected directory",
        ));
    }
    Ok(())
}

impl Service {
    async fn location(&self, options: &Options) -> Result<Value> {
        let value = self
            .get(
                "/api/location",
                &[(
                    "location[directory]",
                    options.directory.to_string_lossy().into_owned(),
                )],
            )
            .await?;
        verify_directory(&options.directory, &value).await?;
        model::text(&value["project"], "id")?;
        Ok(json!({"directory":value["directory"]}))
    }
    pub(crate) async fn verify_binding(
        &self,
        root: &str,
        options: &Options,
        binding: &str,
        generation: i64,
    ) -> Result<()> {
        if root != root_id(binding, generation) {
            return Err(Error::new(
                "NATIVE_IDENTITY_MISMATCH",
                "native root does not belong to this binding",
            ));
        }
        let session = self.check_root(root, options).await?;
        if session["metadata"]["eliot"]["binding"] != binding
            || session["metadata"]["eliot"]["generation"] != generation
        {
            return Err(Error::new(
                "NATIVE_IDENTITY_MISMATCH",
                "native binding metadata changed",
            ));
        }
        Ok(())
    }
    async fn check_model(&self, options: &Options) -> Result<()> {
        let catalog = self
            .get(
                "/api/model",
                &[(
                    "location[directory]",
                    options.directory.to_string_lossy().into_owned(),
                )],
            )
            .await?;
        verify_directory(&options.directory, &catalog["location"]).await?;
        let models = catalog["data"]
            .as_array()
            .ok_or_else(|| Error::new("NATIVE_SCHEMA_ERROR", "missing model catalog"))?;
        let found = models.iter().any(|m| {
            m["id"] == options.model.id
                && m["providerID"] == options.model.provider_id
                && m["enabled"] == true
                && m["variants"].as_array().is_some_and(|variants| {
                    variants.iter().any(|v| v["id"] == options.model.variant)
                })
        });
        if !found {
            return Err(Error::new(
                "NATIVE_MODEL_UNAVAILABLE",
                "the exact provider/model/variant is not enabled in this location",
            ));
        }
        Ok(())
    }
    async fn check_root(&self, root: &str, options: &Options) -> Result<Value> {
        let session = self.session(root).await?;
        let location = self.location(options).await?;
        if !session["parentID"].is_null() || !session["fork"].is_null() {
            return Err(Error::new(
                "NATIVE_SCOPE_MISMATCH",
                "controller-created root became a child or a fork",
            ));
        }
        if session["model"] != json!(options.model) || session["location"] != location {
            return Err(Error::new(
                "NATIVE_SETTINGS_MISMATCH",
                "native model/variant/location differs from the reserved route",
            ));
        }
        Ok(session)
    }
    /// Executes only the already committed command, once. HTTP write errors are
    /// not retried; reconciliation below contains no write requests.
    pub(crate) async fn execute(
        &self,
        command: &RuntimeCommand,
        options: &Options,
    ) -> RuntimeOutcome {
        if let Err(e) = self.verify().await {
            return failed(command, options, &e, false);
        }
        match command.method.as_str() {
            "agent.open" => self.open(command, options).await,
            "task.dispatch" | "agent.send" => self.send(command, options).await,
            "agent.reply" => self.reply(command, options).await,
            "agent.configure" => self.configure(command, options).await,
            _ => failed(
                command,
                options,
                &Error::new(
                    "UNSUPPORTED_CAPABILITY",
                    "this native operation is not implemented by the selected artifact",
                ),
                false,
            ),
        }
    }
    async fn open(&self, command: &RuntimeCommand, options: &Options) -> RuntimeOutcome {
        let preflight = async {
            self.check_model(options).await?;
            self.location(options).await
        }
        .await;
        let location = match preflight {
            Ok(v) => v,
            Err(e) => return failed(command, options, &e, false),
        };
        let root = root_id(&command.binding_id, command.generation);
        let result=self.post("/api/session",json!({"id":root,"location":location,"model":options.model,"metadata":{"eliot":marker(command)}})).await;
        let mut result = match result.and_then(decode::<Data<Value>>) {
            Ok(response) => {
                let valid = super::snapshot::validate_session(&response.data, Some(&root)).is_ok()
                    && response.data["parentID"].is_null()
                    && response.data["fork"].is_null()
                    && response.data["metadata"]["eliot"] == marker(command)
                    && response.data["model"] == json!(options.model)
                    && response.data["location"] == location;
                if valid {
                    outcome(
                        command,
                        EffectOutcome::Applied,
                        options,
                        json!({"completion_condition":"native_session_created","model":options.model,"location":location}),
                    )
                } else {
                    failed(
                        command,
                        options,
                        &Error::new(
                            "NATIVE_SCHEMA_ERROR",
                            "creation readback failed identity/settings verification",
                        ),
                        true,
                    )
                }
            }
            Err(e) => failed(command, options, &e, true),
        };
        // Identity is assigned before the write, so a lost creation response can
        // be investigated without another create and without scanning strangers.
        result.native_root_id = Some(root);
        result
    }
    async fn send(&self, command: &RuntimeCommand, options: &Options) -> RuntimeOutcome {
        let prepare=async{
            let root=command.native_root_id.as_deref().ok_or_else(||Error::invalid("native root is missing"))?;
            if command.method=="agent.send" && command.input["delivery"]!="next_turn" {
                return Err(Error::new("UNSUPPORTED_EXACT_TURN_STEER","V2 inbox steering has no atomic expected-turn guard; it must not emulate exact-turn steering"));
            }
            self.verify_binding(root,options,&command.binding_id,command.generation).await?;
            Ok((root,prompt(command)?))
        }.await;
        let (root, text) = match prepare {
            Ok(v) => v,
            Err(e) => return failed(command, options, &e, false),
        };
        let id = input_id(&command.operation_id);
        let body = json!({"id":id,"text":text,"metadata":{"eliot":marker(command)},"delivery":"queue","resume":true});
        let reply = self
            .post(&format!("/api/session/{root}/prompt"), body)
            .await;
        let mut result = match reply.and_then(decode::<Data<Value>>) {
            Ok(v) if inbox_matches(&v.data, root, &id, &text, command) => outcome(
                command,
                EffectOutcome::Applied,
                options,
                json!({"completion_condition":"native_input_admitted","delivery":"queue","evidence":"prompt_response","execution_complete":false}),
            ),
            Ok(_) => failed(
                command,
                options,
                &Error::new(
                    "NATIVE_SCHEMA_ERROR",
                    "native inbox receipt failed identity/content verification",
                ),
                true,
            ),
            Err(e) => failed(command, options, &e, true),
        };
        result.native_input_id = Some(id);
        result
    }
    /// Confirm exact family ownership before replying to a pending child request.
    pub(super) async fn owns_member(&self, root: &str, member: &str) -> Result<()> {
        let mut current = member.to_owned();
        let mut visited = BTreeSet::new();
        for _ in 0..64 {
            valid_id(&current, "ses")?;
            if !visited.insert(current.clone()) {
                break;
            }
            let session = self.session(&current).await?;
            if current == root {
                return Ok(());
            }
            let Some(parent) = session["parentID"].as_str() else {
                break;
            };
            current = parent.to_owned();
        }
        Err(Error::new(
            "NATIVE_SCOPE_MISMATCH",
            "request is not in the owned root's observed parent chain",
        ))
    }
    async fn reply(&self, command: &RuntimeCommand, options: &Options) -> RuntimeOutcome {
        let r = &command.input["reply"];
        let prepare = async {
            model::fields(
                r,
                &["kind", "session_id", "request_id", "fingerprint", "body"],
            )?;
            let root = command
                .native_root_id
                .as_deref()
                .ok_or_else(|| Error::invalid("native root is missing"))?;
            let kind = model::text(r, "kind")?;
            let session = model::text(r, "session_id")?;
            let request = model::text(r, "request_id")?;
            if !matches!(kind, "form" | "permission") {
                return Err(Error::invalid("reply kind must be form or permission"));
            }
            valid_id(request, if kind == "form" { "frm_" } else { "per" })?;
            self.verify_binding(root, options, &command.binding_id, command.generation)
                .await?;
            self.owns_member(root, session).await?;
            let pending: Data<Vec<Value>> = decode(
                self.get(&format!("/api/session/{session}/{kind}"), &[])
                    .await?,
            )?;
            let found = pending
                .data
                .iter()
                .find(|p| p["id"] == request && p["sessionID"] == session)
                .ok_or_else(|| {
                    Error::new(
                        "NATIVE_REQUEST_NOT_PENDING",
                        "exact native request is no longer pending",
                    )
                })?;
            if model::digest(model::canonical(found)?.as_bytes()) != model::text(r, "fingerprint")?
            {
                return Err(Error::new(
                    "NATIVE_REQUEST_CHANGED",
                    "read the current native request before replying",
                ));
            }
            if kind == "permission" {
                model::fields(&r["body"], &["decision", "message"])?;
                if !matches!(
                    r["body"]["decision"].as_str(),
                    Some("once" | "always" | "reject")
                ) {
                    return Err(Error::invalid("invalid native permission decision"));
                }
            } else {
                model::fields(&r["body"], &["answer"])?;
                if r["body"].get("answer").is_none() {
                    return Err(Error::invalid("native form answer required"));
                }
            }
            Ok(format!("/api/session/{session}/{kind}/{request}/reply"))
        }
        .await;
        let path = match prepare {
            Ok(v) => v,
            Err(e) => return failed(command, options, &e, false),
        };
        match self.post(&path, r["body"].clone()).await {
            Ok(Value::Null) => outcome(
                command,
                EffectOutcome::Applied,
                options,
                json!({"completion_condition":"native_reply_acknowledged","request_id":r["request_id"]}),
            ),
            Ok(_) => failed(
                command,
                options,
                &Error::new("NATIVE_SCHEMA_ERROR", "unexpected reply response"),
                true,
            ),
            Err(e) => failed(command, options, &e, true),
        }
    }
    pub(crate) async fn reconcile(
        &self,
        original: &RuntimeCommand,
        options: &Options,
    ) -> RuntimeOutcome {
        let readback=async {
            self.verify().await?;
            if original.method=="agent.configure" {
                return Ok(self.reconcile_configuration(original, options).await);
            }
            if original.method=="agent.open" {
                let id=root_id(&original.binding_id,original.generation);
                let session=self.check_root(&id,options).await?;
                if session["metadata"]["eliot"]!=marker(original) {return Err(Error::new("NATIVE_IDENTITY_MISMATCH","creation metadata does not match the original operation"));}
                let mut r=outcome(original,EffectOutcome::Applied,options,json!({"completion_condition":"native_session_created","evidence":"exact_session_readback"}));r.native_root_id=Some(id);return Ok(r);
            }
            if !matches!(original.method.as_str(),"agent.send"|"task.dispatch") {return Err(Error::new("NATIVE_EVIDENCE_UNAVAILABLE","no exact readback contract for this operation"));}
            let root=original.native_root_id.as_deref().ok_or_else(||Error::invalid("missing root"))?;
            self.verify_binding(root,options,&original.binding_id,original.generation).await?;
            let id=input_id(&original.operation_id);let text=prompt(original)?;
            let inbox:Data<Vec<Value>>=decode(self.get(&format!("/api/session/{root}/inbox"),&[]).await?)?;
            let queued=inbox.data.iter().any(|item|inbox_matches(item,root,&id,&text,original));
            if !queued {
                let message:Data<Value>=decode(self.get(&format!("/api/session/{root}/message/{id}"),&[]).await?)?;
                if !delivered_matches(&message.data, original)? {
                    return Err(Error::new("NATIVE_EVIDENCE_UNAVAILABLE","exact delivered input was not observed"));
                }
            }
            let mut r=outcome(original,EffectOutcome::Applied,options,json!({"completion_condition":"native_input_admitted","delivery":"queue","evidence":if queued{"inbox_readback"}else{"projected_message_readback"},"execution_complete":false}));r.native_input_id=Some(id);Ok(r)
        }.await;
        match readback {
            Ok(r) => r,
            Err(e) => outcome(original, EffectOutcome::Unknown, options, diagnostic(&e)),
        }
    }
}
fn inbox_matches(item: &Value, root: &str, id: &str, text: &str, command: &RuntimeCommand) -> bool {
    item["id"] == id
        && item["sessionID"] == root
        && item["type"] == "user"
        && item["delivery"] == "queue"
        && item["payload"]["text"] == text
        && item["payload"]["metadata"]["eliot"] == marker(command)
        && no_attachments(&item["payload"])
}

// Exact IDs and the original content/metadata are necessary, not a substring or
// equal text alone. Extra native attachments change the admitted assignment.
fn no_attachments(value: &Value) -> bool {
    ["files", "agents", "skills"].iter().all(|key| {
        value
            .get(*key)
            .is_none_or(|v| v.as_array().is_some_and(Vec::is_empty))
    })
}
pub(super) fn delivered_matches(message: &Value, command: &RuntimeCommand) -> Result<bool> {
    Ok(message["id"] == input_id(&command.operation_id)
        && message["type"] == "user"
        && message
            .get("sessionID")
            .is_none_or(|id| id.as_str() == command.native_root_id.as_deref())
        && message["text"] == prompt(command)?
        && message["metadata"]["eliot"] == marker(command)
        && no_attachments(message))
}
