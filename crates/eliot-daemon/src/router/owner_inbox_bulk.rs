use super::{
    Principal, Router, SubmissionContext, contains_known_secret_including_keys, require_owner,
};
use eliot_core::{
    CommandState, Error, Result,
    api::{
        CommandSubmitResponse, OwnerBulkDecideRequest, OwnerBulkDecideResponse,
        OwnerBulkDecisionApplied, OwnerBulkDecisionFailed, OwnerBulkDecisionSkipped,
        OwnerDecideRequest, OwnerDecideResponse, OwnerInboxItemRef, discovery::decode_params,
    },
    classify_dispatched_error,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use ulid::Ulid;

impl Router {
    pub(super) async fn owner_decide_bulk(
        &self,
        params: &Value,
        principal: &Principal,
        context: SubmissionContext<'_>,
    ) -> Result<Value> {
        require_owner(principal)?;
        if contains_known_secret_including_keys(params, &self.inner.secrets) {
            return Err(Error::new(
                "secret_in_payload",
                "owner decision batch contains a known credential",
            ));
        }
        let request: OwnerBulkDecideRequest = decode_params(params.clone())?;
        request.validate()?;

        // Bind the ordered snapshot before the first item can have an effect.
        // The compact record result is an envelope marker; per-item owner.decide
        // receipts retain the original inner mutation IDs for exact replay.
        let semantic_payload = json!({"items": &request.items});
        self.inner.store.lock().await.record_write(
            &request.id,
            "owner",
            "owner.decide.bulk",
            &semantic_payload,
            &json!({"bound": true}),
            super::now_ms(),
        )?;

        let mut result = OwnerBulkDecideResponse::default();
        for item in request.items {
            let mutation_id = per_item_mutation_id(&request.id, &item)?;
            let decision = OwnerDecideRequest {
                id: mutation_id.clone(),
                item: item.clone(),
                choice: "recommended".to_owned(),
                args: None,
            };
            let decision_params = serde_json::to_value(decision)
                .map_err(|_| Error::new("internal", "owner decision could not be encoded"))?;
            match self
                .owner_decide(&decision_params, principal, context)
                .await
            {
                Ok(value) => match serde_json::from_value::<OwnerDecideResponse>(value) {
                    Ok(response) if !response.snoozed => {
                        let submit_ack = response.result.as_ref().and_then(|value| {
                            serde_json::from_value::<CommandSubmitResponse>(value.clone()).ok()
                        });
                        match submit_ack.map(|ack| ack.state) {
                            Some(CommandState::Rejected) => {
                                result.failed.push(OwnerBulkDecisionFailed {
                                    item,
                                    mutation_id,
                                    code: "command_rejected".to_owned(),
                                    message: "recommended command submission was rejected"
                                        .to_owned(),
                                    uncertain: false,
                                });
                            }
                            Some(CommandState::Failed) => {
                                result.failed.push(OwnerBulkDecisionFailed {
                                    item,
                                    mutation_id,
                                    code: "command_failed".to_owned(),
                                    message: "recommended command execution failed".to_owned(),
                                    uncertain: false,
                                });
                            }
                            Some(CommandState::Cancelled) => {
                                result.failed.push(OwnerBulkDecisionFailed {
                                    item,
                                    mutation_id,
                                    code: "command_cancelled".to_owned(),
                                    message: "recommended command was cancelled".to_owned(),
                                    uncertain: false,
                                });
                            }
                            Some(CommandState::Unknown) => {
                                result.failed.push(OwnerBulkDecisionFailed {
                                    item,
                                    mutation_id,
                                    code: "unknown".to_owned(),
                                    message:
                                        "recommended command submission has an unknown outcome"
                                            .to_owned(),
                                    uncertain: true,
                                });
                            }
                            Some(state) => result.applied.push(OwnerBulkDecisionApplied {
                                item,
                                mutation_id,
                                outcome: command_state_name(state).to_owned(),
                            }),
                            None => result.applied.push(OwnerBulkDecisionApplied {
                                item,
                                mutation_id,
                                outcome: "applied".to_owned(),
                            }),
                        }
                    }
                    Ok(_) => result.failed.push(OwnerBulkDecisionFailed {
                        item,
                        mutation_id,
                        code: "internal".to_owned(),
                        message: "recommended action returned an unexpected decision result"
                            .to_owned(),
                        uncertain: true,
                    }),
                    Err(_) => result.failed.push(OwnerBulkDecisionFailed {
                        item,
                        mutation_id,
                        code: "internal".to_owned(),
                        message: "owner.decide returned an invalid response".to_owned(),
                        uncertain: true,
                    }),
                },
                Err(error) if error.code == "stale_item" => {
                    result.skipped.push(OwnerBulkDecisionSkipped {
                        item,
                        mutation_id,
                        reason: "item_changed_or_removed".to_owned(),
                    });
                }
                Err(error)
                    if error.code == "invalid"
                        && error.message.as_str() == "choice is unavailable for this item" =>
                {
                    result.skipped.push(OwnerBulkDecisionSkipped {
                        item,
                        mutation_id,
                        reason: "no_recommendation".to_owned(),
                    });
                }
                Err(error) => result.failed.push(failed_error(item, mutation_id, error)),
            }
        }
        serde_json::to_value(result)
            .map_err(|_| Error::new("internal", "owner decision batch could not be encoded"))
    }
}

fn per_item_mutation_id(batch_id: &str, item: &OwnerInboxItemRef) -> Result<String> {
    let batch = Ulid::from_string(batch_id).map_err(|_| Error::invalid_mutation_id())?;
    let mut digest = Sha256::new();
    digest.update(b"owner.decide.bulk.v1\0");
    digest.update(batch_id.as_bytes());
    digest.update([0]);
    digest.update(item.kind.as_str().as_bytes());
    digest.update([0]);
    digest.update(item.subject.as_bytes());
    digest.update([0]);
    digest.update(item.revision.as_bytes());
    let digest = digest.finalize();
    let mut randomness = [0_u8; 16];
    randomness[6..].copy_from_slice(&digest[..10]);
    let mut id = Ulid::from_parts(batch.timestamp_ms(), u128::from_be_bytes(randomness));
    if id == batch {
        randomness[15] ^= 1;
        id = Ulid::from_parts(batch.timestamp_ms(), u128::from_be_bytes(randomness));
    }
    Ok(id.to_string())
}

fn failed_error(
    item: OwnerInboxItemRef,
    mutation_id: String,
    error: Error,
) -> OwnerBulkDecisionFailed {
    let uncertain = classify_dispatched_error(&error.code) == CommandState::Unknown;
    OwnerBulkDecisionFailed {
        item,
        mutation_id,
        code: bounded_text(&error.code, 64),
        message: bounded_text(&error.message, 256),
        uncertain,
    }
}

fn bounded_text(value: &str, max_bytes: usize) -> String {
    let mut end = 0;
    for (index, character) in value.char_indices() {
        let next = index + character.len_utf8();
        if next > max_bytes {
            break;
        }
        end = next;
    }
    value[..end].to_owned()
}

fn command_state_name(state: CommandState) -> &'static str {
    match state {
        CommandState::Accepted => "accepted",
        CommandState::Queued => "queued",
        CommandState::Dispatched => "dispatched",
        CommandState::Applied => "applied",
        CommandState::Rejected => "rejected",
        CommandState::Failed => "failed",
        CommandState::Unknown => "unknown",
        CommandState::Cancelled => "cancelled",
    }
}
