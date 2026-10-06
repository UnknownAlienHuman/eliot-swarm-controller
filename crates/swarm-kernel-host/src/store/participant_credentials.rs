//! Launch-scoped adapter for assignment credential registration.
//!
//! The launch operation identity is host context, separate from the ordinary
//! `coordination.participant.register` request body. The Store carries it into
//! the same mutation transaction before applying the ordinary registration.

use crate::{
    config::Config,
    error::{Error, Result},
    store::Store,
    store::launcher::LaunchActor,
};
use rusqlite::Transaction;
use serde_json::Value;

/// The inner Rejected case means the Store committed a durable rejected
/// registration Operation. An outer error means the caller cannot conclude
/// whether the response was lost, so it must preserve the original credential.
pub(crate) enum LaunchRegistrationOutcome {
    Registered(Value),
    Rejected(Error),
}

pub(crate) async fn register_for_launch(
    store: &Store,
    actor: LaunchActor,
    launch_operation_id: String,
    params: Value,
) -> Result<LaunchRegistrationOutcome> {
    store
        .register_participant_for_launch(actor, launch_operation_id, params)
        .await
}

/// Called from the normal registration apply path with the mutation's
/// transaction. This validates the exact launch lease and assignment before
/// the ordinary Participant registration handler runs.
pub(super) fn validate_launch_registration(
    tx: &Transaction<'_>,
    actor: &LaunchActor,
    launch_operation_id: &str,
    config: &Config,
    registration_params: &Value,
) -> Result<()> {
    super::launcher_participant::validate_launch_registration(
        tx,
        actor,
        launch_operation_id,
        config,
        registration_params,
    )
}
