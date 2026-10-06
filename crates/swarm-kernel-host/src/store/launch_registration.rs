//! Store facade for launch-scoped Participant registration.
//!
//! The launch identity is transport-independent host context. This module
//! fixes the method to `coordination.participant.register` and delegates to
//! the typed launch-child mutation path without changing caller identity.

use super::{
    Store, launcher, mutate_launch_child_in_transaction,
    participant_credentials::LaunchRegistrationOutcome,
};
use crate::{error::Result, model, store::launcher::LaunchActor};
use serde_json::Value;

impl Store {
    pub(crate) async fn register_participant_for_launch(
        &self,
        actor: LaunchActor,
        launch_operation_id: String,
        params: Value,
    ) -> Result<LaunchRegistrationOutcome> {
        let config = self.config.clone();
        self.run(move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            let current_actor = launcher::launch_actor(&tx, &launch_operation_id)?;
            if !current_actor.same_authority_identity(&actor) {
                return Err(crate::error::Error::new(
                    "FORBIDDEN",
                    "launch registration actor changed before registration",
                ));
            }
            let now = model::now_ms()?;
            let result = mutate_launch_child_in_transaction(
                &tx,
                &current_actor,
                "coordination.participant.register",
                &params,
                &config,
                now,
                &launch_operation_id,
            )?;
            tx.commit()?;
            Ok(match result {
                Ok(value) => LaunchRegistrationOutcome::Registered(value),
                Err(error) => LaunchRegistrationOutcome::Rejected(error),
            })
        })
        .await
    }
}
