//! Store facade for launch-scoped Participant registration.
//!
//! The launch identity is transport-independent host context. This module
//! fixes the method to `coordination.participant.register` and delegates to
//! the normal mutation path with the launch admission context attached.

use super::{
    Store, current_principal, mutate_in_transaction_with_launch_admission,
    participant_credentials::LaunchRegistrationOutcome,
};
use crate::{
    error::Result,
    model::{self, Principal},
};
use serde_json::Value;

impl Store {
    pub(crate) async fn register_participant_for_launch(
        &self,
        principal: Principal,
        launch_operation_id: String,
        params: Value,
    ) -> Result<LaunchRegistrationOutcome> {
        let config = self.config.clone();
        self.run(move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            let principal = current_principal(&tx, principal)?;
            let now = model::now_ms()?;
            let result = mutate_in_transaction_with_launch_admission(
                &tx,
                &principal,
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
