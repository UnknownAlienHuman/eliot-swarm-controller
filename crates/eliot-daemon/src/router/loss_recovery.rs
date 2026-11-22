use super::*;
use eliot_core::AttentionStep;

impl Router {
    /// A proven process exit can reopen the same retained native session
    /// once. Nothing from the lost generation is submitted as new input.
    pub(super) async fn resume_after_process_loss(
        &self,
        agent: &str,
        generation: i64,
        event_seq: i64,
        incident_id: &str,
    ) -> Result<()> {
        if !self.inner.attention.agent_lost.enabled
            || self.inner.mode.load(Ordering::SeqCst) != MODE_OPEN
        {
            return Ok(());
        }
        let mut request = SubmitCommand::new(ulid::Ulid::new().to_string(), "resume");
        request.target = Some(agent.to_owned());
        request.expected_generation = Some(generation);
        let prepared = self.prepare_submission(request, &Principal::Owner).await;
        let admission = {
            let mut store = self.inner.store.lock().await;
            if !store.process_loss_resume_evidence(agent, generation, event_seq)? {
                return Ok(());
            }
            let reaction = AttentionStep {
                incident_id: incident_id.to_owned(),
                step: "resume_after_process_loss".to_owned(),
                outcome: prepared.as_ref().err().map_or_else(
                    || "unknown".to_owned(),
                    |error| eliot_core::bound_chars(&error.code, 64),
                ),
                command_id: prepared
                    .as_ref()
                    .ok()
                    .map(|prepared| prepared.request.id.clone()),
                turn_id: None,
            };
            let Some(event) = store.claim_attention_step(agent, generation, &reaction, now_ms())?
            else {
                return Ok(());
            };
            self.publish_locked(&[event]);
            let Ok(prepared) = prepared else {
                return Ok(());
            };
            let admission = self.admit_prepared_submission(
                &mut store,
                "owner",
                prepared,
                SubmissionContext::default(),
            )?;
            self.publish_locked(&admission.events);
            admission
        };
        if admission.is_new && command_needs_worker(&admission.command) {
            self.schedule_admission(&admission);
        }
        self.signal_progress();
        Ok(())
    }
}
