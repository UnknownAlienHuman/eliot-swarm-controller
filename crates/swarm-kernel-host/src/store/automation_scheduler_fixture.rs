use super::{DueSourceDisposition, DueSourceOutcome, aggregate_source_disposition};

fn outcome(disposition: DueSourceDisposition) -> DueSourceOutcome {
    DueSourceOutcome {
        kind: "fixture_source",
        disposition,
        cursor_advanced: disposition == DueSourceDisposition::Progressed,
        damaged_subjects: Vec::new(),
        pending_subjects: Vec::new(),
    }
}

#[test]
fn invocation_without_retained_cursor_progress_is_idle() {
    assert_eq!(
        DueSourceDisposition::observe(true, false, false),
        DueSourceDisposition::Idle
    );
    assert_eq!(
        DueSourceDisposition::observe(true, true, false),
        DueSourceDisposition::Progressed
    );
}

#[test]
fn aggregate_degradation_precedes_progress_and_idle() {
    assert_eq!(
        aggregate_source_disposition(&[
            outcome(DueSourceDisposition::Idle),
            outcome(DueSourceDisposition::Progressed),
            outcome(DueSourceDisposition::Degraded),
        ]),
        DueSourceDisposition::Degraded
    );
    assert_eq!(
        aggregate_source_disposition(&[
            outcome(DueSourceDisposition::Idle),
            outcome(DueSourceDisposition::Progressed),
        ]),
        DueSourceDisposition::Progressed
    );
}

#[test]
fn first_retained_schedule_cursor_counts_as_progress() {
    let after = vec![("schedule-a".to_owned(), Some(40), None, None)];
    assert!(super::schedule_cursor_advanced(&[], &after));
    assert!(!super::schedule_cursor_advanced(&after, &after));
}

#[test]
fn schedule_capacity_is_pending_but_check_and_unknown_errors_stay_unclassified() {
    use super::super::automation_reconcile::SubjectErrorDisposition;

    let classified = super::super::schedules::classify_scheduler_error(&crate::error::Error::new(
        "SCHEDULE_REGISTRY_FULL",
        "bounded fixture",
    ));
    match classified {
        Some(SubjectErrorDisposition::Pending { code, .. }) => {
            assert_eq!(code, "SCHEDULE_REGISTRY_FULL");
        }
        _ => panic!("schedule capacity must remain pending"),
    }

    for error in [
        crate::error::Error::new(
            "CHECK_LAUNCH_RECEIPT_INVALID",
            "retained CheckRun evidence is invalid",
        ),
        crate::error::Error::new("STORE_ERROR", "must remain fatal"),
    ] {
        assert!(
            super::super::schedules::classify_scheduler_error(&error).is_none(),
            "{} must not be converted into an interval-schedule pending result",
            error.code
        );
    }

    let wrapped_infrastructure = crate::error::Error::new(
        "CHECK_LAUNCH_RECEIPT_INVALID",
        "receipt failure with a secondary storage error",
    )
    .with_secondary_code("STORE_ERROR");
    let retained = super::check_recovery_failure(wrapped_infrastructure).unwrap_err();
    assert_eq!(retained.code, "CHECK_LAUNCH_RECEIPT_INVALID");
    assert_eq!(retained.secondary_codes, vec!["STORE_ERROR"]);
    let schedule_infrastructure = crate::error::Error::new(
        "SCHEDULE_REGISTRY_FULL",
        "registry capacity with a storage failure",
    )
    .with_secondary_code("STORE_ERROR");
    assert!(super::super::schedules::classify_scheduler_error(&schedule_infrastructure).is_none());
}

#[test]
fn malformed_check_recovery_receipts_are_retained_bounded_and_unknowns_stay_fatal() {
    let error = crate::error::Error::new(
        "CHECK_LAUNCH_RECEIPT_INVALID",
        "x".repeat(super::MAX_CHECK_RECOVERY_ERROR_MESSAGE_CHARS + 1),
    );
    let check_recovery = super::check_recovery_failure(error).unwrap();

    assert_eq!(check_recovery["disposition"], "degraded");
    assert_eq!(
        check_recovery["pending_error"]["code"],
        "CHECK_LAUNCH_RECEIPT_INVALID"
    );
    assert_eq!(check_recovery["pending_error"]["message_truncated"], true);
    assert_eq!(
        check_recovery["pending_error"]["message"]
            .as_str()
            .unwrap()
            .chars()
            .count(),
        super::MAX_CHECK_RECOVERY_ERROR_MESSAGE_CHARS
    );
    assert_eq!(
        check_recovery["pending_error"]["error_digest"]
            .as_str()
            .unwrap()
            .len(),
        64
    );
    assert_eq!(
        super::aggregate_scheduler_disposition(
            &[outcome(DueSourceDisposition::Progressed)],
            &check_recovery
        ),
        DueSourceDisposition::Degraded,
        "a retained CheckRun failure must remain visible in the aggregate"
    );

    for error in [
        crate::error::Error::new(
            "CHECK_LAUNCH_IDENTITY_UNKNOWN",
            "process identity remains uncertain",
        ),
        crate::error::Error::new("STORE_ERROR", "storage failure remains fatal"),
    ] {
        assert_eq!(
            super::check_recovery_failure(error.clone())
                .unwrap_err()
                .code,
            error.code
        );
    }
}

#[test]
fn pending_source_outcome_exposes_exact_subject_evidence_without_degrading() {
    use super::super::automation_reconcile::QuarantineEvidence;
    use super::DueSourceEvidence;

    let outcome = DueSourceOutcome {
        kind: "interval_schedule",
        disposition: DueSourceDisposition::Idle,
        cursor_advanced: false,
        damaged_subjects: Vec::new(),
        pending_subjects: vec![DueSourceEvidence {
            kind: "interval_schedule",
            code: "SCHEDULE_REGISTRY_FULL".to_owned(),
            evidence: QuarantineEvidence {
                subject_identity: "schedule_id:interval-a".to_owned(),
                source_pointer: Some("config/schedules/interval-a".to_owned()),
                source_digest: Some("a".repeat(64)),
            },
            source_key: None,
            source_raw: None,
        }],
    }
    .value();

    assert_eq!(outcome["disposition"], "idle");
    assert_eq!(outcome["cursor_advanced"], false);
    assert_eq!(
        outcome["pending_subjects"][0]["code"],
        "SCHEDULE_REGISTRY_FULL"
    );
    assert_eq!(
        outcome["pending_subjects"][0]["subject_identity"],
        "schedule_id:interval-a"
    );
    assert_eq!(
        outcome["pending_subjects"][0]["source_pointer"],
        "config/schedules/interval-a"
    );
    assert_eq!(
        outcome["pending_subjects"][0]["source_digest"]
            .as_str()
            .unwrap()
            .len(),
        64
    );
}
