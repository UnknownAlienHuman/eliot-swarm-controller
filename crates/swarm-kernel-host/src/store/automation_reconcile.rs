//! Store-private subject isolation primitives for automation consumers.

use crate::automation::config::{self, AutomationEntry};
use crate::{
    error::{Error, Result},
    model,
};
use rusqlite::Transaction;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

const SUBJECT_SAVEPOINT: &str = "automation_subject";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct QuarantineEvidence {
    pub(super) subject_identity: String,
    pub(super) source_pointer: Option<String>,
    pub(super) source_digest: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct QuarantinedSubject {
    pub(super) code: String,
    pub(super) evidence: QuarantineEvidence,
    pub(super) first_seen_at_ms: i64,
    pub(super) last_seen_at_ms: i64,
    pub(super) occurrences: u32,
}

#[derive(Debug)]
pub(super) struct MalformedAutomationEntry {
    pub(super) code: String,
    pub(super) evidence: QuarantineEvidence,
}

#[derive(Debug)]
pub(super) enum SubjectDisposition<T> {
    Applied(T),
    Pending {
        code: String,
        reason: String,
    },
    Skipped {
        code: String,
        reason: String,
    },
    Quarantined {
        code: String,
        evidence: QuarantineEvidence,
    },
}

#[derive(Debug)]
pub(super) enum SubjectErrorDisposition {
    Pending {
        code: String,
        reason: String,
    },
    Skipped {
        code: String,
        reason: String,
    },
    Quarantined {
        code: String,
        evidence: QuarantineEvidence,
    },
}

#[derive(Debug)]
pub(super) enum DomainErrorDisposition {
    Degraded { code: String },
    Fatal(Error),
}

/// Decode a retained automation record while keeping immutable record damage
/// distinct from storage and serializer failures.
pub(super) fn parse_automation_entry(raw: &str, label: &str) -> Result<AutomationEntry> {
    let sealed: Value = serde_json::from_str(raw).map_err(|_| {
        Error::new(
            "AUTOMATION_RECORD_CORRUPT",
            "stored automation record JSON is invalid",
        )
    })?;
    let value = config::open_record(sealed, label)?;
    let entry: AutomationEntry = serde_json::from_value(value).map_err(|_| {
        Error::new(
            "AUTOMATION_RECORD_INVALID",
            "stored automation entry fields are invalid",
        )
    })?;
    config::validate_entry(&entry)?;
    Ok(entry)
}

pub(super) fn automation_entry_evidence(key: &str, raw: &str) -> QuarantineEvidence {
    let key_digest = model::digest(key.as_bytes());
    QuarantineEvidence {
        subject_identity: format!("meta-key-sha256:{key_digest}"),
        source_pointer: Some(format!("meta/key-sha256:{key_digest}")),
        source_digest: Some(model::digest(raw.as_bytes())),
    }
}

/// Run exactly one retained subject behind a savepoint. The domain supplies
/// its own exact error mapping; an unrecognized error remains fatal.
pub(super) fn with_subject_savepoint<T>(
    tx: &Transaction<'_>,
    run: impl FnOnce() -> Result<SubjectDisposition<T>>,
    classify: impl FnOnce(&Error) -> Option<SubjectErrorDisposition>,
) -> Result<SubjectDisposition<T>> {
    tx.execute_batch(&format!("SAVEPOINT {SUBJECT_SAVEPOINT}"))?;
    match run() {
        Ok(SubjectDisposition::Applied(value)) => {
            tx.execute_batch(&format!("RELEASE SAVEPOINT {SUBJECT_SAVEPOINT}"))?;
            Ok(SubjectDisposition::Applied(value))
        }
        Ok(disposition) => {
            let primary = disposition_error(&disposition);
            rollback_subject(tx, SUBJECT_SAVEPOINT, &primary)?;
            Ok(disposition)
        }
        Err(error) => {
            let disposition = classify(&error);
            rollback_subject(tx, SUBJECT_SAVEPOINT, &error)?;
            match disposition {
                Some(SubjectErrorDisposition::Pending { code, reason }) => {
                    Ok(SubjectDisposition::Pending { code, reason })
                }
                Some(SubjectErrorDisposition::Skipped { code, reason }) => {
                    Ok(SubjectDisposition::Skipped { code, reason })
                }
                Some(SubjectErrorDisposition::Quarantined { code, evidence }) => {
                    Ok(SubjectDisposition::Quarantined { code, evidence })
                }
                None => Err(error),
            }
        }
    }
}

fn disposition_error<T>(disposition: &SubjectDisposition<T>) -> Error {
    let code = match disposition {
        SubjectDisposition::Applied(_) => "AUTOMATION_SUBJECT_APPLIED",
        SubjectDisposition::Pending { code, .. }
        | SubjectDisposition::Skipped { code, .. }
        | SubjectDisposition::Quarantined { code, .. } => code.as_str(),
    };
    Error::new(
        code.to_owned(),
        "subject savepoint is being rolled back for a non-applied disposition",
    )
}

fn rollback_subject(tx: &Transaction<'_>, savepoint: &str, primary: &Error) -> Result<()> {
    let rollback_sql = format!("ROLLBACK TO SAVEPOINT {savepoint}; RELEASE SAVEPOINT {savepoint}");
    if let Err(rollback_error) = tx.execute_batch(&rollback_sql) {
        return Err(Error::new(
            "AUTOMATION_SUBJECT_SAVEPOINT_UNCERTAIN",
            "subject rollback or savepoint release failed; abort the domain transaction",
        )
        .with_secondary_error(primary.clone())
        .with_secondary_error(rollback_error.into()));
    }
    Ok(())
}

/// Derive a bounded metadata key from the exact identity and evidence digest.
pub(super) fn quarantine_record_key(prefix: &str, evidence: &QuarantineEvidence) -> Result<String> {
    if prefix.is_empty() || prefix.len() > 128 || prefix.chars().any(char::is_control) {
        return Err(Error::new(
            "AUTOMATION_QUARANTINE_PREFIX_INVALID",
            "internal quarantine namespace is invalid",
        ));
    }
    let source_pointer = evidence.source_pointer.as_deref().unwrap_or_default();
    let source_digest = evidence.source_digest.as_deref().unwrap_or_default();
    let identity = format!(
        "{}\0{}\0{}",
        evidence.subject_identity, source_pointer, source_digest
    );
    Ok(format!("{prefix}{}", model::digest(identity.as_bytes())))
}

/// Persist immutable source evidence in the existing `meta` record store.
/// Repeated observations update only the bounded counter and last-seen time.
pub(super) fn persist_quarantine(
    tx: &Transaction<'_>,
    record_key: &str,
    code: &str,
    evidence: QuarantineEvidence,
    now_ms: i64,
) -> Result<QuarantinedSubject> {
    if !safe_code(code)
        || record_key.is_empty()
        || record_key.len() > 512
        || record_key.chars().any(char::is_control)
        || evidence.subject_identity.is_empty()
        || evidence.subject_identity.len() > 512
        || evidence.subject_identity.chars().any(char::is_control)
        || evidence.source_pointer.as_ref().is_some_and(|pointer| {
            pointer.is_empty() || pointer.len() > 512 || pointer.chars().any(char::is_control)
        })
        || evidence
            .source_digest
            .as_ref()
            .is_some_and(|digest| !is_digest(digest))
        || now_ms < 0
    {
        return Err(Error::new(
            "AUTOMATION_QUARANTINE_EVIDENCE_INVALID",
            "quarantine evidence identity, digest, key, code, or time is invalid",
        ));
    }

    let existing: Option<Value> = config::read_record(tx, record_key, "automation quarantine")?;
    let mut record = match existing {
        Some(value) => {
            let record: QuarantinedSubject = serde_json::from_value(value).map_err(|_| {
                Error::new(
                    "AUTOMATION_QUARANTINE_RECORD_CORRUPT",
                    "retained automation quarantine evidence is invalid",
                )
            })?;
            if record.code != code || record.evidence != evidence {
                return Err(Error::new(
                    "AUTOMATION_QUARANTINE_IDENTITY_CONFLICT",
                    "quarantine metadata key resolves to different subject evidence",
                ));
            }
            if record.first_seen_at_ms < 0
                || record.last_seen_at_ms < record.first_seen_at_ms
                || record.occurrences == 0
            {
                return Err(Error::new(
                    "AUTOMATION_QUARANTINE_RECORD_CORRUPT",
                    "retained automation quarantine bounds are invalid",
                ));
            }
            record
        }
        None => QuarantinedSubject {
            code: code.to_owned(),
            evidence,
            first_seen_at_ms: now_ms,
            last_seen_at_ms: now_ms,
            occurrences: 0,
        },
    };
    record.last_seen_at_ms = now_ms;
    record.occurrences = record.occurrences.saturating_add(1);
    config::write_record(tx, record_key, &json!(&record))?;
    Ok(record)
}

fn safe_code(code: &str) -> bool {
    !code.is_empty()
        && code.len() <= 64
        && code
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
}

fn is_digest(digest: &str) -> bool {
    digest.len() == 64 && digest.bytes().all(|byte| byte.is_ascii_hexdigit())
}
