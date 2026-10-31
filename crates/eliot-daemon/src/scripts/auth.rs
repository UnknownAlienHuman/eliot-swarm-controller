use crate::common::secure_eq;
use eliot_core::{Error, Result, api::ProjectRegistrationView};
use eliot_plugin::sync::LockExt;
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    fmt,
    sync::{Arc, Mutex, Weak},
};

/// Host-derived authority retained for a Run after its caller credential expires.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum SavedRunPrincipal {
    Owner,
    ProjectRule {
        registration: Box<ProjectRegistrationView>,
    },
}

type Entries = HashMap<String, RunCredentialEntry>;

struct RunCredentialEntry {
    token_hash: String,
    saved_principal: SavedRunPrincipal,
}

/// Process-local Run credential registry. A fresh registry has no credentials.
#[derive(Clone, Default)]
pub(crate) struct RunCredentialRegistry {
    entries: Arc<Mutex<Entries>>,
}

impl RunCredentialRegistry {
    /// Register one command-bound bearer token and return its only plaintext holder.
    pub(crate) fn issue(
        &self,
        run_command_id: impl Into<String>,
        saved_principal: SavedRunPrincipal,
    ) -> Result<RunCredentialGuard> {
        let run_command_id = run_command_id.into();
        if run_command_id.trim().is_empty() || run_command_id.contains('\0') {
            return Err(Error::new("invalid", "run command id is invalid"));
        }

        let token = random_256_bit_token();
        let token_hash = sha256_hex(token.as_bytes());
        let mut entries = self.entries.lock_recover();
        if entries.contains_key(&run_command_id) {
            return Err(Error::new(
                "bad_state",
                "run credential is already registered for this command",
            ));
        }
        entries.insert(
            run_command_id.clone(),
            RunCredentialEntry {
                token_hash: token_hash.clone(),
                saved_principal,
            },
        );
        drop(entries);

        Ok(RunCredentialGuard {
            entries: Arc::downgrade(&self.entries),
            run_command_id,
            token,
            token_hash,
            revoked: false,
        })
    }

    /// Resolve a Run token only for its own command, using constant-time digest comparison.
    pub(crate) fn authenticate(
        &self,
        run_command_id: &str,
        token: &str,
    ) -> Option<SavedRunPrincipal> {
        if token.is_empty() {
            return None;
        }
        let presented_hash = sha256_hex(token.as_bytes());
        let entries = self.entries.lock_recover();
        let entry = entries.get(run_command_id)?;
        secure_eq(&entry.token_hash, &presented_hash).then(|| entry.saved_principal.clone())
    }
}

/// Owns the plaintext token until process exit or cleanup; Drop revokes its map entry.
pub(crate) struct RunCredentialGuard {
    entries: Weak<Mutex<Entries>>,
    run_command_id: String,
    token: String,
    token_hash: String,
    revoked: bool,
}

impl RunCredentialGuard {
    /// Borrow the token for the child environment. It is empty after explicit revocation.
    pub(crate) fn token(&self) -> &str {
        &self.token
    }

    /// Revoke before output admission. Repeated calls and Drop are safe.
    pub(crate) fn revoke(&mut self) {
        if self.revoked {
            return;
        }
        if let Some(entries) = self.entries.upgrade() {
            let mut entries = entries.lock_recover();
            let is_ours = entries
                .get(&self.run_command_id)
                .is_some_and(|entry| secure_eq(&entry.token_hash, &self.token_hash));
            if is_ours {
                entries.remove(&self.run_command_id);
            }
        }
        self.revoked = true;
        self.token.clear();
    }
}

impl Drop for RunCredentialGuard {
    fn drop(&mut self) {
        self.revoke();
    }
}

impl fmt::Debug for RunCredentialGuard {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RunCredentialGuard")
            .field("token", &"[REDACTED]")
            .field("active", &!self.revoked)
            .finish()
    }
}

fn random_256_bit_token() -> String {
    let mut bytes = [0_u8; 32];
    for chunk in bytes.chunks_mut(10) {
        // Ulid::new uses rand's thread CSPRNG; its low 80 bits are random, independent
        // of the timestamp field. Four fields provide 320 random bits; retain exactly 256.
        let random = ulid::Ulid::new().random().to_be_bytes();
        let random_bytes = &random[6..];
        let take = chunk.len();
        chunk.copy_from_slice(&random_bytes[..take]);
    }
    lower_hex(&bytes)
}

fn sha256_hex(bytes: &[u8]) -> String {
    lower_hex(&Sha256::digest(bytes))
}

fn lower_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    fn project_principal() -> SavedRunPrincipal {
        SavedRunPrincipal::ProjectRule {
            registration: Box::new(ProjectRegistrationView {
                id: "sample".to_owned(),
                checkout: "C:\\work\\sample".to_owned(),
                repo_common_dir: "C:\\work".to_owned(),
                profiles: Vec::new(),
                actions: vec!["run".to_owned()],
                interpreters: Default::default(),
                run_env: Vec::new(),
                git: None,
                revision: 4,
            }),
        }
    }
    #[test]
    fn token_is_256_bits_and_only_its_sha256_digest_is_registered() {
        let registry = RunCredentialRegistry::default();
        let guard = registry
            .issue("run-1", project_principal())
            .expect("register Run credential");
        assert!(guard.token().len() == 64);
        assert!(
            guard
                .token()
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        );

        let entries = registry.entries.lock_recover();
        let entry = entries.get("run-1").expect("registered Run entry");
        assert!(secure_eq(
            &entry.token_hash,
            &sha256_hex(guard.token().as_bytes())
        ));
        assert!(!secure_eq(&entry.token_hash, guard.token()));
    }

    #[test]
    fn authentication_is_bound_to_command_and_guard_drop_revokes() {
        let registry = RunCredentialRegistry::default();
        let guard = registry
            .issue("run-1", project_principal())
            .expect("register Run credential");
        let token = guard.token().to_owned();

        assert_eq!(
            registry.authenticate("run-1", &token),
            Some(project_principal())
        );
        assert!(registry.authenticate("run-2", &token).is_none());
        assert!(registry.authenticate("run-1", "wrong-token").is_none());
        drop(guard);
        assert!(registry.authenticate("run-1", &token).is_none());
    }

    #[test]
    fn explicit_revocation_clears_token_and_debug_redacts_it() {
        let registry = RunCredentialRegistry::default();
        let mut guard = registry
            .issue("run-1", project_principal())
            .expect("register Run credential");
        let token = guard.token().to_owned();
        let debug = format!("{guard:?}");
        assert!(!debug.contains(&token));
        assert!(debug.contains("[REDACTED]"));

        guard.revoke();
        assert!(guard.token().is_empty());
        assert!(registry.authenticate("run-1", &token).is_none());
        guard.revoke();
    }

    #[test]
    fn authentication_returns_the_saved_project_principal() {
        let saved = project_principal();
        let registry = RunCredentialRegistry::default();
        let guard = registry
            .issue("run-project", saved.clone())
            .expect("register project Run credential");

        assert_eq!(
            registry.authenticate("run-project", guard.token()),
            Some(saved)
        );
    }
    #[test]
    fn fresh_registry_has_no_prior_credentials_and_duplicate_issue_fails_closed() {
        let registry = RunCredentialRegistry::default();
        let guard = registry
            .issue("run-1", project_principal())
            .expect("register Run credential");
        let token = guard.token().to_owned();
        let restarted = RunCredentialRegistry::default();

        assert!(restarted.authenticate("run-1", &token).is_none());
        assert!(registry.issue("run-1", project_principal()).is_err());
        assert!(registry.authenticate("run-1", &token).is_some());
    }
}
