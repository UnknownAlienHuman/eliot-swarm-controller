//! Store errors use the shared core error contract.

pub use eliot_core::{Error, Result};

/// Compatibility name for callers that prefer a Store-specific error name.
pub type StoreError = Error;
