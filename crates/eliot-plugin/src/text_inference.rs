//! Composition binding for the native translator used by one-shot inference.

use eliot_core::{Error, Result};
use std::{ffi::OsString, path::Path, sync::OnceLock};

pub struct TextResult {
    pub text: String,
    pub session_id: String,
}

#[derive(Clone, Copy)]
pub struct TextInferenceAdapter {
    pub arguments: fn() -> Vec<String>,
    pub environment: fn(&Path) -> Vec<(&'static str, OsString)>,
    pub result: fn(&[u8], usize) -> Result<TextResult>,
}

static ADAPTER: OnceLock<TextInferenceAdapter> = OnceLock::new();

pub fn register(adapter: TextInferenceAdapter) -> Result<()> {
    ADAPTER
        .set(adapter)
        .map_err(|_| Error::new("internal", "text inference adapter already registered"))
}

pub fn configured() -> Result<TextInferenceAdapter> {
    ADAPTER
        .get()
        .copied()
        .ok_or_else(|| Error::new("unsupported", "text inference adapter was not registered"))
}
