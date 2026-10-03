//! Bounded read-side contracts for the Swarm launcher surfaces.
//!
//! These types describe projections over existing Store authority. They do
//! not create Tasks, claim Attempts, inspect Git, or start native work.

use crate::{
    error::{Error, Result},
    model,
};
use serde_json::{Value, json};

/// Default rows for an interactive manager page.
pub const DEFAULT_PAGE_SIZE: i64 = 20;
/// Keep launcher pages smaller than the generic Store report ceiling.
pub const MAX_PAGE_SIZE: i64 = 50;
/// Dashboard embeds only a small preview from each underlying projection.
pub const DASHBOARD_PREVIEW_SIZE: i64 = 5;
/// Initial assignment context may include exact retained source text only
/// while the complete brief remains comfortably below a single page item.
pub const MAX_INLINE_BRIEF_BYTES: usize = 32_768;

/// Offset page contract shared by the launcher queue and exception reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PageRequest {
    pub after: i64,
    pub limit: i64,
}

impl PageRequest {
    pub fn parse(params: &Value) -> Result<Self> {
        let integer = |name: &str, default: i64| -> Result<i64> {
            match params.get(name) {
                None => Ok(default),
                Some(value) => value
                    .as_i64()
                    .ok_or_else(|| Error::invalid(format!("{name} must be an integer"))),
            }
        };
        let page = Self {
            after: integer("after", 0)?,
            limit: integer("limit", DEFAULT_PAGE_SIZE)?,
        };
        if !(1..=MAX_PAGE_SIZE).contains(&page.limit) || page.after < 0 {
            return Err(Error::invalid(format!(
                "limit must be 1..={MAX_PAGE_SIZE} and after nonnegative"
            )));
        }
        Ok(page)
    }
}

/// Return the exact source brief when it fits. Oversized briefs stay retained
/// in their authoritative Task/Attempt snapshot and are represented by a
/// digest-addressed reference instead of silent truncation.
pub(crate) fn brief_projection(
    brief: &Value,
    reference: Value,
    max_inline_bytes: usize,
) -> Result<Value> {
    if !brief.is_object() || brief["status"] == "unavailable" {
        return Ok(json!({
            "status": "unavailable",
            "brief": brief,
            "reference": reference,
        }));
    }
    let canonical = model::canonical(brief)?;
    if canonical.len() <= max_inline_bytes {
        Ok(json!({
            "status": "included",
            "serialized_bytes": canonical.len(),
            "brief": brief,
        }))
    } else {
        Ok(json!({
            "status": "detached",
            "serialized_bytes": canonical.len(),
            "digest": model::digest(canonical.as_bytes()),
            "reference": reference,
        }))
    }
}
