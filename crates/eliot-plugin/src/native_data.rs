//! Vendor data can grow without a Swarm upgrade. Ignore extra fields, map
//! new enum values to an explicit `Other`, and report those values and new
//! event kinds as unclassified native facts. Keep processing the stream.
//! An unclassified fact carries no lifecycle, authority or outcome evidence.
//! Preserve framing and exact binding checks at their owning boundaries.

pub use eliot_core::UNCLASSIFIED_NATIVE_EVENT;
use eliot_core::is_native_discriminator;
use serde_json::{Value, json};
/// Preserve a discriminator, never the vendor payload. Omit free-form text
/// and oversized tags. Known secrets are also filtered at daemon intake.
pub fn unclassified(site: &'static str, native_tag: &str) -> Value {
    let native_tag = is_native_discriminator(native_tag).then_some(native_tag);
    json!({ "site": site, "native_tag": native_tag })
}
