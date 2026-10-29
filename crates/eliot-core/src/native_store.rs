//! Harness-neutral effective native-store selection, verified before control.

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeStoreBinding {
    pub home: String,
    pub root: String,
    pub native_scope: String,
}
