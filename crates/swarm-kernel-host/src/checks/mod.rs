//! Fixed-input command execution. The same binary's transient worker outlives a
//! host connection, but never creates agent sessions or accesses the database.
pub mod inputs;
pub mod model;
pub mod scope;
pub mod source;
pub(crate) mod standalone_host;
pub mod worker;
