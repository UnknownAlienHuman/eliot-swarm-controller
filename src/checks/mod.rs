//! Fixed-input command execution. The same binary's transient worker outlives a
//! host connection, but never creates agent sessions or accesses the database.
mod job;
pub mod model;
pub mod source;
pub mod worker;
