//! Optional GitHub Issue intake and one bounded manually invoked managed-label effect.
//!
//! This module deliberately owns no GitHub credentials. The installed `gh`
//! CLI resolves its existing account session; every command is a fixed `api`
//! read built from validated repository coordinates and bounded page numbers.

pub mod client;
pub mod observer;
pub mod protocol;
pub mod work_pool;
