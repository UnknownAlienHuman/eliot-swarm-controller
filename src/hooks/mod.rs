//! Setup-issued, repository-scoped Git hook ingress.
//!
//! The callback is a local ELIOT wrapper around Git's observational
//! `post-commit` hook. It cannot veto a commit and never runs automation inline.

pub mod contract;
pub mod git;
