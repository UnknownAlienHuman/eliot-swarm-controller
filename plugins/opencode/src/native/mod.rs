pub(crate) mod auth;
pub(crate) mod backends;
pub(crate) mod client;
pub(crate) mod requests;
pub(crate) mod server;
pub(crate) mod sse;
pub(crate) mod types;

pub(crate) use auth::{
    AuthSetup, AuthTransfer, IntegrationSelector, ensure_no_credential_for_provider,
    list_providers, transfer_selected_integration,
};
pub(crate) use backends::BackendKind;
pub(crate) use client::{HttpOutcome, OpenCodeHttp};
pub(crate) use server::{LocalProvider, OwnedServer, ServerLaunch};
