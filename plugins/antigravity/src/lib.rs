#![forbid(unsafe_code)]

mod events;
mod hooks;
mod host;
mod native;
mod profile;
mod quota;
mod session;
#[cfg(test)]
pub(crate) mod test_temp;

pub async fn run() -> eliot_core::Result<()> {
    host::run().await
}

pub async fn run_hook_helper() -> eliot_core::Result<()> {
    hooks::run_helper().await
}
