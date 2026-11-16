#![forbid(unsafe_code)]

mod events;
mod host;
pub mod inference;
mod native;
mod owner_scan;
#[cfg(test)]
pub(crate) mod test_temp;

pub async fn run_claude() -> eliot_core::Result<()> {
    host::run().await
}
