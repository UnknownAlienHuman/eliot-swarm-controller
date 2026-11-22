//! Offline owner operations on the private local Store.
use super::args::{StoreArgs, StoreCommand};
use super::support::{Failure, print_value};
use std::path::PathBuf;

pub(crate) fn run_store(
    StoreArgs { command }: StoreArgs,
    data_dir: PathBuf,
    json_output: bool,
) -> Result<u8, Failure> {
    match command {
        StoreCommand::Compact => {
            let outcome = eliot_daemon::compact_store_offline(&data_dir, |estimate| {
                eprintln!(
                    "Store compaction requires an estimated {} bytes (twice the DB and WAL combined). {} bytes are available (DB {} bytes, WAL {} bytes).",
                    estimate.estimated_required_bytes,
                    estimate.available_bytes,
                    estimate.main_database_bytes,
                    estimate.wal_bytes
                );
            })
            .map_err(Failure::for_read)?;
            let value = serde_json::to_value(outcome)
                .map_err(|_| Failure::new(1, "store compaction response is invalid"))?;
            print_value(&value, json_output);
            Ok(0)
        }
    }
}
