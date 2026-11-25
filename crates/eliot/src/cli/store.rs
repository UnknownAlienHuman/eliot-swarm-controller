//! Offline owner operations on the private local Store.
use super::args::{StoreArgs, StoreCommand};
use super::support::{Failure, print_value_or_human};
use std::path::PathBuf;

pub(crate) fn run_store(
    StoreArgs { command }: StoreArgs,
    data_dir: PathBuf,
    json_output: bool,
) -> Result<u8, Failure> {
    match command {
        StoreCommand::RetentionPreview(args) => {
            let request = eliot_core::api::RetentionPreviewRequest {
                events_days: args.events_days,
                events_seconds: args.events_seconds,
                processes_seconds: args.processes_seconds,
                commands_days: args.commands_days,
                mail_days: args.mail_days,
            };
            let preview = eliot_daemon::operator::preview_store_retention(&data_dir, &request)
                .map_err(Failure::for_read)?;
            let value = serde_json::to_value(&preview)
                .map_err(|_| Failure::new(1, "retention preview could not be encoded"))?;
            print_value_or_human(&value, json_output, || {
                println!("Retention preview at {} ms (snapshot)", preview.as_of_ms);
                for row in &preview.rows {
                    let oldest = row
                        .oldest_kept_ms
                        .map_or_else(|| "-".to_owned(), |ms| ms.to_string());
                    println!(
                        "{} / {}  delete {}  keep {}  oldest kept {} ms",
                        row.table, row.class, row.would_delete, row.kept, oldest
                    );
                    for (reason, count) in &row.protected {
                        println!("  {reason}  {count}");
                    }
                }
                println!(
                    "Event cursor floor {} -> {}",
                    preview.current_pruned_through, preview.projected_pruned_through
                );
            });
            Ok(0)
        }
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
            let value = serde_json::to_value(&outcome)
                .map_err(|_| Failure::new(1, "store compaction response is invalid"))?;
            print_value_or_human(&value, json_output, || {
                println!("Store compaction complete.");
                println!("Converted: {}", outcome.converted);
                println!("Pages reclaimed: {}", outcome.pages_reclaimed);
                println!("Freelist pages remaining: {}", outcome.freelist_pages);
                if let Some(estimate) = &outcome.estimate {
                    println!(
                        "Disk preflight: database {} bytes, WAL {} bytes, estimated free space required {} bytes, available {} bytes",
                        estimate.main_database_bytes,
                        estimate.wal_bytes,
                        estimate.estimated_required_bytes,
                        estimate.available_bytes
                    );
                }
            });
            Ok(0)
        }
    }
}
