//! Unified settings use the daemon's canonical request and response validators.
use std::io::Read;
use std::path::PathBuf;

use eliot_core::api::{
    SettingsClientSource, SettingsReadRequest, SettingsReadResponse, SettingsSaveRequest,
    SettingsSaveResponse,
};

use super::args::{SettingsArgs, SettingsCommand};
use super::support::{Failure, print_value};

pub(crate) async fn run(
    args: SettingsArgs,
    data_dir: PathBuf,
    json_output: bool,
) -> Result<u8, Failure> {
    let value = match args.command {
        SettingsCommand::Read => {
            let request = SettingsReadRequest::default();
            request.validate().map_err(Failure::for_read)?;
            let reply: SettingsReadResponse =
                eliot_daemon::client_call_typed(&data_dir, "settings.read", &request)
                    .await
                    .map_err(Failure::for_read)?;
            request
                .validate_response(&reply)
                .map_err(Failure::invalid_response)?;
            serde_json::to_value(reply)
        }
        SettingsCommand::Save { file, dry_run } => {
            let mut bytes = Vec::new();
            std::fs::File::open(&file)
                .and_then(|f| f.take(128 * 1024 + 1).read_to_end(&mut bytes))
                .map_err(|e| Failure::new(2, format!("read settings request: {e}")))?;
            if bytes.len() > 128 * 1024 {
                return Err(Failure::new(2, "settings request exceeds 128 KiB"));
            }
            let mut request: SettingsSaveRequest = serde_json::from_slice(&bytes)
                .map_err(|_| Failure::new(2, "invalid settings.save JSON request"))?;
            request.client = SettingsClientSource::Cli;
            request.dry_run |= dry_run;
            request.validate().map_err(Failure::for_mutation)?;
            let reply: SettingsSaveResponse =
                eliot_daemon::client_call_typed(&data_dir, "settings.save", &request)
                    .await
                    .map_err(Failure::for_mutation)?;
            request
                .validate_response(&reply)
                .map_err(Failure::invalid_response)?;
            serde_json::to_value(reply)
        }
    }
    .map_err(|_| Failure::new(4, "invalid settings response"))?;
    print_value(&value, json_output);
    Ok(0)
}
