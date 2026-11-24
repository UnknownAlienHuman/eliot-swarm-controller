//! Owner-mediated native harness login requests.
use eliot_core::api::auth_login::{AuthLoginRequest, AuthLoginResponse};
use std::path::PathBuf;

use super::support::{Failure, print_value_or_human};

pub(crate) async fn login(
    harness: String,
    data_dir: PathBuf,
    json_output: bool,
) -> Result<u8, Failure> {
    let request = AuthLoginRequest {
        harness,
        revision: None,
    };
    let response: AuthLoginResponse =
        eliot_daemon::client_call_typed(&data_dir, "auth.login", &request)
            .await
            .map_err(Failure::for_mutation)?;
    let value = serde_json::to_value(&response)
        .map_err(|_| Failure::new(4, "auth.login response is invalid"))?;
    print_value_or_human(&value, json_output, || {
        println!("{}", response.status.as_str());
    });
    Ok(0)
}
