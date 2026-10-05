use serde_json::{Value, json};
use swarm_client::Client;
use swarm_contracts::{Error, Result};

/// Perform one read-only call to the existing `check.get` method. The supplied
/// authenticated client keeps its ordinary Store credential and visibility;
/// this helper neither reconnects nor retries automatically.
pub async fn read_check_status(client: &mut Client, check_id: &str) -> Result<Value> {
    uuid::Uuid::parse_str(check_id).map_err(|_| Error::invalid("invalid CheckRun ID"))?;
    let status = client
        .request("check.get", json!({"check_id":check_id}))
        .await?;
    if status["check_id"].as_str() != Some(check_id) {
        return Err(Error::conflict(
            "check.get returned a different CheckRun identity",
        ));
    }
    Ok(status)
}
