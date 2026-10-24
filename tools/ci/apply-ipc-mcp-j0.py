from pathlib import Path

path = Path("crates/swarm-mcp/src/mcp/mod.rs")
text = path.read_text(encoding="utf-8")
old = '''async fn request_on(
    slot: &Mutex<Option<Client>>,
    root: &std::path::Path,
    credential: &Credential,
    ipc_config: &Ipc,
    method: &str,
    params: Value,
) -> Result<Value> {
    let mut client = slot.lock().await;
    if client.is_none() {
        *client = Some(Client::connect(root, credential, ipc_config).await?);
    }
    let connected = client.as_mut().expect("client connected above");
    match connected.request(method, params).await {
        Ok(result) => Ok(result),
        Err(e) => {
            if matches!(
                e.code.as_str(),
                "DISCONNECTED"
                    | "OUTCOME_UNKNOWN"
                    | "PROTOCOL_ERROR"
                    | "IO_ERROR"
                    | "WRITE_TIMEOUT"
            ) {
                *client = None;
            }
            Err(e)
        }
    }
}
'''
new = '''async fn request_on(
    slot: &Mutex<Option<Client>>,
    root: &std::path::Path,
    credential: &Credential,
    ipc_config: &Ipc,
    method: &str,
    params: Value,
) -> Result<Value> {
    let connected = {
        let mut client = slot.lock().await;
        if client.as_ref().is_none_or(|client| !client.is_connected()) {
            *client = Some(Client::connect(root, credential, ipc_config).await?);
        }
        client.as_ref().expect("client connected above").clone()
    };

    let result = connected.request_shared(method, params).await;
    if let Err(error) = &result
        && matches!(
            error.code.as_str(),
            "DISCONNECTED"
                | "OUTCOME_UNKNOWN"
                | "PROTOCOL_ERROR"
                | "IO_ERROR"
                | "WRITE_TIMEOUT"
        )
        && !connected.is_connected()
    {
        // A late failure from an older Client must not erase a healthy
        // replacement installed by another concurrent request. Clearing a
        // disconnected current slot is sufficient: the next request reconnects
        // before sending, while the failed request is never replayed.
        let mut client = slot.lock().await;
        if client
            .as_ref()
            .is_some_and(|current| !current.is_connected())
        {
            *client = None;
        }
    }
    result
}
'''
count = text.count(old)
if count != 1:
    raise SystemExit(f"expected one exact request_on body, found {count}")
path.write_text(text.replace(old, new), encoding="utf-8")
