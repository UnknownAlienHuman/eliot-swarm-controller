//! Internal bounded pages over the actual 2.0.7 non-following SSE log.
//! A page is partial only after observing an additional durable event. A
//! complete page requires a scoped sync watermark; EOF alone proves nothing.
use super::*;

type WireValidator = std::sync::OnceLock<std::result::Result<jsonschema::Validator, ()>>;

fn validate_wire(value: &Value, definition: &str, cached: &WireValidator) -> Result<()> {
    let validator = cached.get_or_init(|| {
        let mut schema: Value = serde_json::from_str(include_str!(
            "../../registration/native-durable-2.0.7.schema.json"
        ))
        .map_err(|_| ())?;
        schema["$ref"] = json!(format!("#/$defs/{definition}"));
        jsonschema::validator_for(&schema).map_err(|_| ())
    });
    match validator {
        Ok(validator) if validator.is_valid(value) => Ok(()),
        // Do not include validation errors: they can contain native payloads.
        _ => Err(Error::new(
            "NATIVE_HISTORY_SCHEMA",
            "native value does not match its pinned wire schema",
        )),
    }
}

pub(super) fn validate_durable_event(value: &Value) -> Result<()> {
    static VALIDATOR: WireValidator = std::sync::OnceLock::new();
    validate_wire(value, "Session.Event.Durable", &VALIDATOR)
}

pub(super) fn log_sync_matches(value: &Value, session: &str, last: u64) -> bool {
    exact_fields(value, &["type", "aggregateID", "seq"], &[])
        && value["type"] == "log.synced"
        && value["aggregateID"] == session
        // The native session reader filters the aggregate bus. Other durable
        // event types occupy sequence numbers, including after the last
        // visible session event. SDK Bus.log captures the aggregate watermark.
        && value["seq"].as_u64().is_some_and(|sequence| sequence >= last && sequence <= 9_007_199_254_740_991)
}

impl NativeClient {
    pub(crate) fn validate_form_info(value: &Value) -> Result<()> {
        static VALIDATOR: WireValidator = std::sync::OnceLock::new();
        validate_wire(value, "Form.Info", &VALIDATOR)
    }

    pub(crate) fn validate_form_detail(value: &Value) -> Result<()> {
        static VALIDATOR: WireValidator = std::sync::OnceLock::new();
        validate_wire(value, "Form.Detail", &VALIDATOR)
    }

    pub(crate) async fn session_log_page(
        &self,
        session: &str,
        after: u64,
        limit: usize,
    ) -> Result<Value> {
        valid_id(session, "ses_")?;
        if limit == 0 || limit > MAX_LOG_EVENTS || after > 9_007_199_254_740_991 {
            return Err(Error::new(
                "NATIVE_HISTORY_LIMIT",
                "native log page bounds are invalid",
            ));
        }
        let mut url = self
            .endpoint
            .join(&format!("/api/experimental/session/{session}/log"))
            .map_err(|_| Error::new("NATIVE_ENDPOINT", "invalid native log route"))?;
        url.query_pairs_mut()
            .append_pair("after", &after.to_string())
            .append_pair("follow", "false");
        let response = self
            .client
            .get(url)
            .header(header::ACCEPT, "text/event-stream")
            .timeout(REQUEST_TIMEOUT)
            .send()
            .await
            .map_err(|_| {
                Error::new(
                    "NATIVE_HISTORY_UNAVAILABLE",
                    "native durable log request failed",
                )
            })?;
        if !response.status().is_success()
            || response
                .headers()
                .get(header::CONTENT_TYPE)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.split(';').next())
                != Some("text/event-stream")
        {
            return Err(Error::new(
                "NATIVE_HISTORY_UNAVAILABLE",
                "native durable log is not the documented SSE response",
            ));
        }
        let mut received_bytes = 0usize;
        let events = response
            .bytes_stream()
            .map(move |chunk| {
                let chunk = chunk.map_err(|_| std::io::Error::other("native log transport"))?;
                received_bytes = received_bytes.saturating_add(chunk.len());
                if received_bytes > MAX_LOG_BYTES {
                    return Err(std::io::Error::other("native log byte bound"));
                }
                Ok(chunk)
            })
            .eventsource();
        futures_util::pin_mut!(events);
        let mut data = Vec::with_capacity(limit.min(100));
        let mut last = after;
        let mut ids = BTreeSet::new();
        let mut synced = false;
        while let Some(item) = events.next().await {
            let item = item.map_err(|_| {
                Error::new(
                    "NATIVE_HISTORY_INCOMPLETE",
                    "native log stream is incomplete or exceeds its byte bound",
                )
            })?;
            if synced || item.event != "message" {
                return Err(Error::new(
                    "NATIVE_HISTORY_INCOMPLETE",
                    "native log continued after sync or reported failure",
                ));
            }
            let value: Value = serde_json::from_str(&item.data)
                .map_err(|_| Error::new("NATIVE_HISTORY_SCHEMA", "native log item is not JSON"))?;
            if value["type"] == "log.synced" {
                if !log_sync_matches(&value, session, last) {
                    return Err(Error::new(
                        "NATIVE_HISTORY_GAP",
                        "native log watermark does not cover this exact page",
                    ));
                }
                synced = true;
                continue;
            }
            // Every event occupying a sequence must be a real SDK Durable
            // member, including events this reader does not otherwise consume.
            validate_durable_event(&value)?;
            let id = required_text(&value, "id")?;
            valid_id(id, "evt_")?;
            let sequence = value["durable"]["seq"]
                .as_u64()
                .filter(|sequence| *sequence <= 9_007_199_254_740_991)
                .ok_or_else(|| {
                    Error::new("NATIVE_HISTORY_SCHEMA", "native log sequence is invalid")
                })?;
            if value["durable"]["aggregateID"] != session
                || value["data"]["sessionID"] != session
                || value["durable"]["version"]
                    .as_u64()
                    .is_none_or(|version| version == 0)
                || value["created"]
                    .as_f64()
                    .is_none_or(|time| !time.is_finite() || time < 0.0)
                || required_text(&value, "type")?.len() > 128
            {
                return Err(Error::new(
                    "NATIVE_HISTORY_SCOPE",
                    "native log event differs from the exact session envelope",
                ));
            }
            // Session.log filters other durable buses sharing this aggregate;
            // gaps are native semantics. Regressions and duplicate IDs are not.
            if sequence <= last || !ids.insert(id.to_owned()) {
                return Err(Error::new(
                    "NATIVE_HISTORY_GAP",
                    "native log page regressed or repeated an event identity",
                ));
            }
            if data.len() == limit {
                return Ok(json!({"data":data,"hasMore":true}));
            }
            last = sequence;
            data.push(value);
        }
        if !synced {
            return Err(Error::new(
                "NATIVE_HISTORY_INCOMPLETE",
                "native log ended without its exact sync watermark",
            ));
        }
        Ok(json!({"data":data,"hasMore":false}))
    }
}
