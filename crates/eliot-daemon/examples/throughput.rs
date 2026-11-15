//! Real IPC measurement, never an executor or a synthetic Store fixture.
//! cargo run -p eliot-daemon --example throughput -- <data-dir> replay <event-limit>
//! cargo run -p eliot-daemon --example throughput -- <data-dir> ingress <seconds> <workers>
//! cargo run -p eliot-daemon --example throughput -- <data-dir> call <method> '<params-json>'
//! Ingress writes ordinary project variables, then counts the committed journal
//! rows for this run. Native work can run concurrently, but is reported separately.
use eliot_core::{Error, Result, api::EventsReadResponse};
use eliot_daemon::{CallTimings, client_call_measured};
use serde::Serialize;
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    path::Path,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{task::JoinSet, time::Instant};
use ulid::Ulid;

#[derive(Default, Serialize)]
struct Samples {
    calls: usize,
    connect_ns: u64,
    round_trip_ns: u64,
    decode_ns: u64,
    consumer_ns: u64,
    client_total_ns: u64,
    handle_ns: u64,
    missing_handle: usize,
    #[serde(skip)]
    latency: Vec<u64>,
    #[serde(skip)]
    handle: Vec<u64>,
}

impl Samples {
    fn add(&mut self, timing: CallTimings, elapsed: Duration) {
        self.calls += 1;
        self.client_total_ns += elapsed.as_nanos() as u64;
        self.connect_ns += timing.connect_ns;
        self.round_trip_ns += timing.round_trip_ns;
        self.decode_ns += timing.decode_ns;
        self.handle_ns += timing.handle_ns.unwrap_or_default();
        self.missing_handle += usize::from(timing.handle_ns.is_none());
        self.latency.push(elapsed.as_nanos() as u64);
        if let Some(ns) = timing.handle_ns {
            self.handle.push(ns);
        }
    }

    fn merge(&mut self, other: Self) {
        self.calls += other.calls;
        self.connect_ns += other.connect_ns;
        self.round_trip_ns += other.round_trip_ns;
        self.decode_ns += other.decode_ns;
        self.consumer_ns += other.consumer_ns;
        self.client_total_ns += other.client_total_ns;
        self.handle_ns += other.handle_ns;
        self.missing_handle += other.missing_handle;
        self.latency.extend(other.latency);
        self.handle.extend(other.handle);
    }

    fn report(mut self) -> Value {
        self.latency.sort_unstable();
        self.handle.sort_unstable();
        let percentile = |samples: &[u64], percent: usize| {
            samples
                .get((samples.len() * percent).div_ceil(100).saturating_sub(1))
                .copied()
                .map(|ns| ns as f64 / 1e6)
        };
        json!({"latency_scope":"successful_calls_before_typed_decode", "totals": self, "rpc_ms_p50": percentile(&self.latency, 50),
            "rpc_ms_p95": percentile(&self.latency, 95),
            "handle_ms_p50": percentile(&self.handle, 50),
            "handle_ms_p95": percentile(&self.handle, 95)})
    }
}

async fn bounds(path: &Path) -> Result<i64> {
    let (value, _) = client_call_measured(path, "events.bounds", json!({})).await?;
    let bounds: eliot_core::api::EventsBoundsResponse =
        serde_json::from_value(value).map_err(|_| Error::new("unknown", "invalid bounds"))?;
    bounds.validate()?;
    Ok(bounds.hi)
}

async fn replay(
    path: &Path,
    after: i64,
    hi: i64,
    limit: usize,
    prefix: Option<&str>,
) -> Result<Value> {
    let started = Instant::now();
    let mut cursor = after;
    let mut events = 0;
    let mut own = 0;
    let mut samples = Samples::default();
    let mut consumer_ns = 0_u64;
    let mut kinds = BTreeMap::<String, usize>::new();
    let mut seconds = BTreeMap::<i64, usize>::new();
    while cursor < hi && events < limit {
        let call_started = Instant::now();
        let (value, timing) = client_call_measured(
            path,
            "events.read",
            json!({
                "after": cursor, "limit": (limit - events).min(1000)
            }),
        )
        .await?;
        samples.add(timing, call_started.elapsed());
        let consume_started = Instant::now();
        let page: EventsReadResponse = serde_json::from_value(value)
            .map_err(|_| Error::new("unknown", "invalid events page"))?;
        page.validate(cursor)?;
        if page.next <= cursor {
            return Err(Error::new(
                "unknown",
                "journal did not advance to captured high water",
            ));
        }
        for event in page.events.into_iter().take_while(|event| event.seq <= hi) {
            events += 1;
            *kinds.entry(event.kind.clone()).or_default() += 1;
            if prefix.is_some_and(|prefix| {
                event.kind == "var.changed"
                    && event
                        .data
                        .get("key")
                        .and_then(Value::as_str)
                        .is_some_and(|key| key.starts_with(prefix))
            }) {
                own += 1;
            }
            *seconds.entry(event.ts / 1000).or_default() += 1;
        }
        cursor = page.next.min(hi);
        consumer_ns += consume_started.elapsed().as_nanos() as u64;
    }
    let elapsed = started.elapsed().as_secs_f64();
    Ok(
        json!({"measurement": "replay_read", "events": events, "from_exclusive": after,
        "captured_hi": hi, "next": cursor, "reached_hi": cursor == hi,
        "elapsed_s": elapsed, "events_per_s": events as f64 / elapsed,
        "consumer_typed_decode_validate_count_ns": consumer_ns,
        "timings": samples.report(), "journal_kinds": kinds,
        "journal_events_per_epoch_second": seconds, "own_committed_events": own}),
    )
}

async fn ingress(path: &Path, seconds: u64, workers: usize) -> Result<Value> {
    if !(1..=300).contains(&seconds) || !(1..=128).contains(&workers) {
        return Err(Error::new(
            "invalid",
            "seconds must be 1..300; workers 1..128",
        ));
    }
    let prefix = format!("throughput-{}-", Ulid::new());
    let before = bounds(path).await?;
    let started_at_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| Error::new("internal", "clock precedes epoch"))?
        .as_millis();
    let started = Instant::now();
    let until = started + Duration::from_secs(seconds);
    let mut tasks = JoinSet::new();
    for worker in 0..workers {
        let path = path.to_owned();
        let key = format!("{prefix}{worker}");
        tasks.spawn(async move {
            let mut samples = Samples::default();
            let mut errors = BTreeMap::<String, usize>::new();
            let mut counter = 0_u64;
            while Instant::now() < until {
                let request = eliot_core::api::VarSetRequest {
                    id: Ulid::new().to_string(),
                    scope: Some("project:throughput".to_owned()),
                    key: key.clone(),
                    value: json!(counter),
                    expect_version: None,
                    note: None,
                };
                let call_started = Instant::now();
                let params = serde_json::to_value(&request).expect("variable request serializes");
                match client_call_measured(&path, "var.set", params).await {
                    Ok((value, timing)) => {
                        let call_elapsed = call_started.elapsed();
                        let consume_started = Instant::now();
                        let validated =
                            serde_json::from_value::<eliot_core::api::VarSetResponse>(value)
                                .map_err(|_| Error::new("unknown", "invalid variable reply"))
                                .and_then(|response| request.validate_response(&response));
                        samples.consumer_ns += consume_started.elapsed().as_nanos() as u64;
                        if let Err(error) = validated {
                            *errors.entry(error.code).or_default() += 1;
                            break;
                        }
                        samples.add(timing, call_elapsed);
                    }
                    Err(error) => {
                        *errors.entry(error.code.clone()).or_default() += 1;
                        // Never retry an uncertain write under a fresh identity.
                        if error.code == "unknown" {
                            break;
                        }
                    }
                }
                counter += 1;
            }
            (samples, errors)
        });
    }
    let mut samples = Samples::default();
    let mut errors = BTreeMap::<String, usize>::new();
    while let Some(result) = tasks.join_next().await {
        let (sample, failures) =
            result.map_err(|_| Error::new("internal", "measurement worker failed"))?;
        samples.merge(sample);
        for (code, count) in failures {
            *errors.entry(code).or_default() += count;
        }
    }
    let after = bounds(path).await?;
    let elapsed = started.elapsed().as_secs_f64();
    let finished_at_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| Error::new("internal", "clock precedes epoch"))?
        .as_millis();
    let journal = replay(path, before, after, usize::MAX, Some(&prefix)).await?;
    let committed = journal["own_committed_events"].as_u64().unwrap_or_default();
    Ok(
        json!({"measurement": "committed_ingress", "operation": "var.set",
        "workers": workers, "elapsed_s": elapsed, "acknowledged": samples.calls,
        "started_at_ms": started_at_ms, "finished_at_ms": finished_at_ms,
        "journal_confirmed_own_events": committed, "events_per_s": committed as f64 / elapsed,
        "ack_journal_counts_match": committed == samples.calls as u64,
        "errors": errors, "timings": samples.report(), "journal": journal}),
    )
}

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let usage = || {
        Error::new(
            "invalid",
            "usage: throughput <data-dir> replay <limit> | ingress <seconds> <workers> | call <method> <params-json>",
        )
    };
    let path = Path::new(args.get(1).ok_or_else(usage)?);
    let result = match args.get(2).map(String::as_str) {
        Some("replay") => {
            let limit = args
                .get(3)
                .ok_or_else(usage)?
                .parse()
                .map_err(|_| usage())?;
            replay(path, 0, bounds(path).await?, limit, None).await?
        }
        Some("ingress") => {
            ingress(
                path,
                args.get(3)
                    .ok_or_else(usage)?
                    .parse()
                    .map_err(|_| usage())?,
                args.get(4)
                    .ok_or_else(usage)?
                    .parse()
                    .map_err(|_| usage())?,
            )
            .await?
        }
        Some("call") => {
            let (value, timing) = client_call_measured(
                path,
                args.get(3).ok_or_else(usage)?,
                serde_json::from_str(args.get(4).ok_or_else(usage)?).map_err(|_| usage())?,
            )
            .await?;
            json!({"result": value, "timing": timing})
        }
        _ => return Err(usage()),
    };
    println!(
        "{}",
        serde_json::to_string(&result)
            .map_err(|_| Error::new("internal", "encode measurements"))?
    );
    Ok(())
}
