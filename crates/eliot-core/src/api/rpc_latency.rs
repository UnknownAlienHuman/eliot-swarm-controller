//! Bounded process-local measurements for the daemon's local RPC transport.
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

pub const RPC_LATENCY_WINDOW_SECONDS: u64 = 300;
pub const RPC_LATENCY_SAMPLES_PER_METHOD: usize = 128;
pub const RPC_LATENCY_SLOW_CALLS: usize = 10;

#[derive(JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RpcLatencyReport {
    pub window_seconds: u64,
    pub samples_per_method: usize,
    pub methods: Vec<RpcMethodLatency>,
    pub slowest: Vec<SlowRpcCall>,
}

#[derive(JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RpcMethodLatency {
    pub method: String,
    pub count: usize,
    pub duration_us: RpcLatencyPercentiles,
    pub store_wait_us: RpcLatencyPercentiles,
}

#[derive(JsonSchema, Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RpcLatencyPercentiles {
    pub p50: u64,
    pub p95: u64,
    pub max: u64,
}

#[derive(JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SlowRpcCall {
    pub method: String,
    pub duration_us: u64,
}
