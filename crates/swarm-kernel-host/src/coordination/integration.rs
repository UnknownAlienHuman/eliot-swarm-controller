//! Typed, revisioned integration-cell inputs and deterministic compatibility.
//!
//! These are advisory coordination facts. A compatible comparison never
//! accepts a Task, changes an assignment, or creates a runtime effect.

use crate::{
    error::{Error, Result},
    model,
};
use serde_json::{Map, Value, json};
use std::collections::BTreeSet;
use swarm_contracts::coordination_limits as limits;

pub const MAX_ASSUMPTIONS: usize = 20;
pub const MAX_REQUIRED_DIMENSIONS: usize = 16;
pub const MAX_OVERLAP_TERMS: usize = 24;
pub const MAX_OVERLAP_RESULTS: usize = 20;

pub const INTEGRATION_DIMENSIONS: &[&str] = &[
    "version",
    "producer",
    "consumer",
    "carrier",
    "contract",
    "inputs",
    "outputs",
    "serialization",
    "ownership",
    "availability",
    "limits",
    "result_disposition",
    "retry_semantics",
    "canonical_sources",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CellState {
    Open,
    Compatible,
    Mismatch,
    Unknown,
    Superseded,
}

impl CellState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Open => "open",
            Self::Compatible => "compatible",
            Self::Mismatch => "mismatch",
            Self::Unknown => "unknown",
            Self::Superseded => "superseded",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DimensionState {
    Match,
    Mismatch,
    Unknown,
    NotApplicable,
}

impl DimensionState {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Match => "match",
            Self::Mismatch => "mismatch",
            Self::Unknown => "unknown",
            Self::NotApplicable => "not_applicable",
        }
    }
}

#[derive(Debug, Clone)]
pub struct Offer {
    pub readiness: String,
    pub will_be_available_at: Value,
    pub candidate_ref: Option<String>,
    pub assumptions: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct Requirement {
    pub consumer_path: Option<String>,
    pub consumer_symbol: Option<String>,
    pub required_dimensions: Value,
    pub must_be_ready_before: String,
    pub assumptions: Vec<String>,
}

#[derive(Debug, Clone)]
pub enum Side {
    Offer(Offer),
    Requirement(Requirement),
}

#[derive(Debug, Clone)]
pub struct SyncRequest {
    pub client_request_id: String,
    pub contract_key: String,
    pub side: Side,
}

/// One authenticated participant's factual position on one exact retained
/// integration-cell revision. Principal identity and participation basis are
/// derived by Store and are never accepted from the request.
#[derive(Debug, Clone)]
pub struct AckRequest {
    pub client_request_id: String,
    pub cell_id: String,
    pub expected_state_revision: i64,
    pub expected_material_digest: String,
    pub expected_membership_digest: String,
    pub task_id: String,
    pub task_revision: i64,
    pub attempt_id: String,
    pub decision: AckDecision,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckDecision {
    Accept,
    Dissent,
}

impl AckDecision {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Accept => "accept",
            Self::Dissent => "dissent",
        }
    }
}

/// Read one exact integration cell and a bounded page of its retained
/// participant positions. Omitted revision/digest selects the current cell
/// version; historical reads must name both.
#[derive(Debug, Clone)]
pub struct AgreementGetRequest {
    pub cell_id: String,
    pub task_id: String,
    pub task_revision: i64,
    pub attempt_id: String,
    pub state_revision: Option<i64>,
    pub material_digest: Option<String>,
    pub limit: i64,
    pub after_position_revision: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct OverlapRequest {
    pub task_id: Option<String>,
    pub task_revision: Option<i64>,
    pub attempt_id: Option<String>,
    pub paths: Vec<String>,
    pub symbols: Vec<String>,
    pub contracts: Vec<String>,
    pub candidate_ref: Option<String>,
}

#[derive(Debug, Clone)]
pub struct DimensionComparison {
    pub dimension: String,
    pub state: DimensionState,
    pub producer_value: Option<Value>,
    pub required_value: Value,
}

#[derive(Debug, Clone)]
pub struct Comparison {
    pub status: String,
    pub dimensions: Vec<DimensionComparison>,
    pub unknown_dimensions: Vec<String>,
    pub mismatches: Vec<String>,
}

impl SyncRequest {
    pub fn parse(value: &Value) -> Result<Self> {
        model::fields(
            value,
            &["client_request_id", "contract_key", "offer", "requirement"],
        )?;
        let client_request_id = bounded_text(value, "client_request_id", 128, true)?;
        let contract_key = bounded_text(value, "contract_key", 256, true)?;
        let offer = value.get("offer").filter(|item| !item.is_null());
        let requirement = value.get("requirement").filter(|item| !item.is_null());
        let side = match (offer, requirement) {
            (Some(offer), None) => Side::Offer(parse_offer(offer)?),
            (None, Some(requirement)) => Side::Requirement(parse_requirement(requirement)?),
            _ => {
                return Err(Error::invalid(
                    "provide exactly one non-null offer or requirement",
                ));
            }
        };
        Ok(Self {
            client_request_id,
            contract_key,
            side,
        })
    }
}

impl AckRequest {
    pub fn parse(value: &Value) -> Result<Self> {
        model::fields(
            value,
            &[
                "client_request_id",
                "cell_id",
                "expected_state_revision",
                "expected_material_digest",
                "expected_membership_digest",
                "task_id",
                "task_revision",
                "attempt_id",
                "decision",
            ],
        )?;
        let client_request_id = bounded_text(
            value,
            "client_request_id",
            limits::MAX_CLIENT_REQUEST_ID_BYTES,
            true,
        )?;
        let cell_id = bounded_text(value, "cell_id", 64, true)?;
        require_hex_digest(&cell_id, "cell_id")?;
        let expected_state_revision = model::positive(value, "expected_state_revision")?;
        let expected_material_digest = bounded_text(value, "expected_material_digest", 64, true)?;
        require_hex_digest(&expected_material_digest, "expected_material_digest")?;
        let expected_membership_digest =
            bounded_text(value, "expected_membership_digest", 64, true)?;
        require_hex_digest(&expected_membership_digest, "expected_membership_digest")?;
        let task_id = bounded_text(value, "task_id", limits::MAX_IDENTIFIER_BYTES, true)?;
        let task_revision = model::positive(value, "task_revision")?;
        let attempt_id = bounded_text(value, "attempt_id", limits::MAX_IDENTIFIER_BYTES, true)?;
        let decision = match model::text(value, "decision")? {
            "accept" => AckDecision::Accept,
            "dissent" => AckDecision::Dissent,
            _ => return Err(Error::invalid("decision must be accept or dissent")),
        };
        Ok(Self {
            client_request_id,
            cell_id,
            expected_state_revision,
            expected_material_digest,
            expected_membership_digest,
            task_id,
            task_revision,
            attempt_id,
            decision,
        })
    }
}

impl AgreementGetRequest {
    pub fn parse(value: &Value) -> Result<Self> {
        model::fields(
            value,
            &[
                "cell_id",
                "task_id",
                "task_revision",
                "attempt_id",
                "state_revision",
                "material_digest",
                "limit",
                "after_position_id",
            ],
        )?;
        let cell_id = bounded_text(value, "cell_id", 64, true)?;
        require_hex_digest(&cell_id, "cell_id")?;
        let task_id = bounded_text(value, "task_id", limits::MAX_IDENTIFIER_BYTES, true)?;
        let task_revision = model::positive(value, "task_revision")?;
        let attempt_id = bounded_text(value, "attempt_id", limits::MAX_IDENTIFIER_BYTES, true)?;
        let state_revision = optional_positive(value, "state_revision")?;
        let material_digest = match value.get("material_digest") {
            None | Some(Value::Null) => None,
            Some(_) => {
                let digest = bounded_text(value, "material_digest", 64, true)?;
                require_hex_digest(&digest, "material_digest")?;
                Some(digest)
            }
        };
        if state_revision.is_some() != material_digest.is_some() {
            return Err(Error::invalid(
                "state_revision and material_digest must be supplied together",
            ));
        }
        let limit = match value.get("limit") {
            None | Some(Value::Null) => limits::DEFAULT_READ_PAGE_SIZE,
            Some(Value::Number(number)) => number
                .as_i64()
                .ok_or_else(|| Error::invalid("limit must be an integer"))?,
            Some(_) => return Err(Error::invalid("limit must be an integer")),
        };
        if !(1..=limits::MAX_READ_PAGE_SIZE).contains(&limit) {
            return Err(Error::invalid(format!(
                "limit must be 1..={}",
                limits::MAX_READ_PAGE_SIZE
            )));
        }
        let after_position_revision = match value.get("after_position_id") {
            None | Some(Value::Null) => None,
            Some(Value::String(id)) => Some(parse_position_id(id)?),
            Some(_) => {
                return Err(Error::invalid(
                    "after_position_id must be p followed by 20 decimal digits or null",
                ));
            }
        };
        Ok(Self {
            cell_id,
            task_id,
            task_revision,
            attempt_id,
            state_revision,
            material_digest,
            limit,
            after_position_revision,
        })
    }
}

fn require_hex_digest(value: &str, field: &str) -> Result<()> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(Error::invalid(format!(
            "{field} must be 64 lowercase hexadecimal characters"
        )));
    }
    Ok(())
}

fn parse_position_id(value: &str) -> Result<i64> {
    let Some(revision) = value.strip_prefix('p') else {
        return Err(Error::invalid(
            "after_position_id must be p followed by 20 decimal digits",
        ));
    };
    if revision.len() != 20 || !revision.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(Error::invalid(
            "after_position_id must be p followed by 20 decimal digits",
        ));
    }
    let parsed = revision
        .parse::<i64>()
        .map_err(|_| Error::invalid("after_position_id revision is out of range"))?;
    if parsed <= 0 {
        return Err(Error::invalid(
            "after_position_id revision must be positive",
        ));
    }
    Ok(parsed)
}

impl OverlapRequest {
    pub fn parse(value: &Value) -> Result<Self> {
        model::fields(
            value,
            &[
                "task_id",
                "task_revision",
                "attempt_id",
                "paths",
                "symbols",
                "contracts",
                "candidate_ref",
            ],
        )?;
        let (task_id, task_revision, attempt_id) = match (
            optional_id(value, "task_id", 256)?,
            optional_positive(value, "task_revision")?,
            optional_id(value, "attempt_id", 256)?,
        ) {
            (None, None, None) => (None, None, None),
            (Some(task_id), Some(task_revision), Some(attempt_id)) => {
                (Some(task_id), Some(task_revision), Some(attempt_id))
            }
            _ => {
                return Err(Error::invalid(
                    "task_id, task_revision, and attempt_id must be supplied together",
                ));
            }
        };
        let paths = string_array(value, "paths", 24, 1024, false, true)?;
        let symbols = string_array(value, "symbols", 24, 1024, true, false)?;
        let contracts = string_array(value, "contracts", 24, 256, true, false)?;
        let total = paths.len() + symbols.len() + contracts.len();
        if total > MAX_OVERLAP_TERMS {
            return Err(Error::invalid(format!(
                "paths, symbols, and contracts may contain at most {MAX_OVERLAP_TERMS} total selectors"
            )));
        }
        let candidate_ref = match value.get("candidate_ref") {
            None | Some(Value::Null) => None,
            Some(Value::String(reference))
                if !reference.trim().is_empty()
                    && reference.len() <= 128
                    && !reference.bytes().any(|byte| byte.is_ascii_control()) =>
            {
                Some(reference.clone())
            }
            Some(_) => {
                return Err(Error::invalid(
                    "candidate_ref must be nonempty text up to 128 bytes or null",
                ));
            }
        };
        if total == 0 && candidate_ref.is_none() {
            return Err(Error::invalid(
                "supply at least one exact path, symbol, contract, or candidate_ref",
            ));
        }
        Ok(Self {
            task_id,
            task_revision,
            attempt_id,
            paths,
            symbols,
            contracts,
            candidate_ref,
        })
    }
}

fn parse_offer(value: &Value) -> Result<Offer> {
    model::fields(
        value,
        &[
            "readiness",
            "will_be_available_at",
            "candidate_ref",
            "assumptions",
        ],
    )?;
    let readiness = bounded_text(value, "readiness", 32, true)?;
    if !["draft", "implementation_ready", "observed"].contains(&readiness.as_str()) {
        return Err(Error::invalid("unsupported offer readiness"));
    }
    let availability = value
        .get("will_be_available_at")
        .ok_or_else(|| Error::invalid("will_be_available_at is required"))?;
    model::fields(availability, &["path", "symbol"])?;
    let path = optional_text(availability, "path", 1024, false)?;
    let symbol = optional_text(availability, "symbol", 1024, true)?;
    if path.is_none() && symbol.is_none() {
        return Err(Error::invalid(
            "will_be_available_at requires a path or symbol",
        ));
    }
    let mut will_be_available_at = Map::new();
    if let Some(path) = path {
        will_be_available_at.insert("path".into(), json!(path));
    }
    if let Some(symbol) = symbol {
        will_be_available_at.insert("symbol".into(), json!(symbol));
    }
    let candidate_ref = match value.get("candidate_ref") {
        None | Some(Value::Null) => None,
        Some(Value::String(reference))
            if !reference.trim().is_empty()
                && reference.len() <= 128
                && !reference.bytes().any(|byte| byte.is_ascii_control()) =>
        {
            Some(reference.clone())
        }
        Some(_) => {
            return Err(Error::invalid(
                "candidate_ref must be nonempty text up to 128 bytes or null",
            ));
        }
    };
    let assumptions = string_array(value, "assumptions", MAX_ASSUMPTIONS, 512, false, false)?;
    Ok(Offer {
        readiness,
        will_be_available_at: Value::Object(will_be_available_at),
        candidate_ref,
        assumptions,
    })
}

fn parse_requirement(value: &Value) -> Result<Requirement> {
    model::fields(
        value,
        &[
            "consumer_path",
            "consumer_symbol",
            "required_dimensions",
            "must_be_ready_before",
            "assumptions",
        ],
    )?;
    let consumer_path = optional_text(value, "consumer_path", 1024, false)?;
    let consumer_symbol = optional_text(value, "consumer_symbol", 1024, true)?;
    if consumer_path.is_none() && consumer_symbol.is_none() {
        return Err(Error::invalid(
            "consumer_path or consumer_symbol is required",
        ));
    }
    let required_dimensions = value
        .get("required_dimensions")
        .ok_or_else(|| Error::invalid("required_dimensions is required"))?;
    let dimensions = required_dimensions
        .as_object()
        .ok_or_else(|| Error::invalid("required_dimensions must be an object"))?;
    if dimensions.is_empty() || dimensions.len() > MAX_REQUIRED_DIMENSIONS {
        return Err(Error::invalid(format!(
            "required_dimensions must contain 1..={MAX_REQUIRED_DIMENSIONS} dimensions"
        )));
    }
    for (name, value) in dimensions {
        if !INTEGRATION_DIMENSIONS.contains(&name.as_str()) || value.is_null() {
            return Err(Error::invalid(format!(
                "required_dimensions contains unsupported or null dimension {name}"
            )));
        }
    }
    if model::canonical(required_dimensions)?.len() > 8192 {
        return Err(Error::invalid(
            "required_dimensions exceeds the 8192-byte limit",
        ));
    }
    let must_be_ready_before = bounded_text(value, "must_be_ready_before", 256, false)?;
    let assumptions = string_array(value, "assumptions", MAX_ASSUMPTIONS, 512, false, false)?;
    Ok(Requirement {
        consumer_path,
        consumer_symbol,
        required_dimensions: required_dimensions.clone(),
        must_be_ready_before,
        assumptions,
    })
}

fn optional_text(
    value: &Value,
    field: &str,
    max_bytes: usize,
    no_whitespace: bool,
) -> Result<Option<String>> {
    match value.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(text))
            if !text.trim().is_empty()
                && text.len() <= max_bytes
                && !text.bytes().any(|byte| {
                    byte.is_ascii_control() || (no_whitespace && byte.is_ascii_whitespace())
                }) =>
        {
            if field.ends_with("path") && !valid_relative_path(text) {
                return Err(Error::invalid(format!(
                    "{field} must be a literal relative path"
                )));
            }
            Ok(Some(text.clone()))
        }
        Some(_) => Err(Error::invalid(format!(
            "{field} must be nonempty text up to {max_bytes} bytes or null"
        ))),
    }
}

fn bounded_text(
    value: &Value,
    field: &str,
    max_bytes: usize,
    no_whitespace: bool,
) -> Result<String> {
    let text = model::text(value, field)?;
    if text.len() > max_bytes
        || text
            .bytes()
            .any(|byte| byte.is_ascii_control() || (no_whitespace && byte.is_ascii_whitespace()))
    {
        return Err(Error::invalid(format!(
            "{field} must be 1..={max_bytes} bytes{}",
            if no_whitespace {
                " without whitespace"
            } else {
                ""
            }
        )));
    }
    Ok(text.to_owned())
}

fn optional_id(value: &Value, field: &str, max_bytes: usize) -> Result<Option<String>> {
    match value.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(_) => bounded_text(value, field, max_bytes, true).map(Some),
    }
}

fn optional_positive(value: &Value, field: &str) -> Result<Option<i64>> {
    match value.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(_) => model::positive(value, field).map(Some),
    }
}

fn string_array(
    value: &Value,
    field: &str,
    max_items: usize,
    max_bytes: usize,
    no_whitespace: bool,
    paths: bool,
) -> Result<Vec<String>> {
    let Some(raw) = value.get(field) else {
        return Ok(Vec::new());
    };
    let items = raw
        .as_array()
        .ok_or_else(|| Error::invalid(format!("{field} must be an array")))?;
    if items.len() > max_items {
        return Err(Error::invalid(format!(
            "{field} may contain at most {max_items} items"
        )));
    }
    let mut seen = BTreeSet::new();
    let mut output = Vec::with_capacity(items.len());
    for item in items {
        let text = item
            .as_str()
            .filter(|text| {
                !text.trim().is_empty()
                    && text.len() <= max_bytes
                    && !text.bytes().any(|byte| {
                        byte.is_ascii_control() || (no_whitespace && byte.is_ascii_whitespace())
                    })
            })
            .ok_or_else(|| {
                Error::invalid(format!(
                    "{field} entries must be nonempty strings up to {max_bytes} bytes"
                ))
            })?;
        if paths && !valid_relative_path(text) {
            return Err(Error::invalid(format!(
                "{field} entries must be literal relative paths"
            )));
        }
        if !seen.insert(text) {
            return Err(Error::invalid(format!("{field} entries must be unique")));
        }
        output.push(text.to_owned());
    }
    Ok(output)
}

fn valid_relative_path(path: &str) -> bool {
    !path.is_empty()
        && !path.starts_with('/')
        && !path.starts_with('\\')
        && !path.contains('\\')
        && !path
            .split('/')
            .any(|component| component.is_empty() || component == "." || component == "..")
}

/// Compare only dimensions explicitly required by the consumer. Missing
/// producer fields remain unknown; they are never treated as a match.
pub fn compare_dimensions(
    producer_fields: &Value,
    required_dimensions: &Value,
) -> Result<Comparison> {
    let requirements = required_dimensions
        .as_object()
        .ok_or_else(|| Error::invalid("required_dimensions must be an object"))?;
    let mut dimensions = Vec::with_capacity(requirements.len());
    let mut unknown_dimensions = Vec::new();
    let mut mismatches = Vec::new();
    for (dimension, required_value) in requirements {
        if !INTEGRATION_DIMENSIONS.contains(&dimension.as_str()) || required_value.is_null() {
            return Err(Error::invalid(format!(
                "unsupported required integration dimension {dimension}"
            )));
        }
        let producer_value = producer_fields
            .get(dimension)
            .filter(|value| !value.is_null())
            .cloned();
        let state = match producer_value.as_ref() {
            None => {
                unknown_dimensions.push(dimension.clone());
                DimensionState::Unknown
            }
            Some(actual) if actual == required_value => DimensionState::Match,
            Some(_) => {
                mismatches.push(dimension.clone());
                DimensionState::Mismatch
            }
        };
        dimensions.push(DimensionComparison {
            dimension: dimension.clone(),
            state,
            producer_value,
            required_value: required_value.clone(),
        });
    }
    let status = if !mismatches.is_empty() {
        "mismatch"
    } else if !unknown_dimensions.is_empty() {
        "unknown"
    } else {
        "compatible"
    };
    Ok(Comparison {
        status: status.to_owned(),
        dimensions,
        unknown_dimensions,
        mismatches,
    })
}

impl Comparison {
    pub fn to_value(&self) -> Value {
        json!({
            "status":self.status,
            "dimensions":self.dimensions.iter().map(|dimension| json!({
                "dimension":dimension.dimension,
                "state":dimension.state.as_str(),
                "producer_value":dimension.producer_value,
                "required_value":dimension.required_value,
            })).collect::<Vec<_>>(),
            "unknown_dimensions":self.unknown_dimensions,
            "mismatches":self.mismatches,
        })
    }
}
