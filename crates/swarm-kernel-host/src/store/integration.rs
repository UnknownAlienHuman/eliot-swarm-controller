//! Exact-scope integration-cell and recorded-overlap projections.
//!
//! These records summarize current coordination facts only. They never alter
//! Task acceptance, assignment, workspace authority, or model scheduling.

use super::{meta, set_meta};
use crate::{
    config::Config,
    coordination::{self as keys, integration as wire},
    error::{Error, Result},
    model::{self, Principal, Role},
};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};

const MAX_CELL_MEMBERS: i64 = 50;
const MAX_CANONICAL_SOURCES: usize = 32;
const MAX_OVERLAP_MATCHES: usize = wire::MAX_OVERLAP_RESULTS;

#[derive(Clone)]
struct Member {
    client_id: String,
    participation_basis: Value,
    card: Value,
    role: Option<String>,
}

struct CellFacts {
    offers: Vec<Value>,
    requirements: Vec<Value>,
    carriers: Vec<Value>,
    comparisons: Vec<Value>,
    comparison_status: String,
    gaps: Vec<Value>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum CellFreshness {
    Current,
    Changed,
    Incomplete,
}

pub(super) fn read(
    db: &Connection,
    principal: &Principal,
    method: &str,
    value: &Value,
) -> Result<Value> {
    match method {
        "swarm.overlap.check" => overlap_check(db, principal, value),
        _ => Err(Error::new(
            "METHOD_NOT_FOUND",
            format!("{method} is not an implemented integration read"),
        )),
    }
}

pub(super) fn apply(
    tx: &Transaction<'_>,
    principal: &Principal,
    method: &str,
    value: &Value,
    _config: &Config,
    operation_id: &str,
    now: i64,
) -> Result<(Value, bool)> {
    match method {
        "coordination.sync_integration" => {
            sync_integration(tx, principal, value, operation_id, now).map(|result| (result, false))
        }
        _ => Err(Error::new(
            "METHOD_NOT_FOUND",
            format!("{method} is not an implemented integration mutation"),
        )),
    }
}

fn sync_integration(
    tx: &Transaction<'_>,
    principal: &Principal,
    value: &Value,
    operation_id: &str,
    now: i64,
) -> Result<Value> {
    principal.require_participant()?;
    let request = wire::SyncRequest::parse(value)?;
    let scope = super::coordination::current_scope(tx, principal)?;
    let scope_id = text_at(&scope, &["scope_id"], "current scope id")?;
    let task_id = text_at(&scope, &["task", "task_id"], "current Task id")?;
    let task_revision = int_at(&scope, &["task", "revision"], "current Task revision")?;
    let attempt_id = text_at(&scope, &["attempt", "attempt_id"], "current Attempt id")?;
    let attempt = scope
        .get("attempt")
        .filter(|attempt| attempt.is_object())
        .ok_or_else(|| Error::new("STORE_INVARIANT", "current scope lacks Attempt projection"))?;

    let page = super::coordination::list_current_contract_participants(
        tx,
        principal,
        &request.contract_key,
        MAX_CELL_MEMBERS,
    )?;
    if page["scope_id"] != scope_id
        || page["task_id"] != task_id
        || page["task_revision"] != task_revision
        || page["attempt_id"] != attempt_id
    {
        return Err(Error::new(
            "STALE_REVISION",
            "contract-card relevance page does not match the authenticated Task/Attempt scope",
        ));
    }
    let mut members = current_members(
        &page,
        task_id,
        task_revision,
        attempt_id,
        &request.contract_key,
    )?;
    let caller = members
        .iter()
        .find(|member| member.client_id == principal.client_id)
        .cloned()
        .ok_or_else(|| {
            Error::new(
                "NOT_FOUND",
                "publish a current contract card for contract_key before syncing integration",
            )
        })?;
    let current_basis = scope
        .pointer("/participant/participation_basis")
        .ok_or_else(|| {
            Error::new(
                "STORE_INVARIANT",
                "current scope lacks caller participation basis",
            )
        })?;
    if &caller.participation_basis != current_basis {
        return Err(Error::new(
            "STALE_PARTICIPANT",
            "contract helper basis does not match the authenticated current registration",
        ));
    }
    let caller_role = caller.role.as_deref().ok_or_else(|| {
        Error::invalid("current contract card must declare producer, consumer, or carrier role")
    })?;
    validate_side_role(&request.side, caller_role)?;
    members.sort_by(|left, right| left.client_id.cmp(&right.client_id));

    let member_basis_set = members
        .iter()
        .map(|member| {
            json!({
                "client_id":member.client_id,
                "participation_basis":member.participation_basis,
            })
        })
        .collect::<Vec<_>>();
    let member_identity_set = members
        .iter()
        .map(|member| {
            json!({
                "client_id":member.client_id,
                "participation_basis_kind":member.participation_basis["kind"],
            })
        })
        .collect::<Vec<_>>();
    let member_card_facts = members
        .iter()
        .map(|member| {
            json!({
                "client_id":member.client_id,
                "role":member.role,
                "card_revision":member.card["card_revision"],
                "card_material_digest":member.card["material_digest"],
            })
        })
        .collect::<Vec<_>>();
    let membership_digest = digest(&json!({
        "task_id":task_id,
        "task_revision":task_revision,
        "attempt_id":attempt_id,
        "contract_key":request.contract_key,
        "member_identity_set":member_identity_set,
    }))?;

    let contribution_key = contribution_key(scope_id, &request.contract_key, &principal.client_id);
    let prior = meta(tx, &contribution_key)?;
    let prior_is_current = prior.as_ref().is_some_and(|prior| {
        prior["scope_id"] == scope_id
            && prior["task_id"] == task_id
            && prior["task_revision"] == task_revision
            && prior["attempt_id"] == attempt_id
            && prior["participation_basis"] == caller.participation_basis
            && prior["card_revision"] == caller.card["card_revision"]
            && prior["card_material_digest"] == caller.card["material_digest"]
            && prior["role"] == caller_role
    });
    let (offer, requirement) = match &request.side {
        wire::Side::Offer(offer) => (
            offer_value(offer),
            preserved_side(&prior, prior_is_current, "requirement"),
        ),
        wire::Side::Requirement(requirement) => (
            preserved_side(&prior, prior_is_current, "offer"),
            requirement_value(requirement),
        ),
    };
    let side_material = json!({"offer":offer,"requirement":requirement});
    let contribution_digest = digest(&json!({
        "task_id":task_id,
        "task_revision":task_revision,
        "attempt_id":attempt_id,
        "contract_key":request.contract_key,
        "client_id":principal.client_id,
        "card_revision":caller.card["card_revision"],
        "card_material_digest":caller.card["material_digest"],
        "role":caller_role,
        "side":side_material,
    }))?;
    let contribution = json!({
        "schema":"eliot.integration.contribution.v1",
        "scope_id":scope_id,
        "task_id":task_id,
        "task_revision":task_revision,
        "attempt_id":attempt_id,
        "contract_key":request.contract_key,
        "client_id":principal.client_id,
        "participation_basis":caller.participation_basis,
        "role":caller_role,
        "card_revision":caller.card["card_revision"],
        "card_material_digest":caller.card["material_digest"],
        "offer":offer,
        "requirement":requirement,
        "material_digest":contribution_digest,
        "client_request_id":request.client_request_id,
        "operation_id":operation_id,
        "updated_at_ms":now,
    });
    set_meta(tx, &contribution_key, &contribution)?;

    let mut coverage = page
        .get("coverage")
        .and_then(Value::as_str)
        .unwrap_or("partial")
        .to_owned();
    let mut gaps = page
        .get("gaps")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    if coverage != "complete" && gaps.is_empty() {
        gaps.push(json!({"kind":"contract_participant_relevance_incomplete"}));
    }
    let cell_facts =
        collect_cell_facts(tx, &members, scope_id, &request.contract_key, operation_id)?;
    gaps.extend(cell_facts.gaps.clone());
    let canonical_sources = canonical_sources(&members, &mut gaps);
    if gaps
        .iter()
        .any(|gap| gap["kind"] == "canonical_source_projection_bound")
    {
        coverage = "partial".to_owned();
    }
    let state = if cell_facts.comparison_status == "mismatch" {
        wire::CellState::Mismatch
    } else if coverage != "complete" || cell_facts.comparison_status == "unknown" {
        wire::CellState::Unknown
    } else if cell_facts.comparison_status == "open" {
        wire::CellState::Open
    } else {
        wire::CellState::Compatible
    };
    let assumptions = cell_assumptions(&cell_facts.offers, &cell_facts.requirements);
    let facts = json!({
        "scope_id":scope_id,
        "task_id":task_id,
        "task_revision_set":[{"task_id":task_id,"task_revision":task_revision,"attempt_id":attempt_id}],
        "attempt_id":attempt_id,
        "contract_key":request.contract_key,
        "membership_digest":membership_digest,
        "member_basis_set":member_basis_set,
        "member_card_facts":member_card_facts,
        "offers":cell_facts.offers,
        "requirements":cell_facts.requirements,
        "carrier_constraints":cell_facts.carriers,
        "comparisons":cell_facts.comparisons,
        "comparison_status":cell_facts.comparison_status,
        "canonical_sources":canonical_sources,
        "assumptions":assumptions,
        "coverage":coverage,
        "gaps":gaps,
    });
    let material = json!({
        "membership_digest":membership_digest,
        "member_card_facts":member_card_facts,
        "offers":material_contributions(&cell_facts.offers, "offer"),
        "requirements":material_contributions(&cell_facts.requirements, "requirement"),
        "carrier_constraints":material_carriers(&cell_facts.carriers),
        "comparisons":cell_facts.comparisons,
        "comparison_status":cell_facts.comparison_status,
        "canonical_sources":canonical_sources,
        "assumptions":assumptions,
        "coverage":coverage,
        "gaps":gaps,
    });
    let material_digest = digest(&material)?;
    let current_index_key = current_cell_index_key(scope_id, &request.contract_key);
    let previous_pointer = meta(tx, &current_index_key)?;
    let previous_id = previous_pointer
        .as_ref()
        .and_then(|pointer| pointer.get("cell_id"))
        .and_then(Value::as_str);
    let previous_cell_key =
        previous_id.map(|cell_id| cell_record_key(scope_id, &request.contract_key, cell_id));
    let previous_cell = previous_cell_key
        .as_deref()
        .map(|key| meta(tx, key))
        .transpose()?
        .flatten();
    let membership_changed = previous_cell
        .as_ref()
        .is_some_and(|cell| cell.get("membership_digest") != Some(&json!(membership_digest)));
    let cell_id = if membership_changed || previous_id.is_none() {
        digest(&json!({
            "scope_id":scope_id,
            "contract_key":request.contract_key,
            "membership_digest":membership_digest,
            "previous_cell_id":if membership_changed { previous_id } else { None },
        }))?
    } else {
        previous_id.unwrap_or_default().to_owned()
    };
    if membership_changed
        && let (Some(old_key), Some(mut old_cell)) =
            (previous_cell_key.as_deref(), previous_cell.clone())
    {
        let old_revision = old_cell["state_revision"].as_i64().unwrap_or(1);
        let old_digest = old_cell["material_digest"].clone();
        old_cell["state"] = json!(wire::CellState::Superseded.as_str());
        old_cell["state_revision"] = json!(old_revision.saturating_add(1));
        old_cell["superseded_by_cell_id"] = json!(cell_id);
        old_cell["updated_at_ms"] = json!(now);
        old_cell["operation_id"] = json!(operation_id);
        old_cell["updated_by_client_id"] = json!(principal.client_id);
        old_cell["previous_material_digest"] = old_digest;
        old_cell["material_digest"] = json!(digest(&json!({
            "prior_material_digest":old_cell["previous_material_digest"],
            "state":"superseded",
            "superseded_by_cell_id":cell_id,
            "state_revision":old_cell["state_revision"],
        }))?);
        set_meta(tx, old_key, &old_cell)?;
    }
    let cell_record_key = cell_record_key(scope_id, &request.contract_key, &cell_id);
    let prior_cell = if membership_changed {
        None
    } else {
        previous_cell
    };
    let prior_revision = prior_cell
        .as_ref()
        .and_then(|cell| cell.get("state_revision"))
        .and_then(Value::as_i64)
        .unwrap_or(0);
    let prior_digest = prior_cell
        .as_ref()
        .and_then(|cell| cell.get("material_digest"))
        .and_then(Value::as_str);
    let changed = prior_digest != Some(material_digest.as_str());
    let state_revision = if changed {
        prior_revision.saturating_add(1).max(1)
    } else {
        prior_revision.max(1)
    };
    let cell = json!({
        "schema":"eliot.integration.cell.v1",
        "cell_id":cell_id,
        "cell_key":format!("sha256:{cell_id}"),
        "previous_cell_id":if membership_changed { previous_id } else { None },
        "state":state.as_str(),
        "state_revision":state_revision,
        "material_digest":material_digest,
        "previous_material_digest":prior_digest,
        "created_at_ms":prior_cell.as_ref().and_then(|item|item["created_at_ms"].as_i64()).unwrap_or(now),
        "updated_at_ms":if changed { now } else { prior_cell.as_ref().and_then(|item|item["updated_at_ms"].as_i64()).unwrap_or(now) },
        "operation_id":operation_id,
        "updated_by_client_id":principal.client_id,
        "membership_digest":membership_digest,
        "purpose":"make producer/carrier/consumer compatible",
        "close_condition":"all required dimensions match or exact mismatch is escalated",
        "facts":facts,
    });
    set_meta(tx, &cell_record_key, &cell)?;
    set_meta(
        tx,
        &current_index_key,
        &json!({"cell_id":cell_id,"cell_record_key":cell_record_key,"membership_digest":membership_digest}),
    )?;
    super::coordination::attach_operation_scope(tx, operation_id, task_id, attempt_id, attempt)?;

    let next_action = match state {
        wire::CellState::Compatible => "continue_with_current_contracts",
        wire::CellState::Mismatch => "resolve_exact_dimension_mismatches",
        wire::CellState::Unknown => "fill_unknown_dimensions_or_refresh_scope",
        wire::CellState::Open => "publish_the_missing_offer_or_requirement",
        wire::CellState::Superseded => "refresh_current_integration_cell",
    };
    Ok(json!({
        "operation_id":operation_id,
        "client_request_id":request.client_request_id,
        "cell":public_cell(&cell),
        "next_action":next_action,
        "changed":changed || membership_changed,
        "coalesced":!changed && !membership_changed,
        "advisory_only":true,
        "coverage":coverage,
        "gaps":gaps,
    }))
}

fn current_members(
    page: &Value,
    task_id: &str,
    task_revision: i64,
    attempt_id: &str,
    contract_key: &str,
) -> Result<Vec<Member>> {
    let items = page
        .get("items")
        .and_then(Value::as_array)
        .ok_or_else(|| Error::new("STORE_INVARIANT", "participant page lacks items"))?;
    let mut members = BTreeMap::new();
    for item in items {
        let client_id = text_at(item, &["client_id"], "participant client id")?.to_owned();
        let basis = item
            .get("participation_basis")
            .filter(|basis| basis.is_object())
            .ok_or_else(|| Error::new("STORE_INVARIANT", "participant page lacks current basis"))?
            .clone();
        let card = item
            .get("card")
            .filter(|card| card["state"] == "current")
            .ok_or_else(|| Error::new("STORE_INVARIANT", "participant page lacks current card"))?
            .clone();
        validate_card_scope(
            &card,
            task_id,
            task_revision,
            attempt_id,
            contract_key,
            &client_id,
        )?;
        let role = card
            .get("fields")
            .and_then(|fields| fields.get("role"))
            .and_then(Value::as_str)
            .filter(|role| matches!(*role, "producer" | "consumer" | "carrier"))
            .map(str::to_owned);
        let member = Member {
            client_id: client_id.clone(),
            participation_basis: basis,
            card,
            role,
        };
        if let Some(prior) = members.insert(client_id, member.clone())
            && (prior.participation_basis != member.participation_basis
                || prior.card["material_digest"] != member.card["material_digest"]
                || prior.card["card_revision"] != member.card["card_revision"])
        {
            return Err(Error::new(
                "STORE_INVARIANT",
                "exact contract relevance returned conflicting current records for one participant",
            ));
        }
    }
    Ok(members.into_values().collect())
}

fn validate_card_scope(
    card: &Value,
    task_id: &str,
    task_revision: i64,
    attempt_id: &str,
    contract_key: &str,
    client_id: &str,
) -> Result<()> {
    if card["state"] != "current"
        || card["card_kind"] != "contract"
        || card["identity"] != contract_key
        || card["task_id"] != task_id
        || card["task_revision"] != task_revision
        || card["attempt_id"] != attempt_id
        || card["client_id"] != client_id
        || card["fields"].as_object().is_none()
        || card["card_revision"].as_i64().is_none()
        || card["material_digest"].as_str().is_none()
    {
        return Err(Error::new(
            "STORE_INVARIANT",
            "contract-card helper returned a card outside the exact current scope",
        ));
    }
    Ok(())
}

fn validate_side_role(side: &wire::Side, role: &str) -> Result<()> {
    match side {
        wire::Side::Offer(_) if matches!(role, "producer" | "carrier") => Ok(()),
        wire::Side::Requirement(_) if role == "consumer" => Ok(()),
        wire::Side::Offer(_) => Err(Error::new(
            "FORBIDDEN",
            "only a current producer or carrier card owner may publish an offer",
        )),
        wire::Side::Requirement(_) => Err(Error::new(
            "FORBIDDEN",
            "only a current consumer card owner may publish a requirement",
        )),
    }
}

fn offer_value(offer: &wire::Offer) -> Value {
    json!({
        "readiness":offer.readiness,
        "will_be_available_at":offer.will_be_available_at,
        "candidate_ref":offer.candidate_ref,
        "assumptions":offer.assumptions,
    })
}

fn requirement_value(requirement: &wire::Requirement) -> Value {
    json!({
        "consumer_path":requirement.consumer_path,
        "consumer_symbol":requirement.consumer_symbol,
        "required_dimensions":requirement.required_dimensions,
        "must_be_ready_before":requirement.must_be_ready_before,
        "assumptions":requirement.assumptions,
    })
}

fn preserved_side(prior: &Option<Value>, keep: bool, field: &str) -> Value {
    if keep {
        prior
            .as_ref()
            .and_then(|record| record.get(field))
            .cloned()
            .unwrap_or(Value::Null)
    } else {
        Value::Null
    }
}

fn collect_cell_facts(
    db: &Connection,
    members: &[Member],
    scope_id: &str,
    contract_key: &str,
    current_operation_id: &str,
) -> Result<CellFacts> {
    let mut offers = Vec::new();
    let mut requirements = Vec::new();
    let mut carriers = Vec::new();
    let mut gaps = Vec::new();
    let mut producer_cards = Vec::new();
    let mut carrier_cards = Vec::new();
    let has_untyped_member = members.iter().any(|member| member.role.is_none());
    let expected_producers = members
        .iter()
        .filter(|member| member.role.as_deref() == Some("producer"))
        .map(|member| member.client_id.clone())
        .collect::<BTreeSet<_>>();
    let expected_consumers = members
        .iter()
        .filter(|member| member.role.as_deref() == Some("consumer"))
        .map(|member| member.client_id.clone())
        .collect::<BTreeSet<_>>();
    for member in members {
        let Some(role) = member.role.as_deref() else {
            gaps.push(json!({"kind":"contract_card_role_missing","client_id":member.client_id}));
            continue;
        };
        if role == "carrier" {
            carrier_cards.push(member);
            carriers.push(json!({
                "client_id":member.client_id,
                "participation_basis":member.participation_basis,
                "card_revision":member.card["card_revision"],
                "card_material_digest":member.card["material_digest"],
                "constraints":selected_card_dimensions(&member.card),
            }));
        }
        if role == "producer" {
            producer_cards.push(member);
        }
        let record = meta(
            db,
            &contribution_key(scope_id, contract_key, &member.client_id),
        )?;
        let Some(record) = record.filter(|record| {
            record["schema"] == "eliot.integration.contribution.v1"
                && record["scope_id"] == scope_id
                && record["task_id"] == member.card["task_id"]
                && record["task_revision"] == member.card["task_revision"]
                && record["attempt_id"] == member.card["attempt_id"]
                && record["contract_key"] == contract_key
                && record["client_id"] == member.client_id
                && record["participation_basis"] == member.participation_basis
                && record["card_revision"] == member.card["card_revision"]
                && record["card_material_digest"] == member.card["material_digest"]
                && record["role"] == role
        }) else {
            if role == "producer" {
                gaps.push(json!({"kind":"producer_offer_missing","client_id":member.client_id}));
            } else if role == "consumer" {
                gaps.push(
                    json!({"kind":"consumer_requirement_missing","client_id":member.client_id}),
                );
            }
            continue;
        };
        let contribution_operation_id = record
            .get("operation_id")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if contribution_operation_id != current_operation_id
            && !sync_operation_matches(
                db,
                contribution_operation_id,
                &member.client_id,
                member.card["task_id"].as_str().unwrap_or_default(),
                member.card["attempt_id"].as_str().unwrap_or_default(),
            )?
        {
            gaps.push(json!({
                "kind":"contribution_operation_provenance_missing",
                "client_id":member.client_id,
            }));
            continue;
        }
        let base = json!({
            "client_id":member.client_id,
            "participation_basis":member.participation_basis,
            "card_revision":member.card["card_revision"],
            "card_material_digest":member.card["material_digest"],
            "contribution_digest":record["material_digest"],
            "operation_id":record["operation_id"],
            "updated_at_ms":record["updated_at_ms"],
        });
        if (role == "producer" || role == "carrier") && !record["offer"].is_null() {
            let mut projection = base.clone();
            projection["offer"] = record["offer"].clone();
            offers.push(projection);
        }
        if role == "consumer" && !record["requirement"].is_null() {
            let mut projection = base;
            projection["requirement"] = record["requirement"].clone();
            requirements.push(projection);
        }
    }
    offers.sort_by(|left, right| left["client_id"].as_str().cmp(&right["client_id"].as_str()));
    requirements
        .sort_by(|left, right| left["client_id"].as_str().cmp(&right["client_id"].as_str()));
    carriers.sort_by(|left, right| left["client_id"].as_str().cmp(&right["client_id"].as_str()));
    let mut comparisons = Vec::new();
    let mut any_mismatch = false;
    let mut all_match = true;
    let producer_offer_ids = offers
        .iter()
        .filter_map(|item| item["client_id"].as_str())
        .filter(|client_id| expected_producers.contains(*client_id))
        .map(str::to_owned)
        .collect::<BTreeSet<_>>();
    let requirement_ids = requirements
        .iter()
        .filter_map(|item| item["client_id"].as_str())
        .map(str::to_owned)
        .collect::<BTreeSet<_>>();
    if expected_producers.is_empty() {
        gaps.push(json!({"kind":"current_producer_card_missing"}));
    } else if producer_offer_ids.is_empty() {
        gaps.push(json!({"kind":"current_producer_offer_missing"}));
    }
    if expected_consumers.is_empty() {
        gaps.push(json!({"kind":"current_consumer_card_missing"}));
    } else if requirement_ids.is_empty() {
        gaps.push(json!({"kind":"current_consumer_requirement_missing"}));
    }
    for requirement in &requirements {
        let dims = &requirement["requirement"]["required_dimensions"];
        let mut dimension_counts = BTreeMap::<String, BTreeMap<String, usize>>::new();
        let mut compared_producers = BTreeSet::new();
        let mut consumer_mismatch = false;
        let mut consumer_unknown = false;
        for offer in &offers {
            let client_id = offer["client_id"].as_str().unwrap_or_default();
            let Some(producer) = producer_cards
                .iter()
                .find(|member| member.client_id == client_id)
            else {
                continue;
            };
            compared_producers.insert(client_id.to_owned());
            let producer_requirements = if !carrier_cards.is_empty() {
                let mut dimensions = dims.as_object().cloned().ok_or_else(|| {
                    Error::new(
                        "STORE_INVARIANT",
                        "stored requirement dimensions are malformed",
                    )
                })?;
                dimensions.remove("carrier");
                Value::Object(dimensions)
            } else {
                dims.clone()
            };
            if producer_requirements
                .as_object()
                .is_some_and(|dimensions| dimensions.is_empty())
            {
                continue;
            }
            let comparison =
                wire::compare_dimensions(&producer.card["fields"], &producer_requirements)?;
            consumer_mismatch |= comparison.status == "mismatch";
            consumer_unknown |= comparison.status == "unknown";
            for dimension in comparison.dimensions {
                let counts = dimension_counts.entry(dimension.dimension).or_default();
                *counts
                    .entry(dimension.state.as_str().to_owned())
                    .or_default() += 1;
            }
        }
        if let Some(required_carrier) = dims.get("carrier") {
            if carrier_cards.is_empty() {
                let counts = dimension_counts.entry("carrier".to_owned()).or_default();
                *counts.entry("unknown".to_owned()).or_default() += 1;
                consumer_unknown = true;
            } else {
                let carrier_requirement = json!({"carrier":required_carrier});
                for carrier in &carrier_cards {
                    let comparison =
                        wire::compare_dimensions(&carrier.card["fields"], &carrier_requirement)?;
                    for dimension in comparison.dimensions {
                        let counts = dimension_counts.entry(dimension.dimension).or_default();
                        *counts
                            .entry(dimension.state.as_str().to_owned())
                            .or_default() += 1;
                    }
                    consumer_mismatch |= comparison.status == "mismatch";
                    consumer_unknown |= comparison.status == "unknown";
                }
            }
        }
        if compared_producers.is_empty() {
            consumer_unknown = true;
            dimension_counts = dims
                .as_object()
                .into_iter()
                .flatten()
                .map(|(dimension, _)| {
                    (
                        dimension.clone(),
                        BTreeMap::from([("unknown".to_owned(), 1usize)]),
                    )
                })
                .collect();
        }
        any_mismatch |= consumer_mismatch;
        all_match &= !consumer_mismatch && !consumer_unknown;
        comparisons.push(json!({
            "consumer_client_id":requirement["client_id"],
            "requirement_digest":requirement["contribution_digest"],
            "producer_client_ids":compared_producers,
            "status":if consumer_mismatch { "mismatch" } else if consumer_unknown { "unknown" } else { "compatible" },
            "dimensions":dimension_counts.into_iter().map(|(dimension, counts)| json!({"dimension":dimension,"counts":counts})).collect::<Vec<_>>(),
        }));
    }
    let status = if any_mismatch {
        "mismatch"
    } else if has_untyped_member {
        "unknown"
    } else if expected_producers.is_empty()
        || expected_consumers.is_empty()
        || producer_offer_ids.is_empty()
        || requirement_ids.is_empty()
    {
        "open"
    } else if expected_consumers
        .iter()
        .any(|client_id| !requirement_ids.contains(client_id))
        || expected_producers
            .iter()
            .any(|client_id| !producer_offer_ids.contains(client_id))
    {
        "unknown"
    } else if all_match {
        "compatible"
    } else {
        "unknown"
    };
    Ok(CellFacts {
        offers,
        requirements,
        carriers,
        comparisons,
        comparison_status: status.to_owned(),
        gaps,
    })
}

fn selected_card_dimensions(card: &Value) -> Value {
    let mut selected = serde_json::Map::new();
    for dimension in wire::INTEGRATION_DIMENSIONS {
        if let Some(value) = card["fields"].get(*dimension) {
            selected.insert((*dimension).to_owned(), value.clone());
        }
    }
    Value::Object(selected)
}

fn cell_assumptions(offers: &[Value], requirements: &[Value]) -> Value {
    let mut all = BTreeSet::new();
    for contribution in offers {
        add_assumptions(&mut all, contribution.pointer("/offer/assumptions"));
    }
    for contribution in requirements {
        add_assumptions(&mut all, contribution.pointer("/requirement/assumptions"));
    }
    json!(all.into_iter().take(40).collect::<Vec<_>>())
}

fn add_assumptions(all: &mut BTreeSet<String>, values: Option<&Value>) {
    if let Some(items) = values.and_then(Value::as_array) {
        for item in items.iter().filter_map(Value::as_str) {
            all.insert(item.to_owned());
        }
    }
}

fn material_contributions(contributions: &[Value], side: &str) -> Value {
    let mut projected = contributions
        .iter()
        .map(|item| {
            json!({
                "client_id":item["client_id"],
                "participation_basis_kind":item.pointer("/participation_basis/kind").cloned().unwrap_or(Value::Null),
                "card_revision":item["card_revision"],
                "card_material_digest":item["card_material_digest"],
                "contribution_digest":item["contribution_digest"],
                (side):item[side],
            })
        })
        .collect::<Vec<_>>();
    projected.sort_by(|left, right| left["client_id"].as_str().cmp(&right["client_id"].as_str()));
    Value::Array(projected)
}

fn material_carriers(carriers: &[Value]) -> Value {
    Value::Array(
        carriers
            .iter()
            .map(|item| {
                json!({
                    "client_id":item["client_id"],
                    "participation_basis_kind":item.pointer("/participation_basis/kind").cloned().unwrap_or(Value::Null),
                    "card_revision":item["card_revision"],
                    "card_material_digest":item["card_material_digest"],
                    "constraints":item["constraints"],
                })
            })
            .collect(),
    )
}

fn canonical_sources(members: &[Member], gaps: &mut Vec<Value>) -> Value {
    let mut sources = BTreeSet::new();
    let mut truncated = false;
    for member in members {
        if let Some(values) = member.card["fields"]["canonical_sources"].as_array() {
            for value in values {
                if let Some(source) = value
                    .as_str()
                    .filter(|source| !source.trim().is_empty() && source.len() <= 1024)
                {
                    if sources.len() == MAX_CANONICAL_SOURCES && !sources.contains(source) {
                        truncated = true;
                        break;
                    }
                    sources.insert(source.to_owned());
                }
            }
        }
    }
    if truncated {
        gaps.push(
            json!({"kind":"canonical_source_projection_bound","limit":MAX_CANONICAL_SOURCES}),
        );
    }
    json!(sources.into_iter().collect::<Vec<_>>())
}

fn public_cell(cell: &Value) -> Value {
    let mut public = cell.clone();
    if let Some(facts) = public.get_mut("facts").and_then(Value::as_object_mut) {
        if let Some(bases) = facts
            .get_mut("member_basis_set")
            .and_then(Value::as_array_mut)
        {
            for member in bases {
                let client_id = member.get("client_id").cloned().unwrap_or(Value::Null);
                let kind = member
                    .pointer("/participation_basis/kind")
                    .cloned()
                    .unwrap_or(Value::Null);
                *member = json!({"client_id":client_id,"participation_basis_kind":kind});
            }
        }
        for field in ["offers", "requirements", "carrier_constraints"] {
            if let Some(items) = facts.get_mut(field).and_then(Value::as_array_mut) {
                for item in items {
                    let client_id = item.get("client_id").cloned().unwrap_or(Value::Null);
                    let kind = item
                        .pointer("/participation_basis/kind")
                        .cloned()
                        .unwrap_or(Value::Null);
                    if let Some(object) = item.as_object_mut() {
                        object.remove("participation_basis");
                        object.insert("participation_basis_kind".into(), kind);
                    }
                    if item.get("client_id").is_none() {
                        item["client_id"] = client_id;
                    }
                }
            }
        }
    }
    public
}

fn overlap_check(db: &Connection, principal: &Principal, value: &Value) -> Result<Value> {
    let request = wire::OverlapRequest::parse(value)?;
    let scope = super::coordination::watch_scope(
        db,
        principal,
        request.task_id.as_deref(),
        request.task_revision,
        request.attempt_id.as_deref(),
    )?;
    let task_id = text_at(&scope, &["task", "task_id"], "Task id")?;
    let task_revision = int_at(&scope, &["task", "revision"], "Task revision")?;
    let attempt_id = text_at(&scope, &["attempt", "attempt_id"], "Attempt id")?;
    let scope_id = text_at(&scope, &["scope_id"], "scope id")?;
    let mut selector_facts = Vec::new();
    let mut gaps = Vec::new();
    for (kind, selector, field) in request
        .paths
        .iter()
        .map(|value| ("path", value, "path"))
        .chain(
            request
                .symbols
                .iter()
                .map(|value| ("symbol", value, "symbol")),
        )
        .chain(
            request
                .contracts
                .iter()
                .map(|value| ("contract", value, "contract_key")),
        )
    {
        let mut params = json!({(field):selector,"limit":MAX_OVERLAP_MATCHES as i64});
        if principal.role != Role::Participant {
            params["task_id"] = json!(task_id);
            params["task_revision"] = json!(task_revision);
            params["attempt_id"] = json!(attempt_id);
        }
        let result = super::coordination::read(db, principal, "coordination.peer.find", &params)?;
        if result["coverage"] != "complete" {
            gaps.push(json!({
                "kind":"relevance_index_partial",
                "selector":{"kind":kind,"value":selector},
                "detail":result["gaps"],
            }));
        }
        let mut matches = result["items"].as_array().cloned().unwrap_or_default();
        matches = matches.iter().map(project_peer_match).collect();
        if principal.role == Role::Participant
            && let Some(own) = caller_card_matches(
                db,
                scope_id,
                &principal.client_id,
                kind,
                selector,
                &mut gaps,
            )?
        {
            matches.push(own);
        }
        matches.sort_by(|left, right| left["client_id"].as_str().cmp(&right["client_id"].as_str()));
        let mut seen = BTreeSet::new();
        matches.retain(|item| {
            item["client_id"]
                .as_str()
                .is_some_and(|client_id| seen.insert(client_id.to_owned()))
        });
        if matches.len() > MAX_OVERLAP_MATCHES {
            matches.truncate(MAX_OVERLAP_MATCHES);
            gaps.push(json!({
                "kind":"overlap_match_bound",
                "selector":{"kind":kind,"value":selector},
                "limit":MAX_OVERLAP_MATCHES,
            }));
        }
        selector_facts.push(json!({
            "selector":{"kind":kind,"value":selector},
            "matches":matches,
        }));
    }
    let mut planned_overlap = Vec::new();
    for fact in &selector_facts {
        let matches = fact["matches"].as_array().cloned().unwrap_or_default();
        let work_owners = matches
            .iter()
            .filter(|item| item["card"]["card_kind"] == "work")
            .filter_map(|item| item["client_id"].as_str())
            .collect::<BTreeSet<_>>();
        if work_owners.len() > 1 {
            planned_overlap.push(json!({
                "kind":fact["selector"]["kind"],
                "value":fact["selector"]["value"],
                "client_ids":work_owners,
                "evidence":"exact current work-card relation",
            }));
        }
    }
    let candidate_ref = request.candidate_ref.as_deref();
    let scope_candidate = scope
        .pointer("/attempt/candidate_ref")
        .and_then(Value::as_str);
    let candidate_fact = candidate_ref.map(|candidate| {
        json!({
            "requested_candidate_ref":candidate,
            "matches_current_attempt_candidate":scope_candidate == Some(candidate),
            "current_attempt_candidate_ref":scope_candidate,
        })
    });
    if candidate_ref.is_some() && scope_candidate != candidate_ref {
        gaps.push(json!({"kind":"candidate_ref_not_current_attempt_candidate"}));
    }
    for contract_key in &request.contracts {
        if let Some(pointer) = meta(db, &current_cell_index_key(scope_id, contract_key))? {
            let record_key = pointer["cell_record_key"].as_str().ok_or_else(|| {
                Error::new(
                    "STORE_INVARIANT",
                    "integration cell pointer lacks record key",
                )
            })?;
            if let Some(cell) = meta(db, record_key)? {
                if !retained_sync_operation_matches(db, &cell, task_id, attempt_id)? {
                    gaps.push(json!({"kind":"integration_cell_operation_provenance_missing","contract_key":contract_key}));
                    selector_facts.push(unknown_cell_summary(
                        contract_key,
                        &cell,
                        "integration_cell_operation_provenance_missing",
                    ));
                    continue;
                }
                let freshness = current_cell_freshness(
                    db,
                    principal,
                    scope_id,
                    task_id,
                    task_revision,
                    attempt_id,
                    contract_key,
                    &cell,
                )?;
                if freshness != CellFreshness::Current {
                    let kind = match freshness {
                        CellFreshness::Changed => {
                            "integration_cell_membership_or_card_facts_changed"
                        }
                        CellFreshness::Incomplete => {
                            "integration_cell_current_membership_incomplete"
                        }
                        CellFreshness::Current => unreachable!(),
                    };
                    gaps.push(json!({"kind":kind,"contract_key":contract_key}));
                    selector_facts.push(unknown_cell_summary(contract_key, &cell, kind));
                    continue;
                }
                selector_facts.push(json!({
                    "selector":{"kind":"integration_cell","value":contract_key},
                    "cell":{
                        "cell_id":cell["cell_id"],
                        "state":cell["state"],
                        "state_revision":cell["state_revision"],
                        "material_digest":cell["material_digest"],
                        "comparison_status":cell["facts"]["comparison_status"],
                        "coverage":cell["facts"]["coverage"],
                        "gaps":cell["facts"]["gaps"],
                    },
                }));
            } else {
                gaps.push(
                    json!({"kind":"integration_cell_pointer_stale","contract_key":contract_key}),
                );
            }
        }
    }
    gaps.push(json!({
        "kind":"git_state_not_recorded",
        "detail":"worktree, branch, dirty paths, baseline changes, and history are not part of this bounded controller read",
    }));
    gaps.push(json!({
        "kind":"write_lease_facts_not_recorded",
        "detail":"current controller cards do not prove exclusive filesystem ownership",
    }));
    Ok(json!({
        "task_id":task_id,
        "task_revision":task_revision,
        "attempt_id":attempt_id,
        "assignment_owner_id":scope["attempt"]["owner_id"],
        "selectors":selector_facts,
        "active_owner":[],
        "planned_overlap":planned_overlap,
        "uncommitted_overlap":[],
        "changed_since_baseline":[],
        "historical_provenance":[],
        "candidate":candidate_fact,
        "unknown_coverage":gaps,
        "controller_coverage":if gaps.iter().any(|gap| gap["kind"] == "relevance_index_partial" || gap["kind"] == "overlap_match_bound") { "partial" } else { "complete" },
        "coverage":"partial",
        "gaps":gaps,
    }))
}

// Each argument is one exact persisted scope address used by the freshness check.
#[allow(clippy::too_many_arguments)]
fn current_cell_freshness(
    db: &Connection,
    principal: &Principal,
    scope_id: &str,
    task_id: &str,
    task_revision: i64,
    attempt_id: &str,
    contract_key: &str,
    cell: &Value,
) -> Result<CellFreshness> {
    let params = json!({
        "task_id":task_id,
        "task_revision":task_revision,
        "attempt_id":attempt_id,
        "contract_key":contract_key,
        "limit":MAX_CELL_MEMBERS,
    });
    let page = if principal.role == Role::Participant {
        super::coordination::list_current_contract_participants(
            db,
            principal,
            contract_key,
            MAX_CELL_MEMBERS,
        )?
    } else {
        super::coordination::read(db, principal, "coordination.peer.find", &params)?
    };
    if page["coverage"] != "complete"
        || page["gaps"].as_array().is_none_or(|gaps| !gaps.is_empty())
        || page
            .get("next_after")
            .is_some_and(|cursor| !cursor.is_null())
    {
        return Ok(CellFreshness::Incomplete);
    }
    if (page.get("scope_id").is_some() && page["scope_id"] != scope_id)
        || page["task_id"] != task_id
        || page["task_revision"] != task_revision
        || page["attempt_id"] != attempt_id
    {
        return Ok(CellFreshness::Changed);
    }
    let Some(items) = page.get("items").and_then(Value::as_array) else {
        return Ok(CellFreshness::Incomplete);
    };
    if items.len() > MAX_CELL_MEMBERS as usize {
        return Ok(CellFreshness::Incomplete);
    }

    let mut member_identity_set = Vec::with_capacity(items.len());
    let mut member_card_facts = Vec::with_capacity(items.len());
    let mut seen = BTreeSet::new();
    for item in items {
        let Some(client_id) = item.get("client_id").and_then(Value::as_str) else {
            return Ok(CellFreshness::Incomplete);
        };
        if !seen.insert(client_id.to_owned()) {
            return Ok(CellFreshness::Changed);
        }
        let card = &item["card"];
        let basis_kind = if principal.role == Role::Participant {
            item.pointer("/participation_basis/kind")
        } else {
            item.pointer("/participant/participation_basis_kind")
        };
        let Some(basis_kind) = basis_kind.and_then(Value::as_str) else {
            return Ok(CellFreshness::Incomplete);
        };
        let card_available = if principal.role == Role::Participant {
            card["state"] == "current"
        } else {
            card["available"] == true
        };
        if !card_available
            || card["card_kind"] != "contract"
            || card["identity"] != contract_key
            || card["task_id"] != task_id
            || card["task_revision"] != task_revision
            || card["attempt_id"] != attempt_id
            || card["client_id"] != client_id
            || card.get("fields").and_then(Value::as_object).is_none()
        {
            return Ok(CellFreshness::Changed);
        }
        let Some(card_revision) = card.get("card_revision").and_then(Value::as_i64) else {
            return Ok(CellFreshness::Incomplete);
        };
        let Some(card_material_digest) = card.get("material_digest").and_then(Value::as_str) else {
            return Ok(CellFreshness::Incomplete);
        };
        let role = card
            .pointer("/fields/role")
            .and_then(Value::as_str)
            .filter(|role| matches!(*role, "producer" | "consumer" | "carrier"));
        member_identity_set.push(json!({
            "client_id":client_id,
            "participation_basis_kind":basis_kind,
        }));
        member_card_facts.push(json!({
            "client_id":client_id,
            "role":role,
            "card_revision":card_revision,
            "card_material_digest":card_material_digest,
        }));
    }
    member_identity_set
        .sort_by(|left, right| left["client_id"].as_str().cmp(&right["client_id"].as_str()));
    member_card_facts
        .sort_by(|left, right| left["client_id"].as_str().cmp(&right["client_id"].as_str()));
    let current_membership_digest = digest(&json!({
        "task_id":task_id,
        "task_revision":task_revision,
        "attempt_id":attempt_id,
        "contract_key":contract_key,
        "member_identity_set":member_identity_set,
    }))?;
    let facts = &cell["facts"];
    let exact_record_scope = cell["schema"] == "eliot.integration.cell.v1"
        && facts["scope_id"] == scope_id
        && facts["task_id"] == task_id
        && facts["attempt_id"] == attempt_id
        && facts["contract_key"] == contract_key
        && facts["task_revision_set"]
            == json!([{"task_id":task_id,"task_revision":task_revision,"attempt_id":attempt_id}]);
    if !exact_record_scope
        || cell["membership_digest"] != current_membership_digest
        || facts["membership_digest"] != current_membership_digest
        || facts["member_card_facts"]
            .as_array()
            .is_none_or(|stored| stored != &member_card_facts)
    {
        return Ok(CellFreshness::Changed);
    }
    Ok(CellFreshness::Current)
}

fn unknown_cell_summary(contract_key: &str, cell: &Value, gap_kind: &str) -> Value {
    json!({
        "selector":{"kind":"integration_cell","value":contract_key},
        "cell":{
            "cell_id":cell["cell_id"],
            "state":"unknown",
            "state_revision":cell["state_revision"],
            "comparison_status":"unknown",
            "coverage":"partial",
            "gaps":[{"kind":gap_kind}],
        },
    })
}

fn retained_sync_operation_matches(
    db: &Connection,
    cell: &Value,
    task_id: &str,
    attempt_id: &str,
) -> Result<bool> {
    let Some(operation_id) = cell.get("operation_id").and_then(Value::as_str) else {
        return Ok(false);
    };
    let Some(caller_id) = cell.get("updated_by_client_id").and_then(Value::as_str) else {
        return Ok(false);
    };
    sync_operation_matches(db, operation_id, caller_id, task_id, attempt_id)
}

fn sync_operation_matches(
    db: &Connection,
    operation_id: &str,
    caller_id: &str,
    task_id: &str,
    attempt_id: &str,
) -> Result<bool> {
    type SyncOperationRow = (String, String, String, Option<String>, Option<String>);
    let row: Option<SyncOperationRow> = db
        .query_row(
            "SELECT method,state,caller_id,task_id,attempt_id FROM operations WHERE operation_id=?1",
            [operation_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
        )
        .optional()?;
    Ok(row.is_some_and(|(method, state, caller, task, attempt)| {
        method == "coordination.sync_integration"
            && state == "settled"
            && caller == caller_id
            && task.as_deref() == Some(task_id)
            && attempt.as_deref() == Some(attempt_id)
    }))
}

fn caller_card_matches(
    db: &Connection,
    scope_id: &str,
    client_id: &str,
    kind: &str,
    selector: &str,
    gaps: &mut Vec<Value>,
) -> Result<Option<Value>> {
    let work_key = keys::card_key(scope_id, "work", "work", client_id);
    let mut cards = Vec::new();
    if let Some(card) = meta(db, &work_key)?.filter(|card| card["state"] == "current") {
        cards.push(card);
    }
    let prefix = format!(
        "coordination:card-owner:{scope_id}:{}:contract:",
        keys::key_component(client_id)
    );
    let upper = format!("{prefix}g");
    let mut statement =
        db.prepare("SELECT value_json FROM meta WHERE key>=?1 AND key<?2 ORDER BY key LIMIT 21")?;
    let rows: Vec<String> = statement
        .query_map(params![prefix, upper], |row| row.get(0))?
        .collect::<std::result::Result<_, _>>()?;
    for raw in rows.iter().take(20) {
        let index: Value = serde_json::from_str(raw)?;
        if let Some(card_key) = index.get("card_key").and_then(Value::as_str)
            && let Some(card) = meta(db, card_key)?.filter(|card| card["state"] == "current")
        {
            cards.push(card);
        } else {
            gaps.push(json!({"kind":"caller_card_owner_index_stale"}));
        }
    }
    if rows.len() > 20 {
        gaps.push(json!({"kind":"caller_card_scan_bound","limit":20}));
    }
    for card in cards {
        let matches = match kind {
            "contract" => card["card_kind"] == "contract" && card["identity"] == selector,
            "path" => card_has_exact_term(&card, "path", selector),
            "symbol" => card_has_exact_term(&card, "symbol", selector),
            _ => false,
        };
        if matches {
            return Ok(Some(json!({
                "client_id":client_id,
                "participant":{"participation_basis_kind":"authenticated_current_scope"},
                "match":{"kind":kind,"value":selector,"reason":"exact_card_index"},
                "card":project_card(&card),
            })));
        }
    }
    Ok(None)
}

fn card_has_exact_term(card: &Value, kind: &str, term: &str) -> bool {
    let names: &[&str] = match kind {
        "path" => &["paths", "planned_scopes", "integration_points"],
        "symbol" => &["symbols", "planned_scopes", "integration_points"],
        _ => &[],
    };
    names.iter().any(|name| {
        card["fields"]
            .get(*name)
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .any(|item| {
                item.as_str() == Some(term)
                    || item
                        .get(kind)
                        .and_then(Value::as_str)
                        .is_some_and(|value| value == term)
            })
    })
}

fn project_card(card: &Value) -> Value {
    json!({
        "card_kind":card["card_kind"],
        "identity":card["identity"],
        "card_revision":card["card_revision"],
        "material_digest":card["material_digest"],
    })
}

fn project_peer_match(item: &Value) -> Value {
    json!({
        "client_id":item["client_id"],
        "participant":item["participant"],
        "match":item["match"],
        "card":project_card(&item["card"]),
    })
}

fn contribution_key(scope_id: &str, contract_key: &str, client_id: &str) -> String {
    format!(
        "coordination:integration-contribution:v1:{scope_id}:{}:{}",
        keys::key_component(contract_key),
        keys::key_component(client_id),
    )
}

fn current_cell_index_key(scope_id: &str, contract_key: &str) -> String {
    format!(
        "coordination:integration-cell-current:v1:{scope_id}:{}",
        keys::key_component(contract_key),
    )
}

fn cell_record_key(scope_id: &str, contract_key: &str, cell_id: &str) -> String {
    format!(
        "coordination:integration-cell:v1:{scope_id}:{}:{}",
        keys::key_component(contract_key),
        keys::key_component(cell_id),
    )
}

fn digest(value: &Value) -> Result<String> {
    Ok(model::digest(model::canonical(value)?.as_bytes()))
}

fn text_at<'a>(value: &'a Value, path: &[&str], name: &str) -> Result<&'a str> {
    value_at(value, path)
        .and_then(Value::as_str)
        .filter(|text| !text.is_empty())
        .ok_or_else(|| Error::new("STORE_INVARIANT", format!("{name} is missing or malformed")))
}

fn int_at(value: &Value, path: &[&str], name: &str) -> Result<i64> {
    value_at(value, path)
        .and_then(Value::as_i64)
        .filter(|number| *number > 0)
        .ok_or_else(|| Error::new("STORE_INVARIANT", format!("{name} is missing or malformed")))
}

fn value_at<'a>(value: &'a Value, path: &[&str]) -> Option<&'a Value> {
    let mut current = value;
    for field in path {
        current = current.get(*field)?;
    }
    Some(current)
}
