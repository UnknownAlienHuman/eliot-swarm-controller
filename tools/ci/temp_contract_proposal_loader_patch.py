from pathlib import Path


def replace_once(text: str, old: str, new: str, label: str) -> str:
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{label}: expected one replacement, found {count}")
    return text.replace(old, new, 1)


coordination_path = Path("crates/swarm-kernel-host/src/store/coordination.rs")
coordination = coordination_path.read_text(encoding="utf-8")

helper_marker = "fn contract_thread_context(\n"
if coordination.count(helper_marker) != 1:
    raise SystemExit("contract_thread_context marker is not unique")
helper = r'''fn is_lower_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn load_contract_proposal_revision_with_head(
    db: &Connection,
    thread_id: &str,
    task_id: &str,
    task_revision: i64,
    attempt_id: &str,
    proposal_revision_id: &str,
) -> Result<(Value, Value)> {
    let pointer = meta(db, &proposal_revision_pointer_key(proposal_revision_id))?
        .ok_or_else(|| {
            Error::new(
                "CONTRACT_PROPOSAL_NOT_FOUND",
                "proposal revision is not retained",
            )
        })?;
    let proposal_id = model::text(&pointer, "proposal_id")?;
    let pointer_revision = pointer["revision"]
        .as_i64()
        .filter(|revision| *revision > 0)
        .ok_or_else(|| {
            Error::new(
                "COORDINATION_INDEX_CORRUPT",
                "proposal revision pointer has no valid revision number",
            )
        })?;
    if pointer["thread_id"] != thread_id {
        return Err(Error::new(
            "FORBIDDEN",
            "proposal revision is outside this exact Thread",
        ));
    }

    let revision = meta(
        db,
        &proposal_revision_key(proposal_id, proposal_revision_id),
    )?
    .ok_or_else(|| {
        Error::new(
            "COORDINATION_INDEX_CORRUPT",
            "proposal revision pointer has no revision record",
        )
    })?;
    let revision_number = revision["revision"]
        .as_i64()
        .filter(|revision| *revision > 0)
        .ok_or_else(|| Error::new("PROPOSAL_DAMAGED", "proposal revision number is invalid"))?;
    if revision["schema_version"] != 1
        || revision["record_type"] != "contract_proposal_revision"
        || revision["proposal_id"] != proposal_id
        || revision["proposal_revision_id"] != proposal_revision_id
        || revision_number != pointer_revision
        || revision["thread_id"] != thread_id
        || revision["task_id"] != task_id
        || revision["task_revision"] != task_revision
        || revision["attempt_id"] != attempt_id
    {
        return Err(Error::new(
            "PROPOSAL_DAMAGED",
            "proposal revision identity or Task scope is inconsistent",
        ));
    }
    let proposal = revision
        .get("proposal")
        .filter(|proposal| proposal.is_object())
        .ok_or_else(|| Error::new("PROPOSAL_DAMAGED", "proposal body is not an object"))?;
    let proposal_digest = model::text(&revision, "proposal_digest")?;
    if !is_lower_sha256(proposal_digest)
        || model::digest(model::canonical(proposal)?.as_bytes()) != proposal_digest
    {
        return Err(Error::new(
            "PROPOSAL_DAMAGED",
            "proposal digest does not match the canonical proposal body",
        ));
    }

    let head = meta(db, &proposal_head_key(proposal_id))?.ok_or_else(|| {
        Error::new(
            "COORDINATION_INDEX_CORRUPT",
            "proposal revision has no proposal header",
        )
    })?;
    let revision_count = head["revision_count"]
        .as_i64()
        .filter(|count| *count > 0)
        .ok_or_else(|| {
            Error::new(
                "COORDINATION_INDEX_CORRUPT",
                "proposal header revision count is invalid",
            )
        })?;
    let latest_revision_id = model::text(&head, "latest_revision_id")?;
    let latest_digest = model::text(&head, "latest_digest")?;
    if head["schema_version"] != 1
        || head["record_type"] != "contract_proposal_head"
        || head["proposal_id"] != proposal_id
        || head["thread_id"] != thread_id
        || head["proposal_sequence"]
            .as_i64()
            .is_none_or(|sequence| sequence <= 0)
        || !is_lower_sha256(latest_digest)
        || revision_number > revision_count
        || (latest_revision_id == proposal_revision_id
            && (latest_digest != proposal_digest || revision_count != revision_number))
        || (latest_revision_id != proposal_revision_id && revision_number >= revision_count)
    {
        return Err(Error::new(
            "COORDINATION_INDEX_CORRUPT",
            "proposal header is inconsistent with the retained revision",
        ));
    }

    let operation_id = model::text(&revision, "operation_id")?;
    let operation = operations::get_operation(db, operation_id)?;
    if operation["method"] != "coordination.contract.propose"
        || operation["state"] != "settled"
        || operation["task_id"] != task_id
        || operation["attempt_id"] != attempt_id
        || operation["result"]["proposal_id"] != proposal_id
        || operation["result"]["proposal_revision_id"] != proposal_revision_id
        || operation["result"]["proposal_digest"] != proposal_digest
        || operation["result"]["revision"] != revision_number
    {
        return Err(Error::new(
            "PROPOSAL_DAMAGED",
            "proposal revision does not match its settled producer Operation",
        ));
    }
    Ok((revision, head))
}

pub(super) fn load_contract_proposal_revision(
    db: &Connection,
    thread_id: &str,
    task_id: &str,
    task_revision: i64,
    attempt_id: &str,
    proposal_revision_id: &str,
) -> Result<Value> {
    load_contract_proposal_revision_with_head(
        db,
        thread_id,
        task_id,
        task_revision,
        attempt_id,
        proposal_revision_id,
    )
    .map(|(revision, _)| revision)
}

pub(super) fn load_current_contract_proposal_revision(
    db: &Connection,
    thread_id: &str,
    task_id: &str,
    task_revision: i64,
    attempt_id: &str,
    proposal_revision_id: &str,
) -> Result<Value> {
    let (revision, head) = load_contract_proposal_revision_with_head(
        db,
        thread_id,
        task_id,
        task_revision,
        attempt_id,
        proposal_revision_id,
    )?;
    if head["latest_revision_id"] != proposal_revision_id
        || head["latest_digest"] != revision["proposal_digest"]
    {
        return Err(Error::new(
            "STALE_CONTRACT_REVISION",
            "proposal revision is not the current exact proposal head",
        ));
    }
    Ok(revision)
}

'''
coordination = coordination.replace(helper_marker, helper + helper_marker, 1)

# Superseding a proposal revision must consume the exact current canonical revision.
start = coordination.index(
    "        if let Some(prior_revision_id) = request.supersedes_revision_id.as_deref() {\n"
)
end = coordination.index("        } else {\n", start)
new_supersede = r'''        if let Some(prior_revision_id) = request.supersedes_revision_id.as_deref() {
            let prior = load_current_contract_proposal_revision(
                tx,
                &context.thread_id,
                &context.task_id,
                context.task_revision,
                &context.attempt_id,
                prior_revision_id,
            )?;
            let proposal_id = model::text(&prior, "proposal_id")?.to_owned();
            let head = meta(tx, &proposal_head_key(&proposal_id))?.ok_or_else(|| {
                Error::new(
                    "COORDINATION_INDEX_CORRUPT",
                    "current proposal revision has no proposal header",
                )
            })?;
            let revision_number = prior["revision"]
                .as_i64()
                .and_then(|revision| revision.checked_add(1))
                .ok_or_else(|| Error::new("REVISION_OVERFLOW", "proposal revision exhausted"))?;
            let proposal_sequence = head["proposal_sequence"]
                .as_i64()
                .filter(|sequence| *sequence > 0)
                .ok_or_else(|| {
                    Error::new("COORDINATION_INDEX_CORRUPT", "proposal sequence is missing")
                })?;
            let created_at_ms = head["created_at_ms"]
                .as_i64()
                .filter(|timestamp| *timestamp > 0)
                .ok_or_else(|| {
                    Error::new(
                        "COORDINATION_INDEX_CORRUPT",
                        "proposal creation time is missing",
                    )
                })?;
            (
                proposal_id,
                revision_number,
                proposal_sequence,
                created_at_ms,
            )
'''
coordination = coordination[:start] + new_supersede + coordination[end:]

# Response path: one canonical loader replaces independent header/revision parsing.
start = coordination.index(
    "    let header = meta(tx, &proposal_head_key(&request.proposal_id))?\n",
    coordination.index("fn respond_contract("),
)
end = coordination.index("    let response_key =", start)
new_response_load = r'''    let revision = load_contract_proposal_revision(
        tx,
        &context.thread_id,
        &context.task_id,
        context.task_revision,
        &context.attempt_id,
        &request.proposal_revision_id,
    )?;
    if revision["proposal_id"] != request.proposal_id
        || revision["proposal_digest"] != request.proposal_digest
    {
        return Err(Error::new(
            "DIGEST_MISMATCH",
            "proposal revision digest or proposal identity does not match",
        ));
    }
'''
coordination = coordination[:start] + new_response_load + coordination[end:]

# Exact contract get consumes the same canonical revision boundary.
start = coordination.index(
    "    let header = meta(db, &proposal_head_key(&request.proposal_id))?\n",
    coordination.index("fn contract_get("),
)
end = coordination.index("    let prefix = proposal_response_page_prefix", start)
new_get_load = r'''    let revision = load_contract_proposal_revision(
        db,
        &context.thread_id,
        &context.task_id,
        context.task_revision,
        &context.attempt_id,
        &request.proposal_revision_id,
    )?;
    if revision["proposal_id"] != request.proposal_id {
        return Err(Error::new(
            "NOT_FOUND",
            "proposal revision is outside this proposal identity",
        ));
    }
'''
coordination = coordination[:start] + new_get_load + coordination[end:]

# List validates each advertised current head through the authoritative loader.
old_list = r'''        if header["thread_id"] != context.thread_id
            || header["proposal_sequence"] != page["proposal_sequence"]
        {
            return Err(Error::new(
                "COORDINATION_INDEX_CORRUPT",
                "Thread proposal page does not match its current header",
            ));
        }
        last_sequence = page["proposal_sequence"].as_i64();
'''
new_list = r'''        if header["thread_id"] != context.thread_id
            || header["proposal_sequence"] != page["proposal_sequence"]
        {
            return Err(Error::new(
                "COORDINATION_INDEX_CORRUPT",
                "Thread proposal page does not match its current header",
            ));
        }
        let latest_revision_id = model::text(&header, "latest_revision_id")?;
        let latest = load_current_contract_proposal_revision(
            db,
            &context.thread_id,
            &context.task_id,
            context.task_revision,
            &context.attempt_id,
            latest_revision_id,
        )?;
        if latest["proposal_id"] != proposal_id
            || latest["proposal_digest"] != header["latest_digest"]
        {
            return Err(Error::new(
                "COORDINATION_INDEX_CORRUPT",
                "proposal header differs from its current canonical revision",
            ));
        }
        last_sequence = page["proposal_sequence"].as_i64();
'''
coordination = replace_once(
    coordination,
    old_list,
    new_list,
    "contract list canonical head",
)

# Operation read: proposal producer path reuses the authoritative revision.
start = coordination.index(
    "        \"coordination.contract.propose\" => {\n",
    coordination.index("fn authorize_contract_operation_read("),
)
end = coordination.index("        \"coordination.contract.respond\" => {\n", start)
new_propose_read = r'''        "coordination.contract.propose" => {
            let proposal_id = model::text(&result, "proposal_id")?;
            let revision_id = model::text(&result, "proposal_revision_id")?;
            let revision = load_contract_proposal_revision(
                db,
                &context.thread_id,
                &context.task_id,
                context.task_revision,
                &context.attempt_id,
                revision_id,
            )?;
            if revision["operation_id"] != operation_id
                || revision["proposal_id"] != proposal_id
                || revision["proposal_revision_id"] != revision_id
                || revision["revision"] != result["revision"]
                || revision["proposal_digest"] != result["proposal_digest"]
                || revision["author"]["client_id"] != caller_id
                || revision["author"]["role"] != author.role
                || revision["author"]["generation"].as_i64() != author.generation
                || revision["author"]["registration_fingerprint"]
                    != author.registration_fingerprint
                || revision["author"]["actor"] != author.actor
                || revision["author"]["scope"] != author.scope
            {
                return Err(Error::new(
                    "NOT_FOUND",
                    "Operation is outside the retained Thread scope",
                ));
            }
        }
'''
coordination = coordination[:start] + new_propose_read + coordination[end:]

# Operation read: response path first validates the referenced canonical revision.
response_marker = r'''        "coordination.contract.respond" => {
            let proposal_id = model::text(&result, "proposal_id")?;
            let response = meta(db, &proposal_response_key(proposal_id, operation_id))?
'''
response_replacement = r'''        "coordination.contract.respond" => {
            let proposal_id = model::text(&result, "proposal_id")?;
            let revision_id = model::text(&result, "proposal_revision_id")?;
            let revision = load_contract_proposal_revision(
                db,
                &context.thread_id,
                &context.task_id,
                context.task_revision,
                &context.attempt_id,
                revision_id,
            )?;
            if revision["proposal_id"] != proposal_id
                || revision["proposal_digest"] != result["proposal_digest"]
            {
                return Err(Error::new(
                    "NOT_FOUND",
                    "Operation proposal revision is outside the retained Thread scope",
                ));
            }
            let response = meta(db, &proposal_response_key(proposal_id, operation_id))?
'''
coordination = replace_once(
    coordination,
    response_marker,
    response_replacement,
    "response Operation canonical revision",
)
coordination_path.write_text(coordination, encoding="utf-8")


threads_path = Path("crates/swarm-kernel-host/src/store/coordination_threads.rs")
threads = threads_path.read_text(encoding="utf-8")

# Resolved contract Thread consumes the exact current canonical revision.
start = threads.index(
    "        let proposal = load_proposal_revision(tx, &context, proposal_revision_id)?;\n"
)
end = threads.index("        let ratification = request\n", start)
new_resolve = r'''        let proposal = coordination_store::load_current_contract_proposal_revision(
            tx,
            &context.thread_id,
            &context.task_id,
            context.task_revision,
            &context.attempt_id,
            proposal_revision_id,
        )?;
'''
threads = threads[:start] + new_resolve + threads[end:]
threads = replace_once(
    threads,
    '            model::text(&proposal, "digest")?,\n',
    '            model::text(&proposal, "proposal_digest")?,\n',
    "Thread ratification digest field",
)

old_validate = r'''fn validate_proposal_revision(
    db: &Connection,
    context: &ThreadContext,
    proposal_revision_id: &str,
) -> Result<()> {
    load_proposal_revision(db, context, proposal_revision_id).map(|_| ())
}

fn load_proposal_revision(
    db: &Connection,
    context: &ThreadContext,
    proposal_revision_id: &str,
) -> Result<Value> {
    let pointer = meta(
        db,
        &format!("coordination:proposal-revision:{proposal_revision_id}"),
    )?
    .ok_or_else(|| Error::new("NOT_FOUND", "selected proposal revision is absent"))?;
    let proposal_id = model::text(&pointer, "proposal_id")?;
    let proposal = meta(
        db,
        &format!("coordination:proposal:{proposal_id}:revision:{proposal_revision_id}"),
    )?
    .ok_or_else(|| Error::new("NOT_FOUND", "selected proposal revision is absent"))?;
    if proposal["thread_id"] != context.thread_id
        || proposal["task_id"] != context.task_id
        || proposal["task_revision"] != context.task_revision
        || proposal["attempt_id"] != context.attempt_id
        || pointer["thread_id"] != context.thread_id
    {
        return Err(Error::new(
            "FORBIDDEN",
            "selected proposal revision is outside this exact Thread scope",
        ));
    }
    let digest = model::text(&proposal, "digest")?;
    if digest.len() != 71
        || !digest.starts_with("sha256:")
        || !digest[7..].bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(Error::new(
            "PROPOSAL_DAMAGED",
            "proposal revision digest is invalid",
        ));
    }
    Ok(proposal)
}

'''
new_validate = r'''fn validate_proposal_revision(
    db: &Connection,
    context: &ThreadContext,
    proposal_revision_id: &str,
) -> Result<()> {
    coordination_store::load_contract_proposal_revision(
        db,
        &context.thread_id,
        &context.task_id,
        context.task_revision,
        &context.attempt_id,
        proposal_revision_id,
    )
    .map(|_| ())
}

'''
threads = replace_once(
    threads,
    old_validate,
    new_validate,
    "remove broken Thread proposal loader",
)
threads_path.write_text(threads, encoding="utf-8")
