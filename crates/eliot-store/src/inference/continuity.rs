//! Fixed-window inventory and bounded inference retention.
//!
//! The inventory uses per-client admission and write sequences. A cursor pins
//! both requested intervals and their query version; a moved retention floor
//! invalidates any unread part of that window with `history_gap`.

use super::*;

const INVENTORY_QUERY_VERSION: u32 = INFERENCE_INVENTORY_QUERY_VERSION;
const RETENTION_METHOD: &str = "inference.retention.advance";

fn inference_id_expired(id: &str, expired_through: i64) -> bool {
    ulid_timestamp_ms(id).is_some_and(|timestamp| timestamp <= expired_through)
}

impl Store {
    pub fn inference_high_water(&self, client: &str) -> Result<InferenceHighWaterResponse> {
        inference_client(client)?;
        self.ensure_committed_reads()?;
        self.connection
            .query_row(
                "SELECT last_admission_seq,retain_after_seq,last_write_seq,write_retain_after_seq
                 FROM inference_clients WHERE client_id=?1",
                [client],
                |row| {
                    Ok(InferenceHighWaterResponse {
                        client_id: client.to_owned(),
                        call_high_water_seq: row.get(0)?,
                        retain_after_seq: row.get(1)?,
                        write_high_water_seq: row.get(2)?,
                        write_retain_after_seq: row.get(3)?,
                    })
                },
            )
            .optional()
            .map_err(sql_error)?
            .ok_or_else(|| Error::new("not_allowed", "inference client is absent"))
    }

    pub fn inference_list(
        &self,
        client: &str,
        request: &InferenceListRequest,
    ) -> Result<InferenceListResponse> {
        request.validate()?;
        inference_client(client)?;
        self.ensure_committed_reads()?;
        let (call_hi, call_floor, write_hi, write_floor): (i64, i64, i64, i64) = self
            .connection
            .query_row(
                "SELECT last_admission_seq,retain_after_seq,last_write_seq,write_retain_after_seq
                 FROM inference_clients WHERE client_id=?1",
                [client],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .optional()
            .map_err(sql_error)?
            .ok_or_else(|| Error::new("not_allowed", "inference client is absent"))?;
        if request.call_through_seq > call_hi || request.write_through_seq > write_hi {
            return Err(Error::new(
                "invalid",
                "inference inventory bounds exceed the observed high-water",
            ));
        }

        let (after_call, after_write) = if let Some(cursor) = &request.cursor {
            if cursor.client_id != client || cursor.query_version != INVENTORY_QUERY_VERSION {
                return Err(Error::new(
                    "invalid",
                    "inference inventory cursor belongs to another client or query version",
                ));
            }
            (cursor.last_call_seq, cursor.last_write_seq)
        } else {
            (request.call_after_seq, request.write_after_seq)
        };
        // The cursor always carries the original interval start. If either
        // floor moved past that start, the fixed window can no longer promise
        // complete membership, even if this particular page looked empty.
        if request.call_after_seq < call_floor || request.write_after_seq < write_floor {
            return Err(Error::new(
                "history_gap",
                "inference inventory window crosses a pruned history floor",
            ));
        }

        let mut calls = Vec::new();
        let call_ids = {
            let mut statement = self
                .connection
                .prepare(
                    "SELECT id FROM inference_calls
                     WHERE client_id=?1 AND admission_seq>?2 AND admission_seq<=?3
                     ORDER BY admission_seq LIMIT ?4",
                )
                .map_err(sql_error)?;
            let rows = statement
                .query_map(
                    params![
                        client,
                        after_call,
                        request.call_through_seq,
                        request.limit as i64 + 1
                    ],
                    |row| row.get::<_, String>(0),
                )
                .map_err(sql_error)?;
            rows.collect::<std::result::Result<Vec<_>, _>>()
                .map_err(sql_error)?
        };
        let more_calls = call_ids.len() > request.limit;
        for id in call_ids.into_iter().take(request.limit) {
            calls.push(call(&self.connection, client, &id)?);
        }

        let mut writes = Vec::new();
        let mut statement = self
            .connection
            .prepare(
                "SELECT inference_seq,id,method,payload_digest,created_at FROM writes
                 WHERE principal=?1 AND inference_seq>?2 AND inference_seq<=?3
                 ORDER BY inference_seq LIMIT ?4",
            )
            .map_err(sql_error)?;
        let rows = statement
            .query_map(
                params![
                    owner(client),
                    after_write,
                    request.write_through_seq,
                    request.limit as i64 + 1
                ],
                |row| {
                    let digest: Vec<u8> = row.get(3)?;
                    Ok(InferenceWriteInventoryItem {
                        write_seq: row.get(0)?,
                        id: row.get(1)?,
                        method: row.get(2)?,
                        payload_sha256: digest.iter().map(|b| format!("{b:02x}")).collect(),
                        created_at: row.get(4)?,
                    })
                },
            )
            .map_err(sql_error)?;
        for row in rows.take(request.limit + 1) {
            writes.push(row.map_err(sql_error)?);
        }
        let more_writes = writes.len() > request.limit;
        writes.truncate(request.limit);

        let next = if more_calls || more_writes {
            Some(InferenceInventoryCursor {
                client_id: client.to_owned(),
                call_after_seq: request.call_after_seq,
                call_through_seq: request.call_through_seq,
                write_after_seq: request.write_after_seq,
                write_through_seq: request.write_through_seq,
                limit: request.limit,
                query_version: INVENTORY_QUERY_VERSION,
                last_call_seq: calls.last().map_or(after_call, |row| row.admission_seq),
                last_write_seq: writes.last().map_or(after_write, |row| row.write_seq),
            })
        } else {
            None
        };
        Ok(InferenceListResponse {
            client_id: client.to_owned(),
            calls,
            writes,
            next,
        })
    }

    /// Advance both retention cuts and prune a bounded batch in the ordinary
    /// writes namespace. Exact replay is checked before credentials, age, or
    /// cut gates so a lost reply cannot repeat pruning under a new binding.
    pub fn inference_retention_advance(
        &mut self,
        client: &str,
        version: i64,
        request: &InferenceRetentionRequest,
        now: i64,
    ) -> Result<Value> {
        request.validate()?;
        inference_client(client)?;
        let payload = serde_json::to_value(request).map_err(json_error)?;
        self.inference_transaction(|tx| {
            if let Some(value) = replay(tx, &request.id, &owner(client), RETENTION_METHOD, &payload)? {
                return serde_json::from_value(value).map_err(json_error);
            }
            current(tx, client, version)?;
            validate_inference_id_age(tx, client, &request.id, now)?;
            let (call_hi, call_floor, write_hi, write_floor, expired_through): (i64, i64, i64, i64, i64) = tx
                .query_row(
                    "SELECT last_admission_seq,retain_after_seq,last_write_seq,write_retain_after_seq,expired_id_through_ms
                     FROM inference_clients WHERE client_id=?1",
                    [client],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
                )
                .map_err(sql_error)?;
            // Expiry is inclusive at millisecond precision, while admission
            // permits IDs exactly 24 hours old. Commit this nondecreasing
            // boundary with pruning so clock rollback cannot revive an ID.
            let expired_through = expired_through.max(now.saturating_sub(RETRY_WINDOW_MS).saturating_sub(1).max(-1));
            if request.observed_call_high_water_seq > call_hi
                || request.observed_write_high_water_seq > write_hi
                || request.retain_after_seq < call_floor
                || request.write_retain_after_seq < write_floor
            {
                return Err(Error::new(
                    "invalid",
                    "inference retention request is stale or moves a floor backward",
                ));
            }

            // A cut cannot pass the first nonterminal or uncleaned call. Rows
            // below the new cut may remain until a later bounded pass, but no
            // row above it can be missing from an inventory page.
            let blocking_call: Option<i64> = tx
                .query_row(
                    "SELECT MIN(admission_seq) FROM inference_calls
                     WHERE client_id=?1 AND admission_seq>?2 AND admission_seq<=?3
                       AND (NOT(state='released' AND cleanup_complete=1)
                         OR EXISTS(SELECT 1 FROM inference_clients c
                           WHERE c.client_id=inference_calls.client_id
                             AND (c.pending_version IS NOT NULL OR c.previous_version IS NOT NULL)
                             AND inference_calls.admission_seq<=c.rotation_seq))",
                    params![client, call_floor, request.retain_after_seq],
                    |row| row.get(0),
                )
                .map_err(sql_error)?;
            let new_call_floor = blocking_call
                .map_or(request.retain_after_seq, |seq| seq.saturating_sub(1));

            let call_ids = {
                let mut statement = tx
                    .prepare(
                        "SELECT id FROM inference_calls
                         WHERE client_id=?1 AND admission_seq<=?2
                           AND state='released' AND cleanup_complete=1
                           AND NOT EXISTS(SELECT 1 FROM inference_clients c
                             WHERE c.client_id=inference_calls.client_id
                               AND (c.pending_version IS NOT NULL OR c.previous_version IS NOT NULL)
                               AND inference_calls.admission_seq<=c.rotation_seq)
                         ORDER BY admission_seq LIMIT ?3",
                    )
                    .map_err(sql_error)?;
                let rows = statement
                    .query_map(
                        params![client, new_call_floor, request.limit as i64],
                        |row| row.get::<_, String>(0),
                    )
                    .map_err(sql_error)?;
                rows.collect::<std::result::Result<Vec<_>, _>>()
                    .map_err(sql_error)?
            };
            let mut pruned_calls = 0usize;
            for id in call_ids {
                pruned_calls += tx
                    .execute(
                        "DELETE FROM inference_calls WHERE id=?1 AND client_id=?2
                         AND state='released' AND cleanup_complete=1",
                        params![id, client],
                    )
                    .map_err(sql_error)?;
            }

            // Preserve every write still inside the live mutation-ID window,
            // and every write referenced by a retained call. Validate the ID's
            // ULID timestamp with the same age rule as admission; created_at
            // alone cannot establish whether a retry ID has expired.
            let scanned_writes = {
                let mut statement = tx
                    .prepare(
                        "SELECT w.inference_seq,w.id,
                           EXISTS(SELECT 1 FROM inference_calls c
                             WHERE c.id=w.id OR c.cancel_id=w.id OR c.release_id=w.id)
                         FROM writes w
                         WHERE w.principal=?1 AND w.inference_seq>?2 AND w.inference_seq<=?3
                         ORDER BY w.inference_seq LIMIT ?4",
                    )
                    .map_err(sql_error)?;
                let rows = statement
                    .query_map(
                        params![
                            owner(client),
                            write_floor,
                            request.write_retain_after_seq,
                            request.limit as i64 + 1
                        ],
                        |row| {
                            Ok((
                                row.get::<_, i64>(0)?,
                                row.get::<_, String>(1)?,
                                row.get::<_, bool>(2)?,
                            ))
                        },
                    )
                    .map_err(sql_error)?;
                rows.collect::<std::result::Result<Vec<_>, _>>()
                    .map_err(sql_error)?
            };
            let mut new_write_floor = request.write_retain_after_seq;
            for (index, (seq, id, referenced)) in scanned_writes.iter().enumerate() {
                if *referenced || !inference_id_expired(id, expired_through) {
                    new_write_floor = seq.saturating_sub(1);
                    break;
                }
                if index + 1 == request.limit {
                    new_write_floor = *seq;
                    break;
                }
            }
            if scanned_writes.len() <= request.limit
                && scanned_writes.iter().all(|(_, id, referenced)| {
                    !*referenced && inference_id_expired(id, expired_through)
                })
            {
                new_write_floor = request.write_retain_after_seq;
            }

            let write_ids = {
                let mut statement = tx
                    .prepare(
                        "SELECT w.id FROM writes w
                         WHERE w.principal=?1 AND w.inference_seq<=?2
                           AND NOT EXISTS(SELECT 1 FROM inference_calls c
                             WHERE c.id=w.id OR c.cancel_id=w.id OR c.release_id=w.id)
                         ORDER BY w.inference_seq LIMIT ?3",
                    )
                    .map_err(sql_error)?;
                let rows = statement
                    .query_map(
                        params![owner(client), new_write_floor, request.limit as i64],
                        |row| row.get::<_, String>(0),
                    )
                    .map_err(sql_error)?;
                rows.collect::<std::result::Result<Vec<_>, _>>()
                    .map_err(sql_error)?
            };
            let mut pruned_writes = 0usize;
            for id in write_ids
                .into_iter()
                .filter(|id| inference_id_expired(id, expired_through))
            {
                pruned_writes += tx
                    .execute(
                        "DELETE FROM writes WHERE id=?1 AND principal=?2 AND inference_seq<=?3",
                        params![id, owner(client), new_write_floor],
                    )
                    .map_err(sql_error)?;
            }

            let retention_receipts: i64 = tx.query_row(
                "SELECT COUNT(*) FROM writes WHERE principal=?1 AND method=?2",
                params![owner(client), RETENTION_METHOD],
                |row| row.get(0),
            ).map_err(sql_error)?;
            if retention_receipts >= MAX_INFERENCE_RETENTION_RECEIPTS {
                return Err(Error::new("capacity", "inference retention receipt capacity reached; reconcile release and retention"));
            }
            db!(tx.execute(
                "UPDATE inference_clients SET retain_after_seq=?2,write_retain_after_seq=?3,expired_id_through_ms=?4
                 WHERE client_id=?1",
                params![client, new_call_floor, new_write_floor, expired_through],
            ));
            let result = InferenceRetentionResponse {
                retain_after_seq: new_call_floor,
                write_retain_after_seq: new_write_floor,
                pruned_calls,
                pruned_writes,
            };
            let value = serde_json::to_value(&result).map_err(json_error)?;
            write(tx, &request.id, &owner(client), RETENTION_METHOD, &payload, &value, now)?;
            Ok(value)
        })
    }
}
