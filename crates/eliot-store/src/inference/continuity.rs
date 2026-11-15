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

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use ulid::Ulid;

    const NOW: i64 = 1_800_000_000_000;
    const DAY_MS: i64 = 24 * 60 * 60 * 1_000;

    fn ulid_at(ts_ms: i64, tag: u8) -> String {
        let mut bytes = [0_u8; 16];
        bytes[..6].copy_from_slice(&(ts_ms as u64).to_be_bytes()[2..8]);
        bytes[6..].copy_from_slice(&[tag; 10]);
        Ulid::from_bytes(bytes).to_string()
    }

    fn seeded_store() -> (Store, String, Vec<String>) {
        let store = Store::open(Path::new(":memory:")).unwrap();
        let client = format!("memory:{}", ulid_at(NOW - DAY_MS * 3, 1));
        store
            .connection
            .execute(
                "INSERT INTO inference_clients(client_id,current_version,current_hash,current_file,
                    last_admission_seq,last_write_seq)
                 VALUES (?1,1,'hash','credential.json',3,3)",
                [&client],
            )
            .unwrap();
        let principal = owner(&client);
        let ids = (0..3)
            .map(|n| ulid_at(NOW - DAY_MS * 3 + n, (n + 10) as u8))
            .collect::<Vec<_>>();
        for (index, id) in ids.iter().enumerate() {
            store
                .connection
                .execute(
                    "INSERT INTO writes(id,principal,method,payload_digest,result,created_at,inference_seq)
                     VALUES (?1,?2,'inference.text',?3,'{}',?4,?5)",
                    params![id, principal, vec![0_u8; 32], NOW - DAY_MS * 3, index as i64 + 1],
                )
                .unwrap();
            let state = if index == 0 {
                "released"
            } else if index == 1 {
                "unknown"
            } else {
                "sending"
            };
            store
                .connection
                .execute(
                    "INSERT INTO inference_calls(id,client_id,admission_seq,state,request,route,
                        cleanup_complete)
                     VALUES (?1,?2,?3,?4,'{}','{}',?5)",
                    params![id, client, index as i64 + 1, state, i64::from(index == 0)],
                )
                .unwrap();
        }
        (store, client, ids)
    }

    #[test]
    fn retention_prunes_only_released_cleaned_prefix_and_reports_gap() {
        let (mut store, client, ids) = seeded_store();
        let request = InferenceRetentionRequest {
            id: ulid_at(NOW, 90),
            observed_call_high_water_seq: 3,
            observed_write_high_water_seq: 3,
            safe_retain_after_seq: 3,
            retain_after_seq: 3,
            safe_write_retain_after_seq: 3,
            write_retain_after_seq: 3,
            limit: 1,
        };
        let first = store
            .inference_retention_advance(&client, 1, &request, NOW)
            .unwrap();
        let response: InferenceRetentionResponse = serde_json::from_value(first.clone()).unwrap();
        assert_eq!(response.retain_after_seq, 1);
        assert_eq!(response.write_retain_after_seq, 1);
        assert_eq!(response.pruned_calls, 1);
        assert_eq!(response.pruned_writes, 1);
        assert_eq!(
            store.inference_get(&client, &ids[1]).unwrap().state,
            InferenceState::Unknown
        );
        assert_eq!(
            store.inference_get(&client, &ids[2]).unwrap().state,
            InferenceState::Sending
        );
        let gap = InferenceListRequest {
            call_after_seq: 0,
            call_through_seq: 3,
            write_after_seq: 0,
            write_through_seq: 3,
            limit: 10,
            cursor: None,
        };
        assert_eq!(
            store.inference_list(&client, &gap).unwrap_err().code,
            "history_gap"
        );
        assert_eq!(
            store
                .inference_retention_advance(&client, 1, &request, NOW)
                .unwrap(),
            first
        );
        let write_seq = store
            .connection
            .query_row(
                "SELECT last_write_seq FROM inference_clients WHERE client_id=?1",
                [&client],
                |row| row.get::<_, i64>(0),
            )
            .unwrap();
        let absent_old = ulid_at(NOW - DAY_MS * 2, 44);
        assert_eq!(
            store
                .inference_write_get(&client, &absent_old, NOW)
                .unwrap()
                .status,
            InferenceWriteStatus::HistoryGap
        );
        assert_eq!(
            store
                .connection
                .query_row(
                    "SELECT last_write_seq FROM inference_clients WHERE client_id=?1",
                    [&client],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            write_seq,
            "a read-only history-gap lookup must not first-admit an old id"
        );
    }

    #[test]
    fn inventory_cursor_pins_both_intervals_and_excludes_later_admissions() {
        let (store, client, _) = seeded_store();
        let window = InferenceListRequest {
            call_after_seq: 0,
            call_through_seq: 3,
            write_after_seq: 0,
            write_through_seq: 3,
            limit: 1,
            cursor: None,
        };
        let first = store.inference_list(&client, &window).unwrap();
        assert_eq!(first.calls[0].admission_seq, 1);
        assert_eq!(first.writes[0].write_seq, 1);
        let cursor = first.next.unwrap();

        let later_id = ulid_at(NOW, 70);
        store
            .connection
            .execute(
                "INSERT INTO writes(id,principal,method,payload_digest,result,created_at,inference_seq)
                 VALUES (?1,?2,'inference.text',?3,'{}',?4,4)",
                params![later_id, owner(&client), vec![0_u8; 32], NOW],
            )
            .unwrap();
        store
            .connection
            .execute(
                "INSERT INTO inference_calls(id,client_id,admission_seq,state,request,route)
                 VALUES (?1,?2,4,'succeeded','{}','{}')",
                params![later_id, client],
            )
            .unwrap();
        store
            .connection
            .execute(
                "UPDATE inference_clients SET last_admission_seq=4,last_write_seq=4 WHERE client_id=?1",
                [&client],
            )
            .unwrap();

        let second_request = InferenceListRequest {
            cursor: Some(cursor.clone()),
            ..window.clone()
        };
        let second = store.inference_list(&client, &second_request).unwrap();
        assert_eq!(second.calls[0].admission_seq, 2);
        assert_eq!(second.writes[0].write_seq, 2);

        let mut changed_limit = second_request.clone();
        changed_limit.limit = 2;
        assert_eq!(changed_limit.validate().unwrap_err().code, "invalid");
        let mut changed_window = second_request.clone();
        changed_window.call_through_seq = 4;
        assert_eq!(changed_window.validate().unwrap_err().code, "invalid");
        let mut changed_client = second_request;
        changed_client.cursor.as_mut().unwrap().client_id = format!("memory:{}", ulid_at(NOW, 71));
        assert_eq!(
            store
                .inference_list(&client, &changed_client)
                .unwrap_err()
                .code,
            "invalid"
        );

        let third = store
            .inference_list(
                &client,
                &InferenceListRequest {
                    cursor: second.next,
                    ..window
                },
            )
            .unwrap();
        assert_eq!(third.calls[0].admission_seq, 3);
        assert_eq!(third.writes[0].write_seq, 3);
        assert!(third.next.is_none());
    }

    #[test]
    fn write_retention_keeps_live_ids_and_only_advances_a_complete_prefix() {
        let (mut store, client, _) = seeded_store();
        store
            .connection
            .execute(
                "UPDATE inference_calls SET state='released',cleanup_complete=1 WHERE client_id=?1",
                [&client],
            )
            .unwrap();
        let live_id = ulid_at(NOW, 80);
        let principal = owner(&client);
        let live_result = serde_json::to_string(&InferenceRetentionResponse {
            retain_after_seq: 0,
            write_retain_after_seq: 0,
            pruned_calls: 0,
            pruned_writes: 0,
        })
        .unwrap();
        store
            .connection
            .execute(
                "INSERT INTO writes(id,principal,method,payload_digest,result,created_at,inference_seq)
                 VALUES (?1,?2,'inference.retention.advance',?3,?4,?5,4)",
                params![live_id, principal, vec![0_u8; 32], live_result, NOW],
            )
            .unwrap();
        store
            .connection
            .execute(
                "UPDATE inference_clients SET last_write_seq=4 WHERE client_id=?1",
                [&client],
            )
            .unwrap();
        let request = InferenceRetentionRequest {
            id: ulid_at(NOW, 81),
            observed_call_high_water_seq: 3,
            observed_write_high_water_seq: 4,
            safe_retain_after_seq: 3,
            retain_after_seq: 3,
            safe_write_retain_after_seq: 4,
            write_retain_after_seq: 4,
            limit: 10,
        };
        let result = store
            .inference_retention_advance(&client, 1, &request, NOW)
            .unwrap();
        let response: InferenceRetentionResponse = serde_json::from_value(result).unwrap();
        assert_eq!(response.retain_after_seq, 3);
        assert_eq!(response.write_retain_after_seq, 3);
        assert_eq!(
            store
                .inference_write_get(&client, &live_id, NOW)
                .unwrap()
                .status,
            InferenceWriteStatus::Retained
        );
    }

    #[test]
    fn retention_preserves_pre_rotation_calls_until_previous_credential_is_retired() {
        let (mut store, client, ids) = seeded_store();
        store
            .connection
            .execute(
                "UPDATE inference_calls SET state='released',cleanup_complete=1 WHERE client_id=?1",
                [&client],
            )
            .unwrap();
        store
            .connection
            .execute(
                "UPDATE inference_clients SET current_version=2,previous_version=1,rotation_seq=3 WHERE client_id=?1",
                [&client],
            )
            .unwrap();
        let first = InferenceRetentionRequest {
            id: ulid_at(NOW, 91),
            observed_call_high_water_seq: 3,
            observed_write_high_water_seq: 3,
            safe_retain_after_seq: 3,
            retain_after_seq: 3,
            safe_write_retain_after_seq: 3,
            write_retain_after_seq: 3,
            limit: 10,
        };
        let result: InferenceRetentionResponse = serde_json::from_value(
            store
                .inference_retention_advance(&client, 2, &first, NOW)
                .unwrap(),
        )
        .unwrap();
        assert_eq!(result.retain_after_seq, 0);
        assert_eq!(result.pruned_calls, 0);
        assert!(store.inference_get(&client, &ids[0]).is_ok());

        store
            .connection
            .execute(
                "UPDATE inference_clients SET previous_version=NULL WHERE client_id=?1",
                [&client],
            )
            .unwrap();
        let second = InferenceRetentionRequest {
            id: ulid_at(NOW, 92),
            observed_call_high_water_seq: 3,
            observed_write_high_water_seq: 4,
            safe_retain_after_seq: 3,
            retain_after_seq: 3,
            safe_write_retain_after_seq: 3,
            write_retain_after_seq: 3,
            limit: 10,
        };
        let result: InferenceRetentionResponse = serde_json::from_value(
            store
                .inference_retention_advance(&client, 2, &second, NOW)
                .unwrap(),
        )
        .unwrap();
        assert_eq!(result.retain_after_seq, 3);
        assert_eq!(result.pruned_calls, 3);
    }
}
