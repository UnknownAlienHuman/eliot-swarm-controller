use super::*;

pub(super) const CURRENT_SCHEMA_VERSION: i64 = 57;

impl Store {
    pub(super) fn migrate(&mut self) -> Result<()> {
        self.connection
            .execute_batch(
                "CREATE TABLE IF NOT EXISTS schema_migrations (
                     version INTEGER PRIMARY KEY
                 );",
            )
            .map_err(sql_error)?;
        let current: i64 = self
            .connection
            .query_row(
                "SELECT COALESCE(MAX(version), 0) FROM schema_migrations",
                [],
                |row| row.get(0),
            )
            .map_err(sql_error)?;
        if current > CURRENT_SCHEMA_VERSION {
            return Err(Error::new(
                "unsupported",
                format!("Store schema version {current} is newer than this binary"),
            ));
        }
        if current == 0 {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            tx.execute_batch(
                "CREATE TABLE agents (
                 id INTEGER PRIMARY KEY AUTOINCREMENT,
                 generation INTEGER NOT NULL DEFAULT 0,
                 name TEXT UNIQUE,
                 project TEXT NOT NULL,
                 harness TEXT NOT NULL,
                 profile TEXT NOT NULL,
                 workspace TEXT,
                 token TEXT UNIQUE,
                 state TEXT NOT NULL,
                 active_turn TEXT,
                 created_at INTEGER NOT NULL,
                 updated_at INTEGER NOT NULL
             );
             CREATE INDEX agents_state ON agents(state);

             CREATE TABLE events (
                 seq INTEGER PRIMARY KEY AUTOINCREMENT,
                 ts INTEGER NOT NULL,
                 kind TEXT NOT NULL,
                 family TEXT,
                 agent INTEGER,
                 generation INTEGER,
                 source TEXT NOT NULL,
                 native_id TEXT,
                 cursor TEXT,
                 payload_digest BLOB,
                 data TEXT NOT NULL,
                 CHECK(payload_digest IS NULL OR length(payload_digest) = 32)
             );
             CREATE INDEX events_agent ON events(agent, seq);
             CREATE INDEX events_kind ON events(kind, seq);
             CREATE UNIQUE INDEX events_native
                 ON events(agent, generation, source, native_id)
                 WHERE native_id IS NOT NULL;

             CREATE TABLE commands (
                 id TEXT PRIMARY KEY,
                 principal TEXT NOT NULL,
                 method TEXT NOT NULL,
                 payload_digest BLOB NOT NULL,
                 target INTEGER REFERENCES agents(id),
                 expected_generation INTEGER,
                 generation INTEGER,
                 expected_turn_id TEXT,
                 parent_command TEXT REFERENCES commands(id),
                 link_kind TEXT,
                 ordinal INTEGER,
                 turn_id TEXT,
                 verb TEXT NOT NULL,
                 args TEXT NOT NULL,
                 state TEXT NOT NULL,
                 outcome TEXT,
                 created_at INTEGER NOT NULL,
                 updated_at INTEGER NOT NULL,
                 CHECK((parent_command IS NULL AND link_kind IS NULL AND ordinal IS NULL)
                    OR (parent_command IS NOT NULL AND link_kind IS NOT NULL
                        AND ordinal IS NOT NULL AND ordinal >= 0))
             );
             CREATE INDEX commands_target ON commands(target, created_at);
             CREATE INDEX commands_open ON commands(state)
                 WHERE state IN ('accepted', 'queued', 'dispatched', 'unknown');
             CREATE UNIQUE INDEX commands_children
                 ON commands(parent_command, ordinal)
                 WHERE parent_command IS NOT NULL;

             CREATE TABLE turns (
                 agent INTEGER NOT NULL REFERENCES agents(id),
                 generation INTEGER NOT NULL,
                 turn_id TEXT NOT NULL,
                 state TEXT NOT NULL,
                 status TEXT,
                 final_text TEXT,
                 started_at INTEGER,
                 completed_at INTEGER,
                 PRIMARY KEY(agent, generation, turn_id)
             );

             CREATE TABLE source_owners (
                 agent INTEGER NOT NULL REFERENCES agents(id),
                 generation INTEGER NOT NULL,
                 family TEXT NOT NULL,
                 source TEXT NOT NULL,
                 mechanism TEXT NOT NULL,
                 PRIMARY KEY(agent, generation, family)
             );
             CREATE TABLE source_state (
                 agent INTEGER NOT NULL REFERENCES agents(id),
                 generation INTEGER NOT NULL,
                 source TEXT NOT NULL,
                 cursor TEXT,
                 last_store_seq INTEGER,
                 last_batch_id TEXT,
                 last_batch_digest BLOB,
                 last_batch_first_seq INTEGER,
                 last_batch_last_seq INTEGER,
                 state TEXT NOT NULL CHECK(state IN ('open', 'closed')),
                 health TEXT NOT NULL CHECK(health IN ('ok', 'slow', 'stale', 'error')),
                 updated_at INTEGER NOT NULL,
                 PRIMARY KEY(agent, generation, source),
                 CHECK(last_batch_digest IS NULL OR length(last_batch_digest) = 32)
             );

             CREATE TABLE writes (
                 id TEXT PRIMARY KEY,
                 principal TEXT NOT NULL,
                 method TEXT NOT NULL,
                 payload_digest BLOB NOT NULL,
                 result TEXT NOT NULL,
                 created_at INTEGER NOT NULL
             );
             INSERT INTO schema_migrations(version) VALUES (1);",
            )
            .map_err(sql_error)?;
            tx.commit().map_err(sql_error)?;
        }
        if current < 2 {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            tx.execute_batch(
                "ALTER TABLE agents ADD COLUMN profile_snapshot TEXT NOT NULL DEFAULT '{}';
                 ALTER TABLE agents ADD COLUMN native_scope TEXT;
                 ALTER TABLE agents ADD COLUMN native_ref TEXT;
                 ALTER TABLE agents ADD COLUMN effective TEXT;
                 CREATE UNIQUE INDEX agents_native
                     ON agents(native_scope, native_ref)
                     WHERE native_scope IS NOT NULL AND native_ref IS NOT NULL;
                 ALTER TABLE source_state ADD COLUMN gap_id TEXT;
                 ALTER TABLE source_state ADD COLUMN gap_digest BLOB
                     CHECK(gap_digest IS NULL OR length(gap_digest) = 32);
                 ALTER TABLE source_state ADD COLUMN gap_from TEXT;
                 ALTER TABLE source_state ADD COLUMN gap_to TEXT;
                 ALTER TABLE source_state ADD COLUMN gap_seq INTEGER;
                 INSERT INTO schema_migrations(version) VALUES (2);",
            )
            .map_err(sql_error)?;
            tx.commit().map_err(sql_error)?;
        }
        if current < 3 {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            tx.execute_batch(
                "ALTER TABLE agents ADD COLUMN pending TEXT NOT NULL DEFAULT '[]';
                 ALTER TABLE commands ADD COLUMN request_id TEXT;
                 CREATE INDEX commands_request_open
                     ON commands(target, generation, request_id, state)
                     WHERE request_id IS NOT NULL;
                 CREATE TABLE pending_requests (
                     agent INTEGER NOT NULL REFERENCES agents(id),
                     generation INTEGER NOT NULL,
                     request_id TEXT NOT NULL,
                     turn_id TEXT NOT NULL,
                     kind TEXT NOT NULL,
                     details TEXT NOT NULL,
                     state TEXT NOT NULL CHECK(state IN ('pending', 'resolved', 'stale')),
                     PRIMARY KEY(agent, generation, request_id)
                 );
                 CREATE INDEX pending_requests_open
                     ON pending_requests(agent, generation, turn_id, request_id)
                     WHERE state = 'pending';
                 INSERT INTO schema_migrations(version) VALUES (3);",
            )
            .map_err(sql_error)?;
            tx.commit().map_err(sql_error)?;
        }
        if current < 4 {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            tx.execute_batch(
                "CREATE TABLE processes (
                     host_boot_id TEXT NOT NULL CHECK(length(host_boot_id) > 0),
                     pid INTEGER NOT NULL CHECK(pid > 0 AND pid <= 4294967295),
                     start_time INTEGER NOT NULL CHECK(start_time >= 0),
                     owner TEXT NOT NULL,
                     role TEXT NOT NULL,
                     state TEXT NOT NULL CHECK(state IN ('running', 'exited', 'unknown')),
                     exit_code INTEGER,
                     updated_at INTEGER NOT NULL,
                     PRIMARY KEY(host_boot_id, pid, start_time),
                     CHECK(exit_code IS NULL OR state = 'exited')
                 );
                 CREATE INDEX processes_owner ON processes(owner, state);
                 INSERT INTO schema_migrations(version) VALUES (4);",
            )
            .map_err(sql_error)?;
            tx.commit().map_err(sql_error)?;
        }
        if current < 5 {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            tx.execute_batch(
                "CREATE TABLE agent_edits (
                     agent INTEGER NOT NULL,
                     generation INTEGER NOT NULL,
                     workspace TEXT NOT NULL,
                     path TEXT NOT NULL,
                     updated_at INTEGER NOT NULL,
                     PRIMARY KEY(agent, generation, path)
                 );
                 CREATE INDEX agent_edits_workspace ON agent_edits(workspace, path);
                 CREATE TABLE git_workspaces (
                     workspace TEXT PRIMARY KEY,
                     head TEXT,
                     updated_at INTEGER NOT NULL
                 );
                 INSERT INTO schema_migrations(version) VALUES (5);",
            )
            .map_err(sql_error)?;
            tx.commit().map_err(sql_error)?;
        }
        if current < 6 {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            tx.execute_batch(
                "CREATE TABLE workspaces (
                     agent INTEGER PRIMARY KEY REFERENCES agents(id),
                     target_generation INTEGER NOT NULL CHECK(target_generation > 0),
                     start_command_id TEXT NOT NULL REFERENCES commands(id),
                     project TEXT NOT NULL, repo_path TEXT NOT NULL, repo_common_dir TEXT NOT NULL,
                     requested_branch TEXT NOT NULL, base_ref TEXT, base_oid TEXT NOT NULL,
                     worktree_path TEXT NOT NULL UNIQUE, frozen_setup TEXT NOT NULL,
                     phase TEXT NOT NULL CHECK(phase IN ('creating','setup_ready','setup_running','ready','unknown','failed','removing','removed')),
                     setup_index INTEGER NOT NULL DEFAULT 0 CHECK(setup_index >= 0),
                     outcome TEXT, updated_at INTEGER NOT NULL
                 );
                 INSERT INTO schema_migrations(version) VALUES (6);",
            ).map_err(sql_error)?;
            tx.commit().map_err(sql_error)?;
        }
        if current < 7 {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            tx.execute_batch(
                "ALTER TABLE agents ADD COLUMN origin TEXT NOT NULL DEFAULT 'eliot' CHECK(origin IN ('eliot','native','external'));
                 ALTER TABLE agents ADD COLUMN observe_only INTEGER NOT NULL DEFAULT 0
                     CHECK(observe_only IN (0,1) AND (state <> 'unknown' OR observe_only = 1) AND (observe_only = 0 OR token IS NULL));
                 INSERT INTO schema_migrations(version) VALUES (7);",
            ).map_err(sql_error)?;
            tx.commit().map_err(sql_error)?;
        }
        if current < 8 {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            tx.execute_batch(
                "ALTER TABLE source_state ADD COLUMN last_handoff_digest BLOB
                     CHECK(last_handoff_digest IS NULL OR length(last_handoff_digest) = 32);
                 ALTER TABLE source_state ADD COLUMN last_handoff TEXT
                     CHECK((last_handoff IS NULL) = (last_handoff_digest IS NULL));
                 INSERT INTO schema_migrations(version) VALUES (8);",
            )
            .map_err(sql_error)?;
            tx.commit().map_err(sql_error)?;
        }
        if current < 9 {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            tx.execute_batch(
                "ALTER TABLE agents ADD COLUMN capacity_generation INTEGER
                     CHECK(capacity_generation IS NULL OR capacity_generation > 0);
                 CREATE INDEX agents_capacity_reserved ON agents(capacity_generation)
                     WHERE capacity_generation IS NOT NULL;",
            )
            .map_err(sql_error)?;
            capacity::migrate_reservations_tx(&tx)?;
            tx.execute("INSERT INTO schema_migrations(version) VALUES (9)", [])
                .map_err(sql_error)?;
            tx.commit().map_err(sql_error)?;
        }
        if current < 10 {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            tx.execute_batch(
                "ALTER TABLE commands ADD COLUMN probe INTEGER NOT NULL DEFAULT 0
                     CHECK(probe IN (0,1));
                 CREATE TABLE provider_holds (
                     scope TEXT PRIMARY KEY CHECK(length(scope) > 0),
                     kind TEXT NOT NULL CHECK(kind IN ('rate_limit','quota','auth','model_removed')),
                     retry_after INTEGER CHECK(retry_after IS NULL OR retry_after >= 0),
                     probe_command TEXT REFERENCES commands(id),
                     state TEXT NOT NULL CHECK(state IN ('held','probing')),
                     updated_at INTEGER NOT NULL,
                     CHECK((state = 'held' AND probe_command IS NULL)
                        OR (state = 'probing' AND probe_command IS NOT NULL))
                 );
                 INSERT INTO schema_migrations(version) VALUES (10);",
            )
            .map_err(sql_error)?;
            tx.commit().map_err(sql_error)?;
        }
        if current < 11 {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            tx.execute_batch(
                "ALTER TABLE events ADD COLUMN subject TEXT;
                 CREATE TABLE rule_state (
                     principal TEXT NOT NULL,
                     rule TEXT NOT NULL,
                     revision INTEGER NOT NULL CHECK(revision >= 0),
                     current_definition TEXT NOT NULL,
                     definition_digest BLOB NOT NULL CHECK(length(definition_digest) = 32),
                     current_cursor INTEGER NOT NULL CHECK(current_cursor >= 0),
                     enabled INTEGER NOT NULL CHECK(enabled IN (0,1)),
                     paused_reason TEXT,
                     updated_at INTEGER NOT NULL,
                     PRIMARY KEY(principal, rule)
                 );
                 CREATE INDEX commands_rule_action_window ON commands(created_at)
                     WHERE link_kind = 'rule_action';
                 INSERT INTO schema_migrations(version) VALUES (11);",
            )
            .map_err(sql_error)?;
            tx.commit().map_err(sql_error)?;
        }
        if current < 12 {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            tx.execute_batch(
                "CREATE TABLE timers (
                     principal TEXT NOT NULL,
                     rule TEXT NOT NULL,
                     schedule_revision INTEGER NOT NULL CHECK(schedule_revision > 0),
                     trigger TEXT NOT NULL,
                     anchor INTEGER NOT NULL,
                     last_key INTEGER,
                     next_due_at INTEGER,
                     catch_up TEXT NOT NULL DEFAULT 'none'
                         CHECK(catch_up IN ('latest','none')),
                     PRIMARY KEY(principal, rule),
                     FOREIGN KEY(principal, rule) REFERENCES rule_state(principal, rule)
                         ON DELETE CASCADE
                 );
                 CREATE INDEX timers_due ON timers(next_due_at, principal, rule)
                     WHERE next_due_at IS NOT NULL;
                 INSERT INTO schema_migrations(version) VALUES (12);",
            )
            .map_err(sql_error)?;
            tx.commit().map_err(sql_error)?;
        }
        if current < 13 {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            tx.execute_batch(
                "ALTER TABLE events ADD COLUMN cause TEXT;
                 ALTER TABLE commands ADD COLUMN cause TEXT;
                 ALTER TABLE rule_state ADD COLUMN retiring_revision INTEGER
                     CHECK(retiring_revision IS NULL OR retiring_revision > 0);
                 ALTER TABLE rule_state ADD COLUMN retiring_definition TEXT;
                 ALTER TABLE rule_state ADD COLUMN retiring_cursor INTEGER
                     CHECK(retiring_cursor IS NULL OR retiring_cursor >= 0);
                 ALTER TABLE rule_state ADD COLUMN retire_through INTEGER
                     CHECK(retire_through IS NULL OR retire_through >= 0);
                 INSERT INTO schema_migrations(version) VALUES (13);",
            )
            .map_err(sql_error)?;
            tx.commit().map_err(sql_error)?;
        }
        if current < 14 {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            tx.execute_batch(
                "ALTER TABLE commands ADD COLUMN project_binding TEXT;
                 CREATE TABLE project_registrations (
                     revision INTEGER PRIMARY KEY AUTOINCREMENT,
                     id TEXT NOT NULL UNIQUE,
                     checkout TEXT NOT NULL,
                     repo_common_dir TEXT NOT NULL,
                     profiles TEXT NOT NULL,
                     actions TEXT NOT NULL
                 );
                 CREATE TABLE rule_history_coverage (
                     singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                     pruned_through INTEGER NOT NULL CHECK(pruned_through >= 0)
                 );
                 INSERT INTO rule_history_coverage(singleton, pruned_through)
                     VALUES (1, 0);
                 INSERT INTO schema_migrations(version) VALUES (14);",
            )
            .map_err(sql_error)?;
            tx.commit().map_err(sql_error)?;
        }
        if current < 15 {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            tx.execute_batch(
                "ALTER TABLE project_registrations
                     ADD COLUMN interpreters TEXT NOT NULL DEFAULT '{}';
                 ALTER TABLE project_registrations
                     ADD COLUMN run_env TEXT NOT NULL DEFAULT '[]';
                 INSERT INTO schema_migrations(version) VALUES (15);",
            )
            .map_err(sql_error)?;
            tx.commit().map_err(sql_error)?;
        }
        if current < 16 {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            tx.execute_batch(
                "CREATE INDEX commands_git_fetch_open_repo ON commands(
                     json_extract(args, '$.common_dir')
                 ) WHERE verb = 'git.fetch'
                     AND state IN ('accepted','queued','dispatched','unknown');
                 ALTER TABLE project_registrations ADD COLUMN git TEXT;
                 CREATE INDEX workspaces_git_repo ON workspaces(repo_common_dir);
                 INSERT INTO schema_migrations(version) VALUES (16);",
            )
            .map_err(sql_error)?;
            tx.commit().map_err(sql_error)?;
        }
        if current < 17 {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            capacity::create_store_identity_tx(&tx)?;
            db!(tx.execute("INSERT INTO schema_migrations(version) VALUES (17)", []));
            tx.commit().map_err(sql_error)?;
        }
        if current < 18 {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            tx.execute_batch(
                "CREATE INDEX commands_unsettled_created ON commands(created_at)
                     WHERE state IN ('accepted', 'queued', 'dispatched', 'unknown');
                 DROP INDEX commands_open;
                 CREATE INDEX processes_owner_identity
                     ON processes(owner, host_boot_id, pid, start_time);
                 INSERT INTO schema_migrations(version) VALUES (18);",
            )
            .map_err(sql_error)?;
            tx.commit().map_err(sql_error)?;
        }
        if current < 19 {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            tx.execute_batch(
                "CREATE TABLE goals (
                     agent INTEGER NOT NULL,
                     mode TEXT NOT NULL,
                     generation INTEGER NOT NULL,
                     revision INTEGER NOT NULL,
                     definition TEXT NOT NULL,
                     state TEXT NOT NULL,
                     last_round_key TEXT,
                     updated_at INTEGER NOT NULL,
                     PRIMARY KEY(agent, mode)
                 );
                 CREATE TABLE goal_markers (
                     agent INTEGER NOT NULL,
                     mode TEXT NOT NULL,
                     generation INTEGER NOT NULL,
                     revision INTEGER NOT NULL,
                     round_key INTEGER NOT NULL,
                     holder TEXT NOT NULL,
                     state TEXT NOT NULL CHECK(state IN ('running', 'unknown')),
                     updated_at INTEGER NOT NULL,
                     PRIMARY KEY(agent, mode, generation, revision, round_key)
                 );
                 INSERT INTO schema_migrations(version) VALUES (19);",
            )
            .map_err(sql_error)?;
            tx.commit().map_err(sql_error)?;
        }
        if current < 20 {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            tx.execute_batch(
                "ALTER TABLE goal_markers
                     ADD COLUMN round_key_kind TEXT NOT NULL DEFAULT 'idle'
                     CHECK(round_key_kind IN ('idle', 'slot'));
                 INSERT INTO schema_migrations(version) VALUES (20);",
            )
            .map_err(sql_error)?;
            tx.commit().map_err(sql_error)?;
        }
        if current < 21 {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            tx.execute_batch(
                "CREATE TABLE messages (
                     id INTEGER PRIMARY KEY AUTOINCREMENT,
                     delivery_id TEXT NOT NULL UNIQUE,
                     sender TEXT NOT NULL, sender_generation INTEGER,
                     recipient INTEGER NOT NULL, recipient_generation INTEGER NOT NULL,
                     thread TEXT, reply_to_delivery TEXT,
                     mode TEXT NOT NULL, text TEXT NOT NULL, content_digest BLOB NOT NULL,
                     admitted_at INTEGER NOT NULL, presented_at INTEGER,
                     presentation_command TEXT, read_at INTEGER
                 );
                 CREATE INDEX messages_inbox ON messages(recipient,read_at,id);
                 CREATE INDEX messages_thread ON messages(thread,id);
                 CREATE TABLE vars (
                     scope TEXT NOT NULL, key TEXT NOT NULL, value TEXT NOT NULL,
                     version INTEGER NOT NULL, set_by TEXT NOT NULL, note TEXT,
                     set_at INTEGER NOT NULL,
                     PRIMARY KEY(scope,key)
                 );
                 CREATE TABLE mail_notices (
                     recipient INTEGER PRIMARY KEY REFERENCES agents(id),
                     last_notice_at INTEGER NOT NULL,
                     open_notice_command TEXT
                 );
                 INSERT INTO schema_migrations(version) VALUES (21);",
            )
            .map_err(sql_error)?;
            tx.commit().map_err(sql_error)?;
        }
        if current < 22 {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            tx.execute_batch(
                "CREATE INDEX messages_prune_acked ON messages(read_at, delivery_id)
                     WHERE read_at IS NOT NULL;
                 INSERT INTO schema_migrations(version) VALUES (22);",
            )
            .map_err(sql_error)?;
            tx.commit().map_err(sql_error)?;
        }
        if current < 23 {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            tx.execute_batch(
                "ALTER TABLE agents ADD COLUMN parent INTEGER REFERENCES agents(id);
                 ALTER TABLE agents ADD COLUMN root INTEGER REFERENCES agents(id);
                 ALTER TABLE agents ADD COLUMN depth INTEGER NOT NULL DEFAULT 0
                     CHECK(depth >= 0);
                 ALTER TABLE agents ADD COLUMN control TEXT NOT NULL DEFAULT 'self'
                     CHECK(control IN ('self', 'tree', 'project'));
                 UPDATE agents SET root = id WHERE root IS NULL;
                 ALTER TABLE commands ADD COLUMN selector TEXT;
                 CREATE INDEX agents_parent ON agents(parent);
                 CREATE INDEX agents_root ON agents(root);
                 INSERT INTO schema_migrations(version) VALUES (23);",
            )
            .map_err(sql_error)?;
            tx.commit().map_err(sql_error)?;
        }
        if current < 24 {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            tx.execute_batch(
                "CREATE TABLE claims (
                     agent INTEGER NOT NULL, generation INTEGER NOT NULL, glob TEXT NOT NULL,
                     note TEXT, expires_at INTEGER NOT NULL, created_at INTEGER NOT NULL,
                     PRIMARY KEY(agent,generation,glob)
                 );
                 CREATE INDEX claims_expiry ON claims(expires_at);
                 INSERT INTO schema_migrations(version) VALUES (24);",
            )
            .map_err(sql_error)?;
            tx.commit().map_err(sql_error)?;
        }
        if current < 25 {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            tx.execute_batch(
                "CREATE TABLE owned_exits (
                     agent INTEGER NOT NULL, generation INTEGER NOT NULL,
                     proven_at INTEGER NOT NULL,
                     PRIMARY KEY(agent, generation)
                 );
                 INSERT INTO schema_migrations(version) VALUES (25);",
            )
            .map_err(sql_error)?;
            tx.commit().map_err(sql_error)?;
        }
        if current < 26 {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            tx.execute_batch(
                "CREATE TABLE native_child_registrations (
                     producer INTEGER NOT NULL REFERENCES agents(id),
                     producer_generation INTEGER NOT NULL CHECK(producer_generation > 0),
                     parent INTEGER NOT NULL REFERENCES agents(id),
                     parent_generation INTEGER NOT NULL CHECK(parent_generation > 0),
                     registration_id TEXT NOT NULL,
                     child INTEGER NOT NULL UNIQUE REFERENCES agents(id),
                     request TEXT NOT NULL, ack TEXT NOT NULL,
                     PRIMARY KEY(parent, parent_generation, registration_id)
                 );
                 INSERT INTO schema_migrations(version) VALUES (26);",
            )
            .map_err(sql_error)?;
            tx.commit().map_err(sql_error)?;
        }
        if current < 27 {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            tx.execute_batch(
                "ALTER TABLE turns ADD COLUMN usage TEXT;
                 INSERT INTO schema_migrations(version) VALUES (27);",
            )
            .map_err(sql_error)?;
            tx.commit().map_err(sql_error)?;
        }
        if current < 28 {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            tx.execute_batch(
                "CREATE INDEX commands_context_pending ON commands(
                     target, generation, json_extract(args, '$.point'), turn_id, created_at
                 ) WHERE verb = 'inject' AND state IN ('accepted', 'queued');
                 INSERT INTO schema_migrations(version) VALUES (28);",
            )
            .map_err(sql_error)?;
            tx.commit().map_err(sql_error)?;
        }
        if current < 29 {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            tx.execute_batch(
                "ALTER TABLE turns ADD COLUMN outcome TEXT
                     CHECK(outcome IS NULL OR outcome IN ('success','failure','interrupted','unknown'));
                 ALTER TABLE turns ADD COLUMN native_status TEXT
                     CHECK(native_status IS NULL OR length(native_status) > 0);
                 INSERT INTO schema_migrations(version) VALUES (29);",
            )
            .map_err(sql_error)?;
            tx.commit().map_err(sql_error)?;
        }
        if current < 30 {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            tx.execute_batch(
                "CREATE INDEX native_children_producer ON native_child_registrations(producer, producer_generation, child);
                 CREATE INDEX turns_native_activity ON turns(agent, generation, state, COALESCE(completed_at, started_at, 0));
                 CREATE INDEX events_native_progress_time ON events(agent, generation, ts DESC)
                     WHERE native_id IS NOT NULL AND source NOT IN ('daemon', 'os', 'git', 'workspace_process')
                     AND kind IN ('turn.started', 'turn.completed', 'message.completed',
                                  'tool.started', 'tool.completed', 'request.opened', 'request.resolved', 'request.closed');
                 CREATE INDEX events_git_progress_time ON events(agent, generation, ts DESC)
                     WHERE source = 'git' AND kind = 'file.changed';
                 ALTER TABLE agents ADD COLUMN native_child_revision INTEGER NOT NULL DEFAULT 0
                     CHECK(typeof(native_child_revision) = 'integer' AND native_child_revision >= 0);
                 CREATE TRIGGER native_child_activity_revision AFTER INSERT ON events
                 WHEN NEW.agent IS NOT NULL AND NEW.generation IS NOT NULL AND (
                     NEW.kind IN ('agent.created', 'turn.started', 'turn.completed', 'turn.unknown', 'turn.resolved',
                                  'message.completed', 'tool.started', 'tool.completed',
                                  'request.opened', 'request.resolved', 'request.closed', 'agent.lost')
                     OR NEW.kind GLOB 'session.*'
                     OR (NEW.kind GLOB 'source.*'
                         AND COALESCE(json_extract(NEW.data, '$.source'), '') NOT IN ('daemon', 'os', 'git', 'workspace_process')))
                 BEGIN
                     UPDATE agents SET native_child_revision = native_child_revision + 1
                     WHERE id = (SELECT producer FROM native_child_registrations WHERE child = NEW.agent)
                       AND generation = (SELECT producer_generation FROM native_child_registrations WHERE child = NEW.agent)
                       AND EXISTS(SELECT 1 FROM agents c WHERE c.id = NEW.agent AND c.generation = NEW.generation);
                     UPDATE agents SET native_child_revision = native_child_revision + 1
                     WHERE id = NEW.agent AND generation = NEW.generation
                       AND (NEW.kind GLOB 'source.*' OR NEW.kind GLOB 'session.*' OR NEW.kind = 'agent.lost')
                       AND EXISTS(SELECT 1 FROM native_child_registrations r
                           WHERE r.producer = NEW.agent AND r.producer_generation = NEW.generation);
                 END;
                 CREATE TRIGGER native_child_progress_pruned AFTER DELETE ON events
                 WHEN OLD.native_id IS NOT NULL AND OLD.source NOT IN ('daemon', 'os', 'git', 'workspace_process')
                     AND OLD.kind IN ('turn.started', 'turn.completed', 'message.completed',
                                      'tool.started', 'tool.completed', 'request.opened', 'request.resolved', 'request.closed')
                 BEGIN
                     UPDATE agents SET native_child_revision = native_child_revision + 1
                     WHERE id = (SELECT producer FROM native_child_registrations WHERE child = OLD.agent)
                       AND generation = (SELECT producer_generation FROM native_child_registrations WHERE child = OLD.agent)
                       AND EXISTS(SELECT 1 FROM agents c WHERE c.id = OLD.agent AND c.generation = OLD.generation);
                 END;
                 INSERT INTO schema_migrations(version) VALUES (30);",
            ).map_err(sql_error)?;
            tx.commit().map_err(sql_error)?;
        }
        if current < 31 {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            tx.execute_batch(
                "CREATE INDEX agents_list_state ON agents(state, id);
                 CREATE INDEX agents_list_project ON agents(project, id);
                 CREATE INDEX agents_list_harness ON agents(harness, id);
                 CREATE INDEX agents_list_parent ON agents(parent, id);
                 CREATE INDEX agents_list_root ON agents(root, id);
                 INSERT INTO schema_migrations(version) VALUES (31);",
            )
            .map_err(sql_error)?;
            tx.commit().map_err(sql_error)?;
        }
        if current < 32 {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            tx.execute_batch(
                "CREATE TABLE attention_wakes (
                     recipient INTEGER NOT NULL REFERENCES agents(id),
                     thread TEXT NOT NULL,
                     woken_at INTEGER NOT NULL,
                     PRIMARY KEY(recipient, thread)
                 );
                 CREATE INDEX attention_wakes_thread ON attention_wakes(thread);
                 INSERT INTO schema_migrations(version) VALUES (32);",
            )
            .map_err(sql_error)?;
            tx.commit().map_err(sql_error)?;
        }
        if current < 33 {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            tx.execute_batch(
                "CREATE TABLE session_usage (
                     agent INTEGER NOT NULL REFERENCES agents(id),
                     generation INTEGER NOT NULL CHECK(generation > 0),
                     snapshot TEXT NOT NULL,
                     observed_at INTEGER NOT NULL CHECK(observed_at >= 0),
                     PRIMARY KEY(agent, generation)
                 );
                 INSERT INTO schema_migrations(version) VALUES (33);",
            )
            .map_err(sql_error)?;
            tx.commit().map_err(sql_error)?;
        }
        if current < 34 {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            tx.execute_batch(
                "CREATE TABLE agent_config (
                     agent INTEGER NOT NULL REFERENCES agents(id),
                     generation INTEGER NOT NULL CHECK(generation > 0),
                     revision INTEGER NOT NULL CHECK(revision > 0),
                     requested TEXT NOT NULL,
                     effective TEXT NOT NULL,
                     application TEXT NOT NULL CHECK(application = 'next_turn'),
                     command_id TEXT NOT NULL UNIQUE REFERENCES commands(id),
                     created_at INTEGER NOT NULL,
                     PRIMARY KEY(agent, generation, revision)
                 );
                 CREATE INDEX agent_config_command ON agent_config(command_id);
                 ALTER TABLE commands ADD COLUMN config_revision INTEGER NOT NULL DEFAULT 0
                     CHECK(typeof(config_revision) = 'integer' AND config_revision >= 0);
                 ALTER TABLE turns ADD COLUMN config_revision INTEGER NOT NULL DEFAULT 0
                     CHECK(typeof(config_revision) = 'integer' AND config_revision >= 0);
                 INSERT INTO schema_migrations(version) VALUES (34);",
            )
            .map_err(sql_error)?;
            tx.commit().map_err(sql_error)?;
        }
        if current < 35 {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            tx.execute_batch(
                "ALTER TABLE agent_config ADD COLUMN provenance TEXT NOT NULL DEFAULT 'configure'
                     CHECK(provenance IN ('configure', 'inherited_from'));
                 INSERT INTO schema_migrations(version) VALUES (35);",
            )
            .map_err(sql_error)?;
            tx.commit().map_err(sql_error)?;
        }
        if current < 36 {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            tx.execute_batch(
                "ALTER TABLE agents ADD COLUMN profile_inherited INTEGER NOT NULL DEFAULT 0
                     CHECK(profile_inherited IN (0, 1));
                 INSERT INTO schema_migrations(version) VALUES (36);",
            )
            .map_err(sql_error)?;
            tx.commit().map_err(sql_error)?;
        }
        if current < 37 {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            tx.execute_batch(
                "CREATE TABLE completion_digests (
                     id TEXT PRIMARY KEY,
                     parent INTEGER NOT NULL REFERENCES agents(id),
                     mode TEXT NOT NULL CHECK(mode IN ('all', 'any')),
                     commands TEXT NOT NULL,
                     turns TEXT NOT NULL,
                     requester TEXT NOT NULL,
                     created_at INTEGER NOT NULL,
                     delivered_mail_id TEXT,
                     delivered_at INTEGER,
                     payload_digest BLOB NOT NULL,
                     hidden TEXT NOT NULL
                 );
                 CREATE INDEX completion_digests_pending
                     ON completion_digests(delivered_at, created_at);
                 INSERT INTO schema_migrations(version) VALUES (37);",
            )
            .map_err(sql_error)?;
            tx.commit().map_err(sql_error)?;
        }
        if current < 38 {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            tx.execute_batch(
                "CREATE TABLE inference_clients (
                     client_id TEXT PRIMARY KEY, current_version INTEGER NOT NULL,
                     current_hash TEXT NOT NULL, current_file TEXT NOT NULL, pending_version INTEGER, pending_hash TEXT, pending_file TEXT,
                     previous_version INTEGER, previous_hash TEXT,
                     rotation_seq INTEGER NOT NULL DEFAULT 0, pending_proven INTEGER NOT NULL DEFAULT 0,
                     last_admission_seq INTEGER NOT NULL DEFAULT 0);
                 CREATE TABLE inference_calls (
                     id TEXT PRIMARY KEY REFERENCES writes(id), client_id TEXT NOT NULL REFERENCES inference_clients(client_id),
                     admission_seq INTEGER NOT NULL, state TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
                     request TEXT NOT NULL, route TEXT NOT NULL, result_sha256 TEXT, result_bytes INTEGER,
                     cleanup_complete INTEGER NOT NULL DEFAULT 0, native_session_id TEXT, native_turns INTEGER,
                     cancel_id TEXT REFERENCES writes(id), release_id TEXT REFERENCES writes(id),
                     UNIQUE(client_id, admission_seq));
                 CREATE INDEX inference_client_calls ON inference_calls(client_id, admission_seq);
                 INSERT INTO schema_migrations(version) VALUES (38);"
            ).map_err(sql_error)?;
            tx.commit().map_err(sql_error)?;
        }
        if current < 39 {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            tx.execute_batch(
                "ALTER TABLE inference_clients ADD COLUMN last_write_seq INTEGER NOT NULL DEFAULT 0 CHECK(last_write_seq>=0);
                 ALTER TABLE inference_clients ADD COLUMN retain_after_seq INTEGER NOT NULL DEFAULT 0 CHECK(retain_after_seq>=0);
                 ALTER TABLE inference_clients ADD COLUMN write_retain_after_seq INTEGER NOT NULL DEFAULT 0 CHECK(write_retain_after_seq>=0);
                 ALTER TABLE inference_clients ADD COLUMN previous_file TEXT;
                 ALTER TABLE inference_clients ADD COLUMN previous_retirement_id TEXT;
                 ALTER TABLE inference_clients ADD COLUMN previous_retirement_complete INTEGER NOT NULL DEFAULT 0;
                 ALTER TABLE inference_clients ADD COLUMN previous_read_call_id TEXT;
                 ALTER TABLE inference_clients ADD COLUMN previous_release_proven INTEGER NOT NULL DEFAULT 0;
                 ALTER TABLE inference_clients ADD COLUMN previous_restarted INTEGER NOT NULL DEFAULT 0;
                 ALTER TABLE writes ADD COLUMN inference_seq INTEGER CHECK(inference_seq>0);
                 WITH ranked AS (
                     SELECT id,ROW_NUMBER() OVER (PARTITION BY principal ORDER BY created_at,id) AS seq
                     FROM writes WHERE substr(principal,1,17)='memory_inference:'
                 ) UPDATE writes SET inference_seq=(SELECT seq FROM ranked WHERE ranked.id=writes.id)
                   WHERE id IN (SELECT id FROM ranked);
                 UPDATE inference_clients SET last_write_seq=COALESCE((
                     SELECT MAX(inference_seq) FROM writes WHERE principal='memory_inference:'||client_id),0);
                 UPDATE inference_clients SET previous_file=(
                     SELECT json_extract(result,'$.credential_file') FROM writes
                     WHERE principal='owner' AND method IN ('inference.client.install','inference.client.rotate')
                       AND json_extract(result,'$.client_id')=inference_clients.client_id
                       AND json_extract(result,'$.token_version')=inference_clients.previous_version
                     ORDER BY created_at DESC,id DESC LIMIT 1)
                   WHERE previous_version IS NOT NULL;
                 CREATE UNIQUE INDEX inference_write_inventory ON writes(principal,inference_seq) WHERE inference_seq IS NOT NULL;
                 INSERT INTO schema_migrations(version) VALUES (39);"
            ).map_err(sql_error)?;
            tx.commit().map_err(sql_error)?;
        }
        if current < 40 {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            tx.execute_batch(
                "ALTER TABLE inference_clients ADD COLUMN expired_id_through_ms INTEGER NOT NULL DEFAULT -1 CHECK(expired_id_through_ms>=-1);"
            ).map_err(sql_error)?;
            // A v39 retention receipt records the clock used for its pruning.
            // Preserve the greatest surviving observation across an upgrade,
            // including when the wall clock has already moved backward.
            tx.execute(
                "UPDATE inference_clients SET expired_id_through_ms=MAX(-1,COALESCE((
                    SELECT MAX(created_at) FROM writes
                    WHERE principal='memory_inference:'||client_id AND method='inference.retention.advance'
                 ),0)-?1-1)",
                [RETRY_WINDOW_MS],
            ).map_err(sql_error)?;
            db!(tx.execute("INSERT INTO schema_migrations(version) VALUES (40)", []));
            tx.commit().map_err(sql_error)?;
        }
        if current < 41 {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            tx.execute_batch(
                "CREATE TABLE settings_history (
                     revision INTEGER PRIMARY KEY AUTOINCREMENT,
                     previous_revision TEXT,
                     current_revision TEXT NOT NULL,
                     changed_sections TEXT NOT NULL,
                     author TEXT NOT NULL,
                     status TEXT NOT NULL CHECK(status IN ('pending','applied','not_applied','unknown')),
                     created_at INTEGER NOT NULL
                 );
                 CREATE INDEX settings_history_created
                     ON settings_history(created_at DESC, revision DESC);
                 INSERT INTO schema_migrations(version) VALUES (41);",
            )
            .map_err(sql_error)?;
            tx.commit().map_err(sql_error)?;
        }
        if current < 42 {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            tx.execute_batch(
                "ALTER TABLE agents ADD COLUMN profile_choice TEXT;
                 CREATE INDEX commands_assignment ON commands(json_extract(args,'$.assignment.key'))
                     WHERE verb='launch';
                 INSERT INTO schema_migrations(version) VALUES (42);",
            )
            .map_err(sql_error)?;
            tx.commit().map_err(sql_error)?;
        }
        if current < 43 {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            tx.execute_batch(
                "ALTER TABLE commands ADD COLUMN assignment_busy INTEGER NOT NULL DEFAULT 0
                     CHECK(assignment_busy IN (0,1));
                 DROP INDEX commands_assignment;
                 CREATE VIEW assignment_busy_membership AS
                   SELECT l.id AS launch_id FROM commands l
                   WHERE l.verb='launch'
                     AND json_extract(l.args,'$.assignment.key') IS NOT NULL
                     AND (l.state IN ('accepted','queued','dispatched','unknown')
                       OR EXISTS (
                         SELECT 1 FROM commands s WHERE s.parent_command=l.id
                           AND s.link_kind='send' AND (
                             s.state IN ('accepted','queued','dispatched','unknown')
                             OR (s.state='applied' AND (s.turn_id IS NULL OR EXISTS (
                               SELECT 1 FROM turns t WHERE t.agent=s.target
                                 AND t.generation=s.generation AND t.turn_id=s.turn_id
                                 AND t.state IN ('starting','running','unknown'))))
                           )
                       ));
                 UPDATE commands SET assignment_busy = EXISTS (
                     SELECT 1 FROM assignment_busy_membership m
                     WHERE m.launch_id=commands.id)
                   WHERE verb='launch' AND json_extract(args,'$.assignment.key') IS NOT NULL;
                 CREATE INDEX commands_assignment_busy
                     ON commands(json_extract(args,'$.assignment.key'))
                     WHERE verb='launch' AND assignment_busy=1;
                 CREATE INDEX commands_assignment_turn
                     ON commands(target,generation,turn_id,parent_command)
                     WHERE link_kind='send' AND state='applied' AND turn_id IS NOT NULL;
                 CREATE TRIGGER assignment_commands_insert
                 AFTER INSERT ON commands BEGIN
                   UPDATE commands SET assignment_busy = EXISTS (
                     SELECT 1 FROM assignment_busy_membership m
                     WHERE m.launch_id=commands.id)
                   WHERE id IN (NEW.id, NEW.parent_command) AND verb='launch';
                 END;
                 CREATE TRIGGER assignment_commands_update
                 AFTER UPDATE OF verb,args,state,parent_command,link_kind,target,generation,turn_id
                 ON commands BEGIN
                   UPDATE commands SET assignment_busy = EXISTS (
                     SELECT 1 FROM assignment_busy_membership m
                     WHERE m.launch_id=commands.id)
                   WHERE id IN (NEW.id, NEW.parent_command, OLD.parent_command)
                     AND verb='launch';
                 END;
                 CREATE TRIGGER assignment_commands_delete
                 AFTER DELETE ON commands BEGIN
                   UPDATE commands SET assignment_busy = EXISTS (
                     SELECT 1 FROM assignment_busy_membership m
                     WHERE m.launch_id=commands.id)
                   WHERE id=OLD.parent_command AND verb='launch';
                 END;
                 CREATE TRIGGER assignment_turns_insert
                 AFTER INSERT ON turns BEGIN
                   UPDATE commands SET assignment_busy = EXISTS (
                     SELECT 1 FROM assignment_busy_membership m
                     WHERE m.launch_id=commands.id)
                   WHERE id IN (
                     SELECT s.parent_command FROM commands s INDEXED BY commands_assignment_turn
                     WHERE s.link_kind='send' AND s.state='applied' AND s.turn_id IS NOT NULL
                       AND s.target=NEW.agent AND s.generation=NEW.generation
                       AND s.turn_id=NEW.turn_id AND s.parent_command IS NOT NULL
                   ) AND verb='launch';
                 END;
                 CREATE TRIGGER assignment_turns_update
                 AFTER UPDATE OF agent,generation,turn_id,state ON turns BEGIN
                   UPDATE commands SET assignment_busy = EXISTS (
                     SELECT 1 FROM assignment_busy_membership m
                     WHERE m.launch_id=commands.id)
                   WHERE id IN (
                     SELECT s.parent_command FROM commands s INDEXED BY commands_assignment_turn
                     WHERE s.link_kind='send' AND s.state='applied' AND s.turn_id IS NOT NULL
                       AND s.target=OLD.agent AND s.generation=OLD.generation
                       AND s.turn_id=OLD.turn_id AND s.parent_command IS NOT NULL
                     UNION
                     SELECT s.parent_command FROM commands s INDEXED BY commands_assignment_turn
                     WHERE s.link_kind='send' AND s.state='applied' AND s.turn_id IS NOT NULL
                       AND s.target=NEW.agent AND s.generation=NEW.generation
                       AND s.turn_id=NEW.turn_id AND s.parent_command IS NOT NULL
                   ) AND verb='launch';
                 END;
                 CREATE TRIGGER assignment_turns_delete
                 AFTER DELETE ON turns BEGIN
                   UPDATE commands SET assignment_busy = EXISTS (
                     SELECT 1 FROM assignment_busy_membership m
                     WHERE m.launch_id=commands.id)
                   WHERE id IN (
                     SELECT s.parent_command FROM commands s INDEXED BY commands_assignment_turn
                     WHERE s.link_kind='send' AND s.state='applied' AND s.turn_id IS NOT NULL
                       AND s.target=OLD.agent AND s.generation=OLD.generation
                       AND s.turn_id=OLD.turn_id AND s.parent_command IS NOT NULL
                   ) AND verb='launch';
                 END;
                 INSERT INTO schema_migrations(version) VALUES (43);",
            )
            .map_err(sql_error)?;
            tx.commit().map_err(sql_error)?;
        }
        if current < 44 {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            tx.execute_batch(
                "CREATE INDEX commands_turn_unknown
                    ON commands(target, generation, turn_id, state, id)
                    WHERE state IN ('accepted','queued','dispatched','unknown');
                  INSERT INTO schema_migrations(version) VALUES (44);",
            )
            .map_err(sql_error)?;
            tx.commit().map_err(sql_error)?;
        }
        if current < 45 {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            tx.execute_batch(
                "ALTER TABLE turns ADD COLUMN preceded_by TEXT;
                  INSERT INTO schema_migrations(version) VALUES (45);",
            )
            .map_err(sql_error)?;
            tx.commit().map_err(sql_error)?;
        }
        if current < 46 {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            tx.execute_batch(
                "CREATE TABLE artifacts (id TEXT PRIMARY KEY, agent INTEGER NOT NULL REFERENCES agents(id), generation INTEGER NOT NULL, path TEXT NOT NULL UNIQUE, body TEXT NOT NULL);
                 CREATE INDEX artifacts_owner ON artifacts(agent,generation);
                 CREATE TABLE cleanup_plans (id TEXT PRIMARY KEY, principal TEXT NOT NULL, body TEXT NOT NULL, apply_id TEXT UNIQUE);
                 INSERT INTO schema_migrations(version) VALUES (46);",
            )
            .map_err(sql_error)?;
            tx.commit().map_err(sql_error)?;
        }
        if current < 47 {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            tx.execute_batch(
                "CREATE INDEX writes_principal_method ON writes(principal, method);
                 INSERT INTO schema_migrations(version) VALUES (47);",
            )
            .map_err(sql_error)?;
            tx.commit().map_err(sql_error)?;
        }
        if current < 48 {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            tx.execute_batch("ALTER TABLE agents ADD COLUMN archived INTEGER NOT NULL DEFAULT 0 CHECK(archived IN (0,1));
                ALTER TABLE agents ADD COLUMN chat INTEGER NOT NULL DEFAULT 0 CHECK(chat IN (0,1));
                CREATE INDEX agents_list_archived ON agents(archived,id);
                INSERT INTO schema_migrations(version) VALUES (48);").map_err(sql_error)?;
            tx.commit().map_err(sql_error)?;
        }
        if current < 49 {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            tx.execute_batch(
                "CREATE INDEX events_heavy_agent_generation
                     ON events(agent, generation, seq) WHERE kind = 'activity.heavy';
                 INSERT INTO schema_migrations(version) VALUES (49);",
            )
            .map_err(sql_error)?;
            tx.commit().map_err(sql_error)?;
        }
        if current < 50 {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            tx.execute_batch(
                "CREATE INDEX processes_live_owner ON processes(owner)
                     WHERE state IN ('running','unknown');
                 INSERT INTO schema_migrations(version) VALUES (50);",
            )
            .map_err(sql_error)?;
            tx.commit().map_err(sql_error)?;
        }
        if current < 51 {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            tx.execute_batch(
                "CREATE INDEX events_git_hints ON events(agent, generation, seq)
                 WHERE kind = 'tool.completed' AND native_id IS NOT NULL
                   AND source NOT IN ('git', 'os', 'daemon')
                   AND json_extract(data, '$.tool') = 'fileChange'
                   AND json_extract(data, '$.status') = 'completed'
                   AND (CASE WHEN json_type(data, '$.changed_paths') = 'array'
                             THEN json_array_length(data, '$.changed_paths') > 0 ELSE 0 END
                        OR json_type(data, '$.changed_paths_truncated') = 'true'
                        OR json_type(data, '$.redacted') = 'true');
                 INSERT INTO schema_migrations(version) VALUES (51);",
            )
            .map_err(sql_error)?;
            tx.commit().map_err(sql_error)?;
        }
        if current < 52 {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            tx.execute_batch(
                "CREATE TABLE custody_binding (
                     singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                     store_id TEXT NOT NULL, data_dir BLOB NOT NULL, namespace TEXT NOT NULL
                 );
                 ALTER TABLE agents ADD COLUMN custody_namespace TEXT;
                 UPDATE agents SET custody_namespace = (SELECT id FROM store_identity WHERE singleton = 1)
                   WHERE observe_only = 0 AND EXISTS (
                     SELECT 1 FROM events WHERE events.agent = agents.id
                       AND events.generation = agents.generation AND kind = 'agent.starting'
                       AND seq > (SELECT since_seq FROM store_identity WHERE singleton = 1)
                   );
                 INSERT INTO schema_migrations(version) VALUES (52);"
            ).map_err(sql_error)?;
            tx.commit().map_err(sql_error)?;
        }
        if current < 53 {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            tx.execute_batch(
                "ALTER TABLE agents ADD COLUMN project_scope TEXT;
                 CREATE INDEX agents_list_project_scope
                   ON agents(COALESCE(project_scope, project), id);
                 INSERT INTO schema_migrations(version) VALUES (53);",
            )
            .map_err(sql_error)?;
            tx.commit().map_err(sql_error)?;
        }
        if current < 54 {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            tx.execute_batch(
                "ALTER TABLE rule_state ADD COLUMN source_present INTEGER NOT NULL DEFAULT 1
                   CHECK(source_present IN (0, 1));
                 CREATE TABLE paused_assignment_timers (rule TEXT NOT NULL PRIMARY KEY);
                 INSERT INTO schema_migrations(version) VALUES (54);",
            )
            .map_err(sql_error)?;
            tx.commit().map_err(sql_error)?;
        }
        if current < 55 {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            tx.execute_batch(
                "CREATE TABLE project_assignments (
                     project TEXT PRIMARY KEY,
                     manager INTEGER,
                     manager_generation INTEGER,
                     epoch INTEGER NOT NULL CHECK(epoch > 0),
                     revision INTEGER NOT NULL CHECK(revision > 0),
                     start_command TEXT,
                     CHECK((manager IS NULL) = (manager_generation IS NULL))
                 );
                 ALTER TABLE commands ADD COLUMN assignment_binding TEXT;
                 ALTER TABLE cleanup_plans ADD COLUMN assignment_binding TEXT;
                 INSERT INTO schema_migrations(version) VALUES (55);",
            )
            .map_err(sql_error)?;
            tx.commit().map_err(sql_error)?;
        }
        if current < 56 {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            tx.execute_batch(
                // Events and write receipts can both expire while a turn survives.
                // Only exact retained evidence can classify a pre-56 completion.
                "ALTER TABLE turns ADD COLUMN resolution_source TEXT
                   CHECK(resolution_source IS NULL OR resolution_source IN ('owner', 'unknown'));
                 UPDATE turns SET resolution_source = 'unknown' WHERE state = 'completed';
                 WITH evidence(agent, generation, turn_id, owner_assertion) AS (
                     SELECT 'a' || agent, generation, json_extract(data, '$.turn_id'),
                            kind = 'turn.resolved'
                       FROM events
                      WHERE kind IN ('turn.completed', 'turn.resolved')
                        AND (kind = 'turn.completed' AND native_id IS NOT NULL
                             OR kind = 'turn.resolved' AND source = 'daemon')
                     UNION ALL
                     SELECT json_extract(result, '$.agent'), json_extract(result, '$.generation'),
                            json_extract(result, '$.turn_id'), 1
                       FROM writes
                      WHERE method = 'turn.resolve' AND principal = 'owner'
                 ), terminal_sources AS (
                     SELECT agent, generation, turn_id, MAX(owner_assertion) AS owner_assertion
                       FROM evidence GROUP BY agent, generation, turn_id
                 )
                 UPDATE turns
                    SET resolution_source = CASE owner_assertion WHEN 1 THEN 'owner' ELSE NULL END
                   FROM terminal_sources
                  WHERE turns.agent = CAST(substr(terminal_sources.agent, 2) AS INTEGER)
                    AND terminal_sources.agent = 'a' || turns.agent
                    AND terminal_sources.generation = turns.generation
                    AND terminal_sources.turn_id = turns.turn_id
                    AND turns.state = 'completed';
                 INSERT INTO schema_migrations(version) VALUES (56);",
            )
            .map_err(sql_error)?;
            tx.commit().map_err(sql_error)?;
        }
        if current < 57 {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            tx.execute_batch(
                "ALTER TABLE project_assignments ADD COLUMN workspace_path TEXT;
                 ALTER TABLE project_assignments ADD COLUMN workspace_adopted INTEGER NOT NULL DEFAULT 0
                     CHECK(workspace_adopted IN (0, 1));
                 INSERT INTO schema_migrations(version) VALUES (57);",
            )
            .map_err(sql_error)?;
            tx.commit().map_err(sql_error)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod continuity_tests {
    use super::*;

    #[test]
    fn v56_upgrade_checks_exact_completion_provenance_after_retention() {
        let mut store = Store::open(Path::new(":memory:")).unwrap();
        store.connection.execute_batch(
            r#"INSERT INTO agents(id, project, harness, profile, state, generation, created_at, updated_at)
                  VALUES (1, 'p', 'codex', 'p', 'stopped', 2, 0, 0);
             INSERT INTO turns(agent,generation,turn_id,state,status,outcome)
                  VALUES (1,1,'reused','completed','completed','success'),
                         (1,2,'reused','completed','completed','success'),
                         (1,2,'receipt','completed','completed','success'),
                         (1,2,'event','completed','completed','success'),
                         (1,2,'unsettled','unknown',NULL,NULL);
             INSERT INTO events(ts,kind,family,agent,generation,source,native_id,data)
                  VALUES (0,'turn.resolved','turn',1,1,'daemon',NULL,'{"turn_id":"reused"}'),
                         (0,'turn.resolved','turn',1,2,'daemon',NULL,'{"turn_id":"receipt"}'),
                         (172800000,'turn.completed','turn',1,2,'stream','native','{"turn_id":"reused"}'),
                         (172800000,'turn.resolved','turn',1,2,'daemon',NULL,'{"turn_id":"event"}');
             INSERT INTO writes(id,principal,method,payload_digest,result,created_at)
                  VALUES ('expired','owner','turn.resolve',zeroblob(32),
                          '{"agent":"a1","generation":1,"turn_id":"reused"}',0),
                         ('retained','owner','turn.resolve',zeroblob(32),
                          '{"agent":"a1","generation":2,"turn_id":"receipt"}',172800000);"#,
        ).unwrap();
        let retention = store
            .prune_retention(Some(1), Some(1), None, 172_800_000)
            .unwrap();
        assert_eq!(retention.events_deleted, 2);
        assert_eq!(retention.writes_deleted, 1);
        store
            .connection
            .execute_batch(
                "ALTER TABLE project_assignments DROP COLUMN workspace_adopted;
                 ALTER TABLE project_assignments DROP COLUMN workspace_path;
                 DELETE FROM schema_migrations WHERE version = 57;
                 ALTER TABLE turns DROP COLUMN resolution_source;
                 DELETE FROM schema_migrations WHERE version = 56;",
            )
            .unwrap();
        store.migrate().unwrap();

        let turn = |generation, id| store.get_turn("a1", generation, id).unwrap().unwrap();
        let pruned = turn(1, "reused");
        assert_eq!(pruned.state, TurnState::Completed);
        assert_eq!(pruned.status.as_deref(), Some("completed"));
        assert_eq!(pruned.outcome, Some(TurnOutcome::Unknown));
        assert_eq!(
            pruned.resolution_source,
            Some(TurnResolutionSource::Unknown)
        );
        let native = turn(2, "reused");
        assert_eq!(native.outcome, Some(TurnOutcome::Success));
        assert_eq!(native.resolution_source, None);
        for id in ["receipt", "event"] {
            let owner = turn(2, id);
            assert_eq!(owner.outcome, Some(TurnOutcome::Success));
            assert_eq!(owner.resolution_source, Some(TurnResolutionSource::Owner));
        }
        assert_eq!(turn(2, "unsettled").resolution_source, None);
    }

    #[test]
    fn v49_upgrade_indexes_only_live_process_owners() {
        let mut store = Store::open(std::path::Path::new(":memory:")).unwrap();
        store
            .connection
            .execute_batch(
                "ALTER TABLE project_assignments DROP COLUMN workspace_adopted;
                 ALTER TABLE project_assignments DROP COLUMN workspace_path;
                 DELETE FROM schema_migrations WHERE version=57;
                 ALTER TABLE turns DROP COLUMN resolution_source;
                 DELETE FROM schema_migrations WHERE version=56;
                 ALTER TABLE commands DROP COLUMN assignment_binding;
                 ALTER TABLE cleanup_plans DROP COLUMN assignment_binding;
                 DROP TABLE project_assignments;
                 DELETE FROM schema_migrations WHERE version=55;
                 DROP TABLE paused_assignment_timers; ALTER TABLE rule_state DROP COLUMN source_present;
                 DELETE FROM schema_migrations WHERE version=54;
                 DROP INDEX agents_list_project_scope; ALTER TABLE agents DROP COLUMN project_scope;
                 DELETE FROM schema_migrations WHERE version=53;
                 DROP TABLE custody_binding; ALTER TABLE agents DROP COLUMN custody_namespace;
                 DELETE FROM schema_migrations WHERE version=52;
                 DROP INDEX events_git_hints; DELETE FROM schema_migrations WHERE version=51;
                 DROP INDEX processes_live_owner;
             DELETE FROM schema_migrations WHERE version=50;
             INSERT INTO processes(host_boot_id,pid,start_time,owner,role,state,updated_at) VALUES
                 ('boot',1,1,'agent:a1#1','plugin','exited',1),
                 ('boot',2,2,'agent:a2#1','plugin','running',1),
                 ('boot',3,3,'agent:a2#1','child','unknown',1),
                 ('boot',4,4,'workspace:a3#1','setup','unknown',1);",
            )
            .unwrap();
        store.migrate().unwrap();
        let (owners, total) = store.live_process_owners(1).unwrap();
        assert_eq!(owners.len(), 1);
        assert_eq!(total, 2);
        // The exited `agent:a1#1` sorts first; a page that dropped the state
        // filter would return it here.
        assert_eq!(
            (
                owners[0].agent.as_str(),
                owners[0].generation,
                owners[0].workspace
            ),
            ("a2", 1, false)
        );
        for sql in [
            "EXPLAIN QUERY PLAN SELECT COUNT(*) FROM (SELECT DISTINCT owner FROM processes INDEXED BY processes_live_owner WHERE state IN ('running','unknown'))",
            "EXPLAIN QUERY PLAN SELECT DISTINCT owner FROM processes INDEXED BY processes_live_owner WHERE state IN ('running','unknown') ORDER BY owner LIMIT 1",
        ] {
            let plans: Vec<String> = store
                .connection
                .prepare(sql)
                .unwrap()
                .query_map([], |row| row.get(3))
                .unwrap()
                .collect::<std::result::Result<_, _>>()
                .unwrap();
            assert!(
                plans
                    .iter()
                    .any(|plan| plan.contains("USING INDEX processes_live_owner")),
                "{plans:?}"
            );
            assert!(
                plans.iter().all(|plan| !plan.contains("TEMP B-TREE")),
                "{plans:?}"
            );
        }
    }

    #[test]
    fn v48_upgrade_indexes_heavy_rollups_by_exact_generation() {
        let mut store = Store::open(std::path::Path::new(":memory:")).unwrap();
        store
            .connection
            .execute_batch(
                "ALTER TABLE project_assignments DROP COLUMN workspace_adopted;
                 ALTER TABLE project_assignments DROP COLUMN workspace_path;
                 DELETE FROM schema_migrations WHERE version=57;
                 ALTER TABLE turns DROP COLUMN resolution_source;
                 DELETE FROM schema_migrations WHERE version=56;
                 ALTER TABLE commands DROP COLUMN assignment_binding;
                 ALTER TABLE cleanup_plans DROP COLUMN assignment_binding;
                 DROP TABLE project_assignments;
                 DELETE FROM schema_migrations WHERE version=55;
                 DROP TABLE paused_assignment_timers; ALTER TABLE rule_state DROP COLUMN source_present;
                 DELETE FROM schema_migrations WHERE version=54;
                 DROP INDEX agents_list_project_scope; ALTER TABLE agents DROP COLUMN project_scope;
                 DELETE FROM schema_migrations WHERE version=53;
                 DROP TABLE custody_binding; ALTER TABLE agents DROP COLUMN custody_namespace;
                 DELETE FROM schema_migrations WHERE version=52;
                 DROP INDEX events_git_hints; DELETE FROM schema_migrations WHERE version=51;
                 DROP INDEX processes_live_owner;
                 DELETE FROM schema_migrations WHERE version=50;
                 DROP INDEX events_heavy_agent_generation;
             DELETE FROM schema_migrations WHERE version=49;",
            )
            .unwrap();
        store.migrate().unwrap();
        for sql in [
            "EXPLAIN QUERY PLAN SELECT COUNT(*) FROM events WHERE kind='activity.heavy' AND agent=?1 AND generation=?2",
            "EXPLAIN QUERY PLAN SELECT data FROM events WHERE kind='activity.heavy' AND agent=?1 AND generation=?2 ORDER BY seq DESC LIMIT 201",
        ] {
            let plans: Vec<String> = store
                .connection
                .prepare(sql)
                .unwrap()
                .query_map(params![1, 2], |row| row.get(3))
                .unwrap()
                .collect::<std::result::Result<_, _>>()
                .unwrap();
            assert!(
                plans
                    .iter()
                    .any(|plan| plan.contains("events_heavy_agent_generation")
                        && plan.contains("agent=? AND generation=?")),
                "{plans:?}"
            );
            assert!(
                plans.iter().all(|plan| !plan.contains("TEMP B-TREE")),
                "{plans:?}"
            );
        }
    }

    #[test]
    fn v39_upgrade_recovers_expiry_from_retention_without_losing_replay() {
        let mut store = Store::open(std::path::Path::new(":memory:")).unwrap();
        store
            .connection
            .execute_batch(
                "ALTER TABLE project_assignments DROP COLUMN workspace_adopted;
                 ALTER TABLE project_assignments DROP COLUMN workspace_path;
                 DELETE FROM schema_migrations WHERE version=57;
                 ALTER TABLE turns DROP COLUMN resolution_source;
                 DELETE FROM schema_migrations WHERE version=56;
                 ALTER TABLE commands DROP COLUMN assignment_binding;
                 ALTER TABLE cleanup_plans DROP COLUMN assignment_binding;
                 DROP TABLE project_assignments;
                 DELETE FROM schema_migrations WHERE version=55;
                 DROP TABLE paused_assignment_timers; ALTER TABLE rule_state DROP COLUMN source_present;
                 DELETE FROM schema_migrations WHERE version=54;
                 DROP INDEX agents_list_project_scope; ALTER TABLE agents DROP COLUMN project_scope;
                 DELETE FROM schema_migrations WHERE version=53;
                 DROP TABLE custody_binding; ALTER TABLE agents DROP COLUMN custody_namespace;
                 DELETE FROM schema_migrations WHERE version=52;
                 DROP INDEX events_git_hints; DELETE FROM schema_migrations WHERE version=51;
                 DROP INDEX processes_live_owner;
                 DELETE FROM schema_migrations WHERE version=50;
                 DROP INDEX events_heavy_agent_generation;
                 DELETE FROM schema_migrations WHERE version=49;
                 DROP INDEX agents_list_archived;
                 ALTER TABLE agents DROP COLUMN archived;
                 ALTER TABLE agents DROP COLUMN chat;
                 DELETE FROM schema_migrations WHERE version=48;
                 DROP INDEX writes_principal_method; DELETE FROM schema_migrations WHERE version=47;
                DROP TABLE artifacts; DROP TABLE cleanup_plans; DELETE FROM schema_migrations WHERE version=46;
              DROP INDEX commands_turn_unknown;
              DELETE FROM schema_migrations WHERE version=44;
              DROP TRIGGER assignment_commands_insert;
             DROP TRIGGER assignment_commands_update;
             DROP TRIGGER assignment_commands_delete;
             DROP TRIGGER assignment_turns_insert;
             DROP TRIGGER assignment_turns_update;
             DROP TRIGGER assignment_turns_delete;
             DROP VIEW assignment_busy_membership;
             DROP INDEX commands_assignment_busy;
             DROP INDEX commands_assignment_turn;
             ALTER TABLE commands DROP COLUMN assignment_busy;
             DELETE FROM schema_migrations WHERE version=43;
             ALTER TABLE agents DROP COLUMN profile_choice;
             DELETE FROM schema_migrations WHERE version=42;
             DROP INDEX settings_history_created;
             DROP TABLE settings_history;
             DELETE FROM schema_migrations WHERE version=41;
             ALTER TABLE inference_clients DROP COLUMN expired_id_through_ms;
             DELETE FROM schema_migrations WHERE version=40;
             ALTER TABLE turns DROP COLUMN preceded_by;
             DELETE FROM schema_migrations WHERE version=45;",
            )
            .unwrap();
        let client = format!("memory:{}", Ulid::new());
        let untouched = format!("memory:{}", Ulid::new());
        for client in [&client, &untouched] {
            store.connection.execute("INSERT INTO inference_clients(client_id,current_version,current_hash,current_file) VALUES (?1,1,'hash','credential.json')", [client]).unwrap();
        }
        let start = 1_800_000_000_000_i64;
        let receipt = Ulid::from(((start + RETRY_WINDOW_MS + 1) as u128) << 80).to_string();
        store.connection.execute(
            "INSERT INTO writes(id,principal,method,payload_digest,result,created_at,inference_seq) VALUES (?1,?2,'inference.retention.advance',?3,?4,?5,1)",
            params![receipt, format!("memory_inference:{client}"), vec![0_u8;32],
                json!({"retain_after_seq":0,"write_retain_after_seq":0,"pruned_calls":0,"pruned_writes":0}).to_string(), start + RETRY_WINDOW_MS + 1],
        ).unwrap();
        store.migrate().unwrap();
        let watermark = |client: &str| {
            store
                .connection
                .query_row(
                    "SELECT expired_id_through_ms FROM inference_clients WHERE client_id=?1",
                    [client],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap()
        };
        assert_eq!(watermark(&client), start);
        assert_eq!(watermark(&untouched), -1);
        let absent = Ulid::from((start as u128) << 80).to_string();
        assert_eq!(
            store
                .inference_write_get(&client, &absent, start + RETRY_WINDOW_MS - 1)
                .unwrap()
                .status,
            eliot_core::api::inference::InferenceWriteStatus::HistoryGap
        );
        assert_eq!(
            store
                .inference_write_get(&client, &receipt, start + RETRY_WINDOW_MS - 1)
                .unwrap()
                .status,
            eliot_core::api::inference::InferenceWriteStatus::Retained
        );
    }

    #[test]
    fn v38_upgrade_preserves_membership_and_previous_publication() {
        let mut store = Store::open(std::path::Path::new(":memory:")).unwrap();
        store
            .connection
            .execute_batch(
                "ALTER TABLE project_assignments DROP COLUMN workspace_adopted;
                 ALTER TABLE project_assignments DROP COLUMN workspace_path;
                 DELETE FROM schema_migrations WHERE version=57;
                 ALTER TABLE turns DROP COLUMN resolution_source;
                 DELETE FROM schema_migrations WHERE version=56;
                 ALTER TABLE commands DROP COLUMN assignment_binding;
                 ALTER TABLE cleanup_plans DROP COLUMN assignment_binding;
                 DROP TABLE project_assignments;
                 DELETE FROM schema_migrations WHERE version=55;
                 DROP TABLE paused_assignment_timers; ALTER TABLE rule_state DROP COLUMN source_present;
                 DELETE FROM schema_migrations WHERE version=54;
                 DROP INDEX agents_list_project_scope; ALTER TABLE agents DROP COLUMN project_scope;
                 DELETE FROM schema_migrations WHERE version=53;
                 DROP TABLE custody_binding; ALTER TABLE agents DROP COLUMN custody_namespace;
                 DELETE FROM schema_migrations WHERE version=52;
                 DROP INDEX events_git_hints; DELETE FROM schema_migrations WHERE version=51;
                 DROP INDEX processes_live_owner;
                 DELETE FROM schema_migrations WHERE version=50;
                 DROP INDEX events_heavy_agent_generation;
                 DELETE FROM schema_migrations WHERE version=49;
                 DROP INDEX agents_list_archived;
                 ALTER TABLE agents DROP COLUMN archived;
                 ALTER TABLE agents DROP COLUMN chat;
                 DELETE FROM schema_migrations WHERE version=48;
                 DROP INDEX writes_principal_method; DELETE FROM schema_migrations WHERE version=47;
                DROP TABLE artifacts; DROP TABLE cleanup_plans; DELETE FROM schema_migrations WHERE version=46;
              DROP INDEX commands_turn_unknown;
              DELETE FROM schema_migrations WHERE version=44;
              DROP TRIGGER assignment_commands_insert;
             DROP TRIGGER assignment_commands_update;
             DROP TRIGGER assignment_commands_delete;
             DROP TRIGGER assignment_turns_insert;
             DROP TRIGGER assignment_turns_update;
             DROP TRIGGER assignment_turns_delete;
             DROP VIEW assignment_busy_membership;
             DROP INDEX commands_assignment_busy;
             DROP INDEX commands_assignment_turn;
             ALTER TABLE commands DROP COLUMN assignment_busy;
             DELETE FROM schema_migrations WHERE version=43;
             ALTER TABLE agents DROP COLUMN profile_choice;
             DELETE FROM schema_migrations WHERE version=42;
             DROP INDEX settings_history_created;
             DROP TABLE settings_history;
             DELETE FROM schema_migrations WHERE version=41;
             DROP INDEX inference_write_inventory;
             ALTER TABLE writes DROP COLUMN inference_seq;
             ALTER TABLE inference_clients DROP COLUMN last_write_seq;
             ALTER TABLE inference_clients DROP COLUMN retain_after_seq;
             ALTER TABLE inference_clients DROP COLUMN write_retain_after_seq;
             ALTER TABLE inference_clients DROP COLUMN previous_file;
             ALTER TABLE inference_clients DROP COLUMN previous_retirement_id;
             ALTER TABLE inference_clients DROP COLUMN previous_retirement_complete;
             ALTER TABLE inference_clients DROP COLUMN previous_read_call_id;
             ALTER TABLE inference_clients DROP COLUMN previous_release_proven;
             ALTER TABLE inference_clients DROP COLUMN previous_restarted;
             ALTER TABLE inference_clients DROP COLUMN expired_id_through_ms;
             ALTER TABLE turns DROP COLUMN preceded_by;
             DELETE FROM schema_migrations WHERE version>=39;",
            )
            .unwrap();
        let client = format!("memory:{}", Ulid::new());
        let publication = json!({"client_id":client,"token_version":1,"credential_file":"inference-credential-original.json","phase":"active"});
        store.connection.execute("INSERT INTO inference_clients(client_id,current_version,current_hash,current_file,previous_version,previous_hash,last_admission_seq) VALUES (?1,2,'new-hash','new.json',1,'old-hash',9)", [&client]).unwrap();
        for (principal, method, result, created) in [
            (
                "owner".to_owned(),
                "inference.client.install",
                publication,
                1,
            ),
            (
                format!("memory_inference:{client}"),
                "inference.text",
                json!({"call_id":"old","admission_seq":9}),
                2,
            ),
            (
                format!("memory_inference:{client}"),
                "inference.cancel",
                json!({"call_id":"old","outcome":"too_late"}),
                3,
            ),
        ] {
            store.connection.execute("INSERT INTO writes(id,principal,method,payload_digest,result,created_at) VALUES (?1,?2,?3,?4,?5,?6)", params![Ulid::new().to_string(),principal,method,vec![0u8;32],result.to_string(),created]).unwrap();
        }
        store.migrate().unwrap();
        let high = store.inference_high_water(&client).unwrap();
        assert_eq!(high.call_high_water_seq, 9);
        assert_eq!(high.write_high_water_seq, 2);
        assert_eq!(high.retain_after_seq, 0);
        let previous: String = store
            .connection
            .query_row(
                "SELECT previous_file FROM inference_clients WHERE client_id=?1",
                [&client],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(previous, "inference-credential-original.json");
        let sequences: Vec<i64> = store
            .connection
            .prepare("SELECT inference_seq FROM writes WHERE principal=?1 ORDER BY created_at")
            .unwrap()
            .query_map([format!("memory_inference:{client}")], |r| r.get(0))
            .unwrap()
            .collect::<std::result::Result<_, _>>()
            .unwrap();
        assert_eq!(sequences, vec![1, 2]);
    }
}
