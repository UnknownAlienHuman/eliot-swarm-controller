use super::*;

pub(super) const CURRENT_SCHEMA_VERSION: i64 = 83;

impl Store {
    pub const SCHEMA_VERSION: i64 = CURRENT_SCHEMA_VERSION;

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
        self.migrate_through_42(current)?;
        self.migrate_after_42(current)
    }

    // Keep the debug-build migration frames small enough for Windows.
    fn migrate_through_42(&mut self, current: i64) -> Result<()> {
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
             CREATE INDEX agents_recent ON agents(updated_at DESC, id DESC);

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
        Ok(())
    }

    fn migrate_after_42(&mut self, current: i64) -> Result<()> {
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
        if current < 58 {
            // v58 adds the four store performance indexes. Databases
            // migrated on this branch before the reconciliation
            // carry the prune index under v55 without the origin/main v55
            // project-assignment tables (max version 55, no assignments).
            // Backfill the assignment tables here so the v58 upgrade heals
            // them; fresh and origin/main databases already have them and
            // take no extra write.
            let has_assignments: bool = self
                .connection
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='project_assignments')",
                    [],
                    |row| row.get(0),
                )
                .map_err(sql_error)?;
            if !has_assignments {
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
                     ALTER TABLE cleanup_plans ADD COLUMN assignment_binding TEXT;",
                )
                .map_err(sql_error)?;
                tx.commit().map_err(sql_error)?;
            }
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            tx.execute_batch(
                "CREATE INDEX IF NOT EXISTS commands_prune_roots ON commands(created_at, id)
                    WHERE parent_command IS NULL;
                 CREATE INDEX IF NOT EXISTS completion_digests_pending_cursor
                    ON completion_digests(delivered_at, created_at, id);
                 CREATE INDEX IF NOT EXISTS agents_list_archived_state
                    ON agents(archived, state, id);
                 CREATE INDEX IF NOT EXISTS workspaces_start_command
                    ON workspaces(start_command_id);
                   INSERT INTO schema_migrations(version) VALUES (58);",
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
        if current < 59 {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            tx.execute_batch(
                "ALTER TABLE turns ADD COLUMN effective_model TEXT;
                 ALTER TABLE turns ADD COLUMN native_error_message TEXT;
                 INSERT INTO schema_migrations(version) VALUES (59);",
            )
            .map_err(sql_error)?;
            tx.commit().map_err(sql_error)?;
        }
        if current < 60 {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            tx.execute_batch(
                "ALTER TABLE artifacts ADD COLUMN seq INTEGER NOT NULL DEFAULT 0;
                 UPDATE artifacts SET seq = (SELECT COUNT(*) FROM artifacts AS older WHERE older.id <= artifacts.id);
                 INSERT INTO schema_migrations(version) VALUES (60);",
            )
            .map_err(sql_error)?;
            tx.commit().map_err(sql_error)?;
        }
        if current < 61 {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            tx.execute_batch(
                "CREATE TABLE IF NOT EXISTS project_transfers (
                     project TEXT PRIMARY KEY,
                     intent TEXT NOT NULL UNIQUE,
                     saved_context TEXT,
                     launch_command TEXT UNIQUE
                 );
                 ALTER TABLE workspaces ADD COLUMN original_preparation TEXT;
                 INSERT INTO schema_migrations(version) VALUES (61);",
            )
            .map_err(sql_error)?;
            tx.commit().map_err(sql_error)?;
        }
        if current < 62 {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            tx.execute_batch(
                "CREATE TABLE launch_intents (
                     id TEXT PRIMARY KEY REFERENCES writes(id),
                     project TEXT NOT NULL,
                     request TEXT NOT NULL,
                     parent_generation INTEGER,
                     assignment_epoch INTEGER,
                     remote_context TEXT,
                     route TEXT
                 );
                 CREATE INDEX launch_intents_project ON launch_intents(project, id);
                 CREATE TABLE assignment_waits (
                     timer TEXT PRIMARY KEY,
                     revision INTEGER NOT NULL,
                     due INTEGER NOT NULL,
                     project TEXT NOT NULL,
                     reason TEXT NOT NULL,
                     route TEXT
                 );
                 CREATE TABLE arbitration_cursor (
                     singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                     project TEXT NOT NULL
                 );
                 CREATE TABLE manager_budgets (
                     project TEXT PRIMARY KEY,
                     manager INTEGER NOT NULL,
                     generation INTEGER NOT NULL,
                     epoch INTEGER NOT NULL,
                     max_live INTEGER,
                     priority INTEGER NOT NULL
                 );
                INSERT INTO schema_migrations(version) VALUES (62);",
            )
            .map_err(sql_error)?;
            tx.commit().map_err(sql_error)?;
        }
        if current < 63 {
            // The sidebar's recent order reads one activity key the Store
            // already maintains on the same writes: `updated_at` is bumped
            // by the core reducer on every native event, so no new column
            // is needed, only the index the ordering seeks.
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            tx.execute_batch(
                "CREATE INDEX IF NOT EXISTS agents_recent ON agents(updated_at DESC, id DESC);
                 INSERT INTO schema_migrations(version) VALUES (63);",
            )
            .map_err(sql_error)?;
            tx.commit().map_err(sql_error)?;
        }
        if current < 64 {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            tx.execute_batch(
                "CREATE TABLE chat_organization (
                     agent INTEGER PRIMARY KEY REFERENCES agents(id),
                     project_link TEXT,
                     tags TEXT NOT NULL DEFAULT '[]'
                 );
                 CREATE INDEX chat_organization_link ON chat_organization(project_link);
                 WITH RECURSIVE chain(chat_id, num) AS (
                     SELECT a.id, a.parent FROM agents a
                     WHERE a.chat = 1 AND a.parent IS NOT NULL
                     UNION ALL
                     SELECT chain.chat_id, p.parent FROM chain
                     JOIN agents p ON p.id = chain.num
                     WHERE chain.num IS NOT NULL
                 ),
                 candidates(chat_id, project) AS (
                     SELECT a.id, a.project FROM agents a
                     WHERE a.chat = 1 AND a.project IS NOT NULL AND a.project <> ''
                       AND EXISTS(SELECT 1 FROM project_registrations r WHERE r.id = a.project)
                     UNION
                     SELECT chain.chat_id, n.project FROM chain
                     JOIN agents n ON n.id = chain.num
                     WHERE n.project IS NOT NULL AND n.project <> ''
                       AND EXISTS(SELECT 1 FROM project_registrations r WHERE r.id = n.project)
                     UNION
                     SELECT a.id, r.id FROM agents a
                     JOIN workspaces w ON w.agent = a.id AND w.phase <> 'removed'
                     JOIN project_registrations r ON r.repo_common_dir = w.repo_common_dir
                     WHERE a.chat = 1
                 ),
                 single(chat_id, project) AS (
                     SELECT chat_id, MIN(project) FROM candidates
                     GROUP BY chat_id HAVING COUNT(DISTINCT project) = 1
                 )
                 INSERT INTO chat_organization(agent, project_link, tags)
                     SELECT single.chat_id, single.project, '[]'
                     FROM single;
                INSERT INTO schema_migrations(version) VALUES (64);",
            )
            .map_err(sql_error)?;
            tx.commit().map_err(sql_error)?;
        }
        if current < 65 {
            // Menu writes carry the overlay revision they observed; the
            // Store refuses a write whose revision moved (k184b P2).
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            tx.execute_batch(
                "ALTER TABLE chat_organization ADD COLUMN revision INTEGER NOT NULL DEFAULT 1;
                 INSERT INTO schema_migrations(version) VALUES (65);",
            )
            .map_err(sql_error)?;
            tx.commit().map_err(sql_error)?;
        }
        if current < 66 {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            // The continue-in-project overlay pointer: which switch source an
            // Agent continues. Plain display text, never a foreign key, so a
            // deleted source leaves it dangling without faulting the writer.
            tx.execute_batch(
                "ALTER TABLE chat_organization ADD COLUMN continued_from TEXT;
                 INSERT INTO schema_migrations(version) VALUES (66);",
            )
            .map_err(sql_error)?;
            tx.commit().map_err(sql_error)?;
        }
        if current < 67 {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            tx.execute_batch(
                "ALTER TABLE turns ADD COLUMN tool_calls INTEGER NOT NULL DEFAULT 0 CHECK(tool_calls >= 0);
                 ALTER TABLE turns ADD COLUMN messages INTEGER NOT NULL DEFAULT 0 CHECK(messages >= 0);
                 CREATE TABLE IF NOT EXISTS git_work_state (
                     workspace TEXT PRIMARY KEY,
                     base_oid TEXT,
                     commits_ahead INTEGER,
                     files_changed INTEGER,
                     insertions INTEGER,
                     deletions INTEGER,
                     diff_digest TEXT,
                     dirty_digest TEXT,
                     work_available INTEGER NOT NULL DEFAULT 0 CHECK(work_available IN (0, 1)),
                     work_error TEXT,
                     last_write_at INTEGER,
                     last_commit_at INTEGER,
                     updated_at INTEGER NOT NULL
                 );
                 INSERT INTO schema_migrations(version) VALUES (67);",
            )
            .map_err(sql_error)?;
            tx.commit().map_err(sql_error)?;
        }
        if current < 68 {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            // Exited child pruning seeks the oldest rows first; the partial
            // index holds only that set, not live, plugin or harness rows.
            tx.execute_batch(
                "CREATE INDEX processes_exited_child_updated ON processes(updated_at)
                     WHERE state = 'exited' AND role = 'child';
                 INSERT INTO schema_migrations(version) VALUES (68);",
            )
            .map_err(sql_error)?;
            tx.commit().map_err(sql_error)?;
        }
        if current < 69 {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            // A manager's strict pause of new admissions, bound to its epoch
            // through the same row as its delegated limit.
            tx.execute_batch(
                "ALTER TABLE manager_budgets ADD COLUMN paused INTEGER NOT NULL DEFAULT 0;
                 INSERT INTO schema_migrations(version) VALUES (69);",
            )
            .map_err(sql_error)?;
            tx.commit().map_err(sql_error)?;
        }
        if current < 70 {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            // The code that skipped or rejected a timer's last scheduled
            // slot; NULL once a slot admits its action.
            tx.execute_batch(
                "ALTER TABLE timers ADD COLUMN last_outcome TEXT;
                 INSERT INTO schema_migrations(version) VALUES (70);",
            )
            .map_err(sql_error)?;
            tx.commit().map_err(sql_error)?;
        }
        if current < 71 {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            tx.execute_batch(
                "ALTER TABLE provider_holds ADD COLUMN next_probe_at INTEGER
                     CHECK(next_probe_at IS NULL OR next_probe_at >= 0);
                 ALTER TABLE provider_holds ADD COLUMN probe_attempts INTEGER NOT NULL DEFAULT 0
                     CHECK(probe_attempts >= 0);
                 ALTER TABLE provider_holds ADD COLUMN probe_profile TEXT
                     CHECK(probe_profile IS NULL OR length(probe_profile) > 0);
                 UPDATE provider_holds
                 SET probe_profile = CASE
                         WHEN scope LIKE 'profile:%' THEN substr(scope, 9)
                         ELSE (SELECT a.profile FROM agents AS a
                               WHERE CASE
                                   WHEN json_type(a.profile_snapshot, '$.account') = 'text'
                                   THEN 'account:' || json_extract(a.profile_snapshot, '$.account')
                                   ELSE 'profile:' || a.profile
                               END = provider_holds.scope
                               ORDER BY a.updated_at DESC, a.id DESC LIMIT 1)
                     END,
                     next_probe_at = CASE
                         WHEN kind IN ('rate_limit','quota') AND retry_after IS NULL
                              AND state = 'held'
                         THEN updated_at + 60000 ELSE NULL
                     END;
                 CREATE INDEX provider_holds_probe_due ON provider_holds(next_probe_at, scope)
                     WHERE next_probe_at IS NOT NULL;
                 CREATE INDEX provider_holds_reset_due ON provider_holds(retry_after, scope)
                     WHERE retry_after IS NOT NULL AND state = 'held'
                       AND kind IN ('rate_limit','quota');
                 INSERT INTO schema_migrations(version) VALUES (71);",
            )
            .map_err(sql_error)?;
            tx.commit().map_err(sql_error)?;
        }
        if current < 72 {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            tx.execute_batch(
                "ALTER TABLE provider_holds ADD COLUMN held_since INTEGER NOT NULL DEFAULT 0
                     CHECK(held_since >= 0);
                 UPDATE provider_holds SET held_since = updated_at;
                 INSERT INTO schema_migrations(version) VALUES (72);",
            )
            .map_err(sql_error)?;
            tx.commit().map_err(sql_error)?;
        }
        if current < 73 {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            let limit = core::MAX_AUTOMATIC_PROVIDER_PROBES;
            // Old retry counters include pre-admission deferrals. Charge them
            // conservatively rather than grant a fresh budget on upgrade.
            tx.execute_batch(&format!(
                "ALTER TABLE provider_holds ADD COLUMN automatic_probes INTEGER NOT NULL DEFAULT 0
                     CHECK(automatic_probes BETWEEN 0 AND {limit});
                 ALTER TABLE provider_holds ADD COLUMN probe_outcome_unknown INTEGER NOT NULL DEFAULT 0
                     CHECK(probe_outcome_unknown IN (0,1));
                 UPDATE provider_holds
                 SET automatic_probes = MIN({limit}, probe_attempts + CASE WHEN state='probing' THEN 1 ELSE 0 END),
                     next_probe_at = CASE WHEN probe_attempts >= {limit} THEN NULL ELSE next_probe_at END;
                 INSERT INTO schema_migrations(version) VALUES (73);"
            )).map_err(sql_error)?;
            tx.commit().map_err(sql_error)?;
        }
        if current < 74 {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            tx.execute_batch(
                "CREATE INDEX source_state_open_gap_seq ON source_state(gap_seq)
                     WHERE state = 'open' AND gap_seq IS NOT NULL AND gap_from IS NOT NULL;
                 INSERT INTO schema_migrations(version) VALUES (74);",
            )
            .map_err(sql_error)?;
            tx.commit().map_err(sql_error)?;
        }
        if current < 75 {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            tx.execute_batch(
                "CREATE INDEX messages_presentation_command ON messages(presentation_command)
                     WHERE presentation_command IS NOT NULL;
                 CREATE INDEX mail_notices_open_notice_command ON mail_notices(open_notice_command)
                     WHERE open_notice_command IS NOT NULL;
                 CREATE INDEX writes_created_at ON writes(created_at, id);
                 INSERT INTO schema_migrations(version) VALUES (75);",
            )
            .map_err(sql_error)?;
            tx.commit().map_err(sql_error)?;
        }
        if current < 76 {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            tx.execute_batch(
                "ALTER TABLE turns ADD COLUMN start_command_id TEXT;
                 WITH event_facts AS (
                     SELECT e.agent, e.generation,
                            json_extract(e.data, '$.turn_id') AS turn_id,
                            json_extract(e.data, '$.command_id') AS command_id
                     FROM events e
                     WHERE e.kind = 'turn.started'
                       AND e.agent IS NOT NULL AND e.generation IS NOT NULL
                       AND json_type(e.data, '$.turn_id') = 'text'
                       AND json_type(e.data, '$.command_id') = 'text'
                       AND length(json_extract(e.data, '$.turn_id')) > 0
                       AND length(json_extract(e.data, '$.command_id')) > 0
                 ), invalid_event_bindings AS (
                     SELECT DISTINCT f.agent, f.generation, f.turn_id
                     FROM event_facts f
                     JOIN commands c ON c.id = f.command_id
                     WHERE c.target IS NOT f.agent
                        OR c.generation IS NOT f.generation
                        OR c.verb != 'send'
                        OR c.turn_id IS NOT f.turn_id
                 ), candidates AS (
                     SELECT f.agent, f.generation, f.turn_id, f.command_id
                     FROM event_facts f
                     LEFT JOIN commands c ON c.id = f.command_id
                     WHERE c.id IS NULL
                        OR (c.target = f.agent AND c.generation = f.generation
                            AND c.verb = 'send' AND c.turn_id = f.turn_id)
                     UNION ALL
                     SELECT c.target, c.generation, c.turn_id, c.id
                     FROM commands c
                     WHERE c.target IS NOT NULL AND c.generation IS NOT NULL
                       AND c.turn_id IS NOT NULL AND c.verb = 'send'
                       AND NOT EXISTS (
                           SELECT 1 FROM invalid_event_bindings i
                           WHERE i.agent = c.target AND i.generation = c.generation
                             AND i.turn_id = c.turn_id
                       )
                 ), unique_bindings AS (
                     SELECT agent, generation, turn_id, MIN(command_id) AS command_id
                     FROM candidates
                     GROUP BY agent, generation, turn_id
                     HAVING COUNT(DISTINCT command_id) = 1
                 )
                 UPDATE turns
                 SET start_command_id = (
                     SELECT u.command_id FROM unique_bindings u
                     WHERE u.agent = turns.agent AND u.generation = turns.generation
                       AND u.turn_id = turns.turn_id
                 )
                 WHERE EXISTS (
                     SELECT 1 FROM unique_bindings u
                     WHERE u.agent = turns.agent AND u.generation = turns.generation
                       AND u.turn_id = turns.turn_id
                 );
                 INSERT INTO schema_migrations(version) VALUES (76);",
            )
            .map_err(sql_error)?;
            tx.commit().map_err(sql_error)?;
        }
        if current < 77 {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            tx.execute_batch(
                "CREATE TABLE git_committed_oids (
                     agent INTEGER NOT NULL,
                     generation INTEGER NOT NULL,
                     source TEXT NOT NULL DEFAULT 'git' CHECK(source = 'git'),
                     oid TEXT NOT NULL,
                     PRIMARY KEY(agent, generation, oid),
                     FOREIGN KEY(agent, generation, source)
                         REFERENCES source_state(agent, generation, source) ON DELETE CASCADE
                 ) WITHOUT ROWID;
                 INSERT OR IGNORE INTO git_committed_oids(agent, generation, oid)
                 SELECT DISTINCT e.agent, e.generation, json_extract(e.data, '$.oid')
                 FROM events e
                 JOIN source_state s ON s.agent = e.agent AND s.generation = e.generation
                     AND s.source = 'git' AND s.state = 'open'
                 JOIN agents a ON a.id = e.agent AND a.generation = e.generation
                 WHERE e.source = 'git' AND e.kind = 'git.committed'
                   AND e.agent IS NOT NULL AND e.generation IS NOT NULL
                   AND json_type(e.data, '$.oid') = 'text'
                   AND length(json_extract(e.data, '$.oid')) > 0;
                 CREATE TRIGGER git_committed_oids_source_closed
                 AFTER UPDATE OF state ON source_state
                 WHEN OLD.source = 'git' AND OLD.state = 'open' AND NEW.state = 'closed'
                 BEGIN
                     DELETE FROM git_committed_oids
                     WHERE agent = NEW.agent AND generation = NEW.generation;
                 END;
                 CREATE TRIGGER git_committed_oids_generation_replaced
                 AFTER UPDATE OF generation ON agents
                 WHEN OLD.generation != NEW.generation
                 BEGIN
                     DELETE FROM git_committed_oids
                     WHERE agent = OLD.id AND generation = OLD.generation;
                 END;
                 INSERT INTO schema_migrations(version) VALUES (77);",
            )
            .map_err(sql_error)?;
            tx.commit().map_err(sql_error)?;
        }
        if current < 78 {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            tx.execute_batch(
                "ALTER TABLE turns ADD COLUMN insertion_seq INTEGER NOT NULL DEFAULT 0;
                 UPDATE turns SET insertion_seq = rowid;
                 CREATE TABLE turn_insertion_sequence (
                     singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                     last_seq INTEGER NOT NULL CHECK(last_seq >= 0)
                 );
                 INSERT INTO turn_insertion_sequence
                     SELECT 1, COALESCE(MAX(insertion_seq), 0) FROM turns;
                 CREATE INDEX turns_agent_result_order
                     ON turns(agent, generation DESC, insertion_seq DESC);
                 CREATE TABLE agent_generation_reasons (
                     agent INTEGER NOT NULL REFERENCES agents(id) ON DELETE CASCADE,
                     generation INTEGER NOT NULL CHECK(generation >= 0),
                     reason TEXT NOT NULL CHECK(reason IN (
                         'launch', 'restart_resume', 'owner_resume', 'agent_resume',
                         'switch', 'handoff', 'attach', 'detach', 'unknown'
                     )),
                     PRIMARY KEY(agent, generation)
                 );
                 CREATE TABLE restart_resume_intents (
                     command_id TEXT PRIMARY KEY REFERENCES commands(id) ON DELETE CASCADE
                 );
                 INSERT INTO agent_generation_reasons(agent, generation, reason)
                 SELECT t.agent, t.generation,
                     CASE
                         WHEN t.generation = 1 AND EXISTS (
                             SELECT 1 FROM commands c
                             WHERE c.target = t.agent AND c.generation = t.generation
                               AND c.verb = 'launch' AND c.link_kind = 'switch' AND c.ordinal = 0
                         ) THEN 'switch'
                         WHEN t.generation = 1 AND EXISTS (
                             SELECT 1 FROM commands c
                             WHERE c.target = t.agent AND c.generation = t.generation
                               AND c.verb = 'launch'
                         ) THEN 'launch'
                         ELSE 'unknown'
                     END
                 FROM (SELECT DISTINCT agent, generation FROM turns) t;
                 INSERT INTO schema_migrations(version) VALUES (78);",
            )
            .map_err(sql_error)?;
            tx.commit().map_err(sql_error)?;
        }
        if current < 79 {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            tx.execute_batch(
                "CREATE INDEX events_monitoring_agent
                 ON events(agent, kind, seq)
                 WHERE kind IN ('session.growth', 'x.user.agent.attention_step');
                 CREATE INDEX events_attention_report
                 ON events(json_extract(data, '$.data.incident_id'), json_extract(data, '$.data.phase'), seq)
                 WHERE kind = 'x.user.agent.attention';
                 CREATE INDEX events_attention_step
                 ON events(json_extract(data, '$.incident_id'), json_extract(data, '$.step'), seq)
                 WHERE kind = 'x.user.agent.attention_step';
                 CREATE INDEX events_tool_turn
                 ON events(agent, generation, kind, json_extract(data, '$.turn_id'), seq)
                 WHERE kind = 'tool.started' OR kind = 'tool.completed';
                 CREATE TABLE turn_attention_steps (
                     agent INTEGER NOT NULL,
                     generation INTEGER NOT NULL,
                     turn_id TEXT NOT NULL,
                     incident_kind TEXT NOT NULL,
                     step TEXT NOT NULL,
                     event_seq INTEGER NOT NULL,
                     claimed_at INTEGER NOT NULL,
                     command_id TEXT,
                     after_question_seq INTEGER,
                     PRIMARY KEY(agent, generation, turn_id, incident_kind, step),
                     FOREIGN KEY(agent, generation, turn_id)
                         REFERENCES turns(agent, generation, turn_id) ON DELETE CASCADE
                 );
                 CREATE UNIQUE INDEX turn_attention_step_command
                     ON turn_attention_steps(command_id) WHERE command_id IS NOT NULL;
                 CREATE UNIQUE INDEX turn_attention_one_question
                     ON turn_attention_steps(agent, generation, turn_id) WHERE step = 'question';
                 ALTER TABLE provider_holds ADD COLUMN attention_notified INTEGER NOT NULL DEFAULT 0
                     CHECK(attention_notified IN (0,1));
                 INSERT INTO schema_migrations(version) VALUES (79);",
            )
            .map_err(sql_error)?;
            tx.commit().map_err(sql_error)?;
        }
        if current < 80 {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            // A legacy cursor preserves only the latest name. Recover older
            // names from surviving launches before command retention runs.
            tx.execute_batch(
                "CREATE TABLE assignment_admissions (
                     assignment_key TEXT NOT NULL,
                     brief TEXT NOT NULL,
                     PRIMARY KEY (assignment_key, brief)
                 ) WITHOUT ROWID;
                 INSERT OR IGNORE INTO assignment_admissions
                 SELECT json_extract(args,'$.assignment.key'),
                        json_extract(args,'$.assignment.brief')
                 FROM commands
                 WHERE verb='launch' AND principal='owner'
                   AND json_type(args,'$.assignment.key')='text'
                   AND json_type(args,'$.assignment.brief')='text';
                 INSERT OR IGNORE INTO assignment_admissions
                 SELECT substr(key, length('assignment.') + 1),
                        json_extract(value,'$.cursor')
                 FROM vars
                 WHERE scope='global' AND key GLOB 'assignment.*'
                   AND set_by='owner' AND note='automation assignment cursor'
                   AND json_type(value,'$.cursor')='text';
                 INSERT INTO schema_migrations(version) VALUES (80);",
            )
            .map_err(sql_error)?;
            tx.commit().map_err(sql_error)?;
        }
        if current < 81 {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            tx.execute_batch(
                "CREATE TABLE assignment_legacy_floors (
                     assignment_key TEXT PRIMARY KEY,
                     cursor TEXT NOT NULL
                 ) WITHOUT ROWID;
                 INSERT OR IGNORE INTO assignment_legacy_floors (assignment_key, cursor)
                 SELECT substr(key, length('assignment.') + 1),
                        json_extract(value,'$.cursor')
                 FROM vars
                 WHERE scope='global' AND key GLOB 'assignment.*'
                   AND set_by='owner' AND note='automation assignment cursor'
                   AND json_type(value,'$.cursor')='text';
                 INSERT INTO schema_migrations(version) VALUES (81);",
            )
            .map_err(sql_error)?;
            tx.commit().map_err(sql_error)?;
        }
        if current < 82 {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            tx.execute_batch(
                "CREATE TABLE agent_wakes (
                     id TEXT PRIMARY KEY,
                     agent INTEGER NOT NULL REFERENCES agents(id),
                     generation INTEGER NOT NULL CHECK(generation > 0),
                     turn_id TEXT NOT NULL,
                     condition TEXT NOT NULL CHECK(json_valid(condition)),
                     state TEXT NOT NULL CHECK(state IN (
                         'waiting', 'satisfied', 'delivery_pending',
                         'delivered', 'not_matched', 'failed', 'unknown', 'stale', 'expired'
                     )),
                     fact TEXT,
                     delivery_command_id TEXT NOT NULL UNIQUE,
                     delivery_attempts INTEGER NOT NULL DEFAULT 0 CHECK(delivery_attempts >= 0),
                     delivery_next_at INTEGER NOT NULL DEFAULT 0,
                     created_at INTEGER NOT NULL,
                     expires_at INTEGER NOT NULL CHECK(expires_at > created_at),
                     settled_at INTEGER,
                     FOREIGN KEY(agent, generation, turn_id)
                         REFERENCES turns(agent, generation, turn_id)
                 );
                 CREATE INDEX agent_wakes_state_cursor
                     ON agent_wakes(state, created_at, id);
                 CREATE UNIQUE INDEX agent_wakes_one_open_per_generation
                     ON agent_wakes(agent, generation)
                     WHERE state IN ('waiting','satisfied','delivery_pending');
                 INSERT INTO schema_migrations(version) VALUES (82);",
            )
            .map_err(sql_error)?;
            tx.commit().map_err(sql_error)?;
        }
        if current < 83 {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            tx.execute_batch(
                "ALTER TABLE agents ADD COLUMN instruction_delivery TEXT;
                 INSERT INTO schema_migrations(version) VALUES (83);",
            )
            .map_err(sql_error)?;
            tx.commit().map_err(sql_error)?;
        }
        Ok(())
    }
}
