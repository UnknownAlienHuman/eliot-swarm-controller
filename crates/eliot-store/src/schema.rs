use super::*;

pub(super) const CURRENT_SCHEMA_VERSION: i64 = 29;

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
                 INSERT INTO schema_migrations(version) VALUES (29);",
            ).map_err(sql_error)?;
            tx.commit().map_err(sql_error)?;
        }
        Ok(())
    }
}
