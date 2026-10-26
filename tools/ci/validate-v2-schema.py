#!/usr/bin/env python3
"""Execute documented v2 SQLite DDL and key cross-document invariants."""

from __future__ import annotations

import re
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCHEMA_DOCS = [
    ROOT / "docs/spec/core.md",
    ROOT / "docs/spec/monitoring.md",
    ROOT / "docs/spec/coordination.md",
    ROOT / "docs/spec/automation.md",
    ROOT / "docs/spec/configuration.md",
]
SQL_FENCE_RE = re.compile(r"^```sql\s*$", re.IGNORECASE)
CLOSE_FENCE_RE = re.compile(r"^```\s*$")
EXPECTED_TABLES = {
    "store_meta",
    "id_counters",
    "write_receipts",
    "agents",
    "agent_bindings",
    "events",
    "plugin_source_watermarks",
    "source_family_owners",
    "native_event_receipts",
    "commands",
    "command_links",
    "native_inputs",
    "turns",
    "turn_inputs",
    "custodies",
    "processes",
    "workspaces",
    "workspace_steps",
    "agent_edits",
    "messages",
    "mail_batches",
    "vars",
    "claims",
    "rule_activations",
    "event_rule_state",
    "automation_runs",
    "automation_actions",
    "timers",
    "goals",
    "hook_decisions",
    "capacity_reservations",
    "provider_holds",
}


def extract_sql_blocks(path: Path) -> list[tuple[int, str]]:
    text = path.read_text(encoding="utf-8")
    blocks: list[tuple[int, str]] = []
    start_line: int | None = None
    current: list[str] = []

    for line_number, line in enumerate(text.splitlines(), 1):
        if start_line is None:
            if SQL_FENCE_RE.match(line):
                start_line = line_number + 1
                current = []
            continue
        if CLOSE_FENCE_RE.match(line):
            sql = "\n".join(current).strip()
            if not sql:
                raise ValueError(f"{path.relative_to(ROOT)}:{start_line}: empty SQL block")
            blocks.append((start_line, sql))
            start_line = None
            current = []
        else:
            current.append(line)

    if start_line is not None:
        raise ValueError(f"{path.relative_to(ROOT)}:{start_line}: unclosed SQL block")
    return blocks


def execute_documented_schema(connection: sqlite3.Connection) -> int:
    count = 0
    for path in SCHEMA_DOCS:
        if not path.is_file():
            raise FileNotFoundError(path.relative_to(ROOT))
        for line_number, sql in extract_sql_blocks(path):
            try:
                connection.executescript(sql)
            except sqlite3.Error as error:
                raise RuntimeError(
                    f"{path.relative_to(ROOT)}:{line_number}: SQLite rejected documented DDL: {error}"
                ) from error
            count += 1

    tables = {
        row[0]
        for row in connection.execute(
            "SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        )
    }
    missing = sorted(EXPECTED_TABLES - tables)
    if missing:
        raise AssertionError(f"documented schema is missing tables: {', '.join(missing)}")
    return count


def expect_integrity(
    connection: sqlite3.Connection,
    sql: str,
    parameters: tuple[object, ...] = (),
    *,
    label: str,
) -> None:
    try:
        connection.execute(sql, parameters)
    except sqlite3.IntegrityError:
        return
    raise AssertionError(f"expected SQLite integrity failure: {label}")


def validate_agent_and_binding(connection: sqlite3.Connection) -> None:
    token_hash = sqlite3.Binary(b"T" * 32)

    connection.execute(
        """
        INSERT INTO agents (
          id,project,root,depth,origin,harness,profile,state,created_at,updated_at
        ) VALUES (1,'p',1,0,'eliot','fake','fake','created',1,1)
        """
    )
    connection.execute(
        """
        INSERT INTO agent_bindings (
          agent,generation,mode,profile_name,profile_revision,binding,created_at
        ) VALUES (1,1,'launch','fake','r1','{}',1)
        """
    )
    connection.execute(
        """
        UPDATE agents
        SET generation=1,token_version=1,routing_enabled=1,token_hash=?,state='starting'
        WHERE id=1
        """,
        (token_hash,),
    )
    connection.execute("UPDATE agents SET state='idle' WHERE id=1")

    # Detach revokes routing without resetting monotonic generation.
    connection.execute(
        "UPDATE agents SET observe_only=1,routing_enabled=0,token_hash=NULL WHERE id=1"
    )
    connection.execute(
        """
        UPDATE agents
        SET observe_only=0,routing_enabled=1,token_version=2,token_hash=?
        WHERE id=1
        """,
        (token_hash,),
    )
    connection.execute(
        "UPDATE agents SET state='stopped',routing_enabled=0,token_hash=NULL WHERE id=1"
    )

    expect_integrity(
        connection,
        """
        UPDATE agents
        SET state='idle',observe_only=0,routing_enabled=1,token_hash=NULL
        WHERE id=1
        """,
        label="directly routed live Agent without token hash",
    )
    expect_integrity(
        connection,
        """
        UPDATE agents
        SET state='idle',observe_only=0,routing_enabled=0,token_hash=?
        WHERE id=1
        """,
        (token_hash,),
        label="routing-disabled Agent with token hash",
    )
    expect_integrity(
        connection,
        "UPDATE agents SET token_hash=? WHERE id=1",
        (sqlite3.Binary(b"short"),),
        label="invalid token hash length",
    )

    # A live native child may be controlled through the parent plugin with no direct token.
    connection.execute(
        """
        INSERT INTO agents (
          id,generation,token_version,routing_enabled,name,project,parent,root,depth,
          origin,harness,profile,state,control,guard_exceeded,created_at,updated_at
        ) VALUES (2,1,0,0,'child','p',1,1,9,'native','fake','observe:fake','idle',
                  'self',1,2,2)
        """
    )
    connection.execute(
        """
        INSERT INTO agent_bindings (
          agent,generation,mode,profile_name,profile_revision,binding,created_at
        ) VALUES (2,1,'observe_attach','observe:fake','r1','{}',2)
        """
    )
    expect_integrity(
        connection,
        """
        INSERT INTO agent_bindings (
          agent,generation,mode,profile_name,profile_revision,binding,created_at
        ) VALUES (2,1,'control_attach','fake','r2','{}',3)
        """,
        label="two bindings for one Agent generation",
    )


def validate_receipts_and_commands(connection: sqlite3.Connection) -> None:
    digest = sqlite3.Binary(b"A" * 32)
    receipt_id = "01ARZ3NDEKTSV4RRFFQ69G5FAV"
    connection.execute(
        """
        INSERT INTO write_receipts (
          id,id_time,principal,method,payload_digest,state,admitted_at,updated_at
        ) VALUES (?,?,?,?,?,'final',?,?)
        """,
        (receipt_id, 1, "owner", "command.submit", digest, 1, 1),
    )
    connection.execute(
        """
        INSERT INTO commands (
          id,receipt_id,target,generation,verb,args,origin,state,created_at,updated_at
        ) VALUES (?,?,?,?,?,?,?,'applied',?,?)
        """,
        (receipt_id, receipt_id, 1, 1, "send", "{}", "owner", 1, 1),
    )
    connection.execute(
        """
        INSERT INTO commands (
          id,target,generation,verb,args,origin,state,created_at,updated_at
        ) VALUES ('d_internal',1,1,'send','{}','rule:r','queued',1,1)
        """
    )
    expect_integrity(
        connection,
        """
        INSERT INTO commands (
          id,target,generation,verb,args,origin,state,created_at,updated_at
        ) VALUES ('not-derived',1,1,'send','{}','rule:r','queued',1,1)
        """,
        label="internal command without derived prefix",
    )
    expect_integrity(
        connection,
        "DELETE FROM write_receipts WHERE id=?",
        (receipt_id,),
        label="receipt deletion while command references it",
    )
    connection.execute("DELETE FROM commands WHERE id=?", (receipt_id,))
    connection.execute("DELETE FROM write_receipts WHERE id=?", (receipt_id,))


def validate_inputs_and_turns(connection: sqlite3.Connection) -> None:
    # Direct command→turn association is valid even without a separate native input id.
    connection.execute(
        """
        INSERT INTO native_inputs (
          command_id,agent,generation,native_input_id,state,first_observed_at,updated_at
        ) VALUES ('pruned-command',1,1,NULL,'associated',2,2)
        """
    )
    connection.execute(
        """
        INSERT INTO turns (
          agent,generation,turn_id,state,status,resolution_source,first_observed_at,
          completed_at
        ) VALUES (1,1,'turn-1','completed','completed','native',2,3)
        """
    )
    connection.execute(
        """
        INSERT INTO turn_inputs (agent,generation,turn_id,command_id,ordinal)
        VALUES (1,1,'turn-1','pruned-command',0)
        """
    )
    connection.execute(
        "UPDATE native_inputs SET native_input_id='native-input-1' WHERE command_id='pruned-command'"
    )
    connection.execute(
        """
        INSERT INTO native_inputs (
          command_id,agent,generation,native_input_id,state,first_observed_at,updated_at
        ) VALUES ('another-command',1,1,NULL,'admitted',3,3)
        """
    )
    expect_integrity(
        connection,
        """
        UPDATE native_inputs
        SET native_input_id='native-input-1'
        WHERE command_id='another-command'
        """,
        label="duplicate non-null native input identity",
    )


def validate_sources_and_replay(connection: sqlite3.Connection) -> None:
    digest_a = sqlite3.Binary(b"A" * 32)
    digest_b = sqlite3.Binary(b"B" * 32)

    connection.execute(
        """
        INSERT INTO plugin_source_watermarks (
          agent,generation,open_id,source_key,source_name,mechanism,dedupe_mode,
          open_digest,high_water_seq,state,updated_at
        ) VALUES (1,1,'open-1','source-1','fake-stream','stdio','event_id',?,0,'open',1)
        """,
        (digest_a,),
    )
    connection.execute(
        """
        INSERT INTO source_family_owners (agent,generation,family,source_key)
        VALUES (1,1,'turn','source-1')
        """
    )
    connection.execute(
        """
        INSERT INTO plugin_source_watermarks (
          agent,generation,open_id,source_key,source_name,mechanism,dedupe_mode,
          open_digest,high_water_seq,state,updated_at
        ) VALUES (1,1,'open-2','source-2','fake-backup','transcript','strict_cursor',?,0,'open',1)
        """,
        (digest_b,),
    )
    expect_integrity(
        connection,
        """
        INSERT INTO source_family_owners (agent,generation,family,source_key)
        VALUES (1,1,'turn','source-2')
        """,
        label="two current owners for one family",
    )
    expect_integrity(
        connection,
        """
        INSERT INTO plugin_source_watermarks (
          agent,generation,open_id,source_key,source_name,mechanism,dedupe_mode,
          open_digest,high_water_seq,state,updated_at
        ) VALUES (1,1,'open-1','source-3','different','stdio','none',?,0,'open',1)
        """,
        (digest_b,),
        label="reusing source open id for another epoch",
    )

    connection.execute(
        """
        INSERT INTO events (
          seq,ts,kind,agent,generation,source,source_key,source_seq,native_id,data
        ) VALUES (1,1,'turn.completed',1,1,'plugin:fake','source-1',1,'native-event-1','{}')
        """
    )
    connection.execute(
        """
        INSERT INTO native_event_receipts (
          agent,generation,kind,native_id,payload_digest,first_store_seq,
          first_source_key,created_at
        ) VALUES (1,1,'turn.completed','native-event-1',?,1,'source-1',1)
        """,
        (digest_a,),
    )
    connection.execute("DELETE FROM events WHERE seq=1")
    # Event retention must not remove the replay receipt.
    remaining = connection.execute(
        """
        SELECT first_store_seq FROM native_event_receipts
        WHERE agent=1 AND generation=1 AND kind='turn.completed'
          AND native_id='native-event-1'
        """
    ).fetchone()
    if remaining != (1,):
        raise AssertionError("native event receipt did not survive event pruning")
    expect_integrity(
        connection,
        """
        INSERT INTO native_event_receipts (
          agent,generation,kind,native_id,payload_digest,first_store_seq,
          first_source_key,created_at
        ) VALUES (1,1,'turn.completed','native-event-1',?,2,'source-2',2)
        """,
        (digest_b,),
        label="same native event identity inserted twice",
    )


def validate_workspace_and_automation(connection: sqlite3.Connection) -> None:
    digest_a = sqlite3.Binary(b"A" * 32)
    digest_b = sqlite3.Binary(b"B" * 32)

    connection.execute(
        """
        INSERT INTO workspaces (
          agent,project,repo_path,requested_branch,base_ref,base_oid,worktree_path,
          setup_revision,state,created_at,updated_at
        ) VALUES (1,'p','/repo','feat','main','abc','/data/w/1','setup-r1','ready',1,1)
        """
    )
    connection.execute(
        """
        INSERT INTO workspace_steps (
          agent,idx,kind,intent,state,created_at,updated_at
        ) VALUES (1,0,'git.worktree','{}','applied',1,1)
        """
    )

    for activation_id, digest, definition, state, activated_at in (
        ("activation-a1", digest_a, "A", "retiring", 1),
        ("activation-b", digest_b, "B", "retiring", 2),
        ("activation-a2", digest_a, "A", "active", 3),
    ):
        connection.execute(
            """
            INSERT INTO rule_activations (
              principal,rule,activation_id,definition_digest,definition,state,activated_at
            ) VALUES ('global','r',?,?,?,?,?)
            """,
            (activation_id, digest, definition, state, activated_at),
        )
    connection.execute(
        """
        INSERT INTO event_rule_state (
          principal,rule,activation_id,event_cursor,retire_at_seq
        ) VALUES ('global','r','activation-a1',10,12)
        """
    )
    connection.execute(
        """
        INSERT INTO event_rule_state (
          principal,rule,activation_id,event_cursor,retire_at_seq
        ) VALUES ('global','r','activation-b',12,20)
        """
    )
    connection.execute(
        """
        INSERT INTO event_rule_state (principal,rule,activation_id,event_cursor)
        VALUES ('global','r','activation-a2',20)
        """
    )

    connection.execute(
        """
        INSERT INTO goals (
          agent,mode,agent_generation,text,cadence,status,max_turns,no_progress,
          eligibility_seq,updated_at
        ) VALUES (1,'eliot',1,'finish','on_idle','active',20,3,42,1)
        """
    )
    connection.execute(
        """
        INSERT INTO automation_runs (
          id,principal,kind,owner_key,owner_activation,trigger_key,state,created_at,updated_at
        ) VALUES ('d_goal_42','goal:1','goal','1/eliot','0','eligible:42','applied',1,1)
        """
    )
    expect_integrity(
        connection,
        """
        INSERT INTO automation_runs (
          id,principal,kind,owner_key,owner_activation,trigger_key,state,created_at,updated_at
        ) VALUES ('d_goal_again','goal:1','goal','1/eliot','0','eligible:42','queued',2,2)
        """,
        label="duplicate Goal round for one eligibility sequence",
    )


def validate_mail_and_claims(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        INSERT INTO mail_batches (id,recipient,message_ids,created_at)
        VALUES ('batch-1',1,'[]',1)
        """
    )
    expect_integrity(
        connection,
        """
        INSERT INTO mail_batches (id,recipient,message_ids,created_at)
        VALUES ('batch-2',1,'[]',2)
        """,
        label="two open inbox batches for one recipient",
    )

    connection.execute(
        """
        INSERT INTO claims (id,project,agent,generation,glob,created_at)
        VALUES (1,'p',1,1,'src/**',1)
        """
    )
    expect_integrity(
        connection,
        """
        INSERT INTO claims (id,project,agent,generation,glob,created_at)
        VALUES (2,'p',1,1,'src/**',2)
        """,
        label="duplicate claim for exact generation/glob",
    )


def validate_invariants(connection: sqlite3.Connection) -> None:
    validate_agent_and_binding(connection)
    validate_receipts_and_commands(connection)
    validate_inputs_and_turns(connection)
    validate_sources_and_replay(connection)
    validate_workspace_and_automation(connection)
    validate_mail_and_claims(connection)

    connection.commit()
    violations = list(connection.execute("PRAGMA foreign_key_check"))
    if violations:
        raise AssertionError(f"foreign_key_check reported violations: {violations}")


def main() -> int:
    connection = sqlite3.connect(":memory:")
    connection.execute("PRAGMA foreign_keys=ON")
    try:
        block_count = execute_documented_schema(connection)
        validate_invariants(connection)
    except (OSError, UnicodeError, ValueError, RuntimeError, AssertionError, sqlite3.Error) as error:
        print(f"v2 SQLite specification validation failed: {error}", file=sys.stderr)
        return 1
    finally:
        connection.close()

    print(
        f"Executed {block_count} documented SQL blocks and validated "
        f"{len(EXPECTED_TABLES)} required tables plus key CHECK/FK/UNIQUE invariants."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
