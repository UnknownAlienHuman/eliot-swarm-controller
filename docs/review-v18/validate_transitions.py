"""Small reference-contract models and SQLite transaction examples, not controller tests."""
from pathlib import Path
import sqlite3,json
P=Path(__file__).resolve().parents[1];S=P/'agent_swarm.spec-v18';checks=[]
def record(name,ok,detail=''):
 checks.append({'name':name,'passed':bool(ok),'detail':detail})
def make():
 c=sqlite3.connect(':memory:',isolation_level=None);c.executescript((S/'migrations/001_core.sql').read_text())
 c.execute("INSERT INTO meta VALUES('execution_mode','{\"new_work\":\"enabled\"}')")
 c.execute("INSERT INTO bindings(binding_id,generation,lane_id,module_instance_id,module_artifact_id,state,native_scope_key,native_root_id,route_json,created_at_ms) VALUES('b',1,'l','m','build-1','ready','fixture-store','fixture-root','{}',0)")
 c.execute("INSERT INTO tasks(task_id,project_id,revision,state,spec_json,created_at_ms,updated_at_ms) VALUES('t','p',1,'open','{}',0,0)")
 c.execute("INSERT INTO attempts(attempt_id,task_id,task_revision,task_snapshot_json,owner_id,binding_id,binding_generation,state,created_at_ms,updated_at_ms) VALUES('a','t',1,'{}','manager','b',1,'reserved',0,0)")
 for oid in ('start','duplicate'):
  c.execute("INSERT INTO operations(operation_id,caller_id,client_request_id,method,original_request_json,effective_request_json,task_id,attempt_id,binding_id,binding_generation,state,due_at_ms,created_at_ms,updated_at_ms) VALUES(?,'caller',?,'task.dispatch','{}','{}','t','a','b',1,'queued',0,0,0)",(oid,oid))
 c.execute("UPDATE attempts SET start_operation_id='start' WHERE attempt_id='a'")
 return c
q=(S/'transactions/begin-initial-send.reference.sql').read_text()
def send(c,oid='start'): return c.execute(q,{'operation_id':oid,'now_ms':20}).fetchall()
for label,mut in [
 ('current request',None),
 ('Task revision changed',"UPDATE tasks SET revision=2 WHERE task_id='t'"),
 ('Task archived',"UPDATE tasks SET state='archived' WHERE task_id='t'"),
 ('Attempt released',"UPDATE attempts SET state='cancelled',released_at_ms=2 WHERE attempt_id='a'"),
 ('line draining',"UPDATE bindings SET state='draining' WHERE binding_id='b'"),
 ('project paused',"UPDATE meta SET value_json='{\"new_work\":\"disabled\"}' WHERE key='execution_mode'"),
 ('producer already bound/running',"UPDATE attempts SET state='running' WHERE attempt_id='a'"),
]:
 c=make()
 if mut:c.execute(mut)
 c.execute('BEGIN IMMEDIATE');got=send(c);c.execute('COMMIT')
 record('begin_send / '+label,bool(got)==(mut is None),str(got));c.close()
c=make();c.execute('BEGIN IMMEDIATE');a=send(c);b=send(c);c.execute('COMMIT')
record('CAS allows one sender',len(a)==1 and not b);c.close()
c=make();c.execute('BEGIN IMMEDIATE');got=send(c,'duplicate');c.execute('COMMIT')
record('fresh request ID cannot dispatch second initial operation',got==[]);c.close()
c=make();c.execute('BEGIN IMMEDIATE');rows=send(c);c.execute('ROLLBACK')
record('RETURNING before rollback is not a committed dispatch ticket',len(rows)==1 and c.execute("SELECT state FROM operations WHERE operation_id='start'").fetchone()[0]=='queued');c.close()
# Contract models with explicit scope; do not pretend they execute a vendor transport.
for name,effective,required,expected in [
 ('Max applied and remains current','max','max',True),
 ('Max applied then overwritten by high','high','max',False),
]:
 record('settings model / '+name,(effective==required)==expected)
for status,newer_revision,expected in [('accepted',False,True),('accepted',True,True),('revoked',False,False)]:
 allowed=status=='accepted'
 record('pinned dependency / '+status+('/newer revision exists' if newer_revision else ''),allowed==expected)
# Counterexample to retention-by-age: a bare completed mutation could lose its dedupe identity.
c=make();c.execute("UPDATE attempts SET start_operation_id=NULL WHERE attempt_id='a'")
c.execute("UPDATE operations SET state='settled', settled_at_ms=1, result_json='{\"outcome\":\"success\"}' WHERE operation_id='start'")
c.execute("DELETE FROM operations WHERE operation_id='start'")
record('SQL does not itself protect idempotency history from retention',c.execute("SELECT 1 FROM operations WHERE client_request_id='start'").fetchone() is None,'v18 fixes by application retention policy, not a delete-blocking trigger');c.close()
result={'scope':'single-connection SQLite DDL/CAS examples plus explicit sequential contract models; no scheduler/Windows/native execution','sqlite_version':sqlite3.sqlite_version,'checks':checks,'passed':sum(x['passed'] for x in checks),'failed':sum(not x['passed'] for x in checks)}
(P/'review-v18/transition-checks.json').write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
print(json.dumps(result,ensure_ascii=False,indent=2))
raise SystemExit(1 if result['failed'] else 0)
