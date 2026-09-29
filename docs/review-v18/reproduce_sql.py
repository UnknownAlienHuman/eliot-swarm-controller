"""Reference schema counterexamples only. No controller, native process or remote effect runs."""
from pathlib import Path
import json,sqlite3
ROOT=Path(__file__).resolve().parents[1]; rows=[]
def db(version):
 c=sqlite3.connect(':memory:')
 schema = ROOT/'review-v18/source-v17/001_core.sql' if version == 17 else ROOT/'agent_swarm.spec-v18/migrations/001_core.sql'
 c.executescript(schema.read_text())
 return c

def task(c,tid,origin=None,version=17):
 spec=json.dumps({'objective':'fixture','phase':'implementation','origin':origin})
 cols='task_id,project_id,revision,state,spec_json,created_at_ms,updated_at_ms'
 vals=[tid,'project-fixture',1,'open',spec,0,0]
 if version>=18: cols+=',origin_key'; vals+=[origin]
 c.execute('INSERT INTO tasks('+cols+') VALUES('+','.join('?' for _ in vals)+')',vals)
 c.execute("INSERT INTO attempts(attempt_id,task_id,task_revision,task_snapshot_json,owner_id,state,created_at_ms,updated_at_ms) VALUES(?,?,1,?,?,'running',0,0)",('a-'+tid,tid,spec,'manager-'+tid))

def artifacts(c):
 for a in ['candidate-fixture','result-fixture']:
  c.execute('INSERT INTO artifacts(artifact_id,relative_path,kind,byte_length,created_at_ms) VALUES(?,?,?,?,?)',(a,a+'.json','fixture',2,0))

def op(c,oid,aid=None):
 c.execute("INSERT INTO operations(operation_id,caller_id,client_request_id,method,original_request_json,effective_request_json,attempt_id,state,due_at_ms,created_at_ms,updated_at_ms) VALUES(?,?,?,'check.run','{}','{}',?,'queued',0,0,0)",(oid,'caller-fixture',oid,aid))

def check(c,cid,tid,key,resource,state='queued',version=17):
 op(c,'op-'+cid,'a-'+tid)
 cols='check_id,operation_id,attempt_id,candidate_ref,cache_key,resource_key,spec_json,state,created_at_ms'
 vals=[cid,'op-'+cid,'a-'+tid,'candidate-fixture',key,resource,'{}',state,0]
 if state=='running':
  cols+=',started_at_ms,process_identity_json';vals += [1,'{"fixture_process_may_be_alive":true}']
  if version>=18: cols+=',resource_claimed_at_ms';vals += [1]
 c.execute('INSERT INTO check_runs('+cols+') VALUES('+','.join('?' for _ in vals)+')',vals)

def rejected(fn):
 try:fn();return False
 except sqlite3.IntegrityError:return True

for v in [17,18]:
 c=db(v);origin='fixture:github.example:Issue:9042'
 task(c,'one',origin,v)
 reject=rejected(lambda:task(c,'two',origin,v))
 rows.append({'case':'H18-01 duplicate imported work with different local Task IDs','schema':v,'rejected':reject,'expected_rejected':v==18})
 c.close()
 c=db(v);artifacts(c);task(c,'one',version=v);task(c,'two',version=v)
 check(c,'first','one','inputs-one','target-fixture','running',v)
 c.execute("UPDATE check_runs SET state='incomplete',finished_at_ms=2,result_ref='result-fixture' WHERE check_id='first'")
 reject=rejected(lambda:check(c,'second','two','inputs-two','target-fixture','running',v))
 rows.append({'case':'H18-03 new target writer after incomplete while process disposition unresolved','schema':v,'rejected':reject,'expected_rejected':v==18})
 if v==18:
  c.execute("UPDATE check_runs SET resource_released_at_ms=3 WHERE check_id='first'")
  # The rejected INSERT above left its fixture Operation; use another operation identity.
  reject2=rejected(lambda:check(c,'third','two','inputs-two','target-fixture','running',v))
  rows.append({'case':'H18-03 known resource release permits next writer','schema':v,'rejected':reject2,'expected_rejected':False})
 c.close()
 c=db(v);artifacts(c);task(c,'one',version=v);task(c,'two',version=v)
 check(c,'first','one','identical-inputs','resource-A',version=v)
 reject=rejected(lambda:check(c,'second','two','identical-inputs','resource-B',version=v))
 rows.append({'case':'H18-04 two Attempts do not share one active check identity','schema':v,'rejected':reject,'expected_rejected':v==17})
 if v==18:
  reject2=rejected(lambda:check(c,'third','one','identical-inputs','resource-A',version=v))
  rows.append({'case':'H18-04 same Attempt still dedupes active check','schema':v,'rejected':reject2,'expected_rejected':True})
 c.close()
 c=db(v)
 c.execute("INSERT INTO artifacts(artifact_id,relative_path,kind,byte_length,created_at_ms) VALUES('history-output','history-output.json','evidence',2,0)")
 c.execute("INSERT INTO observations(source_stream_id,kind,payload_json,recorded_at_ms) VALUES('fixture','past-submission','{\"artifact_ref\":\"history-output\"}',0)")
 deletion_rejected=rejected(lambda:c.execute("DELETE FROM artifacts WHERE artifact_id='history-output'"))
 rows.append({'case':'H18-07 JSON reference not enforced by FK; retained by application retention policy','schema':v,'rejected':deletion_rejected,'expected_rejected':False})
 c.close()
result={'scope':'Synthetic SQL counterexamples and revised DDL only. Process liveness represented by fixture metadata, no actual process run.', 'sqlite_version':sqlite3.sqlite_version,'rows':rows,'all_expected':all(r['rejected']==r['expected_rejected'] for r in rows)}
(ROOT/'review-v18/sql-counterexamples.json').write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
print(json.dumps(result,ensure_ascii=False,indent=2))
if not result['all_expected']:raise SystemExit(1)
