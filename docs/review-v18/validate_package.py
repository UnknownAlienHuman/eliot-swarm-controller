"""Validate documentation and reference SQL. No product/runtime qualification."""
from pathlib import Path
import json,tomllib,sqlite3,re,hashlib,subprocess,sys
from urllib.parse import urlsplit,unquote
P=Path(__file__).resolve().parents[1];S=P/'agent_swarm.spec-v18';results=[]
def check(name,ok,detail=''):
 results.append({'name':name,'passed':bool(ok),'detail':detail})
 if not ok:print('FAIL:',name,detail)
(S/'validation-results.json').write_text('{"status":"in_progress"}\n',encoding='utf-8')
for script in ['reproduce_sql.py','validate_transitions.py']:
 run=subprocess.run([sys.executable,str(P/'review-v18'/script)],capture_output=True,text=True)
 check('execute reference utility '+script,run.returncode==0,run.stderr[-2000:])
for f in list((S/'examples').glob('*.json')):
 try:json.loads(f.read_text());check('JSON '+f.name,True)
 except Exception as exc:check('JSON '+f.name,False,str(exc))
for f in list((S/'config').glob('*.toml'))+[P/'agent_swarm.donors-20260929.toml']:
 try:tomllib.loads(f.read_text());check('TOML '+f.name,True)
 except Exception as exc:check('TOML '+f.name,False,str(exc))
c=sqlite3.connect(':memory:');c.executescript((S/'migrations/001_core.sql').read_text())
check('nine application tables',len(c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name <> 'sqlite_sequence'").fetchall())==9)
check('foreign_keys enabled',c.execute('PRAGMA foreign_keys').fetchone()[0]==1)
check('FK check empty',c.execute('PRAGMA foreign_key_check').fetchall()==[])
for table,cols in {'tasks':['origin_key'],'attempts':['start_operation_id'],'check_runs':['resource_claimed_at_ms','resource_released_at_ms']}.items():
 present={x[1] for x in c.execute(f'PRAGMA table_info({table})')}
 check('v18 columns '+table,set(cols)<=present)
# Existing v17 native-root invariant remains intact.
def add(b,l,scope,root):
 c.execute("INSERT INTO bindings(binding_id,generation,lane_id,module_instance_id,module_artifact_id,state,native_scope_key,native_root_id,route_json,created_at_ms) VALUES(?,1,?,'m','v1','ready',?,?,'{}',0)",(b,l,scope,root))
add('b1','l1','s1','r1')
try:add('b2','l2','s1','r1');collision=False
except sqlite3.IntegrityError:collision=True
check('v17 native alias-owner invariant retained',collision)
add('b3','l3','s2','r1');check('different native namespace allowed',True)
old=tomllib.loads((P/'review-v18/source-v17/packaged-agent_swarm.donors-20260929.toml').read_text());new=tomllib.loads((P/'agent_swarm.donors-20260929.toml').read_text())
for key in set(old)|set(new):
 if key=='architecture':continue
 check('donor field unchanged '+key,old.get(key)==new.get(key))
active=[P/'agent_swarm.md',P/'agent_swarm.implementation-v6.md',P/'agent_swarm.module-contract-v2.md',P/'agent_swarm.design-review-v18-20260929.md',P/'agent_swarm.checkpoint.md',S/'README.md',P/'review-v18/README.md']
links=[]
for f in active:
 text=f.read_text();check('text hygiene '+f.name,not text.startswith('\ufeff') and not any(ord(x)<32 and x not in '\n\r\t' for x in text))
 check('balanced fences '+f.name,sum(l.startswith('```') for l in text.splitlines())%2==0)
 for url in re.findall(r'\]\(([^)]+)\)',text):
  bare=url.split('#',1)[0]
  if not bare or urlsplit(bare).scheme or bare.startswith('//'):continue
  dest=(f.parent/unquote(bare)).resolve()
  # The final ZIP and its manifest are validated by the packaging step, not faked into existence here.
  if dest.name in ('agent_swarm.docs-v18-20260929.zip','agent_swarm.docs-v18-manifest.json'):continue
  links.append({'from':f.name,'target':bare,'exists':dest.exists()})
check('active relative links exist',all(l['exists'] for l in links),json.dumps([l for l in links if not l['exists']],ensure_ascii=False))
check('canonical version labels', 'архитектура v18' in active[0].read_text() and 'v6 к архитектуре v18' in active[1].read_text() and 'интеграции v2' in active[2].read_text())
check('start slot guard in reference SQL', 'a.start_operation_id = op.operation_id' in (S/'transactions/begin-initial-send.reference.sql').read_text())
check('no triggers added',not bool(re.search(r'CREATE\s+TRIGGER',(S/'migrations/001_core.sql').read_text(),re.I)))
model=json.loads((P/'review-v18/transition-checks.json').read_text());sql=json.loads((P/'review-v18/sql-counterexamples.json').read_text())
out={'date':'2026-09-29','scope':'documentation plus reference DDL/SQL and small sequential contract models; not a service test','sqlite_version':sqlite3.sqlite_version,'sql_counterexample_cases':len(sql['rows']),'sql_counterexamples_all_expected':sql['all_expected'],'reference_transition_checks':{'passed':model['passed'],'failed':model['failed']},'structure_checks':results,'structure_summary':{'passed':sum(r['passed'] for r in results),'failed':sum(not r['passed'] for r in results),'active_relative_links':len(links)},'not_executed':['Rust service/API/Store','SDK build or install','Windows IPC/Job/concurrency','live models and quota','vendor API update qualification','product WAL/load benchmark','GitHub writes']}
(S/'validation-results.json').write_text(json.dumps(out,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
print(json.dumps({k:out[k] for k in ['sql_counterexample_cases','sql_counterexamples_all_expected','reference_transition_checks','structure_summary']},ensure_ascii=False,indent=2))
raise SystemExit(1 if out['structure_summary']['failed'] or model['failed'] or not sql['all_expected'] else 0)
