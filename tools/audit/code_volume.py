"""Read-only, exact-Git-tree volume inventory. Never executes repository code."""
import collections, csv, hashlib, importlib.metadata, io, json, pathlib, re, subprocess, sys, tarfile, tomllib
from tree_sitter import Language, Parser
import tree_sitter_rust

REV = sys.argv[1]
OUT = pathlib.Path(sys.argv[2]); OUT.mkdir(parents=True, exist_ok=True)
parser = Parser(Language(tree_sitter_rust.language()))

def git(*args):
    return subprocess.check_output(['git', '--no-replace-objects', *args])

def snapshot(rev):
    raw = git('archive', '--format=tar', rev)
    with tarfile.open(fileobj=io.BytesIO(raw)) as archive:
        return {m.name: archive.extractfile(m).read() for m in archive if m.isfile()}

def rows(data):
    return data.count(b'\n') + int(bool(data) and not data.endswith(b'\n'))

def bucket(path):
    p = pathlib.PurePosixPath(path)
    if p.parts[0] in ('vendor','donors'): return 'third_party'
    if path.startswith('docs/continuation/'): return 'continuation'
    if p.name in ('Cargo.lock','package-lock.json','yarn.lock','pnpm-lock.yaml','uv.lock'): return 'lockfiles'
    if p.suffix == '.md': return 'documentation'
    if 'tests' in p.parts or 'fixtures' in p.parts or re.search(r'(^|/)(selftest|test_|.*_tests|tests)(\.|/|$)',path): return 'test_files'
    if p.suffix == '.rs': return 'rust_source_files'
    if p.suffix in ('.js','.ts','.mjs','.cjs','.py','.ps1','.sh','.bat','.cmd'):
        return 'owned_bridge_scripts' if path.startswith(('modules/','crates/')) else 'tooling_scripts'
    return 'config_data_other'

def source_unit(path):
    parts=path.split('/')
    return '/'.join(parts[:2]) if parts[0] in ('crates','modules','vendor','docs','tools') and len(parts)>2 else parts[0]

def rust_details(data):
    root=parser.parse(data).root_node
    test_rows=set(); functions=[]; external_tests=[]
    def visit(node, inherited=False):
        attrs=[]
        for child in node.named_children:
            if child.type == 'attribute_item':
                attrs.append(child); continue
            prefix=b' '.join(data[a.start_byte:a.end_byte] for a in attrs)
            marked = bool(re.search(rb'#\s*\[\s*cfg\s*\(\s*test\s*\)\s*\]',prefix))
            marked |= bool(re.search(rb'#\s*\[\s*(?:tokio::)?test(?:\s*\(|\s*\])',prefix))
            is_test=inherited or marked
            if marked:
                start=attrs[0].start_point.row if attrs else child.start_point.row
                test_rows.update(range(start,child.end_point.row+1))
                if child.type=='mod_item' and child.child_by_field_name('body') is None:
                    name=child.child_by_field_name('name')
                    if name: external_tests.append(data[name.start_byte:name.end_byte].decode())
            if child.type=='function_item':
                name=child.child_by_field_name('name'); body=child.child_by_field_name('body')
                if name and body:
                    text=data[body.start_byte:body.end_byte]
                    normalized=re.sub(rb'\s+',b' ',text).strip()
                    functions.append({'name':data[name.start_byte:name.end_byte].decode(), 'start':child.start_point.row+1,'end':child.end_point.row+1,'rows':rows(text),'test':is_test,'body_hash':hashlib.sha256(normalized).hexdigest()})
            visit(child,is_test); attrs=[]
    visit(root)
    return test_rows,functions,external_tests,root.has_error

files=snapshot(REV); inventory=[]; funcs=[]; parse_errors=[]; text_files={}; external_test_paths=set()
for path,data in files.items():
    if path.endswith('.rs') and bucket(path)=='rust_source_files':
        testrows,fs,external,err=rust_details(data)
        parent=pathlib.PurePosixPath(path).parent
        for name in external:
            for cand in (parent/(name+'.rs'),parent/name/'mod.rs'):
                if str(cand) in files: external_test_paths.add(str(cand))
for path,data in sorted(files.items()):
    try: data.decode('utf-8'); is_text=b'\0' not in data
    except UnicodeDecodeError: is_text=False
    kind=bucket(path); total=rows(data) if is_text else 0; inline=0; function_count=0
    if path in external_test_paths and kind=='rust_source_files': kind='test_files'
    if path.endswith('.rs') and is_text and kind!='third_party':
        tr,fs,_,err=rust_details(data); inline=len(tr) if kind=='rust_source_files' else 0
        function_count=len(fs)
        if err: parse_errors.append(path)
        for f in fs: f.update(path=path,category=kind); funcs.append(f)
    if is_text: text_files[path]=data
    inventory.append({'path':path,'unit':source_unit(path),'category':kind,'bytes':len(data),'physical_lines':total,'nonblank_lines':sum(bool(l.strip()) for l in data.splitlines()) if is_text else 0,'inline_test_lines':inline,'rust_functions':function_count,'text':is_text,'git_blob':hashlib.sha1(b'blob '+str(len(data)).encode()+b'\0'+data).hexdigest()})

def aggregate(items,key):
    result={}
    for r in items:
        o=result.setdefault(r[key],{'files':0,'bytes':0,'lines':0,'inline_test_lines':0})
        o['files']+=1;o['bytes']+=r['bytes'];o['lines']+=r['physical_lines'];o['inline_test_lines']+=r['inline_test_lines']
    return result

exact=collections.defaultdict(list)
for r in inventory:
    if r['text'] and r['physical_lines']>=20: exact[r['git_blob']].append(r)
exact_groups=[{'lines_each':rs[0]['physical_lines'],'paths':[r['path'] for r in rs]} for rs in exact.values() if len(rs)>1]
body=collections.defaultdict(list)
for f in funcs:
    if f['category']=='rust_source_files' and not f['test'] and f['rows']>=30: body[f['body_hash']].append(f)
clones=[fs for fs in body.values() if len({f['path'] for f in fs})>1]
clones.sort(key=lambda fs:fs[0]['rows']*(len(fs)-1),reverse=True)

hist=[]
for rev in ['cf2c8ccd6c8e8db6224856fff66b7a081cddc0fe','8c9fcfe8896f14dc9341e5d81b36ec4ac109dda4','3ecdf52707731e3f85e85827a88fbdb28d784f3e','35e499ae73b622d873c44873f6993ee3fcbea87b','504199d14135c030ad3951a3c5023a098a3d03f0','6e7c2a6bd6346bd3b42c28eb253334600064ce9f',REV]:
    try:
        snap=snapshot(rev);cats=collections.Counter();rs=0
        for p,d in snap.items():
            try:d.decode('utf-8')
            except UnicodeDecodeError:continue
            if b'\0' in d:continue
            cats[bucket(p)]+=rows(d)
            if p.endswith('.rs'):rs+=rows(d)
        hist.append({'sha':rev,'commit':git('show','-s','--format=%cI %s',rev).decode().strip(),'files':len(snap),'physical_lines':sum(cats.values()),'rust_all_lines':rs,'categories':dict(cats)})
    except Exception as e: hist.append({'sha':rev,'error':str(e)})

manifests=[]
for path,data in files.items():
    if path.endswith('Cargo.toml') and not path.startswith(('vendor/','donors/')):
        try:
            t=tomllib.loads(data.decode());deps=[]
            for section in ('dependencies','build-dependencies','dev-dependencies'):
                for name,v in t.get(section,{}).items():
                    if isinstance(v,dict) and 'path' in v:deps.append({'name':name,'section':section,'path':v['path']})
            manifests.append({'path':path,'package':t.get('package',{}).get('name'),'local_dependencies':deps})
        except Exception as e: manifests.append({'path':path,'error':str(e)})

summary={'revision':REV,'git_tree':git('rev-parse',REV+'^{tree}').decode().strip(),'method':'Tracked Git archive regular files; physical lines include blanks/comments. Named test files plus recognized cfg(test)/test AST spans. No build, no model calls.','files':len(inventory),'text_lines':sum(r['physical_lines'] for r in inventory),'bytes':sum(r['bytes'] for r in inventory),'categories':aggregate(inventory,'category'),'units':aggregate(inventory,'unit'),'rust_all_lines':sum(r['physical_lines'] for r in inventory if r['path'].endswith('.rs')),'parser_versions':{x:importlib.metadata.version(x) for x in ('tree-sitter','tree-sitter-rust')},'rust_parse_errors':parse_errors,'largest_files':sorted(inventory,key=lambda r:r['physical_lines'],reverse=True)[:50],'exact_duplicate_files':exact_groups,'clone_groups':clones[:70],'largest_owned_production_functions':sorted((f for f in funcs if f['category']=='rust_source_files' and not f['test']),key=lambda f:f['rows'],reverse=True)[:70],'history':hist,'manifests':manifests}
(OUT/'summary.json').write_text(json.dumps(summary,ensure_ascii=False,indent=2)+'\n')
with (OUT/'files.csv').open('w',newline='') as h:
    w=csv.DictWriter(h,fieldnames=list(inventory[0]));w.writeheader();w.writerows(inventory)
(OUT/'functions.json').write_text(json.dumps(funcs,indent=2)+'\n')
print(json.dumps({k:summary[k] for k in ('revision','git_tree','files','text_lines','bytes','categories','rust_all_lines','parser_versions','rust_parse_errors','units','history')},indent=2))
