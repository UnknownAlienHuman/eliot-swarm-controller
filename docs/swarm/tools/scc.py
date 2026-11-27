"""Циклы между модулями верхнего уровня одного крейта.
Ссылки: crate::m, use crate::{m, n::x}, super::m (из файла верхнего уровня super — корень крейта).
Использование: scc.py <src-dir> [предел]; код выхода 1, если самый большой цикл больше предела."""
import sys, re, pathlib
src = pathlib.Path(sys.argv[1]); limit = int(sys.argv[2]) if len(sys.argv) > 2 else 10**9
mods = {p.stem for p in src.glob("*.rs") if p.stem not in ("lib", "main", "mod")} | \
       {p.name for p in src.iterdir() if p.is_dir()}

def modpath(f):
    parts = list(f.relative_to(src).parts)
    stem = parts[-1].removesuffix(".rs")
    parts = parts[:-1] + ([] if stem in ("mod", "lib", "main") else [stem])
    return parts

def braced(text, start):
    depth, i, out, cur = 0, start, [], ""
    while i < len(text):
        c = text[i]
        if c == "{":
            depth += 1
            if depth == 1: i += 1; continue
        elif c == "}":
            depth -= 1
            if depth == 0: out.append(cur); return out
        if depth == 1 and c == ",":
            out.append(cur); cur = ""
        elif depth >= 1:
            cur += c
        i += 1
    return out

def targets(text, path):
    found = set(re.findall(r"crate::([a-z_0-9]+)", text))
    for m in re.finditer(r"crate::\{", text):
        for item in braced(text, m.end() - 1):
            name = re.match(r"\s*([a-z_0-9]+)", item)
            if name: found.add(name.group(1))
    for m in re.finditer(r"((?:super::)+)([a-z_0-9]+)", text):
        if m.group(1).count("super::") == len(path):
            found.add(m.group(2))
    for m in re.finditer(r"((?:super::)+)\{", text):
        if m.group(1).count("super::") == len(path):
            for item in braced(text, m.end() - 1):
                name = re.match(r"\s*([a-z_0-9]+)", item)
                if name: found.add(name.group(1))
    return found

edges = {m: set() for m in mods}
for f in src.rglob("*.rs"):
    path = modpath(f)
    if not path or path[0] not in mods: continue
    o = path[0]
    for m in targets(f.read_text(encoding="utf-8-sig", errors="replace"), path):
        if m in mods and m != o: edges[o].add(m)
idx, low, st, on, sccs, n = {}, {}, [], set(), [], [0]
sys.setrecursionlimit(10000)
def dfs(v):
    idx[v] = low[v] = n[0]; n[0] += 1; st.append(v); on.add(v)
    for w in edges[v]:
        if w not in idx: dfs(w); low[v] = min(low[v], low[w])
        elif w in on: low[v] = min(low[v], idx[w])
    if low[v] == idx[v]:
        c = []
        while True:
            w = st.pop(); on.discard(w); c.append(w)
            if w == v: break
        sccs.append(c)
for v in sorted(mods):
    if v not in idx: dfs(v)
big = max(sccs, key=len)
print(f"модулей {len(mods)}, рёбер {sum(map(len, edges.values()))}, самый большой цикл {len(big)}")
if len(big) > 1: print("  " + ", ".join(sorted(big)))
sys.exit(1 if len(big) > limit else 0)
