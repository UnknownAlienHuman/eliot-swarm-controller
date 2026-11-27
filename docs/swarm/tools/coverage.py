"""Проверка покрытия: каждая непустая строка v2.6 есть дословно в документации.

Запуск: python3 -I coverage.py <v2.6.md> <папка docs> [--map map.json]
Сравнение после снятия добавленных ссылок « [↗](...)». Пишет по разделам v2.6:
сколько строк, сколько найдено, где лежат; печатает пропуски. Код 1 — есть пропуски
или незакрытые блоки кода.
"""
import json, os, re, sys
from collections import defaultdict

src, docs = sys.argv[1], sys.argv[2]
map_out = sys.argv[sys.argv.index("--map") + 1] if "--map" in sys.argv else None
ARROW = re.compile(r" \[↗\]\([^)]*\)")

corpus = defaultdict(set)   # строка -> файлы
fence_errors = []
for root, _, files in os.walk(docs):
    for name in files:
        if not (name.endswith(".md") or name in ("check.py", "deps.py", "scc.py")):
            continue
        path = os.path.join(root, name)
        rel = os.path.relpath(path, docs)
        if rel == os.path.join("archive", "spec-v2.6.md"):
            continue
        text = open(path, encoding="utf-8").read()
        if name.endswith(".md") and sum(1 for l in text.split("\n") if l.startswith("```")) % 2:
            fence_errors.append(rel)
        for line in text.split("\n"):
            line = ARROW.sub("", line).rstrip()
            if line.strip():
                corpus[line].add(rel)

sections, cur, total, missing = {}, "преамбула", 0, []
for n, line in enumerate(open(src, encoding="utf-8").read().split("\n"), 1):
    if line.startswith("## ") or line.startswith("# "):
        cur = line.lstrip("# ").strip()
    if not line.strip() or re.fullmatch(r"```\w*", line.strip()):
        continue
    s = sections.setdefault(cur, {"line": n, "lines": 0, "found": 0, "files": defaultdict(int)})
    s["lines"] += 1
    total += 1
    where = corpus.get(line.rstrip())
    if where:
        s["found"] += 1
        for f in where:
            s["files"][f] += 1
    else:
        missing.append((n, cur, line[:100]))

found = total - len(missing)
print(f"строк v2.6 (непустых): {total}; найдено дословно: {found}; пропущено: {len(missing)}")
for n, sec, text in missing[:50]:
    print(f"  ПРОПУСК {n} [{sec[:40]}]: {text}")
for title, s in sections.items():
    if s["found"] < s["lines"]:
        print(f"  раздел «{title[:60]}»: {s['found']}/{s['lines']}")
if fence_errors:
    print("незакрытые блоки кода:", ", ".join(fence_errors))
if map_out:
    json.dump({t: {"line": s["line"], "lines": s["lines"], "found": s["found"],
                   "files": dict(sorted(s["files"].items(), key=lambda kv: -kv[1]))}
               for t, s in sections.items()}, open(map_out, "w", encoding="utf-8"),
              ensure_ascii=False, indent=1)
sys.exit(1 if missing or fence_errors else 0)
