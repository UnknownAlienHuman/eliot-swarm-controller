"""Проверка автоматов v2.6: таблицы читаются из раздела 14 самого документа.
Запуск: python3 check.py architecture.md"""
import re, sys
from collections import defaultdict, deque

GROUPS = {
    "АГЕНТ": ["claimed", "running", "checking", "review", "queued"],
}
GROUPS["РАБОЧИЕ"] = GROUPS["АГЕНТ"] + ["integrating"]
GROUPS["НЕФИНАЛЬНЫЕ"] = ["ready"] + GROUPS["РАБОЧИЕ"] + ["held"]
MACHINES = {  # имя: (начальное, финальные)
    "Task": ("ready", {"done", "cancelled"}),
    "Attempt": ("starting", {"stopped"}),
    "Delivery": ("stored", {"applied", "unknown", "expired", "rejected"}),
    "IntegrationStep": ("planned", {"done", "failed", "cancelled"}),
    "Gate": ("open", {"approved", "rejected", "superseded"}),
}

def clean(s):
    return s.replace("\\_", "_").replace("`", "").strip()

def states_of(cell):
    out = []
    for part in clean(cell).split(","):
        part = part.strip()
        out += GROUPS.get(part, [part])
    return out

def tables(md):
    """Возвращает {заголовок: [строки-ячейки]} для таблиц сразу под заголовками."""
    res, head, rows = {}, None, None
    for line in md.splitlines():
        m = re.match(r"#{2,6} (.+)", line)
        if m:
            head, rows = m.group(1).strip(), None
            continue
        if line.startswith("|") and head:
            cells = [c.strip() for c in line.strip().strip("|").split("|")]
            if rows is None:
                rows = []
                res.setdefault(head, rows)
                continue  # строка заголовка таблицы
            if set("".join(cells)) <= set("-: "):
                continue
            if res.get(head) is rows:
                rows.append(cells)
        else:
            rows = None
    return res

def analyse(name, rows, events=None, declared=None):
    init, finals = MACHINES[name]
    expanded = []  # (из, событие, условие, в, gate)
    for frm, ev, guard, to, gate, *_ in rows:
        for s in states_of(frm):
            expanded.append((s, clean(ev), clean(guard), clean(to), clean(gate)))
    def norm(to, src):
        if to == "=":
            return [src]
        if to.startswith("held("):
            return ["held"]
        return [to]
    gated, to_held = set(), set()
    for s, ev, g, to, gate in expanded:
        t = norm(to, s)
        if gate not in ("—", "-", ""):
            gated.update(t); to_held.add(s)
        if "held" in t and s != "held":
            to_held.add(s)
    edges = defaultdict(set)
    handled = set()
    for s, ev, g, to, gate in expanded:
        handled.add((s, ev))
        if to == "игнор":
            continue
        if to == "prev":
            tgt = to_held
        elif to == "gate_wait":
            tgt = gated
        else:
            tgt = norm(to, s)
        for t in tgt:
            edges[s].add(t)
        if gate not in ("—", "-", ""):
            edges[s].add("held")
    states = {init} | set(finals) | set(edges) | {t for v in edges.values() for t in v}
    probs = []
    if declared is not None and states != declared:
        probs.append(f"состояния не совпадают с объявлением: отсутствуют {sorted(declared - states)}, лишние {sorted(states - declared)}")
    seen, q = {init}, deque([init])
    while q:
        for n in edges[q.popleft()]:
            if n not in seen:
                seen.add(n); q.append(n)
    rev = defaultdict(set)
    for a, bs in edges.items():
        for b in bs:
            rev[b].add(a)
    canfin, q = set(finals), deque(finals)
    while q:
        for p in rev[q.popleft()]:
            if p not in canfin:
                canfin.add(p); q.append(p)
    probs += [f"недостижимо: {s}" for s in sorted(states - seen)]
    probs += [f"нет выхода из нефинального: {s}" for s in sorted(states - finals) if not (edges[s] - {s})]
    probs += [f"из финального есть переход: {s}" for s in sorted(finals) if edges[s]]
    probs += [f"не ведёт к финалу: {s}" for s in sorted(states - canfin)]
    by = defaultdict(list)
    for s, ev, g, to, gate in expanded:
        by[(s, ev)].append(g)
    for (s, ev), gs in sorted(by.items()):
        if len(gs) > 1 and ("—" in gs or len(set(gs)) != len(gs)):
            probs.append(f"недетерминизм: {s} + {ev}: условия {gs}")
    if events:
        for ev, where in events:
            for s in where:
                if s in finals:
                    continue
                if (s, ev) not in handled:
                    probs.append(f"не обработано: {ev} в {s}")
        known = {e for e, _ in events}
        probs += [f"событие не в таблице событий: {e}" for e in sorted({e for _, e, *_ in expanded} - known)]
    print(f"== {name}: состояний {len(states)}, строк {len(rows)}, проблем {len(probs)}")
    for p in probs:
        print("  -", p)
    return len(probs)

def main(path):
    md = open(path, encoding="utf-8").read()
    t = tables(md)
    status = re.search(r'^status\s*=\s*"[^"]+"\s+#\s*(.+)$', md, re.M)
    if not status:
        raise ValueError("не найдено объявление состояний Task в поле status")
    declared_task = set(states_of(status.group(1).replace("|", ",")))
    events = [(clean(r[0]), states_of(r[2])) for r in t["События Task"]]
    rc = 0
    for name in MACHINES:
        rc += analyse(name, t[f"Переходы {name}"], events if name == "Task" else None,
                      declared_task if name == "Task" else None)
    return rc

if __name__ == "__main__":
    sys.exit(1 if main(sys.argv[1]) else 0)
