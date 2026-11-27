"""Проверка разрешённых рёбер между крейтами workspace. Читает Cargo.toml, ничего не пишет.
Использование: deps.py <корень репозитория>; код выхода 1 при запрещённом ребре."""
import sys, tomllib, pathlib, glob

def manifest(d):
    return tomllib.loads((pathlib.Path(d) / "Cargo.toml").read_text(encoding="utf-8-sig"))

root = pathlib.Path(sys.argv[1])
crates, dev = {}, {}
for pat in manifest(root)["workspace"]["members"]:
    for d in glob.glob(str(root / pat)):
        m = manifest(d)
        tables = [m] + list(m.get("target", {}).values())   # и [target.'cfg(...)'.dependencies]
        deps = set().union(*(set(t.get("dependencies", {})) | set(t.get("build-dependencies", {})) for t in tables))
        crates[m["package"]["name"]] = deps
        dev[m["package"]["name"]] = set().union(*(set(t.get("dev-dependencies", {})) for t in tables))
local = set(crates)
CORE = {"eliot", "eliot-core", "eliot-store", "eliot-plugin", "eliot-daemon", "eliot-client", "swarm-desktop"}
PLUGINS = local - CORE
ROOTS = {"eliot"}                 # бинарник swarm: корень сборки, ему можно всё
ALLOWED = {                       # целевой граф: кто от кого зависит
    "eliot-core": set(),
    "eliot-store": {"eliot-core"},
    "eliot-plugin": {"eliot-core"},
    "eliot-client": {"eliot-core"},
    "plugin": {"eliot-core", "eliot-plugin"},   # плагин -> плагин запрещён: группы plugin здесь нет
    "eliot-daemon": {"eliot-core", "eliot-store", "eliot-plugin"},
    "swarm-desktop": {"eliot-core", "eliot-client"},
}
ONLY_IN = {"rusqlite": {"eliot-store"}}   # внешние крейты, разрешённые одному владельцу

def group(n):
    return "plugin" if n in PLUGINS else n

bad = []
for n, deps in sorted(crates.items()):
    if n in ROOTS:
        continue
    for d in sorted(deps & local):
        if group(d) not in ALLOWED.get(group(n), set()):
            bad.append(f"запрещено: {n} -> {d}")
    for ext, owners in ONLY_IN.items():
        if ext in deps | dev[n] and n not in owners:
            bad.append(f"запрещено: {n} -> {ext} (разрешено только {', '.join(sorted(owners))})")
print("\n".join(bad))
print(f"крейтов {len(crates)}, запрещённых рёбер {len(bad)}")
sys.exit(1 if bad else 0)
