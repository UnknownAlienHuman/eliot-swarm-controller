"""Храповик архитектуры: deps.py, scc.py, check.py с порогами из ratchet.toml.

Запуск из корня репозитория: python3 -I docs/swarm/tools/ratchet.py
Код 1 — стало хуже (новое запрещённое ребро, цикл больше порога, проблема в автоматах).
Если стало лучше, печатает, какое число в ratchet.toml уменьшить.
"""
import pathlib, re, subprocess, sys, tomllib

here = pathlib.Path(__file__).resolve().parent
root = pathlib.Path.cwd()
cfg = tomllib.loads((here / "ratchet.toml").read_text(encoding="utf-8"))
fail, better = [], []

def run(script, *args):
    p = subprocess.run([sys.executable, "-I", str(here / script), *args],
                       capture_output=True, text=True, cwd=root)
    print(f"$ {script} {' '.join(args)}\n{p.stdout.rstrip()}")
    if p.stderr.strip():
        print(p.stderr.rstrip())
    return p

# 1. Рёбра крейтов
out = run("deps.py", ".").stdout
found = {l for l in out.splitlines() if l.startswith("запрещено:")}
allowed = set(cfg["allowed_violations"])
fail += [f"новое запрещённое ребро: {l}" for l in sorted(found - allowed)]
better += [f"ребро убрано, удалите из allowed_violations: {l}" for l in sorted(allowed - found)]

# 2. Циклы модулей
for src, limit in cfg["max_cycle"].items():
    p = run("scc.py", src)
    m = re.search(r"самый большой цикл (\d+)", p.stdout)
    if not m:
        fail.append(f"scc.py не дал результата для {src}")
        continue
    n = int(m.group(1))
    if n > limit:
        fail.append(f"цикл модулей в {src}: {n} > порога {limit}")
    elif n < limit:
        better.append(f"цикл модулей в {src}: {n} < порога {limit}, уменьшите max_cycle")

# 3. Автоматы
p = run("check.py", cfg["automata"]["spec"])
if p.returncode != 0:
    fail.append("check.py нашёл проблемы в автоматах")

print()
for b in better:
    print("ЛУЧШЕ:", b)
for f in fail:
    print("ХУЖЕ:", f)
print("храповик:", "провал" if fail else "ok")
sys.exit(1 if fail else 0)
