"""Храповик архитектуры: deps.py, scc.py, check.py с порогами из ratchet.toml.

Запуск из корня репозитория: python3 -I docs/swarm/tools/ratchet.py
Код 1 — стало хуже (новое запрещённое ребро, цикл больше порога, проблема в автоматах).
Если стало лучше, печатает, какое число в ratchet.toml уменьшить.
"""
import argparse, pathlib, re, subprocess, sys, tomllib

parser = argparse.ArgumentParser()
parser.add_argument("--baseline", help="trusted ratchet.toml from the base revision")
args = parser.parse_args()

here = pathlib.Path(__file__).resolve().parent
root = pathlib.Path.cwd()
cfg = tomllib.loads((here / "ratchet.toml").read_text(encoding="utf-8"))
fail, better = [], []
if args.baseline:
    try:
        baseline = tomllib.loads(pathlib.Path(args.baseline).read_text(encoding="utf-8"))
        added = set(cfg["allowed_violations"]) - set(baseline["allowed_violations"])
        fail += [f"baseline: добавлено терпимое ребро: {v}" for v in sorted(added)]
        old_cycles, cycles = baseline["max_cycle"], cfg["max_cycle"]
        fail += [f"baseline: удалён источник SCC: {src}" for src in sorted(set(old_cycles) - set(cycles))]
        fail += [f"baseline: увеличен порог SCC {src}: {cycles[src]} > {old_cycles[src]}"
                 for src in sorted(set(old_cycles) & set(cycles)) if cycles[src] > old_cycles[src]]
        if baseline["automata"]["spec"] != cfg["automata"]["spec"]:
            fail.append("baseline: изменён путь automata.spec")
    except (OSError, tomllib.TOMLDecodeError, KeyError, TypeError) as e:
        print(f"ошибка baseline: {e}", file=sys.stderr)
        sys.exit(2)

def run(script, *args):
    p = subprocess.run([sys.executable, "-I", str(here / script), *args],
                       capture_output=True, text=True, cwd=root)
    print(f"$ {script} {' '.join(args)}\n{p.stdout.rstrip()}")
    if p.stderr.strip():
        print(p.stderr.rstrip())
    return p

# 1. Рёбра крейтов
deps_run = run("deps.py", ".")
out = deps_run.stdout
found = {l for l in out.splitlines() if l.startswith("запрещено:")}
summary = re.search(r"^крейтов (\d+), запрещённых рёбер (\d+)$", out, re.M)
if (not summary or int(summary.group(2)) != len(found) or
        deps_run.returncode != (1 if found else 0)):
    fail.append("deps.py завершился с ошибкой или выдал неполный результат")
allowed = set(cfg["allowed_violations"])
fail += [f"новое запрещённое ребро: {l}" for l in sorted(found - allowed)]
better += [f"ребро убрано, удалите из allowed_violations: {l}" for l in sorted(allowed - found)]

# 2. Циклы модулей
for src, limit in cfg["max_cycle"].items():
    p = run("scc.py", src)
    m = re.search(r"^модулей \d+, рёбер \d+, самый большой цикл (\d+)$", p.stdout, re.M)
    if not m or p.returncode != 0:
        fail.append(f"scc.py не дал результата для {src}")
        continue
    n = int(m.group(1))
    if n > limit:
        fail.append(f"цикл модулей в {src}: {n} > порога {limit}")
    elif n < limit:
        better.append(f"цикл модулей в {src}: {n} < порога {limit}, уменьшите max_cycle")

# 3. Автоматы
p = run("check.py", cfg["automata"]["spec"])
checks = {m.group(1): int(m.group(2)) for m in
          re.finditer(r"^== (Task|Attempt|Delivery|IntegrationStep|Gate): .* проблем (\d+)$", p.stdout, re.M)}
if p.returncode != 0 or set(checks) != {"Task", "Attempt", "Delivery", "IntegrationStep", "Gate"} or any(checks.values()):
    fail.append("check.py нашёл проблемы в автоматах")

print()
for b in better:
    print("ЛУЧШЕ:", b)
for f in fail:
    print("ХУЖЕ:", f)
print("храповик:", "провал" if fail else "ok")
sys.exit(1 if fail else 0)
