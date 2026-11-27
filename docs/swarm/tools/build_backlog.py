"""Генерирует backlog.md из таблицы этапов v2.6 (раздел 12).

Запуск: python3 -I build_backlog.py <v2.6.md> <папка docs>
Пункты «Что делаем» — задачи-чекбоксы; «Что удаляется» и «Критерий приёмки» — дословно.
Контекст этапа и открытые вопросы заданы здесь вручную.
"""
import os, re, sys
from pathlib import Path

if len(sys.argv) != 3:
    raise SystemExit("Usage: python3 -I build_backlog.py <archive/spec-v2.6.md> <external-staging-directory>")
src, docs = sys.argv[1], sys.argv[2]
# Historical reconstruction must never overwrite live contracts or backlog progress.
output_path = Path(docs).resolve()
repository = Path(__file__).resolve().parents[3]
if output_path == repository or repository in output_path.parents:
    raise SystemExit("Historical exporter: choose a staging directory outside the repository; edit active docs directly.")
if any(p.is_symlink() for p in output_path.rglob("*")):
    raise SystemExit("Historical exporter: staging must not contain symlinks.")
lines = open(src, encoding="utf-8").read().split("\n")
rows = [l for l in lines[796:809] if re.match(r"\| \d+\. ", l)]
assert len(rows) == 13, len(rows)

CONTEXT = {
    0: ["implementation/security.md", "implementation/plugins/claude-code.md", "implementation/migration.md", "GRAPH.md"],
    1: ["implementation/cli-mcp.md", "implementation/core.md", "implementation/automata.md", "GRAPH.md"],
    2: ["implementation/store.md", "implementation/automata.md", "implementation/daemon.md"],
    3: ["implementation/task-controller.md", "implementation/automata.md", "implementation/config.md"],
    4: ["implementation/integrator.md", "implementation/automata.md"],
    5: ["implementation/integrator.md", "implementation/plugins/github.md", "implementation/plugins/checks.md", "implementation/security.md"],
    6: ["implementation/runtime.md", "implementation/daemon.md"],
    7: ["implementation/task-controller.md", "implementation/cli-mcp.md"],
    8: ["implementation/hooks.md", "implementation/runtime.md", "implementation/plugins/claude-code.md", "implementation/plugins/codex.md"],
    9: ["implementation/store.md"],
    10: ["implementation/security.md", "implementation/plugins/claude-code.md"],
    11: ["implementation/plugins/cloud-agents.md", "implementation/remote-mcp.md", "implementation/plugins/github.md"],
    12: ["implementation/operations.md", "implementation/plugins/muse-code.md", "implementation/plugins/antigravity.md", "implementation/config.md"],
}

# Сделано PR, который завёл эту документацию в репозиторий (27.11.2026).
DONE = {0: ["AGENTS.md под правила раздела 1",
            "в CI — deps.py, scc.py и check.py из приложения В с порогами по текущим числам (храповик)"]}
FIRST = {0: "удалить plugins/claude/src/quota.rs (читает учётные данные Claude) — первым, до остального"}

OPEN = {
    3: ["Какой из лимитов порции (attempts_left, wall_left_min, usd_left) вызывает budget_exhausted и как списывается расход при usd_left = unknown: инвариант запрещает считать неизвестное нулём, но правило списания не записано.",
        "Как продолжается работа после budget_extended из running: в переходах Attempt нет stopped → running; записать, что это новый Runtime того же Attempt через resume (как при lost) или новый Attempt.",
        "Куда возвращается held(budget), пришедший из integrating через pending: prev после исхода шага не определён."],
    4: ["origin_at_old: таблица IntegrationStep ведёт шаг в failed; записать, что дальше — новый шаг той же очереди или возврат Task в queued.",
        "Сеть без ответа после sending: классы исхода говорят «повтор с backoff», таблица — unknown; записать, что после sending всегда unknown и чтение origin, а backoff — только до sending."],
    8: ["Определить «отпечаток» для детектора прогресса (дерево коммита, ветка, что исключается) и ключ исхода проверок.",
        "Что делает Stop hook при мёртвом демоне со счётчиком блокировок (runtime отвечает из журнала, но кто считает блокировки)."],
    11: ["Перечень состояний сессии Jules и Claude Code on the web, терминальные значения и поиск сессии по маркеру — сверить по документации провайдеров на дату этапа."],
}

def cells(row):
    return [c.strip() for c in row.strip().strip("|").split(" | ")]

def items(text):
    return [p.strip() for p in re.split(r";\s+", text) if p.strip() and p.strip() != "—"]

out = ["""# Бэклог реализации

> Сгенерировано скриптом [tools/build_backlog.py](tools/build_backlog.py) из таблицы этапов v2.6 (раздел 12, теперь [migration.md](implementation/migration.md)). Тексты пунктов, удалений и критериев — дословно из спецификации.

**Как брать задачу.** Этапы идут по порядку; внутри этапа пункт — одна задача или одна ветка с PR. Контекст задачи: [ARCHITECTURE.md](ARCHITECTURE.md) плюс файлы из строки «Контекст». Каждый срез обязан уменьшить хотя бы одно число храповика ([GRAPH.md](GRAPH.md), [tools/ratchet.toml](tools/ratchet.toml)) или не трогать его; что удаляется — удаляется в том же этапе. Открытый вопрос этапа решается в начале этапа, решение записывается в файл блока (implementation/), а не в отдельный отчёт.

**Отметка.** Сделанный пункт отмечается [x] в том же PR, где он сделан; этот файл — единственный список статуса.
"""]
for row in rows:
    name, do, remove, accept = cells(row)
    n = int(name.split(".")[0])
    out.append(f"## Этап {name}\n")
    out.append("Контекст: " + ", ".join(f"[{os.path.basename(f)}]({f})" for f in CONTEXT[n]) + ".\n")
    todo = ([FIRST[n]] if n in FIRST else []) + items(do)
    for it in todo:
        mark = "x" if it in DONE.get(n, []) else " "
        out.append(f"- [{mark}] {it}")
    out.append("")
    out.append(f"**Удаляется в этом этапе.** {remove}\n")
    out.append(f"**Критерий приёмки.** {accept}\n")
    if n in OPEN:
        out.append("**Открытые вопросы спецификации (найдены прогоном задач на свежих агентах).**\n")
        out += [f"- {q}" for q in OPEN[n]]
        out.append("")
out.append(lines[811 - 1] if lines[810].startswith("Canonical") else "")
open(os.path.join(docs, "backlog.md"), "w", encoding="utf-8").write("\n".join(out).rstrip() + "\n")
print("backlog.md:", os.path.getsize(os.path.join(docs, "backlog.md")), "байт")
